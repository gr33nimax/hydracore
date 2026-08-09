package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	neturl "net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	E "github.com/sagernet/sing/common/exceptions"

	"google.golang.org/protobuf/proto"
)

const (
	defaultURLTestTimeout          = 5 * time.Second
	minimumURLTestTimeout          = 500 * time.Millisecond
	maximumURLTestTimeout          = 30 * time.Second
	defaultURLTestDeadline         = 30 * time.Second
	maximumURLTestDeadline         = 2 * time.Minute
	defaultURLTestConcurrency      = 8
	maximumURLTestConcurrency      = 16
	maximumRetainedURLTestSessions = 64
)

type managedURLTestSession struct {
	id           string
	groupTag     string
	instance     *Instance
	cancel       context.CancelFunc
	state        URLTestSessionState
	startedAt    time.Time
	completedAt  time.Time
	total        int32
	completed    int32
	succeeded    int32
	failed       int32
	results      []*URLTestResult
	errorCode    string
	errorMessage string
}

type urlTestSessionOptions struct {
	link        string
	timeout     time.Duration
	deadline    time.Duration
	concurrency int
}

type urlTestTarget struct {
	tag string
	// resultTag aliases only managed result events; probes and history use tag.
	resultTag string
	outbound  adapter.Outbound
}

func (t urlTestTarget) managedResultTag() string {
	if t.resultTag != "" {
		return t.resultTag
	}
	return t.tag
}

type urlTestProbe func(ctx context.Context, link string, outbound adapter.Outbound) (uint16, error)

type urlTestResultHandler func(target urlTestTarget, delay uint16, err error)

func (s *StartedService) startURLTest(request *URLTestRequest) (*URLTestSession, error) {
	if request == nil {
		return nil, E.New("missing URL test request")
	}
	groupTag := strings.TrimSpace(request.GroupTag)
	if groupTag == "" {
		return nil, E.New("missing outbound group tag")
	}
	options := normalizeURLTestOptions(request)
	if err := validateURLTestLink(options.link); err != nil {
		return nil, err
	}

	s.serviceAccess.RLock()
	if s.serviceStatus.Status != ServiceStatus_STARTED || s.instance == nil {
		s.serviceAccess.RUnlock()
		return nil, os.ErrInvalid
	}
	boxService := s.instance

	targets, err := resolveURLTestTargets(
		boxService.instance.Outbound(),
		groupTag,
		strings.TrimSpace(request.TargetOutboundTag),
		strings.TrimSpace(request.PriorityOutboundTag),
		strings.TrimSpace(request.ExcludeOutboundTag),
	)
	if err != nil {
		s.serviceAccess.RUnlock()
		return nil, err
	}
	sessionContext, cancel := context.WithTimeout(boxService.ctx, options.deadline)

	s.urlTestSessionAccess.Lock()
	if existingID := s.urlTestSessionByGroup[groupTag]; existingID != "" {
		existing := s.urlTestSessions[existingID]
		if existing != nil && existing.instance == boxService && existing.state == URLTestSessionState_URL_TEST_SESSION_RUNNING && !request.Force {
			result := cloneURLTestSessionLocked(existing)
			s.urlTestSessionAccess.Unlock()
			s.serviceAccess.RUnlock()
			cancel()
			return result, nil
		}
		if existing != nil && existing.state == URLTestSessionState_URL_TEST_SESSION_RUNNING {
			existing.state = URLTestSessionState_URL_TEST_SESSION_CANCELLED
			existing.completedAt = time.Now()
			existing.errorCode = "replaced"
			existing.errorMessage = "replaced by a forced URL test session"
			existing.cancel()
		}
	}
	s.urlTestSessionSequence++
	session := &managedURLTestSession{
		id:        fmt.Sprintf("urltest-%d", s.urlTestSessionSequence),
		groupTag:  groupTag,
		instance:  boxService,
		cancel:    cancel,
		state:     URLTestSessionState_URL_TEST_SESSION_RUNNING,
		startedAt: time.Now(),
		total:     int32(len(targets)),
	}
	s.urlTestSessions[session.id] = session
	s.urlTestSessionByGroup[groupTag] = session.id
	pruneURLTestSessionsLocked(s.urlTestSessions)
	result := cloneURLTestSessionLocked(session)
	s.urlTestSessionAccess.Unlock()
	s.serviceAccess.RUnlock()

	s.urlTestSessionSubscriber.Emit(struct{}{})
	go s.runURLTestSession(sessionContext, groupTag, session, targets, options)
	return result, nil
}

func normalizeURLTestOptions(request *URLTestRequest) urlTestSessionOptions {
	timeout := durationFromMilliseconds(request.TimeoutMillis, defaultURLTestTimeout)
	timeout = clampDuration(timeout, minimumURLTestTimeout, maximumURLTestTimeout)
	deadline := durationFromMilliseconds(request.DeadlineMillis, defaultURLTestDeadline)
	deadline = clampDuration(deadline, timeout, maximumURLTestDeadline)
	concurrency := int(request.Concurrency)
	if concurrency <= 0 {
		concurrency = defaultURLTestConcurrency
	} else if concurrency > maximumURLTestConcurrency {
		concurrency = maximumURLTestConcurrency
	}
	return urlTestSessionOptions{
		link:        strings.TrimSpace(request.UrlTestUrl),
		timeout:     timeout,
		deadline:    deadline,
		concurrency: concurrency,
	}
}

func durationFromMilliseconds(value int32, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func clampDuration(value time.Duration, minimum time.Duration, maximum time.Duration) time.Duration {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func validateURLTestLink(link string) error {
	if link == "" {
		return nil
	}
	parsed, err := neturl.Parse(link)
	if err != nil {
		return E.Cause(err, "invalid URL test URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return E.New("URL test URL must use http or https")
	}
	if parsed.Hostname() == "" {
		return E.New("URL test URL must contain a host")
	}
	return nil
}

func resolveURLTestTargets(outboundManager adapter.OutboundManager, groupTag string, targetTag string, priorityTag string, excludeTag string) ([]urlTestTarget, error) {
	rootOutbound, loaded := outboundManager.Outbound(groupTag)
	if !loaded {
		return nil, E.New("outbound group not found: ", groupTag)
	}
	rootGroup, isGroup := rootOutbound.(adapter.OutboundGroup)
	if !isGroup {
		return nil, E.New("outbound is not a group: ", groupTag)
	}

	targets, err := collectConcreteURLTestTargets(outboundManager, rootGroup.All())
	if err != nil {
		return nil, err
	}
	memberIndex := make(map[string]int, len(targets))
	for index, target := range targets {
		memberIndex[target.tag] = index
	}

	if excludeTag != "" {
		excluded := make(map[string]bool)
		if excludeOutbound, found := outboundManager.Outbound(excludeTag); found {
			if excludeGroup, groupFound := excludeOutbound.(adapter.OutboundGroup); groupFound {
				excludeTargets, collectErr := collectConcreteURLTestTargets(outboundManager, excludeGroup.All())
				if collectErr != nil {
					return nil, collectErr
				}
				for _, excludeTarget := range excludeTargets {
					excluded[excludeTarget.tag] = true
				}
			} else {
				excluded[excludeOutbound.Tag()] = true
			}
		}
		if len(excluded) > 0 {
			filtered := targets[:0]
			for _, candidate := range targets {
				if !excluded[candidate.tag] {
					filtered = append(filtered, candidate)
				}
			}
			targets = filtered
			memberIndex = make(map[string]int, len(targets))
			for index, candidate := range targets {
				memberIndex[candidate.tag] = index
			}
		}
	}

	if targetTag != "" {
		targetOutbound, resolveErr := resolveSelectedURLTestOutbound(outboundManager, targetTag)
		if resolveErr != nil {
			return nil, resolveErr
		}
		index, isMember := memberIndex[targetOutbound.Tag()]
		if !isMember {
			return nil, E.New("target outbound is not a member of group ", groupTag, ": ", targetTag)
		}
		target := targets[index]
		if targetTag != target.tag {
			target.resultTag = targetTag
		}
		return []urlTestTarget{target}, nil
	}

	if priorityTag != "" {
		if priorityOutbound, resolveErr := resolveSelectedURLTestOutbound(outboundManager, priorityTag); resolveErr == nil {
			if index, isMember := memberIndex[priorityOutbound.Tag()]; isMember && index > 0 {
				priorityTarget := targets[index]
				copy(targets[1:index+1], targets[0:index])
				targets[0] = priorityTarget
			}
		}
	}
	if len(targets) == 0 {
		return nil, E.New("outbound group has no testable members: ", groupTag)
	}
	return targets, nil
}

func collectConcreteURLTestTargets(outboundManager adapter.OutboundManager, tags []string) ([]urlTestTarget, error) {
	seen := make(map[string]bool)
	visiting := make(map[string]bool)
	var targets []urlTestTarget
	var visit func(tag string) error
	visit = func(tag string) error {
		outbound, loaded := outboundManager.Outbound(tag)
		if !loaded {
			return E.New("outbound not found: ", tag)
		}
		if outboundGroup, isGroup := outbound.(adapter.OutboundGroup); isGroup {
			if visiting[tag] {
				return E.New("cyclic outbound group reference: ", tag)
			}
			visiting[tag] = true
			for _, childTag := range outboundGroup.All() {
				if err := visit(childTag); err != nil {
					return err
				}
			}
			delete(visiting, tag)
			return nil
		}
		realTag := outbound.Tag()
		if seen[realTag] {
			return nil
		}
		seen[realTag] = true
		targets = append(targets, urlTestTarget{tag: realTag, outbound: outbound})
		return nil
	}
	for _, tag := range tags {
		if err := visit(tag); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func resolveSelectedURLTestOutbound(outboundManager adapter.OutboundManager, tag string) (adapter.Outbound, error) {
	visited := make(map[string]bool)
	for {
		outbound, loaded := outboundManager.Outbound(tag)
		if !loaded {
			return nil, E.New("outbound not found: ", tag)
		}
		outboundGroup, isGroup := outbound.(adapter.OutboundGroup)
		if !isGroup {
			return outbound, nil
		}
		if visited[tag] {
			return nil, E.New("cyclic outbound group selection: ", tag)
		}
		visited[tag] = true
		tag = strings.TrimSpace(outboundGroup.Now())
		if tag == "" {
			return nil, E.New("outbound group has no selected member: ", outboundGroup.Tag())
		}
	}
}

func (s *StartedService) runURLTestSession(ctx context.Context, groupTag string, session *managedURLTestSession, targets []urlTestTarget, options urlTestSessionOptions) {
	defer session.cancel()
	defer s.finishURLTestSession(ctx, groupTag, session)

	probe := func(probeContext context.Context, link string, outbound adapter.Outbound) (uint16, error) {
		return urltest.URLTest(probeContext, link, outbound)
	}
	runURLTestTargets(ctx, targets, options, probe, func(target urlTestTarget, delay uint16, err error) {
		if !s.isCurrentURLTestSession(groupTag, session) {
			return
		}
		now := time.Now()
		result := &URLTestResult{
			OutboundTag: target.managedResultTag(),
			ObservedAt:  now.UnixMilli(),
		}
		if err != nil {
			errorCode, errorMessage := classifyURLTestError(err)
			result.Status = string(adapter.URLTestStatusUnavailable)
			result.ErrorCode = errorCode
			result.ErrorMessage = errorMessage
			if !s.recordURLTestResult(session, result, false) {
				return
			}
			session.instance.urlTestHistoryStorage.StoreURLTestHistory(target.tag, &adapter.URLTestHistory{
				Time:      now,
				Status:    adapter.URLTestStatusUnavailable,
				Error:     errorMessage,
				ErrorCode: errorCode,
			})
			return
		}
		if delay == 0 {
			delay = 1
		}
		result.DelayMillis = int64(delay)
		result.Status = string(adapter.URLTestStatusAvailable)
		if !s.recordURLTestResult(session, result, true) {
			return
		}
		session.instance.urlTestHistoryStorage.StoreURLTestHistory(target.tag, &adapter.URLTestHistory{
			Time:   now,
			Delay:  delay,
			Status: adapter.URLTestStatusAvailable,
		})
	})
	if s.isCurrentURLTestSession(groupTag, session) {
		refreshURLTestGroupSelections(session.instance.instance.Outbound(), groupTag)
	}
}

func refreshURLTestGroupSelections(outboundManager adapter.OutboundManager, rootTag string) {
	visited := make(map[string]bool)
	var visit func(string)
	visit = func(tag string) {
		if visited[tag] {
			return
		}
		visited[tag] = true
		outbound, loaded := outboundManager.Outbound(tag)
		if !loaded {
			return
		}
		if refresher, isRefresher := outbound.(adapter.URLTestSelectionRefresher); isRefresher {
			refresher.RefreshURLTestSelection()
		}
		if group, isGroup := outbound.(adapter.OutboundGroup); isGroup {
			for _, childTag := range group.All() {
				visit(childTag)
			}
		}
	}
	visit(rootTag)
}

func runURLTestTargets(ctx context.Context, targets []urlTestTarget, options urlTestSessionOptions, probe urlTestProbe, handleResult urlTestResultHandler) {
	if len(targets) == 0 || probe == nil || handleResult == nil {
		return
	}
	workerCount := options.concurrency
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(targets) {
		workerCount = len(targets)
	}
	jobs := make(chan urlTestTarget, len(targets))
	for _, target := range targets {
		jobs <- target
	}
	close(jobs)

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case target, loaded := <-jobs:
					if !loaded {
						return
					}
					// A buffered job and cancellation may become ready together.
					// Re-check the context before starting network work so a
					// session deadline never drains queued probes by accident.
					if ctx.Err() != nil {
						return
					}
					probeContext, cancel := context.WithTimeout(ctx, options.timeout)
					delay, err := probe(probeContext, options.link, target.outbound)
					contextErr := probeContext.Err()
					cancel()
					if err == nil && contextErr != nil {
						err = contextErr
					}
					handleResult(target, delay, err)
				}
			}
		}()
	}
	workers.Wait()
}

func (s *StartedService) isCurrentURLTestSession(groupTag string, session *managedURLTestSession) bool {
	s.urlTestSessionAccess.Lock()
	defer s.urlTestSessionAccess.Unlock()
	return s.urlTestSessionByGroup[groupTag] == session.id && session.state == URLTestSessionState_URL_TEST_SESSION_RUNNING
}

func (s *StartedService) recordURLTestResult(session *managedURLTestSession, result *URLTestResult, succeeded bool) bool {
	s.urlTestSessionAccess.Lock()
	if session.state != URLTestSessionState_URL_TEST_SESSION_RUNNING {
		s.urlTestSessionAccess.Unlock()
		return false
	}
	session.results = append(session.results, result)
	session.completed++
	if succeeded {
		session.succeeded++
	} else {
		session.failed++
	}
	s.urlTestSessionAccess.Unlock()
	s.urlTestSessionSubscriber.Emit(struct{}{})
	return true
}

func (s *StartedService) finishURLTestSession(ctx context.Context, groupTag string, session *managedURLTestSession) {
	s.urlTestSessionAccess.Lock()
	if session.state == URLTestSessionState_URL_TEST_SESSION_RUNNING {
		session.completedAt = time.Now()
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			session.state = URLTestSessionState_URL_TEST_SESSION_CANCELLED
			session.errorCode = "cancelled"
			session.errorMessage = "URL test session cancelled"
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			session.state = URLTestSessionState_URL_TEST_SESSION_FAILED
			session.errorCode = "deadline"
			session.errorMessage = "URL test session deadline exceeded"
		case session.succeeded > 0:
			session.state = URLTestSessionState_URL_TEST_SESSION_SUCCEEDED
		default:
			session.state = URLTestSessionState_URL_TEST_SESSION_FAILED
			session.errorCode = "all_probes_failed"
			session.errorMessage = "all URL test probes failed"
		}
	}
	if s.urlTestSessionByGroup[groupTag] == session.id {
		delete(s.urlTestSessionByGroup, groupTag)
	}
	pruneURLTestSessionsLocked(s.urlTestSessions)
	s.urlTestSessionAccess.Unlock()
	s.urlTestSessionSubscriber.Emit(struct{}{})
}

func cloneURLTestSessionLocked(session *managedURLTestSession) *URLTestSession {
	result := &URLTestSession{
		Id:           session.id,
		GroupTag:     session.groupTag,
		State:        session.state,
		StartedAt:    session.startedAt.UnixMilli(),
		Total:        session.total,
		Completed:    session.completed,
		Succeeded:    session.succeeded,
		Failed:       session.failed,
		ErrorCode:    session.errorCode,
		ErrorMessage: session.errorMessage,
	}
	if !session.completedAt.IsZero() {
		result.CompletedAt = session.completedAt.UnixMilli()
	}
	result.Results = make([]*URLTestResult, 0, len(session.results))
	for _, item := range session.results {
		result.Results = append(result.Results, proto.Clone(item).(*URLTestResult))
	}
	return result
}

func (s *StartedService) readURLTestSessions() []*URLTestSession {
	s.urlTestSessionAccess.Lock()
	defer s.urlTestSessionAccess.Unlock()
	result := make([]*URLTestSession, 0, len(s.urlTestSessions))
	for _, session := range s.urlTestSessions {
		result = append(result, cloneURLTestSessionLocked(session))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StartedAt == result[j].StartedAt {
			return result[i].Id < result[j].Id
		}
		return result[i].StartedAt < result[j].StartedAt
	})
	return result
}

func (s *StartedService) getURLTestSession(id string) (*URLTestSession, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, E.New("missing URL test session ID")
	}
	s.urlTestSessionAccess.Lock()
	defer s.urlTestSessionAccess.Unlock()
	session := s.urlTestSessions[id]
	if session == nil {
		return nil, E.New("URL test session not found")
	}
	return cloneURLTestSessionLocked(session), nil
}

func (s *StartedService) cancelURLTestSession(id string) (*URLTestSession, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, E.New("missing URL test session ID")
	}
	s.urlTestSessionAccess.Lock()
	session := s.urlTestSessions[id]
	if session == nil {
		s.urlTestSessionAccess.Unlock()
		return nil, E.New("URL test session not found")
	}
	if session.state == URLTestSessionState_URL_TEST_SESSION_RUNNING {
		session.state = URLTestSessionState_URL_TEST_SESSION_CANCELLED
		session.completedAt = time.Now()
		session.errorCode = "cancelled"
		session.errorMessage = "URL test session cancelled"
		if s.urlTestSessionByGroup[session.groupTag] == session.id {
			delete(s.urlTestSessionByGroup, session.groupTag)
		}
		session.cancel()
	}
	result := cloneURLTestSessionLocked(session)
	s.urlTestSessionAccess.Unlock()
	s.urlTestSessionSubscriber.Emit(struct{}{})
	return result, nil
}

func (s *StartedService) cancelURLTestSessionsForInstance(instance *Instance, errorCode string) {
	if instance == nil {
		return
	}
	now := time.Now()
	changed := false
	s.urlTestSessionAccess.Lock()
	for _, session := range s.urlTestSessions {
		if session.instance != instance || session.state != URLTestSessionState_URL_TEST_SESSION_RUNNING {
			continue
		}
		session.state = URLTestSessionState_URL_TEST_SESSION_CANCELLED
		session.completedAt = now
		session.errorCode = errorCode
		session.errorMessage = "runtime instance stopped"
		if s.urlTestSessionByGroup[session.groupTag] == session.id {
			delete(s.urlTestSessionByGroup, session.groupTag)
		}
		session.cancel()
		changed = true
	}
	s.urlTestSessionAccess.Unlock()
	if changed {
		s.urlTestSessionSubscriber.Emit(struct{}{})
	}
}

func pruneURLTestSessionsLocked(sessions map[string]*managedURLTestSession) {
	completed := make([]*managedURLTestSession, 0, len(sessions))
	for _, session := range sessions {
		if session.state != URLTestSessionState_URL_TEST_SESSION_RUNNING {
			completed = append(completed, session)
		}
	}
	if len(completed) <= maximumRetainedURLTestSessions {
		return
	}
	sort.Slice(completed, func(i, j int) bool {
		return completed[i].completedAt.Before(completed[j].completedAt)
	})
	for _, session := range completed[:len(completed)-maximumRetainedURLTestSessions] {
		delete(sessions, session.id)
	}
}

func classifyURLTestError(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	for {
		urlError, isURLError := err.(*neturl.Error)
		if !isURLError || urlError.Err == nil {
			break
		}
		err = urlError.Err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", "request timed out"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled", "request cancelled"
	}
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return "dns", safeProbeError(err)
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "timeout", "request timed out"
	}
	if errors.Is(err, io.EOF) || strings.Contains(strings.ToLower(err.Error()), "unexpected eof") {
		return "eof", "connection closed before a response was received"
	}
	lowerMessage := strings.ToLower(err.Error())
	if strings.Contains(lowerMessage, "tls") || strings.Contains(lowerMessage, "certificate") || strings.Contains(lowerMessage, "x509") {
		return "tls", safeProbeError(err)
	}
	return "network", safeProbeError(err)
}

func safeProbeError(err error) string {
	message := strings.Join(strings.Fields(err.Error()), " ")
	const maximumRunes = 240
	runes := []rune(message)
	if len(runes) > maximumRunes {
		message = string(runes[:maximumRunes]) + "…"
	}
	if message == "" {
		return "network request failed"
	}
	return message
}
