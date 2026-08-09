package daemon

import (
	"context"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	E "github.com/sagernet/sing/common/exceptions"
)

type StandaloneURLTestResult struct {
	Tag         string
	DelayMillis int64
	TimeSeconds int64
	Status      string
	Error       string
	ErrorCode   string
}

func (s *StartedService) RunStandaloneURLTest(
	ctx context.Context,
	groupTag string,
	targetTag string,
	link string,
	timeoutMillis int32,
	deadlineMillis int32,
) (*StandaloneURLTestResult, error) {
	groupTag = strings.TrimSpace(groupTag)
	targetTag = strings.TrimSpace(targetTag)
	if groupTag == "" || targetTag == "" {
		return nil, E.New("standalone URL test requires group and target tags")
	}
	options := normalizeURLTestOptions(&URLTestRequest{
		UrlTestUrl:     strings.TrimSpace(link),
		TimeoutMillis:  timeoutMillis,
		DeadlineMillis: deadlineMillis,
		Concurrency:    1,
	})
	if err := validateURLTestLink(options.link); err != nil {
		return nil, err
	}

	s.serviceAccess.RLock()
	if s.serviceStatus.Status != ServiceStatus_STARTED || s.instance == nil {
		s.serviceAccess.RUnlock()
		return nil, E.New("standalone URL test runtime is not started")
	}
	instance := s.instance
	targets, err := resolveURLTestTargets(instance.instance.Outbound(), groupTag, targetTag, "", "")
	s.serviceAccess.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(targets) != 1 {
		return nil, E.New("standalone URL test requires exactly one concrete target")
	}

	runContext, cancel := context.WithTimeout(ctx, options.deadline)
	defer cancel()
	return runStandaloneURLTestProbe(
		runContext,
		targets[0],
		options,
		func(probeContext context.Context, link string, outbound adapter.Outbound) (uint16, error) {
			return urltest.URLTest(probeContext, link, outbound)
		},
	), nil
}

func runStandaloneURLTestProbe(
	ctx context.Context,
	target urlTestTarget,
	options urlTestSessionOptions,
	probe urlTestProbe,
) *StandaloneURLTestResult {
	probeContext, cancel := context.WithTimeout(ctx, options.timeout)
	delay, err := probe(probeContext, options.link, target.outbound)
	contextErr := probeContext.Err()
	cancel()
	if err == nil && contextErr != nil {
		err = contextErr
	}
	now := time.Now().Unix()
	if err != nil {
		code, message := classifyURLTestError(err)
		return &StandaloneURLTestResult{
			Tag:         target.tag,
			TimeSeconds: now,
			Status:      "unavailable",
			Error:       message,
			ErrorCode:   code,
		}
	}
	if delay == 0 {
		delay = 1
	}
	return &StandaloneURLTestResult{
		Tag:         target.tag,
		DelayMillis: int64(delay),
		TimeSeconds: now,
		Status:      "available",
	}
}
