package vk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/sync/singleflight"
)

const (
	turnCredentialCacheTTL     = 9 * time.Minute
	turnCredentialMinimumReuse = 15 * time.Second
	vkControlPlaneMinSpacing   = 3 * time.Second
	vkControlPlaneMaxSpacing   = 6 * time.Second
	vkFloodControlCooldown     = 2 * time.Minute
	authFailureWindow          = 10 * time.Second
	authFailureThreshold       = 3
)

type cachedTURNCredentials struct {
	server       TurnServer
	expires      time.Time
	refreshAfter time.Time
}

type vkControlPlaneLimiter struct {
	mu         sync.Mutex
	fetchGate  chan struct{}
	floodUntil time.Time
	lastFetch  time.Time
}

func newVKControlPlaneLimiter() *vkControlPlaneLimiter {
	limiter := &vkControlPlaneLimiter{fetchGate: make(chan struct{}, 1)}
	limiter.fetchGate <- struct{}{}
	return limiter
}

var sharedVKControlPlaneLimiter = newVKControlPlaneLimiter()

type TURNCredentialProvider struct {
	dialer  N.Dialer
	logger  logger.ContextLogger
	mu      sync.Mutex
	cache   map[string]cachedTURNCredentials
	group   singleflight.Group
	metrics *telemetry.Accumulator
	limiter *vkControlPlaneLimiter
	authFailures map[string][]time.Time
}

func (p *TURNCredentialProvider) SetTelemetry(metrics *telemetry.Accumulator) {
	p.metrics = metrics
}

func NewTURNCredentialProvider(dialer N.Dialer, log logger.ContextLogger) *TURNCredentialProvider {
	if log == nil {
		log = logger.NOP()
	}
	provider := &TURNCredentialProvider{
		dialer:  dialer,
		logger:  log,
		cache:   make(map[string]cachedTURNCredentials),
		limiter: sharedVKControlPlaneLimiter,
		authFailures: make(map[string][]time.Time),
	}
	return provider
}

func (p *TURNCredentialProvider) Fetch(ctx context.Context, joinLink string) (TurnServer, error) {
	metrics := telemetry.FromContext(ctx)
	if metrics == nil {
		metrics = p.metrics
	}
	metrics.Add(telemetry.VKCredentialRequestTotal, 1)
	if server, loaded := p.cached(joinLink); loaded {
		metrics.Add(telemetry.VKCredentialCacheHitTotal, 1)
		return server, nil
	}
	if err := p.floodControlError(time.Now()); err != nil {
		return TurnServer{}, err
	}
	result := p.group.DoChan(joinLink, func() (any, error) {
		if server, loaded := p.cached(joinLink); loaded {
			metrics.Add(telemetry.VKCredentialCacheHitTotal, 1)
			return server, nil
		}
		select {
		case <-ctx.Done():
			return TurnServer{}, ctx.Err()
		case <-p.limiter.fetchGate:
		}
		defer func() { p.limiter.fetchGate <- struct{}{} }()
		if server, loaded := p.cached(joinLink); loaded {
			metrics.Add(telemetry.VKCredentialCacheHitTotal, 1)
			return server, nil
		}
		if err := p.floodControlError(time.Now()); err != nil {
			return TurnServer{}, err
		}
		if err := p.waitForFetchSpacing(ctx, joinLink); err != nil {
			return TurnServer{}, err
		}
		metrics.Add(telemetry.VKCredentialFetchTotal, 1)
		fetchContext := telemetry.ContextWithAccumulator(ctx, metrics)
		server, err := FetchTURNCredentials(fetchContext, p.dialer, joinLink, "HydraCore", p.logger)
		if err != nil {
			if errors.Is(err, ErrVKFloodControl) {
				p.activateFloodControl(time.Now())
			}
			return TurnServer{}, err
		}
		now := time.Now()
		p.mu.Lock()
		p.cache[joinLink] = cachedTURNCredentials{
			server:       cloneTurnServer(server),
			expires:      now.Add(turnCredentialCacheTTL),
			refreshAfter: now.Add(turnCredentialMinimumReuse),
		}
		p.mu.Unlock()
		return server, nil
	})
	select {
	case response := <-result:
		if response.Err != nil {
			return TurnServer{}, response.Err
		}
		return cloneTurnServer(response.Val.(TurnServer)), nil
	case <-ctx.Done():
		return TurnServer{}, ctx.Err()
	}
}

func (p *TURNCredentialProvider) activateFloodControl(now time.Time) {
	p.limiter.mu.Lock()
	until := now.Add(vkFloodControlCooldown)
	if until.After(p.limiter.floodUntil) {
		p.limiter.floodUntil = until
	}
	p.limiter.mu.Unlock()
}

func (p *TURNCredentialProvider) waitForFetchSpacing(ctx context.Context, joinLink string) error {
	p.limiter.mu.Lock()
	spacing := vkControlPlaneSpacing(joinLink)
	wait := time.Until(p.limiter.lastFetch.Add(spacing))
	p.limiter.mu.Unlock()
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.limiter.mu.Lock()
	p.limiter.lastFetch = time.Now()
	p.limiter.mu.Unlock()
	return nil
}

func vkControlPlaneSpacing(joinLink string) time.Duration {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(joinLink))
	span := vkControlPlaneMaxSpacing - vkControlPlaneMinSpacing
	if span <= 0 {
		return vkControlPlaneMinSpacing
	}
	return vkControlPlaneMinSpacing + time.Duration(hasher.Sum32()%uint32(span/time.Millisecond+1))*time.Millisecond
}

func (p *TURNCredentialProvider) floodControlError(now time.Time) error {
	p.limiter.mu.Lock()
	until := p.limiter.floodUntil
	p.limiter.mu.Unlock()
	if !until.After(now) {
		return nil
	}
	remaining := until.Sub(now).Round(time.Second)
	return &ControlPlaneError{Stage: "vk_auth", Kind: "rate_limit", Code: "9", RetryAfter: remaining, Cause: ErrVKFloodControl}
}

// Invalidate lets the next physical TURN connection obtain a new allocation
// identity after a minimum reuse window. The window prevents several lanes
// observing the same network handover from turning one transport failure into
// a burst of VK control-plane joins.
func (p *TURNCredentialProvider) Invalidate(joinLink string) {
	p.mu.Lock()
	now := time.Now()
	failures := p.authFailures[joinLink][:0]
	for _, observed := range p.authFailures[joinLink] {
		if now.Sub(observed) <= authFailureWindow {
			failures = append(failures, observed)
		}
	}
	failures = append(failures, now)
	p.authFailures[joinLink] = failures
	if len(failures) < authFailureThreshold {
		p.mu.Unlock()
		return
	}
	delete(p.authFailures, joinLink)
	if entry, loaded := p.cache[joinLink]; loaded && time.Now().Before(entry.refreshAfter) {
		p.mu.Unlock()
		return
	}
	delete(p.cache, joinLink)
	p.mu.Unlock()
	p.group.Forget(joinLink)
}

func (p *TURNCredentialProvider) cached(joinLink string) (TurnServer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, loaded := p.cache[joinLink]
	if !loaded || time.Now().After(entry.expires) {
		delete(p.cache, joinLink)
		return TurnServer{}, false
	}
	return cloneTurnServer(entry.server), true
}

func cloneTurnServer(server TurnServer) TurnServer {
	server.URLs = append([]string(nil), server.URLs...)
	return server
}

// FetchTURNCredentials joins only the VK signaling control plane. It does not
// create WebRTC media tracks or connect to the VK SFU; the returned TURN
// allocation is used to relay DTLS datagrams directly to the native VPS.
func FetchTURNCredentials(
	ctx context.Context,
	dialer N.Dialer,
	joinLink string,
	displayName string,
	log logger.ContextLogger,
) (TurnServer, error) {
	metrics := telemetry.FromContext(ctx)
	succeeded := false
	defer func() {
		if succeeded {
			metrics.Add(telemetry.VKAuthSuccessTotal, 1)
		} else if !errors.Is(ctx.Err(), context.Canceled) {
			metrics.Add(telemetry.VKAuthFailureTotal, 1)
		}
	}()
	authStarted := time.Now()
	authJSON, err := RunVKAuthContext(ctx, dialer, joinLink, displayName, log)
	metrics.Set(telemetry.VKAuthLatencyMS, telemetry.LatencyMS(authStarted))
	if err != nil {
		if !errors.Is(ctx.Err(), context.Canceled) {
			reason := "control_plane"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				reason = "timeout"
			}
			metrics.RecordEvent("vk_auth_failed", "vk_auth", reason, nil)
		}
		if _, typed := AsControlPlaneError(err); typed {
			return TurnServer{}, err
		}
		return TurnServer{}, newControlPlaneError("vk_auth", "request", "auth_failed", err)
	}
	var params VKAuthParams
	if err = json.Unmarshal([]byte(authJSON), &params); err != nil {
		metrics.RecordEvent("vk_auth_failed", "vk_auth", "invalid_parameters", nil)
		return TurnServer{}, newControlPlaneError("vk_auth", "response", "invalid_parameters", errors.New("VK TURN auth returned invalid parameters"))
	}
	if params.APIBaseURL == "" || params.SessionKey == "" || params.ApplicationKey == "" || params.JoinLink == "" {
		metrics.RecordEvent("vk_auth_failed", "vk_auth", "incomplete_parameters", nil)
		return TurnServer{}, newControlPlaneError("vk_auth", "response", "incomplete_parameters", errors.New("VK TURN auth returned incomplete parameters"))
	}
	mediaSettings := `{"isAudioEnabled":false,"isVideoEnabled":true,"isScreenSharingEnabled":false}`
	body := url.Values{
		"method":          {"vchat.joinConversationByLink"},
		"session_key":     {params.SessionKey},
		"application_key": {params.ApplicationKey},
		"joinLink":        {params.JoinLink},
		"anonymToken":     {params.AnonymToken},
		"isVideo":         {"true"},
		"isAudio":         {"false"},
		"mediaSettings":   {mediaSettings},
		"format":          {"json"},
	}
	joinStarted := time.Now()
	defer func() {
		metrics.Set(telemetry.VKJoinConversationLatencyMS, telemetry.LatencyMS(joinStarted))
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, params.APIBaseURL, strings.NewReader(body.Encode()))
	if err != nil {
		metrics.RecordEvent("vk_join_failed", "vk_join_conversation", "request", nil)
		return TurnServer{}, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("User-Agent", common.UserAgent)
	client := common.HttpClient(dialer)
	client.Timeout = 20 * time.Second
	response, err := client.Do(request)
	if err != nil {
		metrics.RecordEvent("vk_join_failed", "vk_join_conversation", "transport", nil)
		return TurnServer{}, newControlPlaneError("vk_join_conversation", "transport", "request_failed", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		metrics.RecordEvent("vk_join_failed", "vk_join_conversation", "http_status", nil)
		return TurnServer{}, controlPlaneErrorf("vk_join_conversation", "http", fmt.Sprint(response.StatusCode), "VK TURN join returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		metrics.RecordEvent("vk_join_failed", "vk_join_conversation", "read", nil)
		return TurnServer{}, err
	}
	var joined VKJoinResponse
	if err = json.Unmarshal(raw, &joined); err != nil {
		metrics.RecordEvent("vk_join_failed", "vk_join_conversation", "invalid_json", nil)
		return TurnServer{}, newControlPlaneError("vk_join_conversation", "response", "invalid_json", errors.New("VK TURN join returned invalid JSON"))
	}
	server := TurnServer{
		URLs:       append([]string(nil), joined.TurnServer.URLs...),
		Username:   joined.TurnServer.Username,
		Credential: joined.TurnServer.Credential,
	}
	if len(server.URLs) == 0 || server.Username == "" || server.Credential == "" {
		metrics.RecordEvent("vk_join_failed", "vk_join_conversation", "incomplete_credentials", nil)
		return TurnServer{}, newControlPlaneError("vk_join_conversation", "response", "incomplete_credentials", errors.New("VK TURN join returned incomplete credentials"))
	}
	succeeded = true
	return server, nil
}
