package vkparasite

import (
	"context"
	"sync"
	"time"

	HC "github.com/sagernet/sing-box/common/hydracore"
	callvk "github.com/sagernet/sing-box/transport/call/vk"
)

const (
	turnConcurrencyLimit   = 2
	dtlsConcurrencyLimit   = 4
	turnAllocationSpacing  = 250 * time.Millisecond
	transportDegradedLimit = 15 * time.Second
	transportFailureLimit  = 30 * time.Second
)

type transportSupervisor struct {
	turnGate chan struct{}
	dtlsGate chan struct{}
	turnMu   sync.Mutex
	lastTURN time.Time
}

var sharedTransportSupervisor = newTransportSupervisor()

func newTransportSupervisor() *transportSupervisor {
	return &transportSupervisor{
		turnGate: make(chan struct{}, turnConcurrencyLimit),
		dtlsGate: make(chan struct{}, dtlsConcurrencyLimit),
	}
}

func (s *transportSupervisor) reset() {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	for len(s.turnGate) > 0 {
		<-s.turnGate
	}
	for len(s.dtlsGate) > 0 {
		<-s.dtlsGate
	}
	s.lastTURN = time.Time{}
}

func acquireSupervisorPermit(ctx context.Context, gate chan struct{}) (func(), error) {
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *transportSupervisor) acquireTURN(ctx context.Context) (func(), error) {
	release, err := acquireSupervisorPermit(ctx, s.turnGate)
	if err != nil {
		return nil, err
	}
	s.turnMu.Lock()
	wait := time.Until(s.lastTURN.Add(turnAllocationSpacing))
	s.turnMu.Unlock()
	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			release()
			return nil, ctx.Err()
		}
	}
	s.turnMu.Lock()
	s.lastTURN = time.Now()
	s.turnMu.Unlock()
	return release, nil
}

func (s *transportSupervisor) acquireDTLS(ctx context.Context) (func(), error) {
	return acquireSupervisorPermit(ctx, s.dtlsGate)
}

func transportFailure(err error) *HC.TransportFailure {
	if err == nil {
		return nil
	}
	if controlError, ok := callvk.AsControlPlaneError(err); ok {
		return &HC.TransportFailure{
			Stage: controlError.Stage, Kind: controlError.Kind, Code: controlError.Code,
			RetryAfterMS: controlError.RetryAfter.Milliseconds(), ChallengeID: controlError.ChallengeID,
		}
	}
	return &HC.TransportFailure{Stage: "transport", Kind: "network", Code: "worker_failed"}
}

func (c *Client) healthLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	c.publishObservedHealth(time.Now())
	for {
		select {
		case now := <-ticker.C:
			c.publishObservedHealth(now)
		case <-c.ctx.Done():
			return
		}
	}
}

// publishObservedHealth снимает текущее состояние пула путей. Платформа держит
// старт открытым, пока состояние не станет healthy или degraded, поэтому цикл
// обязан публиковать снимок в течение всего времени жизни клиента.
func (c *Client) publishObservedHealth(now time.Time) {
	activePaths := int32(0)
	if c.relay != nil {
		activePaths = int32(c.relay.ActivePaths())
	}
	if activePaths > 0 {
		c.sawPath.Store(true)
	}
	health := HC.TransportHealthSnapshot{
		ActiveLanes: activePaths,
		TotalLanes:  int32(c.options.Workers),
		ObservedAt:  now.UnixMilli(),
	}
	challenge := HC.CurrentRuntimeChallenge()
	switch {
	case challenge != nil:
		health.State = HC.TransportStateWaitingUser
		health.Failure = &HC.TransportFailure{Stage: "vk_auth", Kind: "captcha", Code: "14", ChallengeID: challenge.ID}
	case activePaths == int32(c.options.Workers):
		health.State = HC.TransportStateHealthy
	case activePaths > 0:
		health.State = HC.TransportStateDegraded
	case !c.sawPath.Load():
		// Первичный дозвон линий: это ещё старт, а не потеря транспорта.
		health.State = HC.TransportStateStarting
	default:
		health.State = HC.TransportStateRecovering
	}
	if health.Failure == nil && health.State != HC.TransportStateHealthy {
		health.Failure = c.lastFailure.Load()
	}
	HC.PublishTransportHealth(health)
}
