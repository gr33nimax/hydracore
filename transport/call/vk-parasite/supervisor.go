package vkparasite

import (
	"context"
	"sync"
	"time"

	HC "github.com/sagernet/sing-box/common/hydracore"
	callvk "github.com/sagernet/sing-box/transport/call/vk"
)

const (
	turnAllocationSpacing = 250 * time.Millisecond
	transportDegradedLimit = 15 * time.Second
	transportFailureLimit = 30 * time.Second
)

type transportSupervisor struct {
	turnGate      chan struct{}
	dtlsGate      chan struct{}
	recoveryGate  chan struct{}
	migrationGate chan struct{}
	turnMu        sync.Mutex
	lastTURN      time.Time
}

var sharedTransportSupervisor = newTransportSupervisor()

func newTransportSupervisor() *transportSupervisor {
	return &transportSupervisor{
		turnGate:      make(chan struct{}, 1),
		dtlsGate:      make(chan struct{}, 2),
		recoveryGate:  make(chan struct{}, 1),
		migrationGate: make(chan struct{}, 2),
	}
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

func (s *transportSupervisor) acquireRecovery(ctx context.Context) (func(), error) {
	return acquireSupervisorPermit(ctx, s.recoveryGate)
}

func (s *transportSupervisor) acquireMigration(ctx context.Context) (func(), error) {
	return acquireSupervisorPermit(ctx, s.migrationGate)
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

func (c *Client) publishHealth(state string, failure *HC.TransportFailure) {
	health := c.tunnel.transportHealthSnapshot()
	health.State = state
	health.Failure = failure
	HC.PublishTransportHealth(health)
}

func (c *Client) healthLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			health := c.tunnel.transportHealthSnapshot()
			rebinding := c.rebindActive()
			challenge := HC.CurrentRuntimeChallenge()
			progressAge := now.Sub(time.UnixMilli(health.LastAggregateProgressAt))
			switch {
			case challenge != nil:
				health.State = HC.TransportStateWaitingUser
				health.Failure = &HC.TransportFailure{Stage: "vk_auth", Kind: "captcha", Code: "14", ChallengeID: challenge.ID}
			case health.Demand && progressAge >= transportFailureLimit:
				health.State = HC.TransportStateFailed
				health.Failure = &HC.TransportFailure{Stage: "transport", Kind: "no_progress", Code: "aggregate_timeout"}
			case health.ActiveLanes == LaneCount && (!health.Demand || progressAge < transportDegradedLimit):
				health.State = HC.TransportStateHealthy
			case health.ActiveLanes > 0 && !rebinding:
				health.State = HC.TransportStateDegraded
			default:
				health.State = HC.TransportStateRecovering
			}
			HC.PublishTransportHealth(health)
		case <-c.ctx.Done():
			return
		case <-c.tunnel.Done():
			return
		}
	}
}

func (c *Client) rebindActive() bool {
	c.rebindMu.Lock()
	active := c.rebindCancel != nil
	c.rebindMu.Unlock()
	return active
}
