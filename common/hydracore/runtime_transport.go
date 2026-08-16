package hydracore

import (
	"sync"
	"time"
)

const (
	TransportStateStarting    = "starting"
	TransportStateWaitingUser = "waiting_user"
	TransportStateHealthy     = "healthy"
	TransportStateDegraded    = "degraded"
	TransportStateRecovering  = "recovering"
	TransportStateFailed      = "failed"
)

type TransportFailure struct {
	Stage        string `json:"stage,omitempty"`
	Kind         string `json:"kind,omitempty"`
	Code         string `json:"code,omitempty"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
	ChallengeID string `json:"challenge_id,omitempty"`
}

type TransportHealthSnapshot struct {
	State                   string            `json:"state"`
	ActiveLanes             int32             `json:"active_lanes"`
	TotalLanes              int32             `json:"total_lanes"`
	Demand                  bool              `json:"demand"`
	LastProgressAt          int64             `json:"last_progress_at"`
	LastAggregateProgressAt int64             `json:"last_aggregate_progress_at"`
	LastInboundAt           int64             `json:"last_inbound_at"`
	ObservedAt              int64             `json:"observed_at"`
	Failure                 *TransportFailure `json:"failure,omitempty"`
}

type RuntimeChallenge struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	URL       string `json:"url"`
	CreatedAt int64  `json:"created_at"`
	ExpiresAt int64  `json:"expires_at"`
}

var runtimeTransportState struct {
	sync.RWMutex
	health          TransportHealthSnapshot
	challenge       *RuntimeChallenge
	cancelChallenge func()
}

func PublishTransportHealth(snapshot TransportHealthSnapshot) {
	if snapshot.State == "" {
		snapshot.State = TransportStateStarting
	}
	if snapshot.ObservedAt == 0 {
		snapshot.ObservedAt = time.Now().UnixMilli()
	}
	runtimeTransportState.Lock()
	runtimeTransportState.health = cloneTransportHealth(snapshot)
	runtimeTransportState.Unlock()
}

func CurrentTransportHealth() TransportHealthSnapshot {
	runtimeTransportState.RLock()
	snapshot := cloneTransportHealth(runtimeTransportState.health)
	runtimeTransportState.RUnlock()
	if snapshot.State == "" {
		snapshot.State = TransportStateStarting
		snapshot.ObservedAt = time.Now().UnixMilli()
	}
	return snapshot
}

func PublishRuntimeChallenge(challenge RuntimeChallenge, cancel func()) {
	copy := challenge
	runtimeTransportState.Lock()
	runtimeTransportState.challenge = &copy
	runtimeTransportState.cancelChallenge = cancel
	runtimeTransportState.Unlock()
}

func ClearRuntimeChallenge(id string) {
	runtimeTransportState.Lock()
	if runtimeTransportState.challenge != nil && (id == "" || runtimeTransportState.challenge.ID == id) {
		runtimeTransportState.challenge = nil
		runtimeTransportState.cancelChallenge = nil
	}
	runtimeTransportState.Unlock()
}

func CurrentRuntimeChallenge() *RuntimeChallenge {
	runtimeTransportState.RLock()
	defer runtimeTransportState.RUnlock()
	if runtimeTransportState.challenge == nil {
		return nil
	}
	copy := *runtimeTransportState.challenge
	return &copy
}

func CancelRuntimeChallenge(id string) bool {
	runtimeTransportState.Lock()
	if runtimeTransportState.challenge == nil || (id != "" && runtimeTransportState.challenge.ID != id) {
		runtimeTransportState.Unlock()
		return false
	}
	cancel := runtimeTransportState.cancelChallenge
	runtimeTransportState.challenge = nil
	runtimeTransportState.cancelChallenge = nil
	runtimeTransportState.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

func ResetRuntimeTransportState() {
	CancelRuntimeChallenge("")
	PublishTransportHealth(TransportHealthSnapshot{State: TransportStateStarting})
}

func cloneTransportHealth(snapshot TransportHealthSnapshot) TransportHealthSnapshot {
	if snapshot.Failure != nil {
		failure := *snapshot.Failure
		snapshot.Failure = &failure
	}
	return snapshot
}
