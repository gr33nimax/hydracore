package hydracore

import (
	"reflect"
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
	ChallengeID  string `json:"challenge_id,omitempty"`
	Domain       string `json:"domain,omitempty"`
	Terminal     bool   `json:"terminal"`
}

type TransportHealthSnapshot struct {
	TransportTag            string            `json:"transport_tag,omitempty"`
	State                   string            `json:"state"`
	ActiveLanes             int32             `json:"active_lanes"`
	TotalLanes              int32             `json:"total_lanes"`
	Demand                  bool              `json:"demand"`
	LastProgressAt          int64             `json:"last_progress_at"`
	LastAggregateProgressAt int64             `json:"last_aggregate_progress_at"`
	LastInboundAt           int64             `json:"last_inbound_at"`
	ObservedAt              int64             `json:"observed_at"`
	Applicable              bool              `json:"applicable"`
	RuntimeGeneration       uint64            `json:"runtime_generation"`
	NetworkGeneration       uint64            `json:"network_generation"`
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
	generation      uint64
	health          TransportHealthSnapshot
	challenge       *RuntimeChallenge
	cancelChallenge func()
	healthChanged   chan struct{}
}

func init() {
	runtimeTransportState.healthChanged = make(chan struct{})
}

func SetRuntimeGeneration(generation uint64) {
	runtimeTransportState.Lock()
	runtimeTransportState.generation = generation
	runtimeTransportState.Unlock()
}

func CurrentRuntimeGeneration() uint64 {
	runtimeTransportState.RLock()
	generation := runtimeTransportState.generation
	runtimeTransportState.RUnlock()
	return generation
}

func PublishTransportHealth(generation uint64, snapshot TransportHealthSnapshot) {
	if snapshot.State == "" {
		snapshot.State = TransportStateStarting
	}
	if snapshot.ObservedAt == 0 {
		snapshot.ObservedAt = time.Now().UnixMilli()
	}
	runtimeTransportState.Lock()
	if generation < runtimeTransportState.generation {
		runtimeTransportState.Unlock()
		return
	}
	runtimeTransportState.generation = generation
	snapshot.RuntimeGeneration = generation
	changed := !equalMaterialTransportHealth(runtimeTransportState.health, snapshot)
	runtimeTransportState.health = cloneTransportHealth(snapshot)
	if changed {
		notifyTransportHealthChangedLocked()
	}
	runtimeTransportState.Unlock()
}

// TransportHealthChanged closes when a materially different transport snapshot is published.
// Callers fetch a new channel after every wake-up.
func TransportHealthChanged() <-chan struct{} {
	runtimeTransportState.RLock()
	changed := runtimeTransportState.healthChanged
	runtimeTransportState.RUnlock()
	return changed
}

func CurrentTransportHealth() TransportHealthSnapshot {
	runtimeTransportState.RLock()
	snapshot := cloneTransportHealth(runtimeTransportState.health)
	generation := runtimeTransportState.generation
	runtimeTransportState.RUnlock()
	snapshot.RuntimeGeneration = generation
	if snapshot.State == "" {
		snapshot.State = TransportStateStarting
		snapshot.ObservedAt = time.Now().UnixMilli()
	}
	if CurrentRuntimeChallenge() != nil {
		snapshot.State = TransportStateWaitingUser
	}
	return snapshot
}

func PublishRuntimeChallenge(challenge RuntimeChallenge, cancel func()) {
	copy := challenge
	runtimeTransportState.Lock()
	changed := runtimeTransportState.challenge == nil || *runtimeTransportState.challenge != challenge
	runtimeTransportState.challenge = &copy
	runtimeTransportState.cancelChallenge = cancel
	if changed {
		notifyTransportHealthChangedLocked()
	}
	runtimeTransportState.Unlock()
}

func ClearRuntimeChallenge(id string) {
	runtimeTransportState.Lock()
	if runtimeTransportState.challenge != nil && (id == "" || runtimeTransportState.challenge.ID == id) {
		runtimeTransportState.challenge = nil
		runtimeTransportState.cancelChallenge = nil
		notifyTransportHealthChangedLocked()
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
	notifyTransportHealthChangedLocked()
	runtimeTransportState.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

func ResetRuntimeTransportState() {
	CancelRuntimeChallenge("")
	runtimeTransportState.Lock()
	runtimeTransportState.generation = 0
	runtimeTransportState.health = TransportHealthSnapshot{
		State:      TransportStateStarting,
		ObservedAt: time.Now().UnixMilli(),
	}
	notifyTransportHealthChangedLocked()
	runtimeTransportState.Unlock()
}

func cloneTransportHealth(snapshot TransportHealthSnapshot) TransportHealthSnapshot {
	if snapshot.Failure != nil {
		failure := *snapshot.Failure
		snapshot.Failure = &failure
	}
	return snapshot
}

func equalMaterialTransportHealth(left, right TransportHealthSnapshot) bool {
	left.ObservedAt, right.ObservedAt = 0, 0
	left.LastProgressAt, right.LastProgressAt = 0, 0
	left.LastAggregateProgressAt, right.LastAggregateProgressAt = 0, 0
	left.LastInboundAt, right.LastInboundAt = 0, 0
	return reflect.DeepEqual(left, right)
}

func notifyTransportHealthChangedLocked() {
	close(runtimeTransportState.healthChanged)
	runtimeTransportState.healthChanged = make(chan struct{})
}
