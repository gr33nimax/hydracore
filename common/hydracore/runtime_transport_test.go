package hydracore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransportHealthNotifiesOnlyMaterialChanges(t *testing.T) {
	ResetRuntimeTransportState()
	t.Cleanup(ResetRuntimeTransportState)

	changed := TransportHealthChanged()
	PublishTransportHealth(1, TransportHealthSnapshot{TransportTag: "call-vk", State: TransportStateHealthy, ActiveLanes: 8})
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("material health change was not published")
	}

	unchanged := TransportHealthChanged()
	PublishTransportHealth(1, TransportHealthSnapshot{TransportTag: "call-vk", State: TransportStateHealthy, ActiveLanes: 8, ObservedAt: 123})
	select {
	case <-unchanged:
		t.Fatal("timestamp-only update was published")
	default:
	}

	PublishTransportHealth(1, TransportHealthSnapshot{TransportTag: "call-vk", State: TransportStateDegraded, ActiveLanes: 7})
	select {
	case <-unchanged:
	case <-time.After(time.Second):
		t.Fatal("state change was not published")
	}
}

func TestRuntimeChallengeNotifiesTransportHealthSubscribers(t *testing.T) {
	ResetRuntimeTransportState()
	t.Cleanup(ResetRuntimeTransportState)

	changed := TransportHealthChanged()
	PublishRuntimeChallenge(RuntimeChallenge{ID: "captcha-1"}, nil)
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("challenge did not publish waiting-user health")
	}

	changed = TransportHealthChanged()
	ClearRuntimeChallenge("captcha-1")
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("cleared challenge did not publish health")
	}
}

func TestTransportHealthRejectsStaleGeneration(t *testing.T) {
	ResetRuntimeTransportState()
	t.Cleanup(ResetRuntimeTransportState)

	SetRuntimeGeneration(2)
	PublishTransportHealth(1, TransportHealthSnapshot{State: TransportStateFailed})

	require.Equal(t, TransportStateStarting, CurrentTransportHealth().State)
}

func TestCurrentTransportHealthDerivesWaitingUserFromChallenge(t *testing.T) {
	ResetRuntimeTransportState()
	t.Cleanup(ResetRuntimeTransportState)

	PublishRuntimeChallenge(RuntimeChallenge{ID: "captcha-1", Kind: "vk_captcha"}, nil)
	require.Equal(t, TransportStateWaitingUser, CurrentTransportHealth().State)
	require.Nil(t, CurrentTransportHealth().Failure)

	ClearRuntimeChallenge("captcha-1")
	health := CurrentTransportHealth()
	require.NotEqual(t, TransportStateWaitingUser, health.State)
	require.Nil(t, health.Failure)
}

func TestTransportHealthJSONIsAdditive(t *testing.T) {
	payload, err := json.Marshal(struct {
		SchemaVersion int                     `json:"schema_version"`
		Health        TransportHealthSnapshot `json:"health"`
	}{
		SchemaVersion: 2,
		Health: TransportHealthSnapshot{
			Applicable:        true,
			RuntimeGeneration: 3,
			NetworkGeneration: 4,
			Failure: &TransportFailure{
				Domain:   "AUTH",
				Terminal: true,
			},
		},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"schema_version":2,"health":{"state":"","active_lanes":0,"total_lanes":0,"demand":false,"last_progress_at":0,"last_aggregate_progress_at":0,"last_inbound_at":0,"observed_at":0,"applicable":true,"runtime_generation":3,"network_generation":4,"quic_rtt_millis":0,"failure":{"domain":"AUTH","terminal":true}}}`, string(payload))
}

func TestTransportHealthJSONOmitsNilFailure(t *testing.T) {
	payload, err := json.Marshal(TransportHealthSnapshot{})
	require.NoError(t, err)
	require.NotContains(t, string(payload), "failure")
}
