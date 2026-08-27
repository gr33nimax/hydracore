package hydracore

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	require.JSONEq(t, `{"schema_version":2,"health":{"state":"","active_lanes":0,"total_lanes":0,"demand":false,"last_progress_at":0,"last_aggregate_progress_at":0,"last_inbound_at":0,"observed_at":0,"applicable":true,"runtime_generation":3,"network_generation":4,"failure":{"domain":"AUTH","terminal":true}}}`, string(payload))
}

func TestTransportHealthJSONOmitsNilFailure(t *testing.T) {
	payload, err := json.Marshal(TransportHealthSnapshot{})
	require.NoError(t, err)
	require.NotContains(t, string(payload), "failure")
}
