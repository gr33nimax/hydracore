package vkparasite

import (
	"errors"
	"testing"
	"time"

	HC "github.com/sagernet/sing-box/common/hydracore"
	callvk "github.com/sagernet/sing-box/transport/call/vk"
	"github.com/stretchr/testify/require"
)

func TestHealthSnapshotReportsCaptchaAsWaitingUser(t *testing.T) {
	HC.ResetRuntimeTransportState()
	defer HC.ResetRuntimeTransportState()
	client := &Client{options: ClientOptions{Workers: DefaultWorkerCount}, startedAt: time.Now()}
	HC.PublishRuntimeChallenge(HC.RuntimeChallenge{ID: "captcha-1", Kind: "vk_captcha"}, func() {})

	health := client.healthSnapshot(time.Now())
	require.Equal(t, HC.TransportStateWaitingUser, health.State)
	require.Equal(t, "captcha", health.Failure.Kind)
	require.Equal(t, "captcha-1", health.Failure.ChallengeID)
}

func TestHealthSnapshotKeepsInitialControlPlaneFailureDistinct(t *testing.T) {
	HC.ResetRuntimeTransportState()
	defer HC.ResetRuntimeTransportState()
	client := &Client{options: ClientOptions{Workers: DefaultWorkerCount}, startedAt: time.Now()}
	client.recordPathFailure(&callvk.ControlPlaneError{
		Stage: "vk_auth", Kind: "rate_limit", Code: "9", Cause: errors.New("flood control"),
	})

	health := client.healthSnapshot(time.Now())
	require.Equal(t, HC.TransportStateFailed, health.State)
	require.Equal(t, "rate_limit", health.Failure.Kind)
	require.Empty(t, health.Failure.ChallengeID)
}

func TestHealthSnapshotOmitsFailureWithActiveLanes(t *testing.T) {
	client := &Client{
		options: ClientOptions{Workers: DefaultWorkerCount},
		relay:   &QUICRelay{paths: []*quicPathConn{{}}},
	}
	client.recordPathFailure(errors.New("dial failed"))

	health := client.healthSnapshot(time.Now())
	require.Equal(t, HC.TransportStateDegraded, health.State)
	require.Nil(t, health.Failure)
}

func TestHealthSnapshotClearsCaptchaFailureAfterChallengeIsCleared(t *testing.T) {
	HC.ResetRuntimeTransportState()
	defer HC.ResetRuntimeTransportState()
	client := &Client{options: ClientOptions{Workers: DefaultWorkerCount}}
	client.recordPathFailure(&callvk.ControlPlaneError{
		Stage: "vk_auth", Kind: "captcha", Code: "14", Cause: errors.New("captcha required"),
	})
	HC.PublishRuntimeChallenge(HC.RuntimeChallenge{ID: "captcha-1", Kind: "vk_captcha"}, func() {})
	_ = client.healthSnapshot(time.Now())
	HC.ClearRuntimeChallenge("captcha-1")

	health := client.healthSnapshot(time.Now())
	require.Nil(t, health.Failure)
}

func TestHealthSnapshotStartsBeforeAnyPathWasActive(t *testing.T) {
	client := &Client{options: ClientOptions{Workers: DefaultWorkerCount}}

	health := client.healthSnapshot(time.Now())
	require.Equal(t, HC.TransportStateStarting, health.State)
}
