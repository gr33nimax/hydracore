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
