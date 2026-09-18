package vkparasite

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	HC "github.com/sagernet/sing-box/common/hydracore"
	callvk "github.com/sagernet/sing-box/transport/call/vk"
	"github.com/stretchr/testify/require"
)

func TestReconnectPathStopsForTerminalFailure(t *testing.T) {
	assertReconnectAttempts(t, &dialOutcome{err: errors.New("dial failed"), failure: &HC.TransportFailure{Domain: "AUTH", Terminal: true}}, 1)
}

func TestReconnectPathRetriesNonTerminalFailure(t *testing.T) {
	assertReconnectAttempts(t, &dialOutcome{err: errors.New("dial failed"), failure: &HC.TransportFailure{Domain: "AUTH"}}, 2)
}

func TestReconnectPathRetriesUnclassifiedFailure(t *testing.T) {
	assertReconnectAttempts(t, errors.New("dial failed"), 2)
}

func TestTerminalFailureHealthSnapshotIsFailed(t *testing.T) {
	var attempts atomic.Int32
	client := &Client{options: ClientOptions{Workers: 1}, startedAt: time.Now()}
	controlErr := &callvk.ControlPlaneError{Stage: "vk_auth", Kind: "captcha", Terminal: true, Cause: errors.New("cancelled")}
	failure := client.recordPathFailure(controlErr)
	client.relay = NewQUICRelay(t.Context(), QUICRelayOptions{
		PathCount: 1,
		DialPath: func(_ context.Context, _ uint16) (*quic.Conn, io.Closer, error) {
			attempts.Add(1)
			return nil, nil, &dialOutcome{err: controlErr, failure: failure}
		},
	})
	defer client.relay.Close()
	client.relay.Start()
	require.Eventually(t, func() bool { return attempts.Load() == 1 }, time.Second, 10*time.Millisecond)

	health := client.healthSnapshot(time.Now())
	require.Equal(t, HC.TransportStateFailed, health.State)
	require.NotNil(t, health.Failure)
	require.Equal(t, "AUTH", health.Failure.Domain)
	require.True(t, health.Failure.Terminal)
}

func assertReconnectAttempts(t *testing.T, dialErr error, wantAttempts int32) {
	t.Helper()
	var attempts atomic.Int32
	relay := NewQUICRelay(t.Context(), QUICRelayOptions{
		PathCount: 1,
		DialPath: func(_ context.Context, _ uint16) (*quic.Conn, io.Closer, error) {
			attempts.Add(1)
			return nil, nil, dialErr
		},
	})
	defer relay.Close()
	relay.Start()
	require.Eventually(t, func() bool { return attempts.Load() >= wantAttempts }, 2*time.Second, 10*time.Millisecond)
	if wantAttempts == 1 {
		time.Sleep(600 * time.Millisecond)
		require.Equal(t, wantAttempts, attempts.Load())
	}
}
