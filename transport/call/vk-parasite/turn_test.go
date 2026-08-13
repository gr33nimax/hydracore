package vkparasite

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/stretchr/testify/require"
)

func TestParseTURNUDPURL(t *testing.T) {
	t.Parallel()
	address, err := parseTURNUDPURL("turn:relay.example.invalid:3478?transport=udp")
	require.NoError(t, err)
	require.Equal(t, "relay.example.invalid", address.Fqdn)
	require.Equal(t, uint16(3478), address.Port)
	address, err = parseTURNUDPURL("turn://192.0.2.10")
	require.NoError(t, err)
	require.Equal(t, uint16(3478), address.Port)
	_, err = parseTURNUDPURL("turns:relay.example.invalid:5349?transport=tcp")
	require.Error(t, err)
}

func TestRebindInterruptionsAreNotTransportFailures(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	metrics := telemetry.NewAccumulator()
	recordTURNFailure(ctx, metrics, time.Now(), 2, "all_endpoints")
	require.Zero(t, metrics.Value(telemetry.TURNAllocateFailureTotal))
	events := metrics.DrainEvents(1)
	require.Len(t, events, 1)
	require.Equal(t, "turn_allocate_interrupted", events[0].Event)
	require.Equal(t, "rebind", events[0].Reason)

	client := &Client{metrics: metrics}
	client.recordInnerAuthFailure(client.metrics, ctx, time.Now(), 2, "read")
	require.Zero(t, metrics.Value(telemetry.InnerAuthFailureTotal))
	events = metrics.DrainEvents(1)
	require.Len(t, events, 1)
	require.Equal(t, "inner_auth_interrupted", events[0].Event)
	require.Equal(t, "rebind", events[0].Reason)
}

func TestSetupDeadlineRemainsFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	metrics := telemetry.NewAccumulator()
	recordTURNFailure(ctx, metrics, time.Now(), 1, "all_endpoints")
	require.Equal(t, float64(1), metrics.Value(telemetry.TURNAllocateFailureTotal))
	events := metrics.DrainEvents(1)
	require.Len(t, events, 1)
	require.Equal(t, "timeout", events[0].Reason)
}
