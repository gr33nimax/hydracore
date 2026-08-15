package vkparasite

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/stretchr/testify/require"
)

func TestKCPRTTMatchesRetransmissionTimestamp(t *testing.T) {
	t.Parallel()
	lane := &kcpLane{
		metrics: telemetry.NewAccumulator(),
		kcpSent: make(map[uint32]kcpSentSegment),
	}
	lane.metrics.SetCollectionActive(true)
	first := testKCPSegment(kcpCommandPush, 7, 100, 0)
	second := testKCPSegment(kcpCommandPush, 7, 101, 0)
	lane.observeKCPOutput(first)
	lane.observeKCPOutput(second)
	sent := lane.kcpSent[7]
	sent.attempts[0].sentAt = time.Now().Add(-500 * time.Millisecond)
	sent.attempts[1].sentAt = time.Now().Add(-40 * time.Millisecond)
	lane.kcpSent[7] = sent

	lane.observeKCPInput(testKCPSegment(kcpCommandACK, 7, 101, 8))

	require.InDelta(t, 40, lane.kcpSRTTMS, 20)
	require.Equal(t, float64(1), lane.metrics.Value(telemetry.KCPRTTSamplesTotal))
	require.Equal(t, float64(1), lane.metrics.Value(telemetry.KCPAckProgressSegmentsTotal))
	require.Empty(t, lane.kcpSent)
}

func TestTelemetryLeaseRenewalDoesNotResetLaneRTTState(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x12344321, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	tunnel.SetTelemetryCollectionActive(true)
	lane := tunnel.lanes[0]
	lane.mu.Lock()
	lane.kcpSRTTMS = 57
	lane.kcpRTTVARMS = 9
	lane.kcpSent[3] = kcpSentSegment{lastSentAt: time.Now()}
	lane.mu.Unlock()

	// The server renews the client telemetry lease every snapshot. A renewal is
	// not a collection transition and must preserve the running estimator.
	tunnel.SetTelemetryCollectionActive(true)
	lane.mu.Lock()
	require.Equal(t, float64(57), lane.kcpSRTTMS)
	require.Equal(t, float64(9), lane.kcpRTTVARMS)
	require.Contains(t, lane.kcpSent, uint32(3))
	lane.mu.Unlock()

	tunnel.SetTelemetryCollectionActive(false)
	tunnel.SetTelemetryCollectionActive(true)
	lane.mu.Lock()
	require.Zero(t, lane.kcpSRTTMS)
	require.Zero(t, lane.kcpRTTVARMS)
	require.Empty(t, lane.kcpSent)
	lane.mu.Unlock()
}

func testKCPSegment(command byte, sequence, timestamp, una uint32) []byte {
	segment := make([]byte, kcpHeaderSize)
	segment[4] = command
	binary.LittleEndian.PutUint32(segment[8:12], timestamp)
	binary.LittleEndian.PutUint32(segment[12:16], sequence)
	binary.LittleEndian.PutUint32(segment[16:20], una)
	return segment
}
