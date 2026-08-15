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

func testKCPSegment(command byte, sequence, timestamp, una uint32) []byte {
	segment := make([]byte, kcpHeaderSize)
	segment[4] = command
	binary.LittleEndian.PutUint32(segment[8:12], timestamp)
	binary.LittleEndian.PutUint32(segment[12:16], sequence)
	binary.LittleEndian.PutUint32(segment[16:20], una)
	return segment
}
