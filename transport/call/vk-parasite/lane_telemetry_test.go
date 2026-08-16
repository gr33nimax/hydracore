package vkparasite

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/stretchr/testify/require"
)

func TestKCPRTTSkipsRetransmittedSegment(t *testing.T) {
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

	require.Zero(t, lane.kcpSRTTMS)
	require.Zero(t, lane.metrics.Value(telemetry.KCPRTTSamplesTotal))
	require.Equal(t, float64(1), lane.metrics.Value(telemetry.KCPAckProgressSegmentsTotal))
	require.Empty(t, lane.kcpSent)
}

func TestKCPRTTSamplesUnambiguousSegment(t *testing.T) {
	t.Parallel()
	lane := &kcpLane{
		metrics: telemetry.NewAccumulator(),
		kcpSent: make(map[uint32]kcpSentSegment),
	}
	lane.metrics.SetCollectionActive(true)
	lane.observeKCPOutput(testKCPSegment(kcpCommandPush, 7, 100, 0))
	sent := lane.kcpSent[7]
	sent.attempts[0].sentAt = time.Now().Add(-40 * time.Millisecond)
	lane.kcpSent[7] = sent

	lane.observeKCPInput(testKCPSegment(kcpCommandACK, 7, 100, 8))

	require.InDelta(t, 40, lane.kcpSRTTMS, 20)
	require.Equal(t, float64(1), lane.metrics.Value(telemetry.KCPRTTSamplesTotal))
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

func TestAckedBytesCountedIncludingUNAPrune(t *testing.T) {
	t.Parallel()
	lane := &kcpLane{
		metrics: telemetry.NewAccumulator(),
		kcpSent: make(map[uint32]kcpSentSegment),
	}
	lane.metrics.SetCollectionActive(true)
	lane.observeKCPOutput(testKCPSegment(kcpCommandPush, 7, 100, 0))
	lane.observeKCPOutput(testKCPSegment(kcpCommandPush, 9, 101, 0))

	// Explicit per-segment ACK for sequence 7.
	lane.observeKCPInput(testKCPSegment(kcpCommandACK, 7, 101, 8))
	require.Equal(t, uint64(kcpHeaderSize), lane.ackedBytesTotal)

	// A cumulative ACK advances una past sequence 9 without a per-segment
	// record; the prune path must still fold its bytes into the delivery
	// accounting.
	lane.observeKCPInput(testKCPSegment(kcpCommandACK, 0, 102, 10))
	require.Equal(t, uint64(2*kcpHeaderSize), lane.ackedBytesTotal)
	require.Empty(t, lane.kcpSent)
}

func TestAdmittedBytesCountedOnPacing(t *testing.T) {
	t.Parallel()
	lane := &kcpLane{
		metrics:          telemetry.NewAccumulator(),
		pacingRateBPS:    1_000_000,
		pacingTokens:     0,
		pacingLastRefill: time.Unix(1, 0),
	}
	now := time.Unix(1, 0)
	admitted := 0
	for step := 0; step < 100; step++ {
		now = now.Add(10 * time.Millisecond)
		for lane.admitPacingLocked(976, false, now) {
			admitted++
			if admitted > 4000 {
				break
			}
		}
	}
	require.InDelta(t, 1024, admitted, 120)
	require.Equal(t, uint64(admitted)*976, lane.admittedBytesTotal)
}

func TestRetransmissionDebtShrinksNewDataBudget(t *testing.T) {
	t.Parallel()
	lane := &kcpLane{
		metrics:          telemetry.NewAccumulator(),
		pacingRateBPS:    1_000_000,
		retxRateBPS:      400_000,
		pacingTokens:     0,
		pacingLastRefill: time.Unix(1, 0),
	}
	require.Equal(t, 600_000.0, lane.newDataBudgetBPSLocked())
	// The floor sits below lanePacingMinimumBPS so the debt stays effective
	// while the target is clamped at its own floor.
	require.Less(t, laneNewDataFloorBPS, lanePacingMinimumBPS)

	now := time.Unix(1, 0)
	admitted := 0
	for step := 0; step < 100; step++ {
		now = now.Add(10 * time.Millisecond)
		for lane.admitPacingLocked(976, false, now) {
			admitted++
			if admitted > 4000 {
				break
			}
		}
	}
	// The 600 KB/s debt-limited budget admits roughly 615 segments of 976
	// bytes per simulated second, well below the 1024 of the debt-free run.
	require.InDelta(t, 615, admitted, 120)

	// A retransmission storm larger than the total target clamps the budget
	// to the small floor instead of starving the lane completely.
	lane.pacingRateBPS = 400_000
	lane.retxRateBPS = 900_000
	require.Equal(t, float64(laneNewDataFloorBPS), lane.newDataBudgetBPSLocked())
}
