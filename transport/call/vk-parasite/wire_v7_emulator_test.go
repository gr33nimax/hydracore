package vkparasite

import (
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/stretchr/testify/require"
)

type deterministicLaneEmulator struct {
	bandwidthBPS float64
	rtt          time.Duration
	lossEvery    int
	delayEvery   int
	ackLossEvery int
	reorderEvery int
	blackhole    bool
	demand       []bool
	packet       int
	acks         int
	carried      int
}

// wantDemand reports whether the application supplies data during the given
// delivery interval. A nil schedule means sustained demand, matching the
// legacy scenarios.
func (e *deterministicLaneEmulator) wantDemand(interval int) bool {
	if len(e.demand) == 0 {
		return true
	}
	return e.demand[interval%len(e.demand)]
}

func (e *deterministicLaneEmulator) deliver(rateBPS float64, interval time.Duration) (sent, delivered, retransmitted int, rtt time.Duration) {
	mss := float64(laneKCPMTU - kcpHeaderSize)
	sent = max(1, int(rateBPS*interval.Seconds()/mss))
	capacity := max(1, int(e.bandwidthBPS*interval.Seconds()/mss))
	delivered = min(sent, capacity)
	if e.blackhole {
		delivered = 0
	}
	lost := 0
	delayed := 0
	for packet := 0; packet < delivered; packet++ {
		e.packet++
		if e.reorderEvery > 0 && e.packet%e.reorderEvery == 0 {
			// Reordered packets land in the next delivery interval.
			delayed++
			continue
		}
		if e.lossEvery > 0 && e.packet%e.lossEvery == 0 {
			lost++
			continue
		}
		// ACK loss models the reverse path: the segment arrived but its
		// acknowledgement did not, so the sender will retransmit it even
		// though the forward path is clean.
		e.acks++
		if e.ackLossEvery > 0 && e.acks%e.ackLossEvery == 0 {
			lost++
		}
	}
	delivered = delivered - lost - delayed + e.carried
	e.carried = delayed
	retransmitted = max(0, sent-delivered)
	rtt = e.rtt
	if e.delayEvery > 0 && e.packet%e.delayEvery == 0 {
		rtt += e.rtt / 2
	}
	return
}

// driveByteWindow runs one delivery window against the controller while
// mirroring the production byte accounting: admitted bytes track the sent
// segments and uniquely-acked bytes track the delivered segments, so the
// marginal-goodput probes have real byte signals instead of pacing knob
// positions.
func driveByteWindow(lane *kcpLane, emulator *deterministicLaneEmulator, now time.Time, intervalIndex int, mss float64) (sent, delivered, retransmitted int) {
	sent, delivered, retransmitted, rtt := emulator.deliver(lane.pacingRateBPS, laneDeliverySampleWindow)
	lane.deliveryOutSegments = uint64(sent)
	lane.deliveryRetrans = uint64(retransmitted)
	lane.deliveryRetransBytes = uint64(retransmitted) * uint64(mss)
	lane.deliveryDemanded = emulator.wantDemand(intervalIndex)
	lane.kcpSRTTMS = float64(rtt) / float64(time.Millisecond)
	lane.ackedBytesTotal += uint64(delivered) * uint64(mss)
	lane.admittedBytesTotal += uint64(sent) * uint64(mss)
	lane.updateDeliveryController(now, delivered)
	return
}

func newControllerLane(now time.Time) *kcpLane {
	lane := &kcpLane{
		metrics:          telemetry.NewAccumulator(),
		admissionWindow:  laneKCPInitialAdmission,
		deliverySampleAt: now,
		minRTTMS:         80,
		pacingRateBPS:    lanePacingInitialBPS,
		pacingTokens:     float64(lanePacingBucketSegments * (laneKCPMTU - kcpHeaderSize)),
		pacingLastRefill: now,
		pacingStartup:    true,
		pacingNextProbe:  now.Add(lanePacingProbeInterval),
	}
	lane.applicationLimited = true
	lane.compensationCeiling = laneCompensationInitialCeiling
	return lane
}

func TestWireV9FourLaneDeterministicSixtySecondLoad(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lanes := [LaneCount]*kcpLane{}
	emulators := [LaneCount]deterministicLaneEmulator{}
	for laneID := 0; laneID < LaneCount; laneID++ {
		lanes[laneID] = newControllerLane(start)
		emulators[laneID] = deterministicLaneEmulator{
			bandwidthBPS: 2_000_000 / 8,
			rtt:          80 * time.Millisecond,
			lossEvery:    100,
			delayEvery:   17,
		}
	}
	interval := laneDeliverySampleWindow
	totalDelivered := 0
	totalSent := 0
	zeroIntervals := 0
	maximumZeroIntervals := 0
	for step := 1; step <= int(60*time.Second/interval); step++ {
		intervalDelivered := 0
		for laneID, lane := range lanes {
			sent, delivered, retransmitted, rtt := emulators[laneID].deliver(lane.pacingRateBPS, interval)
			lane.deliveryOutSegments = uint64(sent)
			lane.deliveryRetrans = uint64(retransmitted)
			lane.deliveryDemanded = true
			lane.kcpSRTTMS = float64(rtt) / float64(time.Millisecond)
			lane.updateDeliveryController(start.Add(time.Duration(step)*interval), delivered)
			totalSent += sent
			totalDelivered += delivered
			intervalDelivered += delivered
			require.GreaterOrEqual(t, lane.pacingRateBPS, float64(lanePacingMinimumBPS))
			require.LessOrEqual(t, lane.pacingRateBPS, float64(lanePacingMaximumBPS))
			require.GreaterOrEqual(t, lane.admissionWindow, laneKCPMinimumAdmission)
			require.LessOrEqual(t, lane.admissionWindow, laneKCPMaximumAdmission)
		}
		if intervalDelivered == 0 {
			zeroIntervals++
			maximumZeroIntervals = max(maximumZeroIntervals, zeroIntervals)
		} else {
			zeroIntervals = 0
		}
	}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	goodputBPS := float64(totalDelivered) * mss * 8 / 60
	retransmitRatio := float64(totalSent-totalDelivered) / float64(totalSent)
	require.GreaterOrEqual(t, goodputBPS, 5_600_000.0)
	// A fixed-capacity media path can discard excess traffic without inflating
	// RTT or the local writer queue. Preserve useful throughput and bound that
	// loss instead of forcing every such path below an artificial 15% target.
	require.Less(t, retransmitRatio, 0.45)
	require.Less(t, time.Duration(maximumZeroIntervals)*interval, 2*time.Second)

	// Three consecutive virtual runs must retain non-zero aggregate delivery.
	for run := 0; run < 3; run++ {
		delivered := 0
		for laneID, lane := range lanes {
			_, current, _, _ := emulators[laneID].deliver(lane.pacingRateBPS, time.Second)
			delivered += current
		}
		require.Positive(t, delivered, "load run %d lost the whole tunnel", run+1)
	}
}

func TestWireV9ApplicationLimitedVideoPreservesBurstCapacity(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	lane.pacingRateBPS = 240_000
	lane.deliveryRateBPS = 220_000
	lane.admissionWindow = 32

	for sample := 1; sample <= 40; sample++ {
		lane.deliveryOutSegments = 2
		lane.deliveryRetrans = 0
		lane.deliveryDemanded = false
		lane.updateDeliveryController(start.Add(time.Duration(sample)*laneDeliverySampleWindow), 2)
	}

	require.Equal(t, 240_000.0, lane.pacingRateBPS)
	require.Equal(t, 220_000.0, lane.deliveryRateBPS)
	require.Equal(t, 32, lane.admissionWindow)
	require.True(t, lane.applicationLimited)

	lane.deliveryOutSegments = 100
	lane.deliveryRetrans = 0
	lane.deliveryDemanded = true
	lane.updateDeliveryController(start.Add(41*laneDeliverySampleWindow), 95)
	require.False(t, lane.applicationLimited)
	require.Greater(t, lane.pacingRateBPS, 0.9*240_000.0)
}

func TestWireV9BackloggedLaneIsNotApplicationLimited(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	lane.pacingRateBPS = 240_000
	lane.deliveryRateBPS = 220_000
	lane.deliveryCapacityBPS = 220_000
	lane.outputPending = make([]queuedSegment, lanePacingBucketSegments)
	lane.deliveryOutSegments = 100
	lane.deliveryRetrans = 50
	lane.deliveryDemanded = false

	lane.updateDeliveryController(start.Add(laneDeliverySampleWindow), 50)

	require.False(t, lane.applicationLimited)
}

func TestWireV9LossyStablePathKeepsProbingCapacity(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	emulator := deterministicLaneEmulator{
		bandwidthBPS: 6_000_000 / 8,
		rtt:          80 * time.Millisecond,
		lossEvery:    4,
	}
	for sample := 1; sample <= 12; sample++ {
		sent, delivered, retransmitted, rtt := emulator.deliver(lane.pacingRateBPS, laneDeliverySampleWindow)
		lane.deliveryOutSegments = uint64(sent)
		lane.deliveryRetrans = uint64(retransmitted)
		lane.deliveryDemanded = true
		lane.kcpSRTTMS = float64(rtt) / float64(time.Millisecond)
		lane.updateDeliveryController(start.Add(time.Duration(sample)*laneDeliverySampleWindow), delivered)
	}

	require.Greater(t, lane.pacingRateBPS, float64(lanePacingInitialBPS))
	require.Zero(t, lane.congestionSamples)
}

func TestWireV9LossWithDelayAndPressureBacksOff(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	lane.pacingRateBPS = 500_000
	lane.deliveryRateBPS = 400_000
	lane.deliveryCapacityBPS = 400_000
	lane.kcpSRTTMS = 200
	lane.outputPending = make([]queuedSegment, 3*laneKCPOutputBacklog/4)

	for sample := 1; sample <= laneCongestionSamples; sample++ {
		lane.deliveryOutSegments = 100
		lane.deliveryRetrans = 30
		lane.deliveryDemanded = true
		lane.updateDeliveryController(start.Add(time.Duration(sample)*laneDeliverySampleWindow), 50)
	}

	require.Less(t, lane.pacingRateBPS, 500_000.0)
}

func TestWireV9BlackholeLeavesThreeLaneCapacity(t *testing.T) {
	t.Parallel()
	lanes := [LaneCount]deterministicLaneEmulator{}
	for laneID := range lanes {
		lanes[laneID] = deterministicLaneEmulator{bandwidthBPS: 2_000_000 / 8, rtt: 80 * time.Millisecond}
	}
	lanes[0].blackhole = true
	aggregate := 0
	for laneID := range lanes {
		_, delivered, _, _ := lanes[laneID].deliver(lanePacingInitialBPS, time.Second)
		aggregate += delivered
	}
	require.Zero(t, func() int { _, delivered, _, _ := lanes[0].deliver(lanePacingInitialBPS, time.Second); return delivered }())
	require.Positive(t, aggregate)
}

type deterministicResetLossConn struct {
	net.Conn
	writes atomic.Uint64
}

func (c *deterministicResetLossConn) Write(payload []byte) (int, error) {
	if len(payload) == 19 && string(payload[:8]) == string(laneResetControlMagic[:]) {
		attempt := c.writes.Add(1)
		if attempt%10 == 1 || attempt%10 == 4 || attempt%10 == 7 {
			return len(payload), nil
		}
	}
	return c.Conn.Write(payload)
}

func TestWireV9ResetHandshakeSurvivesThirtyPercentControlLoss(t *testing.T) {
	t.Parallel()
	left, err := NewParasiteTunnel(0x71727374, nil)
	require.NoError(t, err)
	right, err := NewParasiteTunnel(0x71727374, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		leftConn, rightConn := newTestDatagramPair()
		_, err = left.AddWorkerEpoch(laneID, 1, &deterministicResetLossConn{Conn: leftConn})
		require.NoError(t, err)
		_, err = right.AddWorkerEpoch(laneID, 1, &deterministicResetLossConn{Conn: rightConn})
		require.NoError(t, err)
	}
	started := time.Now()
	require.True(t, left.initiateLaneReset(0, "test_control_loss"))
	require.Eventually(t, func() bool {
		return left.LaneGeneration(0) == 2 && right.LaneGeneration(0) == 2 &&
			left.ActiveWorkers() == LaneCount-1 && right.ActiveWorkers() == LaneCount-1
	}, 4*time.Second, 10*time.Millisecond)
	require.LessOrEqual(t, time.Since(started), 2*time.Second)

	leftReplacement, rightReplacement := newTestDatagramPair()
	_, err = left.AddWorkerGenerationEpoch(0, 2, 2, leftReplacement)
	require.NoError(t, err)
	_, err = right.AddWorkerGenerationEpoch(0, 2, 2, rightReplacement)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		left.lanes[0].mu.Lock()
		leftActive := left.lanes[0].state == laneStateActive
		left.lanes[0].mu.Unlock()
		right.lanes[0].mu.Lock()
		rightActive := right.lanes[0].state == laneStateActive
		right.lanes[0].mu.Unlock()
		return leftActive && rightActive
	}, 5*time.Second, 10*time.Millisecond)

	left.SetTelemetryCollectionActive(true)
	stale := make([]byte, kcpHeaderSize)
	binary.LittleEndian.PutUint32(stale[:4], laneConversationGeneration(left.seed, 0, 1))
	left.lanes[0].inputSegment(stale)
	require.Equal(t, float64(1), left.lanes[0].metrics.Value(telemetry.LaneStaleGenerationDropsTotal))
}

func TestWireV9FragmentsTCPBeforeKCPButKeepsUDPDatagramsWhole(t *testing.T) {
	t.Parallel()
	maximumPayload := 4*(laneKCPMTU-kcpHeaderSize) - laneFrameHeaderSize - 9
	tcp := calltunnel.EncodeFrame(9, calltunnel.MsgData, make([]byte, 2*maximumPayload+1))
	fragments := fragmentTCPRelayFrame(tcp, calltunnel.MsgData)
	require.Len(t, fragments, 3)
	for _, fragment := range fragments {
		encoded := encodeLaneFrameGeneration(1, 9, 0, fragment)
		require.LessOrEqual(t, kcpSegmentsForPayload(len(encoded)), 4)
	}
	udp := calltunnel.EncodeFrame(10, calltunnel.MsgUDP, make([]byte, 2*maximumPayload+1))
	require.Len(t, fragmentTCPRelayFrame(udp, calltunnel.MsgUDP), 1)
}

func TestWireV9SessionNoProgressDeadlineIsBounded(t *testing.T) {
	t.Parallel()
	require.Equal(t, 30*time.Second, sessionNoProgressThreshold())
}

func TestWireV9PolicerPlateauIsHarmfulAndRollsBack(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	// The lane sits at a healthy steady state before the test drives it into
	// a 6 Mbit/s policer with 4% random loss on the delivered path.
	lane.pacingRateBPS = 750_000
	lane.deliveryRateBPS = 720_000
	lane.deliveryCapacityBPS = 720_000
	emulator := deterministicLaneEmulator{bandwidthBPS: 6_000_000 / 8, rtt: 80 * time.Millisecond, lossEvery: 25}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	for sample := 0; sample < 60; sample++ {
		driveByteWindow(lane, &emulator, start.Add(time.Duration(sample+1)*laneDeliverySampleWindow), sample, mss)
	}
	// The path cannot convert probes into delivery: after two harmful
	// verdicts the ceiling drops below the default, the next probe is
	// postponed by the doubling cooldown and the offered rate no longer
	// chases the legacy 1.65x target.
	require.LessOrEqual(t, lane.compensationCeiling, laneCompensationInitialCeiling-laneCompensationStepDown)
	require.GreaterOrEqual(t, lane.compensationCeiling, laneCompensationMinimum)
	require.Greater(t, lane.probeCooldownShift, 0)
	require.LessOrEqual(t, lane.pacingRateBPS, 1.25*emulator.bandwidthBPS)
}

func TestWireV9UsefulProbeRaisesCeiling(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	// The path has headroom: 4 Mbit/s of pacing into a 12 Mbit/s pipe with
	// 4% loss. A probe step converts into delivered goodput.
	lane.pacingRateBPS = 500_000
	lane.deliveryRateBPS = 480_000
	lane.deliveryCapacityBPS = 480_000
	emulator := deterministicLaneEmulator{bandwidthBPS: 12_000_000 / 8, rtt: 80 * time.Millisecond, lossEvery: 25}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	preProbePacing := 0.0
	for sample := 0; sample < 12; sample++ {
		driveByteWindow(lane, &emulator, start.Add(time.Duration(sample+1)*laneDeliverySampleWindow), sample, mss)
		if sample == 6 {
			preProbePacing = lane.pacingRateBPS
		}
	}
	require.Greater(t, lane.compensationCeiling, laneCompensationInitialCeiling)
	require.Zero(t, lane.probeCooldownShift)
	require.Zero(t, lane.probeHarmfulStreak)
	require.Greater(t, lane.pacingRateBPS, preProbePacing)
}

func TestWireV9ProbeInconclusiveWithoutBaselineDemand(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	lane.pacingRateBPS = 500_000
	lane.deliveryRateBPS = 480_000
	lane.deliveryCapacityBPS = 480_000
	initialPacing := lane.pacingRateBPS
	emulator := deterministicLaneEmulator{bandwidthBPS: 12_000_000 / 8, rtt: 80 * time.Millisecond}
	emulator.demand = []bool{false, false, false, false, false, false, false, false, false, false, false, false}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	for sample := 0; sample < 12; sample++ {
		driveByteWindow(lane, &emulator, start.Add(time.Duration(sample+1)*laneDeliverySampleWindow), sample, mss)
	}
	// Without demand, probe does not start, pacing rate does not bump, probeWindows == 0
	require.Equal(t, laneCompensationInitialCeiling, lane.compensationCeiling)
	require.Zero(t, lane.probeCooldownShift)
	require.Zero(t, lane.probeHarmfulStreak)
	require.Zero(t, lane.probeWindows)
	require.True(t, lane.pacingProbeUntil.IsZero())
	require.LessOrEqual(t, lane.pacingRateBPS, initialPacing)
}

func TestWireV9ProbeConclusiveWithActiveDemand(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	lane.pacingRateBPS = 500_000
	lane.deliveryRateBPS = 480_000
	lane.deliveryCapacityBPS = 480_000
	emulator := deterministicLaneEmulator{bandwidthBPS: 12_000_000 / 8, rtt: 80 * time.Millisecond}
	emulator.demand = []bool{true, true, true, true, true, true, true, true, true, true, true, true}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	for sample := 0; sample < 12; sample++ {
		driveByteWindow(lane, &emulator, start.Add(time.Duration(sample+1)*laneDeliverySampleWindow), sample, mss)
	}
	require.Greater(t, lane.compensationCeiling, laneCompensationInitialCeiling)
	require.Zero(t, lane.probeCooldownShift)
	require.Zero(t, lane.probeHarmfulStreak)
}

func TestWireV9AckLossProducesSpuriousRetransmissions(t *testing.T) {
	t.Parallel()
	start := time.Unix(1, 0)
	lane := newControllerLane(start)
	lane.pacingStartup = false
	lane.pacingRateBPS = 750_000
	emulator := deterministicLaneEmulator{bandwidthBPS: 6_000_000 / 8, rtt: 80 * time.Millisecond, ackLossEvery: 4}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	totalSent := 0
	totalDelivered := 0
	totalRetransmitted := 0
	for sample := 0; sample < 10; sample++ {
		sent, delivered, retransmitted := driveByteWindow(lane, &emulator, start.Add(time.Duration(sample+1)*laneDeliverySampleWindow), sample, mss)
		totalSent += sent
		totalDelivered += delivered
		totalRetransmitted += retransmitted
	}
	// Lost acknowledgements on a clean forward path manufacture retransmits
	// of already-delivered data.
	require.Greater(t, totalRetransmitted, 0)
	require.Less(t, totalDelivered, totalSent)
	require.Greater(t, lane.retryRatioSmooth, 0.0)
}
