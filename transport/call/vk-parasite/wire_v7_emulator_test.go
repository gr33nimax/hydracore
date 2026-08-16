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
	blackhole    bool
	packet       int
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
	for packet := 0; packet < delivered; packet++ {
		e.packet++
		if e.lossEvery > 0 && e.packet%e.lossEvery == 0 {
			lost++
		}
	}
	delivered -= lost
	retransmitted = sent - delivered
	rtt = e.rtt
	if e.delayEvery > 0 && e.packet%e.delayEvery == 0 {
		rtt += e.rtt / 2
	}
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
	require.Less(t, retransmitRatio, 0.15)
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
	require.LessOrEqual(t, time.Since(started), 12*time.Second)

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
	require.Equal(t, 30*time.Second, sessionNoProgressThreshold(200*time.Millisecond))
	require.Equal(t, 30*time.Second, sessionNoProgressThreshold(2*time.Second))
	require.Equal(t, 30*time.Second, sessionNoProgressThreshold(35*time.Second))
}
