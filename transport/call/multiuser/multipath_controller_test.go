package multiuser

import (
	"math"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveSendWindowAggregatesLivePathWindows(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	workers := []*pooledWorker{
		newSchedulerTestWorker(0),
		newSchedulerTestWorker(1),
		newSchedulerTestWorker(2),
		newSchedulerTestWorker(3),
	}
	for _, worker := range workers {
		scheduler.registerWorker(worker)
	}
	require.Equal(t, 160, scheduler.sendWindow())
	require.Equal(t, 640, scheduler.pendingLimit())

	scheduler.removeWorker(workers[3])
	require.Equal(t, 120, scheduler.sendWindow())
	require.Equal(t, 480, scheduler.pendingLimit())
}

func TestAdaptivePathWindowBacksOffOnlyUnderSustainedRetryPressure(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	worker := newSchedulerTestWorker(0)
	worker.metrics.SetCollectionActive(true)
	scheduler.registerWorker(worker)
	packet := testKCPPushPacket(1, 100)
	started := time.Unix(100, 0)

	scheduler.assignOutput(packet, worker, started)
	scheduler.assignOutput(packet, worker, started.Add(10*time.Millisecond))
	scheduler.assignOutput(packet, worker, started.Add(20*time.Millisecond))
	require.Equal(t, adaptiveInitialPathWindow, scheduler.paths[worker.id].window)

	scheduler.assignOutput(packet, worker, started.Add(30*time.Millisecond))
	require.Equal(t, adaptiveInitialPathWindow*adaptiveWindowBackoffFactor, scheduler.paths[worker.id].window)
	require.Equal(t, float64(1), worker.metrics.Value(telemetry.WorkerPathBackoffTotal))

	scheduler.assignOutput(packet, worker, started.Add(40*time.Millisecond))
	require.Equal(t, adaptiveInitialPathWindow*adaptiveWindowBackoffFactor, scheduler.paths[worker.id].window)
	scheduler.assignOutput(packet, worker, started.Add(140*time.Millisecond))
	require.Less(t, scheduler.paths[worker.id].window, adaptiveInitialPathWindow*adaptiveWindowBackoffFactor)
	require.Equal(t, float64(2), worker.metrics.Value(telemetry.WorkerPathBackoffTotal))
}

func TestAdaptiveACKReleasesFlightAndGrowsHealthyPath(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	worker := newSchedulerTestWorker(0)
	worker.metrics.SetCollectionActive(true)
	scheduler.registerWorker(worker)
	started := time.Unix(100, 0)

	first := testKCPPushPacket(1, 100)
	scheduler.assignOutput(first, worker, started)
	scheduler.commitWrite(first, worker, started)
	require.Equal(t, 1, scheduler.paths[worker.id].inflight)
	scheduler.observeInput(testKCPAckPacket(1), started.Add(50*time.Millisecond))
	require.Zero(t, scheduler.paths[worker.id].inflight)
	require.Greater(t, scheduler.paths[worker.id].window, adaptiveInitialPathWindow)

	second := testKCPPushPacket(2, 100)
	scheduler.assignOutput(second, worker, started.Add(60*time.Millisecond))
	scheduler.commitWrite(second, worker, started.Add(60*time.Millisecond))
	scheduler.observeInput(testKCPAckPacket(2), started.Add(160*time.Millisecond))
	scheduler.publishWorkerMetrics(worker)
	require.Positive(t, worker.metrics.Value(telemetry.WorkerPathDeliveryRateBPS))
	require.Zero(t, worker.metrics.Value(telemetry.WorkerPathInflightSegments))
	require.Greater(t, worker.metrics.Value(telemetry.WorkerPathWindowSegments), adaptiveInitialPathWindow)
}

func TestAdaptiveCumulativeACKReleasesFlightWithoutExactACKs(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	worker := newSchedulerTestWorker(0)
	worker.metrics.SetCollectionActive(true)
	scheduler.registerWorker(worker)
	started := time.Unix(100, 0)

	for sequence := uint32(10); sequence <= 12; sequence++ {
		packet := testKCPPushPacket(sequence, 100)
		scheduler.assignOutput(packet, worker, started)
		scheduler.commitWrite(packet, worker, started)
	}
	require.Equal(t, 3, scheduler.paths[worker.id].inflight)

	// KCP applies UNA on every input segment before processing its command.
	// A peer PUSH with UNA=12 cumulatively acknowledges 10 and 11 even when
	// their individual ACK packets did not reach the scheduler observer.
	scheduler.observeInput(testKCPPushPacketWithUNA(99, 12, 1), started.Add(50*time.Millisecond))
	require.Equal(t, 1, scheduler.paths[worker.id].inflight)
	require.NotContains(t, scheduler.sent, uint32(10))
	require.NotContains(t, scheduler.sent, uint32(11))
	require.Contains(t, scheduler.sent, uint32(12))
	require.Equal(t, float64(2*(kcpHeaderSize+100)), worker.metrics.Value(telemetry.WorkerPathAckedBytesTotal))

	scheduler.observeInput(testKCPPushPacketWithUNA(100, 13, 1), started.Add(60*time.Millisecond))
	require.Zero(t, scheduler.paths[worker.id].inflight)
	require.Empty(t, scheduler.sent)
	require.True(t, kcpSequenceBefore(math.MaxUint32, 0), "sequence comparison must survive KCP wraparound")
	require.False(t, kcpSequenceBefore(0, math.MaxUint32))
}

func TestKCPTelemetryCumulativeACKPrunesSentTracking(t *testing.T) {
	t.Parallel()
	metrics := telemetry.NewAccumulator()
	metrics.SetCollectionActive(true)
	tunnel := &PooledTunnel{
		metrics: metrics,
		kcpSent: map[uint32]kcpSentSegment{
			10: {sentAt: time.Unix(100, 0)},
			11: {sentAt: time.Unix(100, 0)},
			12: {sentAt: time.Unix(100, 0)},
		},
	}

	tunnel.observeKCPInput(testKCPPushPacketWithUNA(99, 12, 1))
	require.NotContains(t, tunnel.kcpSent, uint32(10))
	require.NotContains(t, tunnel.kcpSent, uint32(11))
	require.Contains(t, tunnel.kcpSent, uint32(12))
}

func TestAdaptiveCumulativeACKAdvancesIncrementallyAndBoundsLargeJumps(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	worker := newSchedulerTestWorker(0)
	scheduler.registerWorker(worker)
	started := time.Unix(100, 0)

	for _, sequence := range []uint32{100, 101, 102, 1 << 20} {
		scheduler.assignOutput(testKCPPushPacket(sequence, 1), worker, started)
	}
	scheduler.observeInput(testKCPPushPacketWithUNA(1, 101, 1), started)
	require.NotContains(t, scheduler.sent, uint32(100))
	require.Contains(t, scheduler.sent, uint32(101))

	scheduler.observeInput(testKCPPushPacketWithUNA(2, 103, 1), started)
	require.NotContains(t, scheduler.sent, uint32(101))
	require.NotContains(t, scheduler.sent, uint32(102))

	// A corrupt or mid-session UNA jump must scan only the bounded tracked map,
	// not iterate across every missing sequence number.
	scheduler.observeInput(testKCPPushPacketWithUNA(3, (1<<20)+1, 1), started)
	require.Empty(t, scheduler.sent)
}

func TestAdaptiveChunkMovesWhenPreferredPathWindowIsFull(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	workers := []*pooledWorker{newSchedulerTestWorker(0), newSchedulerTestWorker(1)}
	for _, worker := range workers {
		scheduler.registerWorker(worker)
	}
	scheduler.paths[0].window = 1
	now := time.Unix(100, 0)
	first := testKCPPushPacket(1, 100)
	selected := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), first, now)[0]
	require.Equal(t, uint16(0), selected.id)
	scheduler.assignOutput(first, selected, now)

	second := testKCPPushPacket(2, 100)
	selected = scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), second, now.Add(time.Millisecond))[0]
	require.Equal(t, uint16(1), selected.id)
}

func TestAdaptiveQueueDelayBacksOffPathWindow(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	worker := newSchedulerTestWorker(0)
	worker.metrics.SetCollectionActive(true)
	scheduler.registerWorker(worker)

	scheduler.observeQueueDelay(worker, testKCPPushPacket(1, 100), adaptiveQueueBackoffDelay, time.Unix(100, 0))
	require.Equal(t, adaptiveInitialPathWindow*adaptiveWindowBackoffFactor, scheduler.paths[worker.id].window)
	require.Equal(t, float64(1), worker.metrics.Value(telemetry.WorkerPathBackoffTotal))
}
