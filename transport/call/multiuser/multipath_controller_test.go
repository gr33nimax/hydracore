package multiuser

import (
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
