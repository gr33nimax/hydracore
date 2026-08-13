package multiuser

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/stretchr/testify/require"
)

func TestMultipathProfilesPreserveLegacyAndBoundAdaptiveDefaults(t *testing.T) {
	t.Parallel()
	legacy, err := multipathConfigFor("")
	require.NoError(t, err)
	require.Equal(t, MultipathProfileLegacy, legacy.profile)
	require.Equal(t, 2, legacy.fastResend)
	require.Equal(t, 1, legacy.noCongestionWindow, "legacy KCP behavior must remain unchanged")
	require.Zero(t, legacy.chunkPackets)
	require.Zero(t, legacy.chunkDwell)

	adaptive, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	require.Equal(t, 4, adaptive.fastResend)
	require.Equal(t, 1, adaptive.noCongestionWindow, "one KCP cwnd must not throttle four independent TURN paths")
	require.Positive(t, adaptive.chunkPackets)
	require.Positive(t, adaptive.chunkDwell)

	_, err = multipathConfigFor("raw")
	require.ErrorContains(t, err, "legacy or adaptive")
}

func TestAdaptiveMultipathKeepsChunksTogetherAndMovesRetransmissions(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	workers := []*pooledWorker{
		newSchedulerTestWorker(0),
		newSchedulerTestWorker(1),
	}
	for _, worker := range workers {
		scheduler.registerWorker(worker)
		worker.metrics.SetCollectionActive(true)
	}

	firstPacket := testKCPPushPacket(10, 100)
	first := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), firstPacket, time.Now())[0]
	require.Equal(t, uint16(0), first.id)
	scheduler.assignOutput(firstPacket, first, time.Now())
	require.Equal(t, float64(1), first.metrics.Value(telemetry.WorkerPathAttemptSegmentsTotal))

	secondPacket := testKCPPushPacket(11, 100)
	second := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), secondPacket, time.Now())[0]
	require.Equal(t, first.id, second.id, "a bounded data chunk must stay on one TURN path")
	scheduler.assignOutput(secondPacket, second, time.Now())
	require.Equal(t, float64(2), first.metrics.Value(telemetry.WorkerPathAttemptSegmentsTotal))

	retransmit := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), firstPacket, time.Now())[0]
	require.NotEqual(t, first.id, retransmit.id, "a retransmission should avoid the path that lost the first copy")
	scheduler.assignOutput(firstPacket, retransmit, time.Now())
	require.Equal(t, float64(1), workers[0].metrics.Value(telemetry.WorkerPathRetransSegmentsTotal))
	require.Equal(t, float64(1), retransmit.metrics.Value(telemetry.WorkerPathAttemptSegmentsTotal))
	require.Equal(t, float64(1), workers[1].metrics.Value(telemetry.WorkerPathSwitchesTotal))

	secondRetransmit := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), firstPacket, time.Now())[0]
	require.Equal(t, first.id, secondRetransmit.id, "a second retry must leave the path that lost the first retry")
	scheduler.assignOutput(firstPacket, secondRetransmit, time.Now())
	require.Equal(t, float64(1), retransmit.metrics.Value(telemetry.WorkerPathRetransSegmentsTotal))
	require.Equal(t, float64(3), first.metrics.Value(telemetry.WorkerPathAttemptSegmentsTotal))
}

func TestAdaptivePathRTTStartsAtSocketWrite(t *testing.T) {
	t.Parallel()
	config, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	scheduler := newMultipathScheduler(config)
	worker := newSchedulerTestWorker(0)
	worker.metrics.SetCollectionActive(true)
	scheduler.registerWorker(worker)

	packet := testKCPPushPacket(42, 100)
	queuedAt := time.Unix(100, 0)
	writtenAt := queuedAt.Add(500 * time.Millisecond)
	scheduler.assignOutput(packet, worker, queuedAt)
	require.True(t, scheduler.sent[42].sentAt.IsZero())
	scheduler.commitWrite(packet, worker, writtenAt)
	scheduler.observeInput(testKCPAckPacket(42), writtenAt.Add(50*time.Millisecond))
	require.True(t, scheduler.consumeControlFrame(
		worker,
		encodeMultipathFeedback(worker.id, 1, 1),
		writtenAt.Add(50*time.Millisecond),
	))
	scheduler.publishWorkerMetrics(worker)

	require.InDelta(t, 50, worker.metrics.Value(telemetry.WorkerPathRTTMS), 0.001)
	require.Zero(t, worker.metrics.Value(telemetry.WorkerPacingRateBPS))
	require.Zero(t, worker.metrics.Value(telemetry.WorkerPacingPacketsTotal))
}

func newSchedulerTestWorker(id uint16) *pooledWorker {
	return &pooledWorker{
		id:           id,
		metrics:      telemetry.NewAccumulator(),
		sendQueue:    make(chan queuedSegment, 4),
		controlQueue: make(chan queuedSegment, 4),
		done:         make(chan struct{}),
	}
}

func testKCPPushPacket(sequence uint32, payloadSize int) []byte {
	return testKCPPushPacketWithUNA(sequence, 0, payloadSize)
}

func testKCPPushPacketWithUNA(sequence, una uint32, payloadSize int) []byte {
	packet := make([]byte, kcpHeaderSize+payloadSize)
	packet[4] = kcpCommandPush
	binary.LittleEndian.PutUint32(packet[12:16], sequence)
	binary.LittleEndian.PutUint32(packet[16:20], una)
	binary.LittleEndian.PutUint32(packet[20:24], uint32(payloadSize))
	return packet
}

func testKCPAckPacket(sequence uint32) []byte {
	packet := make([]byte, kcpHeaderSize)
	packet[4] = kcpCommandACK
	binary.LittleEndian.PutUint32(packet[12:16], sequence)
	return packet
}
