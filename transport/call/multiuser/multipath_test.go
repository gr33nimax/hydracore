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
	require.Zero(t, legacy.initialRateBPS)

	adaptive, err := multipathConfigFor(MultipathProfileAdaptive)
	require.NoError(t, err)
	require.Equal(t, 4, adaptive.fastResend)
	require.Positive(t, adaptive.chunkPackets)
	require.Positive(t, adaptive.chunkDwell)
	require.Greater(t, adaptive.initialRateBPS, adaptive.minimumRateBPS)
	require.Greater(t, adaptive.maximumRateBPS, adaptive.initialRateBPS)

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
	scheduler.commitOutput(firstPacket, first, time.Now())

	secondPacket := testKCPPushPacket(11, 100)
	second := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), secondPacket, time.Now())[0]
	require.Equal(t, first.id, second.id, "a bounded data chunk must stay on one TURN path")
	scheduler.commitOutput(secondPacket, second, time.Now())

	retransmit := scheduler.rankWorkers(append([]*pooledWorker(nil), workers...), firstPacket, time.Now())[0]
	require.NotEqual(t, first.id, retransmit.id, "a retransmission should avoid the path that lost the first copy")
	scheduler.commitOutput(firstPacket, retransmit, time.Now())
	require.Less(t, workers[0].pacingRateBPS.Load(), uint64(adaptiveInitialRateBPS))
	require.Equal(t, float64(1), workers[0].metrics.Value(telemetry.WorkerPathRetransSegmentsTotal))
	require.Equal(t, float64(1), workers[1].metrics.Value(telemetry.WorkerPathSwitchesTotal))
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
	packet := make([]byte, kcpHeaderSize+payloadSize)
	packet[4] = kcpCommandPush
	binary.LittleEndian.PutUint32(packet[12:16], sequence)
	binary.LittleEndian.PutUint32(packet[20:24], uint32(payloadSize))
	return packet
}
