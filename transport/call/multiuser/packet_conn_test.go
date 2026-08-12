package multiuser

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/stretchr/testify/require"
)

type inertPacketConn struct{}

func (inertPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, errors.New("not implemented")
}

func (inertPacketConn) WriteTo(payload []byte, _ net.Addr) (int, error) {
	return len(payload), nil
}

func (inertPacketConn) Close() error                       { return nil }
func (inertPacketConn) LocalAddr() net.Addr                { return testAddr("local") }
func (inertPacketConn) SetDeadline(time.Time) error        { return nil }
func (inertPacketConn) SetReadDeadline(time.Time) error    { return nil }
func (inertPacketConn) SetWriteDeadline(time.Time) error   { return nil }

func TestPeerPacketConnAppliesDeadlineChangedAfterReadStarted(t *testing.T) {
	t.Parallel()
	key, err := deriveWrapKey("outer-secret")
	require.NoError(t, err)
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	peer := newPeerPacketConn(
		inertPacketConn{},
		testAddr("remote"),
		codec,
		telemetry.NewAccumulator(),
		16,
	)
	t.Cleanup(func() { _ = peer.Close() })

	result := make(chan error, 1)
	go func() {
		_, _, readErr := peer.ReadFrom(make([]byte, 1500))
		result <- readErr
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(20*time.Millisecond)))

	select {
	case readErr := <-result:
		require.Error(t, readErr)
		timeout, ok := readErr.(net.Error)
		require.True(t, ok)
		require.True(t, timeout.Timeout())
	case <-time.After(time.Second):
		t.Fatal("read did not observe a deadline installed after it blocked")
	}
}

func TestPeerPacketConnIgnoresSupersededDeadlineTimer(t *testing.T) {
	t.Parallel()
	key, err := deriveWrapKey("outer-secret")
	require.NoError(t, err)
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	peer := newPeerPacketConn(
		inertPacketConn{},
		testAddr("remote"),
		codec,
		telemetry.NewAccumulator(),
		16,
	)
	t.Cleanup(func() { _ = peer.Close() })

	type readResult struct {
		payload string
		err     error
	}
	result := make(chan readResult, 1)
	go func() {
		buffer := make([]byte, 64)
		n, _, readErr := peer.ReadFrom(buffer)
		result <- readResult{payload: string(buffer[:n]), err: readErr}
	}()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(30*time.Millisecond)))
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	time.Sleep(50 * time.Millisecond)
	require.True(t, peer.enqueue([]byte("still-open"), testAddr("remote")))

	select {
	case read := <-result:
		require.NoError(t, read.err)
		require.Equal(t, "still-open", read.payload)
	case <-time.After(time.Second):
		t.Fatal("read did not survive a superseded deadline")
	}
}

func TestDispatchSegmentCountsOneDropWhenEveryWorkerQueueIsFull(t *testing.T) {
	t.Parallel()
	tunnel, err := NewPooledTunnel(0x10203040, 2, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	tunnel.SetTelemetryCollectionActive(true)

	workers := make([]*pooledWorker, 0, 2)
	for id := uint16(0); id < 2; id++ {
		connection, peer := newTestDatagramPair()
		t.Cleanup(func() { _ = peer.Close() })
		worker := &pooledWorker{
			id:        id,
			conn:      connection,
			parent:    tunnel,
			metrics:   tunnel.telemetryWorker(id),
			sendQueue: make(chan queuedSegment, 1),
			controlQueue: make(chan queuedSegment, 1),
			done:      make(chan struct{}),
		}
		worker.sendQueue <- queuedSegment{payload: []byte("already-full")}
		worker.controlQueue <- queuedSegment{payload: []byte("already-full")}
		workers = append(workers, worker)
	}
	tunnel.workersMu.Lock()
	for _, worker := range workers {
		tunnel.workers[worker.id] = worker
		tunnel.workerIDs = append(tunnel.workerIDs, worker.id)
	}
	tunnel.workersMu.Unlock()

	tunnel.dispatchSegment([]byte("dropped"))

	require.Equal(t, float64(1), tunnel.metrics.Value(telemetry.WorkerSendQueueDropsTotal))
	require.Equal(t, float64(1), tunnel.metrics.Value(telemetry.WorkerNoAvailableDropsTotal))
}
