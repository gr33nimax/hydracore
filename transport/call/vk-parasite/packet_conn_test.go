package vkparasite

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
	peer := newPeerPacketConn(inertPacketConn{}, testAddr("remote"), codec, telemetry.NewAccumulator(), 16)
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
		t.Fatal("read did not observe a newly installed deadline")
	}
}

func TestPeerPacketConnIgnoresSupersededDeadlineTimer(t *testing.T) {
	t.Parallel()
	key, err := deriveWrapKey("outer-secret")
	require.NoError(t, err)
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	peer := newPeerPacketConn(inertPacketConn{}, testAddr("remote"), codec, telemetry.NewAccumulator(), 16)
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

func TestLaneCountsOneDropWhenItsOutputQueueIsFull(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x10203040, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	tunnel.SetTelemetryCollectionActive(true)

	connection, peer := newTestDatagramPair()
	t.Cleanup(func() { _ = peer.Close() })
	lane := tunnel.lanes[0]
	worker := &laneWorker{
		id:        0,
		conn:      connection,
		lane:      lane,
		parent:    tunnel,
		metrics:   lane.metrics,
		sendQueue: make(chan queuedSegment, 1),
		done:      make(chan struct{}),
	}
	worker.sendQueue <- queuedSegment{payload: []byte("already-full")}
	lane.worker = worker
	lane.dispatchSegment([]byte("dropped"))

	require.Equal(t, float64(1), lane.metrics.Value(telemetry.WorkerSendQueueDropsTotal))
	require.Equal(t, float64(1), tunnel.metrics.Value(telemetry.WorkerNoAvailableDropsTotal))
}
