package vkparasite

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

type testDatagramConn struct {
	incoming chan []byte
	peer     *testDatagramConn
	closed   chan struct{}
	once     sync.Once
	writes   atomic.Int32
}

func newTestDatagramPair() (*testDatagramConn, *testDatagramConn) {
	left := &testDatagramConn{incoming: make(chan []byte, 256), closed: make(chan struct{})}
	right := &testDatagramConn{incoming: make(chan []byte, 256), closed: make(chan struct{})}
	left.peer = right
	right.peer = left
	return left, right
}

func (c *testDatagramConn) Read(buffer []byte) (int, error) {
	select {
	case payload := <-c.incoming:
		return copy(buffer, payload), nil
	case <-c.closed:
		return 0, io.EOF
	}
}

func (c *testDatagramConn) Write(payload []byte) (int, error) {
	copyPayload := append([]byte(nil), payload...)
	select {
	case c.peer.incoming <- copyPayload:
		c.writes.Add(1)
		return len(payload), nil
	case <-c.peer.closed:
		return 0, io.ErrClosedPipe
	}
}

func (c *testDatagramConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *testDatagramConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *testDatagramConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *testDatagramConn) SetDeadline(time.Time) error      { return nil }
func (c *testDatagramConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testDatagramConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func connectTestLanes(t *testing.T, left, right *ParasiteTunnel) {
	t.Helper()
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		leftConn, rightConn := newTestDatagramPair()
		_, err := left.AddWorker(laneID, leftConn)
		require.NoError(t, err)
		_, err = right.AddWorker(laneID, rightConn)
		require.NoError(t, err)
	}
}

func TestLaneConversationsAreStableUniqueAndNonzero(t *testing.T) {
	t.Parallel()
	seen := make(map[uint32]struct{}, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		conversation := laneConversation(0x11223344, laneID)
		require.NotZero(t, conversation)
		require.Equal(t, conversation, laneConversation(0x11223344, laneID))
		_, duplicate := seen[conversation]
		require.False(t, duplicate)
		seen[conversation] = struct{}{}
	}
}

func TestParasiteTunnelUsesFourIndependentLanes(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x11223344, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x11223344, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	received := make(chan uint32, 8)
	server.SetOnData(func(frame []byte) {
		_, _, ok := relayFrameIdentity(frame)
		require.True(t, ok)
		received <- binary.BigEndian.Uint32(frame[4:8])
	})
	for connID := uint32(1); connID <= 8; connID++ {
		client.SendData(calltunnel.EncodeFrame(connID, calltunnel.MsgData, []byte{byte(connID)}))
	}
	for count := 0; count < 8; count++ {
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for lane payload")
		}
	}
	used := make(map[uint16]struct{}, LaneCount)
	client.sendMu.Lock()
	for _, flow := range client.sendFlows {
		for laneID := uint16(0); laneID < LaneCount; laneID++ {
			if flow.laneMask&(1<<laneID) != 0 {
				used[laneID] = struct{}{}
			}
		}
	}
	client.sendMu.Unlock()
	require.Len(t, used, LaneCount)
}

func TestParasiteTunnelPinsAFlowAndPreservesOrder(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x22334455, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x22334455, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	received := make(chan byte, 64)
	server.SetOnData(func(frame []byte) { received <- frame[9] })
	for sequence := byte(0); sequence < 64; sequence++ {
		client.SendData(calltunnel.EncodeFrame(99, calltunnel.MsgData, []byte{sequence}))
	}
	for expected := byte(0); expected < 64; expected++ {
		select {
		case actual := <-received:
			require.Equal(t, expected, actual)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for ordered flow payload")
		}
	}
	client.sendMu.Lock()
	require.Len(t, client.sendFlows, 1)
	require.Equal(t, uint64(64), client.sendFlows[99].nextSequence)
	require.Equal(t, uint8(0x0f), client.sendFlows[99].laneMask)
	client.sendMu.Unlock()
}

func TestParasiteTunnelHigherEpochImmediatelyReplacesLane(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x33445566, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	first, firstPeer := newTestDatagramPair()
	second, secondPeer := newTestDatagramPair()
	firstDone, err := tunnel.AddWorkerEpoch(0, 7, first)
	require.NoError(t, err)
	secondDone, err := tunnel.AddWorkerEpoch(0, 8, second)
	require.NoError(t, err)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("older lane transport was not replaced")
	}
	select {
	case <-secondDone:
		t.Fatal("newer lane transport was closed")
	default:
	}
	_, err = tunnel.AddWorkerEpoch(0, 7, firstPeer)
	require.ErrorContains(t, err, "stale lane epoch")
	_ = firstPeer.Close()
	_ = secondPeer.Close()
}

func TestParasiteTunnelCarriesTelemetryControl(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x44556677, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x44556677, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	leaseReceived := make(chan time.Duration, 1)
	client.SetTelemetryControlHandler(func(lease time.Duration) { leaseReceived <- lease })
	require.True(t, server.RequestClientTelemetry(6*time.Second))
	select {
	case lease := <-leaseReceived:
		require.Equal(t, 6*time.Second, lease)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for telemetry lease")
	}

	recordReceived := make(chan []byte, 1)
	server.SetTelemetryClientRecordHandler(func(record []byte) { recordReceived <- record })
	record := telemetry.Snapshot("client", "", "", telemetry.NewAccumulator().Snapshot(telemetry.ClientRequired))
	payload, err := telemetry.Marshal(record)
	require.NoError(t, err)
	require.True(t, client.SendClientTelemetry(payload))
	select {
	case received := <-recordReceived:
		_, err = telemetry.DecodeClientRecord(received)
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for telemetry record")
	}
}

func TestServerRequiresExactlyFourLanes(t *testing.T) {
	t.Parallel()
	base := ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}
	normalized, _, err := validateServerOptions(base)
	require.NoError(t, err)
	require.Equal(t, LaneCount, normalized.MaxWorkersPerSession)
	base.MaxWorkersPerSession = LaneCount - 1
	_, _, err = validateServerOptions(base)
	require.ErrorContains(t, err, "must be four")

	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	request := authRequest{SessionID: [16]byte{1}, Conv: 1, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	session, created, err := server.getOrCreateSession(request)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(session)
}
