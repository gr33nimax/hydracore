package multiuser

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestPooledTunnelDistributesOneKCPConversationAcrossWorkers(t *testing.T) {
	t.Parallel()
	client, err := NewPooledTunnel(0x11223344, 2, logger.NOP())
	require.NoError(t, err)
	server, err := NewPooledTunnel(0x11223344, 2, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	left0, right0 := newTestDatagramPair()
	left1, right1 := newTestDatagramPair()
	_, err = client.AddWorker(0, left0)
	require.NoError(t, err)
	_, err = server.AddWorker(0, right0)
	require.NoError(t, err)
	_, err = client.AddWorker(1, left1)
	require.NoError(t, err)
	_, err = server.AddWorker(1, right1)
	require.NoError(t, err)

	received := make(chan []byte, 1)
	server.SetOnData(func(payload []byte) { received <- append([]byte(nil), payload...) })
	payload := bytes.Repeat([]byte("pooled-kcp-"), 2048)
	client.SendData(payload)
	select {
	case actual := <-received:
		require.Equal(t, payload, actual)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pooled KCP payload")
	}
	require.Positive(t, left0.writes.Load())
	require.Positive(t, left1.writes.Load())
}

func TestPooledTunnelHeartbeatKeepsIdleWorkersAliveWithoutApplicationData(t *testing.T) {
	t.Parallel()
	client, err := NewPooledTunnel(0x22334455, 1, logger.NOP())
	require.NoError(t, err)
	server, err := NewPooledTunnel(0x22334455, 1, logger.NOP())
	require.NoError(t, err)
	client.heartbeatInterval = 10 * time.Millisecond
	client.livenessTimeout = 250 * time.Millisecond
	server.heartbeatInterval = 10 * time.Millisecond
	server.livenessTimeout = 250 * time.Millisecond
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	var delivered atomic.Int32
	client.SetOnData(func([]byte) { delivered.Add(1) })
	server.SetOnData(func([]byte) { delivered.Add(1) })
	left, right := newTestDatagramPair()
	clientDone, err := client.AddWorker(0, left)
	require.NoError(t, err)
	serverDone, err := server.AddWorker(0, right)
	require.NoError(t, err)

	timer := time.NewTimer(600 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-clientDone:
		t.Fatal("client worker expired despite bidirectional heartbeats")
	case <-serverDone:
		t.Fatal("server worker expired despite bidirectional heartbeats")
	case <-timer.C:
	}
	require.Equal(t, 1, client.ActiveWorkers())
	require.Equal(t, 1, server.ActiveWorkers())
	require.Zero(t, delivered.Load(), "heartbeat records must not enter the KCP application stream")
	require.Positive(t, left.writes.Load())
	require.Positive(t, right.writes.Load())
}

func TestPooledTunnelWatchdogRemovesSilentWorker(t *testing.T) {
	t.Parallel()
	tunnel, err := NewPooledTunnel(0x33445566, 1, logger.NOP())
	require.NoError(t, err)
	tunnel.heartbeatInterval = 10 * time.Millisecond
	tunnel.livenessTimeout = 50 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })

	left, right := newTestDatagramPair()
	t.Cleanup(func() { _ = right.Close() })
	done, err := tunnel.AddWorker(0, left)
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("silent worker was not removed by the liveness watchdog")
	}
	require.Zero(t, tunnel.ActiveWorkers())
}

func TestServerLatestSessionTakeoverWaitsForPendingAttach(t *testing.T) {
	t.Parallel()
	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "user-secret"}},
		MaxSessions:  1,
		SessionHandler: func(SessionInfo, *PooledTunnel) error {
			return nil
		},
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	firstRequest := authRequest{
		SessionID:   [16]byte{1},
		Conv:        0x77889900,
		WorkerID:    0,
		WorkerTotal: 1,
		User:        "alice",
		Password:    "user-secret",
	}
	first, created, err := server.getOrCreateSession(firstRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.sessionsMu.Lock()
	first.createdAt = time.Now().Add(-sessionTakeoverGrace - time.Second)
	server.sessionsMu.Unlock()

	secondRequest := firstRequest
	secondRequest.SessionID = [16]byte{2}
	secondRequest.Conv++
	_, _, err = server.getOrCreateSession(secondRequest)
	require.ErrorContains(t, err, "user session limit reached")

	server.releaseSessionAttach(first)
	second, created, err := server.getOrCreateSession(secondRequest)
	require.NoError(t, err)
	require.True(t, created)
	require.NotSame(t, first, second)
	server.releaseSessionAttach(second)

	left, right := newTestDatagramPair()
	t.Cleanup(func() { _ = right.Close() })
	_, err = first.tunnel.AddWorker(0, left)
	require.ErrorContains(t, err, "session already closed")
	server.sessionsMu.Lock()
	require.Len(t, server.sessions, 1)
	require.Equal(t, 1, server.userSessions["alice"])
	server.sessionsMu.Unlock()
}

func TestServerOptionsRejectDuplicateUsersAndHardCaps(t *testing.T) {
	t.Parallel()
	base := ServerOptions{
		ObfsPassword: "outer-secret",
		Users: []ServerUser{
			{Name: "alice", Password: "first"},
			{Name: "alice", Password: "second"},
		},
		SessionHandler: func(SessionInfo, *PooledTunnel) error { return nil },
	}
	_, _, err := validateServerOptions(base)
	require.Error(t, err)
	base.Users = []ServerUser{{Name: "alice", Password: "first"}}
	base.MaxWorkersPerSession = HardMaxWorkers + 1
	_, _, err = validateServerOptions(base)
	require.Error(t, err)
}
