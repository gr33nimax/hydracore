package multiuser

import (
	"bytes"
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

func (c *testDatagramConn) LocalAddr() net.Addr                { return testAddr("local") }
func (c *testDatagramConn) RemoteAddr() net.Addr               { return testAddr("remote") }
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
