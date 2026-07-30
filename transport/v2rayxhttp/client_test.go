package xhttp

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/xray/buf"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/stretchr/testify/require"
)

func TestXmuxPoolIsHardBounded(t *testing.T) {
	var created atomic.Int32
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{
		MaxConcurrency: optionRange(1),
	}, func() XmuxConn {
		created.Add(1)
		return &testXmuxConn{}
	})

	for range hardXmuxPoolLimit * 4 {
		client := manager.GetXmuxClient(t.Context())
		client.AddOpenUsage(1)
	}
	require.Equal(t, int32(hardXmuxPoolLimit), created.Load())
	require.Len(t, manager.xmuxClients, hardXmuxPoolLimit)
	manager.Close()
}

func TestXmuxConfiguredConnectionsAreClamped(t *testing.T) {
	var created atomic.Int32
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{
		MaxConnections: optionRange(100),
	}, func() XmuxConn {
		created.Add(1)
		return &testXmuxConn{}
	})
	for range hardXmuxPoolLimit * 2 {
		manager.GetXmuxClient(t.Context())
	}
	require.LessOrEqual(t, created.Load(), int32(hardXmuxPoolLimit))
	manager.Close()
}

func TestXmuxCloseForcesPhysicalTransportsClosed(t *testing.T) {
	var closed atomic.Int32
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{
		MaxConnections: optionRange(4),
	}, func() XmuxConn {
		return &testXmuxConn{closedCount: &closed}
	})
	for range 4 {
		client := manager.GetXmuxClient(t.Context())
		client.AddOpenUsage(1)
	}
	manager.Close()
	manager.Close()
	require.Equal(t, int32(4), closed.Load())
}

func TestXmuxCloseIsTerminal(t *testing.T) {
	var created atomic.Int32
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn {
		created.Add(1)
		return &testXmuxConn{}
	})
	require.NotNil(t, manager.GetXmuxClient(t.Context()))
	manager.Close()
	require.Nil(t, manager.GetXmuxClient(t.Context()))
	require.Equal(t, int32(1), created.Load())
}

func TestH2KeepAlivePeriodIsBounded(t *testing.T) {
	require.Equal(t, defaultH2ReadIdle, normalizedH2ReadIdle(0))
	require.Equal(t, time.Duration(0), normalizedH2ReadIdle(-1))
	require.Equal(t, minimumH2ReadIdle, normalizedH2ReadIdle(1))
	require.Equal(t, maximumH2ReadIdle, normalizedH2ReadIdle(1<<30))
	require.Equal(t, maximumH2ReadIdle, normalizedH2ReadIdle(math.MaxInt64))
}

func TestSplitConnCloseIsIdempotent(t *testing.T) {
	var callbacks atomic.Int32
	left, right := net.Pipe()
	conn := &splitConn{
		writer:  left,
		reader:  right,
		onClose: func() { callbacks.Add(1) },
	}
	require.NoError(t, conn.Close())
	require.NoError(t, conn.Close())
	require.Equal(t, int32(1), callbacks.Load())
}

func TestSplitConnCloseAllowsPartialConstruction(t *testing.T) {
	var callbacks atomic.Int32
	conn := &splitConn{onClose: func() { callbacks.Add(1) }}
	require.NoError(t, conn.Close())
	require.NoError(t, conn.Close())
	require.Equal(t, int32(1), callbacks.Load())
}

func TestDialContextOpenStreamFailureReleasesUsage(t *testing.T) {
	dialer := &failingDialerClient{}
	xmuxClient := &XmuxClient{XmuxConn: dialer}
	client := &Client{
		options: &option.V2RayXHTTPOptions{Mode: "stream-one"},
		getHTTPClient: func() (DialerClient, *XmuxClient) {
			return dialer, xmuxClient
		},
		getHTTPClient2: func() (DialerClient, *XmuxClient) {
			return dialer, xmuxClient
		},
	}
	_, err := client.DialContext(t.Context())
	require.ErrorIs(t, err, errOpenStream)
	require.Zero(t, xmuxClient.GetOpenUsage())
}

func TestClientCloseWinsConcurrentDial(t *testing.T) {
	dialer := &blockingDialerClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
		reader:  &trackingReadCloser{},
	}
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn {
		return dialer
	})
	var selected *XmuxClient
	getHTTPClient := func() (DialerClient, *XmuxClient) {
		xmuxClient := manager.GetXmuxClient(t.Context())
		if xmuxClient == nil {
			return nil, nil
		}
		selected = xmuxClient
		return xmuxClient.XmuxConn.(DialerClient), xmuxClient
	}
	client := &Client{
		options:        &option.V2RayXHTTPOptions{Mode: "stream-one"},
		getHTTPClient:  getHTTPClient,
		getHTTPClient2: getHTTPClient,
		xmuxManager:    manager,
	}

	result := make(chan error, 1)
	go func() {
		conn, err := client.DialContext(t.Context())
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()

	<-dialer.started
	require.NoError(t, client.Close())
	close(dialer.release)
	require.ErrorIs(t, <-result, net.ErrClosed)
	require.True(t, dialer.reader.closed.Load())
	require.NotNil(t, selected)
	require.Zero(t, selected.GetOpenUsage())
	require.True(t, dialer.closed.Load())

	_, err := client.DialContext(t.Context())
	require.ErrorIs(t, err, net.ErrClosed)
}

func optionRange(value int) badoption.Range[int] {
	return badoption.Range[int]{From: value, To: value}
}

type testXmuxConn struct {
	closed      atomic.Bool
	closedCount *atomic.Int32
}

func (c *testXmuxConn) Close() {
	if c.closed.Swap(true) {
		return
	}
	if c.closedCount != nil {
		c.closedCount.Add(1)
	}
}

func (c *testXmuxConn) IsClosed() bool {
	return c.closed.Load()
}

var _ XmuxConn = (*testXmuxConn)(nil)
var _ io.Closer = (*splitConn)(nil)

var errOpenStream = errors.New("open stream failed")

type failingDialerClient struct{}

func (*failingDialerClient) IsClosed() bool {
	return false
}

func (*failingDialerClient) Close() {}

func (*failingDialerClient) OpenStream(
	context.Context,
	string,
	string,
	io.Reader,
	bool,
) (io.ReadCloser, net.Addr, net.Addr, error) {
	return nil, nil, nil, errOpenStream
}

func (*failingDialerClient) PostPacket(
	context.Context,
	string,
	string,
	string,
	buf.MultiBuffer,
) error {
	return nil
}

var _ DialerClient = (*failingDialerClient)(nil)

type blockingDialerClient struct {
	started chan struct{}
	release chan struct{}
	reader  *trackingReadCloser
	closed  atomic.Bool
}

func (c *blockingDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *blockingDialerClient) Close() {
	c.closed.Store(true)
}

func (c *blockingDialerClient) OpenStream(
	context.Context,
	string,
	string,
	io.Reader,
	bool,
) (io.ReadCloser, net.Addr, net.Addr, error) {
	close(c.started)
	<-c.release
	return c.reader, nil, nil, nil
}

func (*blockingDialerClient) PostPacket(
	context.Context,
	string,
	string,
	string,
	buf.MultiBuffer,
) error {
	return nil
}

type trackingReadCloser struct {
	closed atomic.Bool
}

func (*trackingReadCloser) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *trackingReadCloser) Close() error {
	c.closed.Store(true)
	return nil
}

var _ DialerClient = (*blockingDialerClient)(nil)
