package xhttp

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	"golang.org/x/net/http2"
)

func TestWrapH2ErrorNormalisesStreamError(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{"cancel", http2.StreamError{StreamID: 7, Code: http2.ErrCodeCancel}},
		{"internal", http2.StreamError{StreamID: 7, Code: http2.ErrCodeInternal}},
		{"refused", http2.StreamError{StreamID: 7, Code: http2.ErrCodeRefusedStream}},
		{"caused", E.Cause(http2.StreamError{StreamID: 7, Code: http2.ErrCodeInternal}, "read stream")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			wrapped := wrapH2Error(testCase.err)
			var streamError http2.StreamError
			if errors.As(wrapped, &streamError) {
				t.Fatalf("wrapH2Error(%v) = %v, still an http2.StreamError", testCase.err, wrapped)
			}
			if !errors.Is(wrapped, net.ErrClosed) {
				t.Fatalf("wrapH2Error(%v) = %v, want net.ErrClosed", testCase.err, wrapped)
			}
		})
	}
}

func TestWrapH2ErrorKeepsTheCauseReadable(t *testing.T) {
	wrapped := wrapH2Error(http2.StreamError{StreamID: 7, Code: http2.ErrCodeInternal})
	if !strings.Contains(wrapped.Error(), "INTERNAL_ERROR") {
		t.Fatalf("wrapH2Error() = %q, want the stream error code in the message", wrapped)
	}
}

func TestWrapH2ErrorPassesEverythingElseThrough(t *testing.T) {
	if wrapH2Error(nil) != nil {
		t.Fatal("wrapH2Error(nil) is not nil")
	}
	if err := wrapH2Error(http2.StreamError{StreamID: 7, Code: http2.ErrCodeNo}); !errors.Is(err, io.EOF) {
		t.Fatalf("wrapH2Error(NO_ERROR) = %v, want io.EOF", err)
	}
	refused := errors.New("dial tcp: connection refused")
	if err := wrapH2Error(refused); !errors.Is(err, refused) {
		t.Fatalf("wrapH2Error(%v) = %v, want the error itself", refused, err)
	}
}

func TestSplitConnReadDoesNotLeakStreamError(t *testing.T) {
	conn := &splitConn{reader: &streamErrorReader{
		err: http2.StreamError{StreamID: 7, Code: http2.ErrCodeInternal},
	}}
	_, err := conn.Read(make([]byte, 1))
	var streamError http2.StreamError
	if errors.As(err, &streamError) {
		t.Fatalf("splitConn.Read() = %v, still an http2.StreamError", err)
	}
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("splitConn.Read() = %v, want net.ErrClosed", err)
	}
}

type streamErrorReader struct {
	err error
}

func (r *streamErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func (r *streamErrorReader) Close() error {
	return nil
}

// The fault the normalisation exists for. An HTTP/2 client above this transport,
// reading a conn whose error is an http2.StreamError, never leaves its read loop:
// x/net/http2 takes the stream id for one of its own, finds no such stream on its
// connection, and reads the next frame off an error that costs no I/O. That spin
// holds a full core and outlives every Close, because the loop never touches the
// socket again. Normalised, the loop ends and the connection is closed.
func TestNormalisedErrorLetsAnHTTP2ReadLoopEnd(t *testing.T) {
	conn := &countingConn{
		err:    wrapH2Error(http2.StreamError{StreamID: 7, Code: http2.ErrCodeInternal}),
		closed: make(chan struct{}),
	}
	if _, err := (&http2.Transport{AllowHTTP: true}).NewClientConn(conn); err != nil {
		t.Fatal(err)
	}

	select {
	case <-conn.closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("read loop still running after %d reads", conn.reads.Load())
	}
	if reads := conn.reads.Load(); reads > 16 {
		t.Fatalf("read loop read %d times before ending, want a handful", reads)
	}
}

type countingConn struct {
	err       error
	reads     atomic.Int64
	closeOnce sync.Once
	closed    chan struct{}
}

func (c *countingConn) Read([]byte) (int, error) {
	c.reads.Add(1)
	return 0, c.err
}

func (c *countingConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *countingConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *countingConn) LocalAddr() net.Addr {
	return stubAddr{}
}

func (c *countingConn) RemoteAddr() net.Addr {
	return stubAddr{}
}

func (c *countingConn) SetDeadline(time.Time) error {
	return nil
}

func (c *countingConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *countingConn) SetWriteDeadline(time.Time) error {
	return nil
}

type stubAddr struct{}

func (stubAddr) Network() string {
	return "tcp"
}

func (stubAddr) String() string {
	return "192.0.2.1:443"
}
