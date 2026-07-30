package xhttp

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/common/xray/signal/done"
)

type splitConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()

	closeOnce       sync.Once
	closeErr        error
	writerCloseOnce sync.Once
	readerCloseOnce sync.Once
	writerCloseErr  error
	readerCloseErr  error
	deadlineAccess  sync.Mutex
	readDeadline    time.Time
	writeDeadline   time.Time
	readGeneration  uint64
	writeGeneration uint64
	readTimer       *time.Timer
	writeTimer      *time.Timer
	readExpired     atomic.Bool
	writeExpired    atomic.Bool
}

func (c *splitConn) Write(b []byte) (int, error) {
	written, err := c.writer.Write(b)
	if err != nil && c.writeExpired.Load() {
		return written, os.ErrDeadlineExceeded
	}
	return written, err
}

func (c *splitConn) Read(b []byte) (int, error) {
	read, err := c.reader.Read(b)
	if err != nil && c.readExpired.Load() {
		return read, os.ErrDeadlineExceeded
	}
	return read, err
}

func (c *splitConn) Close() error {
	c.closeOnce.Do(func() {
		c.stopDeadlineTimers()
		writerErr := c.closeWriter()
		if c.onClose != nil {
			c.onClose()
		}
		readerErr := c.closeReader()
		c.closeErr = errors.Join(writerErr, readerErr)
	})
	return c.closeErr
}

func (c *splitConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *splitConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *splitConn) SetDeadline(deadline time.Time) error {
	c.setReadDeadline(deadline)
	c.setWriteDeadline(deadline)
	return nil
}

func (c *splitConn) SetReadDeadline(deadline time.Time) error {
	c.setReadDeadline(deadline)
	return nil
}

func (c *splitConn) SetWriteDeadline(deadline time.Time) error {
	c.setWriteDeadline(deadline)
	return nil
}

// NeedAdditionalReadDeadline tells the adapter that this split stream has its
// own deadline enforcement and should not rely on an underlying socket.
func (c *splitConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *splitConn) setReadDeadline(deadline time.Time) {
	c.deadlineAccess.Lock()
	if c.readTimer != nil {
		c.readTimer.Stop()
		c.readTimer = nil
	}
	c.readDeadline = deadline
	c.readGeneration++
	generation := c.readGeneration
	c.readExpired.Store(false)
	if !deadline.IsZero() {
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		c.readTimer = time.AfterFunc(delay, func() { c.expireRead(deadline, generation) })
	}
	c.deadlineAccess.Unlock()
}

func (c *splitConn) setWriteDeadline(deadline time.Time) {
	c.deadlineAccess.Lock()
	if c.writeTimer != nil {
		c.writeTimer.Stop()
		c.writeTimer = nil
	}
	c.writeDeadline = deadline
	c.writeGeneration++
	generation := c.writeGeneration
	c.writeExpired.Store(false)
	if !deadline.IsZero() {
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		c.writeTimer = time.AfterFunc(delay, func() { c.expireWrite(deadline, generation) })
	}
	c.deadlineAccess.Unlock()
}

func (c *splitConn) expireRead(deadline time.Time, generation uint64) {
	c.deadlineAccess.Lock()
	if c.readGeneration != generation || c.readDeadline != deadline || deadline.IsZero() {
		c.deadlineAccess.Unlock()
		return
	}
	c.readTimer = nil
	c.readExpired.Store(true)
	c.deadlineAccess.Unlock()
	_ = c.closeReader()
}

func (c *splitConn) expireWrite(deadline time.Time, generation uint64) {
	c.deadlineAccess.Lock()
	if c.writeGeneration != generation || c.writeDeadline != deadline || deadline.IsZero() {
		c.deadlineAccess.Unlock()
		return
	}
	c.writeTimer = nil
	c.writeExpired.Store(true)
	c.deadlineAccess.Unlock()
	_ = c.closeWriter()
}

func (c *splitConn) stopDeadlineTimers() {
	c.deadlineAccess.Lock()
	if c.readTimer != nil {
		c.readTimer.Stop()
		c.readTimer = nil
	}
	if c.writeTimer != nil {
		c.writeTimer.Stop()
		c.writeTimer = nil
	}
	c.readDeadline = time.Time{}
	c.writeDeadline = time.Time{}
	c.deadlineAccess.Unlock()
}

func (c *splitConn) closeWriter() error {
	c.writerCloseOnce.Do(func() {
		if c.writer != nil {
			c.writerCloseErr = c.writer.Close()
		}
	})
	return c.writerCloseErr
}

func (c *splitConn) closeReader() error {
	c.readerCloseOnce.Do(func() {
		if c.reader != nil {
			c.readerCloseErr = c.reader.Close()
		}
	})
	return c.readerCloseErr
}

type H1Conn struct {
	UnreadedResponsesCount int
	RespBufReader          *bufio.Reader
	net.Conn
}

func NewH1Conn(conn net.Conn) *H1Conn {
	return &H1Conn{
		RespBufReader: bufio.NewReader(conn),
		Conn:          conn,
	}
}

type httpServerConn struct {
	sync.Mutex
	*done.Instance
	io.Reader // no need to Close request.Body
	http.ResponseWriter
}

func (c *httpServerConn) Write(b []byte) (int, error) {
	c.Lock()
	defer c.Unlock()
	if c.Done() {
		return 0, io.ErrClosedPipe
	}
	n, err := c.ResponseWriter.Write(b)
	if err == nil {
		c.ResponseWriter.(http.Flusher).Flush()
	}
	return n, err
}

func (c *httpServerConn) Close() error {
	c.Lock()
	defer c.Unlock()
	return c.Instance.Close()
}
