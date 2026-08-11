package multiuser

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

var errPacketConnClosed = errors.New("call multi_user: packet connection closed")

type receivedPacket struct {
	payload []byte
	addr    net.Addr
}

// peerPacketConn gives one DTLS server a connected view of a shared UDP
// listener. The owner decodes the shared RTP wrapper before enqueueing data.
type peerPacketConn struct {
	base          net.PacketConn
	remote        net.Addr
	codec         *rtpCodec
	metrics       *telemetry.Accumulator
	readQueue     chan receivedPacket
	closed        chan struct{}
	closeOnce     sync.Once
	readDeadline  atomic.Int64
	writeDeadline atomic.Int64
}

func newPeerPacketConn(base net.PacketConn, remote net.Addr, codec *rtpCodec, metrics *telemetry.Accumulator) *peerPacketConn {
	return &peerPacketConn{
		base:      base,
		remote:    remote,
		codec:     codec,
		metrics:   metrics,
		readQueue: make(chan receivedPacket, 64),
		closed:    make(chan struct{}),
	}
}

func (c *peerPacketConn) enqueue(payload []byte, addr net.Addr) bool {
	copyPayload := append([]byte(nil), payload...)
	select {
	case c.readQueue <- receivedPacket{payload: copyPayload, addr: addr}:
		c.metrics.AddHotGauge(telemetry.PeerReadQueueDepth, 1)
		return true
	case <-c.closed:
		return false
	default:
		c.metrics.AddHot(telemetry.PeerReadQueueDropsTotal, 1)
		return false
	}
}

func (c *peerPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	timer, timeout := packetDeadline(c.readDeadline.Load())
	if timer != nil {
		defer timer.Stop()
	}
	select {
	case packet := <-c.readQueue:
		c.metrics.AddHotGauge(telemetry.PeerReadQueueDepth, -1)
		return copy(buffer, packet.payload), packet.addr, nil
	case <-c.closed:
		return 0, nil, errPacketConnClosed
	case <-timeout:
		return 0, nil, timeoutError{}
	}
}

func (c *peerPacketConn) WriteTo(payload []byte, _ net.Addr) (int, error) {
	if deadlineExpired(c.writeDeadline.Load()) {
		return 0, timeoutError{}
	}
	select {
	case <-c.closed:
		return 0, errPacketConnClosed
	default:
	}
	wire, err := c.codec.wrap(payload)
	if err != nil {
		c.metrics.AddHot(telemetry.OuterWrapFailuresTotal, 1)
		return 0, err
	}
	if _, err = c.base.WriteTo(wire, c.remote); err != nil {
		return 0, err
	}
	c.metrics.AddHot(telemetry.OuterPacketsOutTotal, 1)
	c.metrics.AddHot(telemetry.OuterBytesOutTotal, uint64(len(wire)))
	return len(payload), nil
}

func (c *peerPacketConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *peerPacketConn) LocalAddr() net.Addr { return c.base.LocalAddr() }

func (c *peerPacketConn) SetDeadline(deadline time.Time) error {
	nanos := deadlineNanos(deadline)
	c.readDeadline.Store(nanos)
	c.writeDeadline.Store(nanos)
	return nil
}

func (c *peerPacketConn) SetReadDeadline(deadline time.Time) error {
	c.readDeadline.Store(deadlineNanos(deadline))
	return nil
}

func (c *peerPacketConn) SetWriteDeadline(deadline time.Time) error {
	c.writeDeadline.Store(deadlineNanos(deadline))
	return nil
}

type obfsPacketConn struct {
	base     net.PacketConn
	remote   net.Addr
	codec    *rtpCodec
	metrics  *telemetry.Accumulator
	readLock sync.Mutex
	readBuf  []byte
}

func newObfsPacketConn(base net.PacketConn, remote net.Addr, codec *rtpCodec, metrics *telemetry.Accumulator) *obfsPacketConn {
	return &obfsPacketConn{base: base, remote: remote, codec: codec, metrics: metrics, readBuf: make([]byte, maximumWirePacket)}
}

func (c *obfsPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()
	for {
		n, addr, err := c.base.ReadFrom(c.readBuf)
		if err != nil {
			return 0, nil, err
		}
		if !samePacketAddress(addr, c.remote) {
			continue
		}
		plain, err := c.codec.unwrap(c.readBuf[:n])
		if err != nil {
			c.metrics.AddHot(telemetry.OuterAuthFailuresTotal, 1)
			continue
		}
		c.metrics.AddHot(telemetry.OuterPacketsInTotal, 1)
		c.metrics.AddHot(telemetry.OuterBytesInTotal, uint64(n))
		if c.metrics.CollectionActive() {
			c.metrics.ObserveOuterPacket(c.readBuf[:n], time.Now())
		}
		if len(plain) > len(buffer) {
			copy(buffer, plain)
			return len(buffer), addr, io.ErrShortBuffer
		}
		return copy(buffer, plain), addr, nil
	}
}

func (c *obfsPacketConn) WriteTo(payload []byte, addr net.Addr) (int, error) {
	if !samePacketAddress(addr, c.remote) {
		return 0, errors.New("call multi_user: unexpected DTLS destination")
	}
	wire, err := c.codec.wrap(payload)
	if err != nil {
		c.metrics.AddHot(telemetry.OuterWrapFailuresTotal, 1)
		return 0, err
	}
	if _, err = c.base.WriteTo(wire, c.remote); err != nil {
		return 0, err
	}
	c.metrics.AddHot(telemetry.OuterPacketsOutTotal, 1)
	c.metrics.AddHot(telemetry.OuterBytesOutTotal, uint64(len(wire)))
	return len(payload), nil
}

func (c *obfsPacketConn) Close() error                       { return c.base.Close() }
func (c *obfsPacketConn) LocalAddr() net.Addr                { return c.base.LocalAddr() }
func (c *obfsPacketConn) SetDeadline(t time.Time) error      { return c.base.SetDeadline(t) }
func (c *obfsPacketConn) SetReadDeadline(t time.Time) error  { return c.base.SetReadDeadline(t) }
func (c *obfsPacketConn) SetWriteDeadline(t time.Time) error { return c.base.SetWriteDeadline(t) }

func samePacketAddress(left, right net.Addr) bool {
	if left == nil || right == nil {
		return false
	}
	leftUDP, leftIsUDP := left.(*net.UDPAddr)
	rightUDP, rightIsUDP := right.(*net.UDPAddr)
	if leftIsUDP && rightIsUDP {
		return leftUDP.Port == rightUDP.Port && leftUDP.Zone == rightUDP.Zone && leftUDP.IP.Equal(rightUDP.IP)
	}
	return left.Network() == right.Network() && left.String() == right.String()
}

func packetDeadline(nanos int64) (*time.Timer, <-chan time.Time) {
	if nanos == 0 {
		return nil, nil
	}
	duration := time.Until(time.Unix(0, nanos))
	if duration < 0 {
		duration = 0
	}
	timer := time.NewTimer(duration)
	return timer, timer.C
}

func deadlineExpired(nanos int64) bool {
	return nanos != 0 && time.Now().After(time.Unix(0, nanos))
}

func deadlineNanos(deadline time.Time) int64 {
	if deadline.IsZero() {
		return 0
	}
	return deadline.UnixNano()
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
