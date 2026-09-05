package vkparasite

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var errPacketConnClosed = errors.New("call vk_parasite: packet connection closed")

type receivedPacket struct {
	payload []byte
	addr    net.Addr
}

// peerPacketConn gives one DTLS server a connected view of a shared UDP
// listener. The owner decodes the shared RTP wrapper before enqueueing data.
type peerPacketConn struct {
	base            net.PacketConn
	remote          net.Addr
	codec           *rtpCodec
	readQueue       chan receivedPacket
	deadlineChanged chan struct{}
	closed          chan struct{}
	closeOnce       sync.Once
	established     atomic.Bool
	clientHello     atomic.Pointer[[32]byte]
	readDeadline    atomic.Int64
	writeDeadline   atomic.Int64
}

func newPeerPacketConn(base net.PacketConn, remote net.Addr, codec *rtpCodec, queueCapacity int) *peerPacketConn {
	if queueCapacity < 1 {
		queueCapacity = defaultPeerReadQueuePackets
	}
	connection := &peerPacketConn{
		base:            base,
		remote:          remote,
		codec:           codec,
		readQueue:       make(chan receivedPacket, queueCapacity),
		deadlineChanged: make(chan struct{}, 1),
		closed:          make(chan struct{}),
	}
	return connection
}

func (c *peerPacketConn) markEstablished() { c.established.Store(true) }

func (c *peerPacketConn) isEstablished() bool { return c.established.Load() }

func (c *peerPacketConn) rememberClientHello(identity [32]byte) bool {
	stored := c.clientHello.Load()
	if stored == nil {
		candidate := identity
		if c.clientHello.CompareAndSwap(nil, &candidate) {
			return false
		}
		stored = c.clientHello.Load()
	}
	return stored != nil && *stored != identity
}

func (c *peerPacketConn) enqueue(payload []byte, addr net.Addr) bool {
	copyPayload := append([]byte(nil), payload...)
	select {
	case c.readQueue <- receivedPacket{payload: copyPayload, addr: addr}:
		return true
	case <-c.closed:
		return false
	default:
		return false
	}
}

func (c *peerPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	for {
		timer, timeout := packetDeadline(c.readDeadline.Load())
		select {
		case packet := <-c.readQueue:
			stopPacketTimer(timer)
			return copy(buffer, packet.payload), packet.addr, nil
		case <-c.closed:
			stopPacketTimer(timer)
			return 0, nil, errPacketConnClosed
		case <-timeout:
			if deadlineExpired(c.readDeadline.Load()) {
				return 0, nil, timeoutError{}
			}
			continue
		case <-c.deadlineChanged:
			stopPacketTimer(timer)
			// A DTLS cancellation changes the read deadline after ReadFrom is
			// already blocked. Recalculate it instead of leaving the handshake
			// goroutine asleep forever.
			continue
		}
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
	wire, rawBuf, err := c.codec.wrap(payload)
	if err != nil {
		return 0, err
	}
	_, err = c.base.WriteTo(wire, c.remote)
	c.codec.putBuffer(rawBuf)
	if err != nil {
		return 0, err
	}
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
	c.signalDeadlineChanged()
	return nil
}

func (c *peerPacketConn) SetReadDeadline(deadline time.Time) error {
	c.readDeadline.Store(deadlineNanos(deadline))
	c.signalDeadlineChanged()
	return nil
}

func (c *peerPacketConn) SetWriteDeadline(deadline time.Time) error {
	c.writeDeadline.Store(deadlineNanos(deadline))
	return nil
}

func (c *peerPacketConn) signalDeadlineChanged() {
	select {
	case c.deadlineChanged <- struct{}{}:
	default:
	}
}

type obfsPacketConn struct {
	base     net.PacketConn
	remote   net.Addr
	codec    *rtpCodec
	readLock sync.Mutex
	readBuf  []byte
}

func newObfsPacketConn(base net.PacketConn, remote net.Addr, codec *rtpCodec) *obfsPacketConn {
	return &obfsPacketConn{base: base, remote: remote, codec: codec, readBuf: make([]byte, maximumWirePacket)}
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
		plain, err := c.codec.unwrap(buffer[:0], c.readBuf[:n])
		if err != nil {
			continue
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
		return 0, errors.New("call vk_parasite: unexpected DTLS destination")
	}
	wire, rawBuf, err := c.codec.wrap(payload)
	if err != nil {
		return 0, err
	}
	_, err = c.base.WriteTo(wire, c.remote)
	c.codec.putBuffer(rawBuf)
	if err != nil {
		return 0, err
	}
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

func stopPacketTimer(timer *time.Timer) {
	if timer != nil {
		timer.Stop()
	}
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
