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
	// owner — буфер из пула, если payload взят оттуда. Освобождает его тот, кто
	// пакет прочитал: очередь передаёт владение вместе с записью.
	owner *wireBuffer
}

// Пул буферов под один внешний пакет.
//
// Через очереди сервера пакет обязан ехать в собственной памяти: буфер чтения
// перезаписывается следующим read, а запись ждёт другую горутину. Копия нужна,
// источник памяти — нет. Размер покрывает любой реальный пакет при path MTU
// 1400; всё, что больше, аллоцируется обычным образом и в пул не возвращается,
// иначе полная очередь держала бы память по максимальному размеру пакета.
var packetBufferPool = sync.Pool{New: func() any { return new(wireBuffer) }}

// takePacketCopy копирует пакет в память, которой владеет вызывающий.
func takePacketCopy(payload []byte) ([]byte, *wireBuffer) {
	if len(payload) > maxCodecWireBuffer {
		return append([]byte(nil), payload...), nil
	}
	owner := packetBufferPool.Get().(*wireBuffer)
	return owner[:copy(owner[:], payload)], owner
}

// releasePacketCopy возвращает буфер в пул. Пакеты, которые в пул не влезли,
// собирает сборщик, поэтому nil здесь — нормальный случай.
func releasePacketCopy(owner *wireBuffer) {
	if owner != nil {
		packetBufferPool.Put(owner)
	}
}

// peerPacketConn gives one QUIC listener a connected view of a shared UDP
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
	attach          atomic.Pointer[[32]byte]
	authAck         atomic.Pointer[[]byte]
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

// rememberAttach запоминает попытку присоединения и отвечает, отличается ли
// она от уже запомненной.
func (c *peerPacketConn) rememberAttach(identity [32]byte) bool {
	stored := c.attach.Load()
	if stored == nil {
		candidate := identity
		if c.attach.CompareAndSwap(nil, &candidate) {
			return false
		}
		stored = c.attach.Load()
	}
	return stored != nil && *stored != identity
}

// storeAuthAck запоминает ответ на auth-фрейм.
//
// Обёртка ненадёжна, а DTLS, который раньше переспрашивал потерянное сам,
// больше нет: если ack не дошёл, клиент повторит запрос, и ответить на повтор
// нужно тем же ack, а не тишиной.
func (c *peerPacketConn) storeAuthAck(ack []byte) {
	frozen := append([]byte(nil), ack...)
	c.authAck.Store(&frozen)
}

// replayAuthAck переотправляет запомненный ответ. Возвращает false, если
// отвечать пока нечем.
func (c *peerPacketConn) replayAuthAck() bool {
	ack := c.authAck.Load()
	if ack == nil {
		return false
	}
	_, err := c.WriteTo(*ack, c.remote)
	return err == nil
}

func (c *peerPacketConn) enqueue(payload []byte, addr net.Addr) bool {
	copyPayload, owner := takePacketCopy(payload)
	select {
	case c.readQueue <- receivedPacket{payload: copyPayload, addr: addr, owner: owner}:
		return true
	case <-c.closed:
		releasePacketCopy(owner)
		return false
	default:
		releasePacketCopy(owner)
		return false
	}
}

func (c *peerPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	for {
		timer, timeout := packetDeadline(c.readDeadline.Load())
		select {
		case packet := <-c.readQueue:
			stopPacketTimer(timer)
			// Очередь отдаёт запись ровно одному читателю, и после копирования
			// payload больше не нужен — здесь и есть точка освобождения.
			n := copy(buffer, packet.payload)
			releasePacketCopy(packet.owner)
			return n, packet.addr, nil
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
			// Отмена меняет read deadline, когда ReadFrom уже заблокирован.
			// Пересчитываем его, а не оставляем горутину спать навсегда.
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
