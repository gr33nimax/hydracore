package vkparasite

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// serverObfsConn — общий вид на UDP-листенер сервера: снимает RTP-обёртку на
// приёме и надевает её на отправке.
//
// Раньше каждый пир получал собственный peerPacketConn с очередью пакетов,
// шардированный ingress-пул раскладывал по ним расшифрованные датаграммы, а
// на каждом пире стоял свой QUIC-листенер. Демультиплексирование по адресу и
// по auth-фрейму сервер делал сам. Всё это умеет QUIC: connection ID в
// заголовке — и есть идентификатор соединения, а quic.Transport разбирает по
// нему пакеты без копий и очередей.
//
// Обёртка на отправке остаётся своя на каждый удалённый адрес: SSRC и
// последовательность RTP описывают один поток одного отправителя, и делить их
// между пирами значило бы отдавать каждому поток с дырами.
type serverObfsConn struct {
	base     net.PacketConn
	key      [wrapKeyLength]byte
	readLock sync.Mutex
	readBuf  []byte
	decoder  *rtpCodec
	codecsMu sync.Mutex
	codecs   map[string]*remoteCodec
}

type remoteCodec struct {
	codec    *rtpCodec
	lastUsed atomic.Int64
}

func newServerObfsConn(base net.PacketConn, key [wrapKeyLength]byte) (*serverObfsConn, error) {
	decoder, err := newRTPCodec(key)
	if err != nil {
		return nil, err
	}
	return &serverObfsConn{
		base:    base,
		key:     key,
		readBuf: make([]byte, maximumWirePacket),
		decoder: decoder,
		codecs:  make(map[string]*remoteCodec),
	}, nil
}

// ReadFrom снимает обёртку прямо в буфер вызывающего.
//
// Читатель у транспорта один, поэтому замок здесь — страховка, а не рабочая
// нагрузка. Пакет, который не проходит аутентификацию обёртки, пропускается:
// это чужой трафик на общем порту, а не ошибка сокета.
func (c *serverObfsConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()
	for {
		n, addr, err := c.base.ReadFrom(c.readBuf)
		if err != nil {
			return 0, nil, err
		}
		inPlace := len(buffer) >= n
		dst := buffer
		if !inPlace {
			dst = nil
		}
		plain, err := c.decoder.unwrap(dst, c.readBuf[:n])
		if err != nil {
			continue
		}
		if inPlace {
			return len(plain), addr, nil
		}
		if len(plain) > len(buffer) {
			copy(buffer, plain)
			return len(buffer), addr, io.ErrShortBuffer
		}
		return copy(buffer, plain), addr, nil
	}
}

func (c *serverObfsConn) WriteTo(payload []byte, addr net.Addr) (int, error) {
	codec, err := c.codecFor(addr)
	if err != nil {
		return 0, err
	}
	wire, rawBuf, err := codec.wrap(payload)
	if err != nil {
		return 0, err
	}
	_, err = c.base.WriteTo(wire, addr)
	codec.putBuffer(rawBuf)
	if err != nil {
		return 0, err
	}
	return len(payload), nil
}

func (c *serverObfsConn) codecFor(addr net.Addr) (*rtpCodec, error) {
	key := addr.String()
	now := time.Now().UnixNano()
	c.codecsMu.Lock()
	defer c.codecsMu.Unlock()
	if existing, ok := c.codecs[key]; ok {
		existing.lastUsed.Store(now)
		return existing.codec, nil
	}
	codec, err := newRTPCodec(c.key)
	if err != nil {
		return nil, err
	}
	entry := &remoteCodec{codec: codec}
	entry.lastUsed.Store(now)
	c.codecs[key] = entry
	return codec, nil
}

// reapCodecs выбрасывает обёртки адресов, которые давно молчат. Без этого карта
// растёт на каждого пира за всё время работы сервера.
func (c *serverObfsConn) reapCodecs(idleFor time.Duration) {
	cutoff := time.Now().Add(-idleFor).UnixNano()
	c.codecsMu.Lock()
	defer c.codecsMu.Unlock()
	for key, entry := range c.codecs {
		if entry.lastUsed.Load() < cutoff {
			delete(c.codecs, key)
		}
	}
}

func (c *serverObfsConn) codecCount() int {
	c.codecsMu.Lock()
	defer c.codecsMu.Unlock()
	return len(c.codecs)
}

func (c *serverObfsConn) Close() error                       { return c.base.Close() }
func (c *serverObfsConn) LocalAddr() net.Addr                { return c.base.LocalAddr() }
func (c *serverObfsConn) SetDeadline(t time.Time) error      { return c.base.SetDeadline(t) }
func (c *serverObfsConn) SetReadDeadline(t time.Time) error  { return c.base.SetReadDeadline(t) }
func (c *serverObfsConn) SetWriteDeadline(t time.Time) error { return c.base.SetWriteDeadline(t) }

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
