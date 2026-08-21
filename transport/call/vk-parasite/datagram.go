package vkparasite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

// ai-generated: UDP datagram routing over QUIC datagrams with association tracking
type datagramRouter struct {
	mu           sync.RWMutex
	associations map[uint64]*datagramAssociation
	nextID       atomic.Uint64
	closed       atomic.Bool
}

func newDatagramRouter() *datagramRouter {
	return &datagramRouter{
		associations: make(map[uint64]*datagramAssociation),
	}
}

func (r *datagramRouter) newAssociation(conn *quic.Conn, dest M.Socksaddr) *datagramAssociation {
	id := r.nextID.Add(1)
	assoc := &datagramAssociation{
		id:          id,
		router:      r,
		conn:        conn,
		destination: dest,
		recvQueue:   make(chan []byte, 256),
		closed:      make(chan struct{}),
	}
	r.mu.Lock()
	if !r.closed.Load() {
		r.associations[id] = assoc
	}
	r.mu.Unlock()
	return assoc
}

func (r *datagramRouter) removeAssociation(id uint64) {
	r.mu.Lock()
	delete(r.associations, id)
	r.mu.Unlock()
}

func (r *datagramRouter) routeReceivedDatagram(raw []byte, conn *quic.Conn) {
	reader := bytes.NewReader(raw)
	id, err := binary.ReadUvarint(reader)
	if err != nil {
		return
	}
	dest, err := M.SocksaddrSerializer.ReadAddrPort(reader)
	if err != nil {
		return
	}
	_ = dest
	payloadOffset := len(raw) - reader.Len()
	payload := raw[payloadOffset:]

	r.mu.RLock()
	assoc := r.associations[id]
	r.mu.RUnlock()

	if assoc != nil {
		assoc.deliver(payload)
	}
}

func (r *datagramRouter) close() {
	r.closed.Store(true)
	r.mu.Lock()
	for _, assoc := range r.associations {
		_ = assoc.Close()
	}
	r.associations = make(map[uint64]*datagramAssociation)
	r.mu.Unlock()
}

type datagramAssociation struct {
	id          uint64
	router      *datagramRouter
	conn        *quic.Conn
	destination M.Socksaddr
	recvQueue   chan []byte
	closed      chan struct{}
	closeOnce   sync.Once
}

func (a *datagramAssociation) deliver(payload []byte) {
	select {
	case a.recvQueue <- payload:
	default:
	}
}

func (a *datagramAssociation) Read(p []byte) (int, error) {
	select {
	case payload, ok := <-a.recvQueue:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, payload)
		return n, nil
	case <-a.closed:
		return 0, io.EOF
	}
}

func (a *datagramAssociation) Write(p []byte) (int, error) {
	if a.conn == nil {
		return 0, errors.New("call vk_parasite: QUIC connection is nil")
	}
	var headerBuf [16]byte
	varLen := binary.PutUvarint(headerBuf[:], a.id)

	var addrBuf bytes.Buffer
	if err := M.SocksaddrSerializer.WriteAddrPort(&addrBuf, a.destination); err != nil {
		return 0, err
	}

	totalLen := varLen + addrBuf.Len() + len(p)
	const maxAllowedDatagram = quicPacketSize
	if totalLen > maxAllowedDatagram {
		return 0, fmt.Errorf("call vk_parasite: datagram oversize for %s: %d > %d", a.destination, totalLen, maxAllowedDatagram)
	}

	packet := make([]byte, 0, totalLen)
	packet = append(packet, headerBuf[:varLen]...)
	packet = append(packet, addrBuf.Bytes()...)
	packet = append(packet, p...)

	if err := a.conn.SendDatagram(packet); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (a *datagramAssociation) Close() error {
	a.closeOnce.Do(func() {
		close(a.closed)
		a.router.removeAssociation(a.id)
	})
	return nil
}

func (a *datagramAssociation) LocalAddr() net.Addr {
	if a.conn != nil {
		return a.conn.LocalAddr()
	}
	return &net.UDPAddr{}
}

func (a *datagramAssociation) RemoteAddr() net.Addr {
	return a.destination
}

func (a *datagramAssociation) SetDeadline(t time.Time) error {
	return nil
}

func (a *datagramAssociation) SetReadDeadline(t time.Time) error {
	return nil
}

func (a *datagramAssociation) SetWriteDeadline(t time.Time) error {
	return nil
}
