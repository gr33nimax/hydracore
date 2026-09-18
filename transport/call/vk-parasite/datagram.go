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
	M "github.com/sagernet/sing/common/metadata"
)

// Формат DATAGRAM-фрейма:
//
//	uvarint   идентификатор ассоциации
//	socksaddr назначение
//	uvarint   идентификатор датаграммы
//	uint8     всего фрагментов (>= 1)
//	uint8     индекс фрагмента (< всего)
//	bytes     фрагмент
//
// Фрагментация обязательна: payload одного фрейма ограничен транспортом
// (maxDatagramFramePayload), а QUIC-приложения внутри туннеля штатно шлют
// UDP-датаграммы по 1250 байт, которые в один фрейм не влезают.
const (
	// Индекс и общее число фрагментов едут по одному байту.
	maxDatagramFragments = 255

	// Длина полей фрагментации в заголовке фрейма.
	datagramFragmentHeader = 2

	// Сколько места бюджет отводит под uvarint'ы заголовка.
	//
	// Оба растут: идентификатор ассоциации — с их числом, номер пакета — со
	// временем жизни ассоциации. Если считать бюджет по текущей длине, он молча
	// уменьшается на ходу, и датаграмма, которая утром влезала в один фрейм,
	// вечером начинает делиться на два. Резерв делает бюджет постоянным: четыре
	// байта покрывают 2^28 ассоциаций, пять — 2^35 датаграмм на ассоциацию.
	datagramAssocIDReserve  = 4
	datagramPacketIDReserve = 5

	// UDP-датаграмма не может быть длиннее этого ни на одном конце.
	maxDatagramPayload = 65535

	// Сколько недособранных датаграмм держим на одну ассоциацию.
	maxPendingDatagrams = 16

	// Через сколько недособранная датаграмма выбрасывается.
	datagramReassemblyTimeout = 5 * time.Second

	// Глубина очереди собранных датаграмм на ассоциацию.
	datagramReceiveQueue = 256
)

var (
	errDatagramTooLarge    = errors.New("call vk_parasite: UDP datagram is too large for the datagram path")
	errDatagramNoBudget    = errors.New("call vk_parasite: datagram header leaves no room for payload")
	errDatagramMalformed   = errors.New("call vk_parasite: malformed datagram fragment header")
	errAssociationClosed   = errors.New("call vk_parasite: datagram association closed")
	errAssociationNoTunnel = errors.New("call vk_parasite: QUIC connection is nil")
)

type datagramRouter struct {
	mu           sync.RWMutex
	associations map[uint64]*datagramAssociation
	nextID       atomic.Uint64
	closed       atomic.Bool
	onAccept     atomic.Pointer[func(net.Conn, string)]
}

func newDatagramRouter() *datagramRouter {
	return &datagramRouter{
		associations: make(map[uint64]*datagramAssociation),
	}
}

// setAcceptHandler включает приём ассоциаций, открытых удалённой стороной.
// Без обработчика датаграммы с неизвестным идентификатором отбрасываются:
// клиентская сторона туннеля входящие UDP-сессии не принимает.
func (r *datagramRouter) setAcceptHandler(handler func(net.Conn, string)) {
	r.onAccept.Store(&handler)
}

func (r *datagramRouter) newAssociation(conn *quic.Conn, dest M.Socksaddr) *datagramAssociation {
	id := r.nextID.Add(1)
	assoc := newDatagramAssociation(id, r, conn, dest)
	r.mu.Lock()
	if !r.closed.Load() {
		r.associations[id] = assoc
	}
	r.mu.Unlock()
	return assoc
}

// acceptAssociation создаёт ассоциацию под идентификатором, выбранным
// удалённой стороной. Номера выдаёт только инициатор, поэтому пространства
// идентификаторов двух концов туннеля не пересекаются.
func (r *datagramRouter) acceptAssociation(id uint64, dest M.Socksaddr, conn *quic.Conn) *datagramAssociation {
	handler := r.onAccept.Load()
	if handler == nil || *handler == nil || !dest.IsValid() {
		return nil
	}
	r.mu.Lock()
	if r.closed.Load() {
		r.mu.Unlock()
		return nil
	}
	if existing := r.associations[id]; existing != nil {
		r.mu.Unlock()
		return existing
	}
	assoc := newDatagramAssociation(id, r, conn, dest)
	r.associations[id] = assoc
	r.mu.Unlock()
	go (*handler)(assoc, dest.String())
	return assoc
}

func (r *datagramRouter) removeAssociation(id uint64) {
	r.mu.Lock()
	delete(r.associations, id)
	r.mu.Unlock()
}

func (r *datagramRouter) routeReceivedDatagram(raw []byte, conn *quic.Conn) {
	frame, err := parseDatagramFrame(raw)
	if err != nil {
		return
	}
	r.mu.RLock()
	assoc := r.associations[frame.associationID]
	r.mu.RUnlock()
	if assoc == nil {
		assoc = r.acceptAssociation(frame.associationID, frame.destination, conn)
		if assoc == nil {
			return
		}
	}
	assoc.deliverFragment(frame)
}

// closeConn снимает ассоциации умершего пути: их Write всё равно уже никуда
// не доедет, а без этого они висят до закрытия всего relay.
func (r *datagramRouter) closeConn(conn *quic.Conn) {
	r.mu.Lock()
	doomed := make([]*datagramAssociation, 0, len(r.associations))
	for id, assoc := range r.associations {
		if assoc.conn == conn {
			doomed = append(doomed, assoc)
			delete(r.associations, id)
		}
	}
	r.mu.Unlock()
	for _, assoc := range doomed {
		_ = assoc.Close()
	}
}

func (r *datagramRouter) close() {
	r.closed.Store(true)
	r.mu.Lock()
	doomed := make([]*datagramAssociation, 0, len(r.associations))
	for _, assoc := range r.associations {
		doomed = append(doomed, assoc)
	}
	r.associations = make(map[uint64]*datagramAssociation)
	r.mu.Unlock()
	for _, assoc := range doomed {
		_ = assoc.Close()
	}
}

type datagramFrame struct {
	associationID uint64
	destination   M.Socksaddr
	packetID      uint64
	total         uint8
	index         uint8
	fragment      []byte
}

func parseDatagramFrame(raw []byte) (datagramFrame, error) {
	var frame datagramFrame
	reader := bytes.NewReader(raw)
	associationID, err := binary.ReadUvarint(reader)
	if err != nil {
		return frame, err
	}
	destination, err := M.SocksaddrSerializer.ReadAddrPort(reader)
	if err != nil {
		return frame, err
	}
	packetID, err := binary.ReadUvarint(reader)
	if err != nil {
		return frame, err
	}
	var fragmentHeader [datagramFragmentHeader]byte
	if _, err = io.ReadFull(reader, fragmentHeader[:]); err != nil {
		return frame, err
	}
	if fragmentHeader[0] == 0 || fragmentHeader[1] >= fragmentHeader[0] {
		return frame, errDatagramMalformed
	}
	frame.associationID = associationID
	frame.destination = destination
	frame.packetID = packetID
	frame.total = fragmentHeader[0]
	frame.index = fragmentHeader[1]
	frame.fragment = raw[len(raw)-reader.Len():]
	return frame, nil
}

// pendingDatagram собирает одну фрагментированную датаграмму.
type pendingDatagram struct {
	packetID  uint64
	total     uint8
	remaining uint8
	size      int
	fragments [][]byte
	expiresAt time.Time
}

type datagramAssociation struct {
	id          uint64
	router      *datagramRouter
	conn        *quic.Conn
	destination M.Socksaddr
	recvQueue   chan []byte
	closed      chan struct{}
	closeOnce   sync.Once
	nextPacket  atomic.Uint64
	writeMu     sync.Mutex
	headerBase  []byte
	header      bytes.Buffer
	pendingMu   sync.Mutex
	pending     []*pendingDatagram
}

func newDatagramAssociation(id uint64, router *datagramRouter, conn *quic.Conn, dest M.Socksaddr) *datagramAssociation {
	return &datagramAssociation{
		id:          id,
		router:      router,
		conn:        conn,
		destination: dest,
		recvQueue:   make(chan []byte, datagramReceiveQueue),
		closed:      make(chan struct{}),
	}
}

func (a *datagramAssociation) deliver(payload []byte) {
	select {
	case a.recvQueue <- payload:
	case <-a.closed:
	default:
	}
}

// deliverFragment собирает датаграмму из фрагментов. Датаграммы QUIC
// ненадёжны и неупорядочены, поэтому недособранное просто истекает.
func (a *datagramAssociation) deliverFragment(frame datagramFrame) {
	select {
	case <-a.closed:
		return
	default:
	}
	// quic-go копирует каждую принятую датаграмму в свой буфер и отдаёт его
	// нам во владение (datagramQueue.HandleDatagramFrame), поэтому фрагмент уже
	// принадлежит нам и второй копии не требует.
	if frame.total == 1 {
		a.deliver(frame.fragment)
		return
	}
	a.pendingMu.Lock()
	pending := a.pendingFor(frame.packetID, frame.total, time.Now())
	if pending == nil || pending.fragments[frame.index] != nil {
		a.pendingMu.Unlock()
		return
	}
	pending.fragments[frame.index] = frame.fragment
	pending.size += len(frame.fragment)
	pending.remaining--
	if pending.size > maxDatagramPayload {
		a.dropPending(frame.packetID)
		a.pendingMu.Unlock()
		return
	}
	if pending.remaining > 0 {
		a.pendingMu.Unlock()
		return
	}
	payload := make([]byte, 0, pending.size)
	for _, fragment := range pending.fragments {
		payload = append(payload, fragment...)
	}
	a.dropPending(frame.packetID)
	a.pendingMu.Unlock()
	a.deliver(payload)
}

// pendingFor возвращает сборщик датаграммы, попутно выбрасывая истёкшие.
// Вызывается под pendingMu.
func (a *datagramAssociation) pendingFor(packetID uint64, total uint8, now time.Time) *pendingDatagram {
	kept := a.pending[:0]
	var found *pendingDatagram
	for _, candidate := range a.pending {
		switch {
		case candidate.packetID == packetID:
			found = candidate
			kept = append(kept, candidate)
		case now.Before(candidate.expiresAt):
			kept = append(kept, candidate)
		}
	}
	for index := len(kept); index < len(a.pending); index++ {
		a.pending[index] = nil
	}
	a.pending = kept
	if found != nil {
		if found.total != total {
			return nil
		}
		return found
	}
	if len(a.pending) >= maxPendingDatagrams {
		oldest := 0
		for index := 1; index < len(a.pending); index++ {
			if a.pending[index].expiresAt.Before(a.pending[oldest].expiresAt) {
				oldest = index
			}
		}
		a.pending = append(a.pending[:oldest], a.pending[oldest+1:]...)
	}
	created := &pendingDatagram{
		packetID:  packetID,
		total:     total,
		remaining: total,
		fragments: make([][]byte, total),
		expiresAt: now.Add(datagramReassemblyTimeout),
	}
	a.pending = append(a.pending, created)
	return created
}

// dropPending вызывается под pendingMu.
func (a *datagramAssociation) dropPending(packetID uint64) {
	for index, candidate := range a.pending {
		if candidate.packetID != packetID {
			continue
		}
		a.pending = append(a.pending[:index], a.pending[index+1:]...)
		return
	}
}

func (a *datagramAssociation) Read(p []byte) (int, error) {
	select {
	case payload, ok := <-a.recvQueue:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, payload)
		if n < len(payload) {
			return n, io.ErrShortBuffer
		}
		return n, nil
	case <-a.closed:
		return 0, io.EOF
	}
}

func (a *datagramAssociation) Write(p []byte) (int, error) {
	if a.conn == nil {
		return 0, errAssociationNoTunnel
	}
	select {
	case <-a.closed:
		return 0, errAssociationClosed
	default:
	}
	if len(p) > maxDatagramPayload {
		return 0, fmt.Errorf("%w: %d bytes for %s", errDatagramTooLarge, len(p), a.destination)
	}
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	prefix, err := a.framePrefix()
	if err != nil {
		return 0, err
	}
	if len(prefix) > a.framePrefixReserve() {
		return 0, fmt.Errorf("%w: %s", errDatagramNoBudget, a.destination)
	}
	budget := a.fragmentBudget()
	if budget < 1 {
		return 0, fmt.Errorf("%w: %s", errDatagramNoBudget, a.destination)
	}
	total := 1
	if len(p) > budget {
		total = (len(p) + budget - 1) / budget
	}
	if total > maxDatagramFragments {
		return 0, fmt.Errorf("%w: %d bytes for %s", errDatagramTooLarge, len(p), a.destination)
	}
	for index := range total {
		start := index * budget
		end := start + budget
		if end > len(p) {
			end = len(p)
		}
		// Буфер уходит в quic-go, поэтому он свой на каждый фрагмент.
		packet := make([]byte, 0, len(prefix)+datagramFragmentHeader+(end-start))
		packet = append(packet, prefix...)
		packet = append(packet, byte(total), byte(index))
		packet = append(packet, p[start:end]...)
		if err = a.conn.SendDatagram(packet); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// framePrefixReserve — сколько места бюджет отводит заголовку фрейма.
func (a *datagramAssociation) framePrefixReserve() int {
	return datagramAssocIDReserve + M.SocksaddrSerializer.AddrPortLen(a.destination) + datagramPacketIDReserve
}

// fragmentBudget — сколько байт внутренней датаграммы уезжает в одном фрейме.
// Постоянен на всю жизнь ассоциации.
func (a *datagramAssociation) fragmentBudget() int {
	return maxDatagramFramePayload - a.framePrefixReserve() - datagramFragmentHeader
}

// framePrefix собирает заголовок фрейма в буфер, живущий на ассоциации.
//
// Раньше это был свежий bytes.Buffer на каждую исходящую датаграмму: четыре
// аллокации ради заголовка длиной девять байт. Идентификатор ассоциации и
// назначение за её жизнь не меняются, поэтому они кодируются один раз, а на
// датаграмму приходится только uvarint номера пакета — в установившемся режиме
// заголовок не аллоцирует ничего.
//
// Возвращённый срез смотрит в буфер ассоциации и действителен, пока держится
// writeMu. Вызывается под writeMu.
func (a *datagramAssociation) framePrefix() ([]byte, error) {
	var varint [binary.MaxVarintLen64]byte
	if a.headerBase == nil {
		var base bytes.Buffer
		base.Write(varint[:binary.PutUvarint(varint[:], a.id)])
		if err := M.SocksaddrSerializer.WriteAddrPort(&base, a.destination); err != nil {
			return nil, err
		}
		a.headerBase = base.Bytes()
	}
	a.header.Reset()
	a.header.Write(a.headerBase)
	a.header.Write(varint[:binary.PutUvarint(varint[:], a.nextPacket.Add(1))])
	return a.header.Bytes(), nil
}

func (a *datagramAssociation) Close() error {
	a.closeOnce.Do(func() {
		close(a.closed)
		a.router.removeAssociation(a.id)
		a.pendingMu.Lock()
		a.pending = nil
		a.pendingMu.Unlock()
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
