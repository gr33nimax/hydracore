package vkparasite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/logger"
	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	LaneCount                = 4
	laneKCPMTU               = 1000
	laneKCPSendWindow        = 128
	laneKCPReceiveWindow     = 128
	laneKCPMaxPending        = 256
	laneKCPPreferredPending  = laneKCPSendWindow / 2
	laneKCPMaxFragments      = 255
	laneKCPUpdateInterval    = 10 * time.Millisecond
	laneKCPFastResend        = 2
	laneKCPNoCongestion      = 1
	laneKCPReceiveBuffer     = 32 * 1024
	laneSendQueueDepth       = 96
	laneReorderFrameLimit    = 4096
	laneSendRetryInterval    = 2 * time.Millisecond
	laneSendStallTimeout     = 8 * time.Second
	laneReorderGapTimeout    = 15 * time.Second
	laneReorderCheckInterval = time.Second
	workerHeartbeatInterval  = 15 * time.Second
	workerLivenessTimeout    = 60 * time.Second
	workerWriteTimeout       = 5 * time.Second
	workerStaleReplacement   = 2 * workerHeartbeatInterval
)

var workerHeartbeat = [8]byte{'H', 'C', 'V', 'K', 'H', 'B', 2, 0}
var laneFrameMagic = [8]byte{'H', 'C', 'V', 'K', 'L', 'N', 6, 0}

type queuedSegment struct {
	payload    []byte
	enqueuedAt time.Time
}

type kcpSentSegment struct {
	lastSentAt time.Time
	attempts   []kcpSendAttempt
}

type kcpSendAttempt struct {
	timestamp uint32
	sentAt    time.Time
}

type laneRecoveryResult uint8

const (
	laneRecoveryUnavailable laneRecoveryResult = iota
	laneRecoveryStarted
	laneRecoveryInProgress
	laneRecoveryTimedOut
)

type sendFlowState struct {
	mu           sync.Mutex
	nextSequence uint64
	laneMask     uint8
	laneID       uint16
	laneAssigned bool
	initialized  bool
	unordered    bool
	closed       bool
}

type receiveFlowState struct {
	nextSequence uint64
	pending      map[uint64][]byte
	gapSince     time.Time
	unordered    bool
}

type laneWorker struct {
	id          uint16
	epoch       uint64
	conn        net.Conn
	lane        *kcpLane
	parent      *ParasiteTunnel
	metrics     *telemetry.Accumulator
	sendQueue   chan queuedSegment
	done        chan struct{}
	closeOnce   sync.Once
	lastInbound atomic.Int64
}

func (w *laneWorker) close() {
	w.closeOnce.Do(func() {
		close(w.done)
		_ = w.conn.Close()
	})
}

type kcpLane struct {
	id      uint16
	parent  *ParasiteTunnel
	mu      sync.Mutex
	kcp     *kcp.KCP
	recvBuf []byte

	workerMu sync.RWMutex
	worker   *laneWorker

	metrics   *telemetry.Accumulator
	kcpSent   map[uint32]kcpSentSegment
	kcpSRTTMS float64
	kcpRTTVARMS float64
	kcpLastUNA uint32
	kcpHasUNA  bool
	flowCount  atomic.Int64
}

type ParasiteTunnel struct {
	logger logger.ContextLogger
	lanes  [LaneCount]*kcpLane

	sendMu              sync.Mutex
	sendFlows           map[uint32]*sendFlowState
	controlSendMu       sync.Mutex
	controlSendSequence uint64
	controlLaneID       uint16
	controlLaneAssigned bool
	receiveFlows        map[uint32]*receiveFlowState
	nextLane            atomic.Uint32

	callbackMu sync.RWMutex
	deliverMu  sync.Mutex
	onData     func([]byte)
	onClose    func()

	lastActivity atomic.Int64
	lastProgress atomic.Int64
	metrics      *telemetry.Accumulator

	sendStallTimeout  time.Duration
	reorderGapTimeout time.Duration
	recoveryMu        sync.Mutex
	recoveryActive    bool
	recoveryLane      uint16
	recoveryDeadline  time.Time
	recoveryStartedAt time.Time

	telemetryMu             sync.RWMutex
	onTelemetryControl      func(time.Duration)
	onTelemetryClientRecord func([]byte)
	onTelemetryEvent        func(telemetry.Event)
	closed                  chan struct{}
	closeOnce               sync.Once
}

func NewParasiteTunnel(seed uint32, log logger.ContextLogger) (*ParasiteTunnel, error) {
	return newParasiteTunnel(seed, log, nil)
}

func newParasiteTunnel(seed uint32, log logger.ContextLogger, metrics *telemetry.Accumulator) (*ParasiteTunnel, error) {
	if seed == 0 {
		return nil, errors.New("call vk_parasite: KCP seed must not be zero")
	}
	if log == nil {
		log = logger.NOP()
	}
	if metrics == nil {
		metrics = telemetry.NewAccumulator()
	}
	tunnel := &ParasiteTunnel{
		logger:  log,
		sendFlows:    make(map[uint32]*sendFlowState),
		receiveFlows: make(map[uint32]*receiveFlowState),
		metrics: metrics,
		closed:  make(chan struct{}),
	}
	now := time.Now().UnixNano()
	tunnel.lastActivity.Store(now)
	tunnel.lastProgress.Store(now)
	tunnel.sendStallTimeout = laneSendStallTimeout
	tunnel.reorderGapTimeout = laneReorderGapTimeout
	for index := 0; index < LaneCount; index++ {
		lane := &kcpLane{
			id:      uint16(index),
			parent:  tunnel,
			recvBuf: make([]byte, laneKCPReceiveBuffer),
			metrics: telemetry.NewAccumulator(),
			kcpSent: make(map[uint32]kcpSentSegment),
		}
		lane.metrics.SetCounterParent(metrics)
		laneID := lane.id
		lane.kcp = kcp.NewKCP(laneConversation(seed, laneID), func(buffer []byte, size int) {
			if size > 0 && lane.dispatchSegment(buffer[:size]) {
				lane.observeKCPOutput(buffer[:size])
			}
		})
		// TURN relay loss includes delayed/duplicated delivery and is not a safe
		// congestion signal. Native Reno repeatedly collapsed a lane to one
		// segment after RTO and imposed the measured per-client speed ceiling.
		// Bounded per-lane send/pending queues provide backpressure instead.
		lane.kcp.NoDelay(1, int(laneKCPUpdateInterval/time.Millisecond), laneKCPFastResend, laneKCPNoCongestion)
		lane.kcp.WndSize(laneKCPSendWindow, laneKCPReceiveWindow)
		lane.kcp.SetMtu(laneKCPMTU)
		tunnel.lanes[index] = lane
		go lane.updateLoop()
	}
	go tunnel.reorderWatchLoop()
	return tunnel, nil
}

func laneConversation(seed uint32, laneID uint16) uint32 {
	value := seed + 0x9e3779b9*uint32(laneID+1)
	value ^= value >> 16
	value *= 0x7feb352d
	value ^= value >> 15
	value *= 0x846ca68b
	value ^= value >> 16
	if value == 0 {
		value = uint32(laneID) + 1
	}
	return value
}

func (t *ParasiteTunnel) SendData(frame []byte) {
	if len(frame) == 0 {
		return
	}
	connID, msgType := frameIdentity(frame)
	if connID == calltunnel.ControlConnID {
		t.controlSendMu.Lock()
		defer t.controlSendMu.Unlock()
		encoded := encodeLaneFrame(connID, t.controlSendSequence, frame)
		lane, sent := t.sendEncoded(encoded, true, t.preferredControlLane())
		if sent {
			t.controlLaneID = lane.id
			t.controlLaneAssigned = true
			t.controlSendSequence++
			t.touch()
		}
		return
	}
	state := t.sendFlow(connID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return
	}
	state.initialize(msgType)
	encoded := encodeLaneFrame(connID, state.nextSequence, frame)
	lane, sent := t.sendEncoded(encoded, true, state.preferredLane())
	if !sent {
		return
	}
	t.touch()
	t.markProgress()
	t.commitFlowFrame(connID, msgType, state, lane)
}

func (t *ParasiteTunnel) trySendData(frame []byte) bool {
	return t.trySendDataWithActivity(frame, true)
}

func (t *ParasiteTunnel) trySendControlData(frame []byte) bool {
	return t.trySendDataWithActivity(frame, false)
}

func (t *ParasiteTunnel) trySendDataWithActivity(frame []byte, activity bool) bool {
	if len(frame) == 0 {
		return false
	}
	select {
	case <-t.closed:
		return false
	default:
	}
	connID, msgType := frameIdentity(frame)
	if connID == calltunnel.ControlConnID {
		if !t.controlSendMu.TryLock() {
			return false
		}
		defer t.controlSendMu.Unlock()
		encoded := encodeLaneFrame(connID, t.controlSendSequence, frame)
		lane, sent := t.sendEncoded(encoded, false, t.preferredControlLane())
		if !sent {
			return false
		}
		t.controlLaneID = lane.id
		t.controlLaneAssigned = true
		t.controlSendSequence++
		if activity {
			t.touch()
		}
		return true
	}
	state := t.sendFlow(connID)
	if !state.mu.TryLock() {
		return false
	}
	defer state.mu.Unlock()
	if state.closed {
		return false
	}
	state.initialize(msgType)
	encoded := encodeLaneFrame(connID, state.nextSequence, frame)
	lane, sent := t.sendEncoded(encoded, false, state.preferredLane())
	if !sent {
		return false
	}
	if activity {
		t.touch()
	}
	t.markProgress()
	t.commitFlowFrame(connID, msgType, state, lane)
	return true
}

func (t *ParasiteTunnel) SetOnData(callback func([]byte)) {
	t.callbackMu.Lock()
	t.onData = callback
	t.callbackMu.Unlock()
}

func (t *ParasiteTunnel) SetOnClose(callback func()) {
	t.callbackMu.Lock()
	t.onClose = callback
	t.callbackMu.Unlock()
}

func (t *ParasiteTunnel) Reconfigure(_, _ int) {}

func (t *ParasiteTunnel) FlowControlEnabled() bool { return true }

func (t *ParasiteTunnel) SetTelemetryCounterParent(parent *telemetry.Accumulator) {
	t.metrics.SetCounterParent(parent)
}

func (t *ParasiteTunnel) SetTelemetryCollectionActive(active bool) {
	t.metrics.SetCollectionActive(active)
	for _, lane := range t.lanes {
		lane.metrics.SetCollectionActive(active)
		if active {
			lane.mu.Lock()
			lane.kcpSent = make(map[uint32]kcpSentSegment)
			lane.kcpSRTTMS = 0
			lane.kcpRTTVARMS = 0
			lane.kcpLastUNA = 0
			lane.kcpHasUNA = false
			lane.mu.Unlock()
		}
	}
}

func (t *ParasiteTunnel) telemetryWorker(id uint16) *telemetry.Accumulator {
	if id >= LaneCount {
		return t.metrics
	}
	return t.lanes[id].metrics
}

func (t *ParasiteTunnel) SetTelemetryControlHandler(handler func(time.Duration)) {
	t.telemetryMu.Lock()
	t.onTelemetryControl = handler
	t.telemetryMu.Unlock()
}

func (t *ParasiteTunnel) SetTelemetryClientRecordHandler(handler func([]byte)) {
	t.telemetryMu.Lock()
	t.onTelemetryClientRecord = handler
	t.telemetryMu.Unlock()
}

func (t *ParasiteTunnel) SetTelemetryEventHandler(handler func(telemetry.Event)) {
	t.telemetryMu.Lock()
	t.onTelemetryEvent = handler
	t.telemetryMu.Unlock()
}

func (t *ParasiteTunnel) AddWorker(id uint16, conn net.Conn) (<-chan struct{}, error) {
	return t.AddWorkerEpoch(id, 0, conn)
}

func (t *ParasiteTunnel) AddWorkerEpoch(id uint16, epoch uint64, conn net.Conn) (<-chan struct{}, error) {
	worker, err := t.reserveWorker(id, epoch, conn)
	if err != nil {
		return nil, err
	}
	t.startWorker(worker)
	return worker.done, nil
}

func (t *ParasiteTunnel) AttachWorker(id uint16, conn net.Conn, beforeStart func() error) (<-chan struct{}, error) {
	return t.AttachWorkerEpoch(id, 0, conn, beforeStart)
}

func (t *ParasiteTunnel) AttachWorkerEpoch(id uint16, epoch uint64, conn net.Conn, beforeStart func() error) (<-chan struct{}, error) {
	worker, err := t.reserveWorker(id, epoch, conn)
	if err != nil {
		return nil, err
	}
	if err = beforeStart(); err != nil {
		t.removeWorker(worker)
		return nil, err
	}
	t.startWorker(worker)
	return worker.done, nil
}

func (t *ParasiteTunnel) reserveWorker(id uint16, epoch uint64, conn net.Conn) (*laneWorker, error) {
	select {
	case <-t.closed:
		return nil, errors.New("call vk_parasite: session already closed")
	default:
	}
	if id >= LaneCount {
		return nil, errors.New("call vk_parasite: invalid lane id")
	}
	lane := t.lanes[id]
	worker := &laneWorker{
		id:        id,
		epoch:     epoch,
		conn:      conn,
		lane:      lane,
		parent:    t,
		metrics:   lane.metrics,
		sendQueue: make(chan queuedSegment, laneSendQueueDepth),
		done:      make(chan struct{}),
	}
	worker.lastInbound.Store(time.Now().UnixNano())
	lane.workerMu.Lock()
	replaced := lane.worker
	if replaced != nil && epoch > 0 && epoch <= replaced.epoch {
		lane.workerMu.Unlock()
		return nil, errors.New("call vk_parasite: stale lane epoch")
	}
	if replaced != nil && epoch == 0 && time.Since(time.Unix(0, replaced.lastInbound.Load())) < workerStaleReplacement {
		lane.workerMu.Unlock()
		return nil, errors.New("call vk_parasite: duplicate active lane attach")
	}
	lane.worker = worker
	lane.workerMu.Unlock()
	t.completeLaneRecovery(id)
	worker.metrics.Set(telemetry.WorkerActive, 1)
	if replaced != nil {
		replaced.close()
	}
	t.touch()
	return worker, nil
}

func (t *ParasiteTunnel) DropWorker(id uint16) {
	if id >= LaneCount {
		return
	}
	lane := t.lanes[id]
	lane.workerMu.RLock()
	worker := lane.worker
	lane.workerMu.RUnlock()
	if worker != nil {
		t.removeWorker(worker)
	}
}

func (t *ParasiteTunnel) startWorker(worker *laneWorker) {
	go worker.readLoop()
	go worker.writeLoop()
	go worker.watchdogLoop()
}

func (w *laneWorker) readLoop() {
	buffer := make([]byte, laneKCPMTU+128)
	for {
		n, err := w.conn.Read(buffer)
		if err != nil {
			w.parent.removeWorker(w)
			return
		}
		if n == 0 {
			continue
		}
		w.lastInbound.Store(time.Now().UnixNano())
		if bytes.Equal(buffer[:n], workerHeartbeat[:]) {
			continue
		}
		w.lane.inputSegment(buffer[:n])
	}
}

func (w *laneWorker) writeLoop() {
	ticker := time.NewTicker(workerHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case segment := <-w.sendQueue:
			started := time.Now()
			if err := w.write(segment.payload); err != nil {
				w.parent.removeWorker(w)
				return
			}
			delay := started.Sub(segment.enqueuedAt)
			w.metrics.Set(telemetry.WorkerOutputQueueDelayMS, float64(delay)/float64(time.Millisecond))
			if delay >= 2*laneKCPUpdateInterval {
				w.metrics.AddHot(telemetry.WorkerOutputQueueLateTotal, 1)
			}
			w.parent.touch()
		case <-ticker.C:
			if err := w.write(workerHeartbeat[:]); err != nil {
				w.parent.removeWorker(w)
				return
			}
		case <-w.done:
			return
		case <-w.parent.closed:
			w.parent.removeWorker(w)
			return
		}
	}
}

func (w *laneWorker) write(payload []byte) error {
	_ = w.conn.SetWriteDeadline(time.Now().Add(workerWriteTimeout))
	_, err := w.conn.Write(payload)
	_ = w.conn.SetWriteDeadline(time.Time{})
	return err
}

func (w *laneWorker) watchdogLoop() {
	ticker := time.NewTicker(workerHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			if now.Sub(time.Unix(0, w.lastInbound.Load())) >= workerLivenessTimeout {
				w.metrics.Add(telemetry.WorkerLivenessExpiredTotal, 1)
				workerID := w.id
				w.metrics.RecordEvent("lane_liveness_expired", "lane", "timeout", &workerID)
				w.parent.removeWorker(w)
				return
			}
		case <-w.done:
			return
		case <-w.parent.closed:
			return
		}
	}
}

func (l *kcpLane) dispatchSegment(segment []byte) bool {
	l.workerMu.RLock()
	worker := l.worker
	l.workerMu.RUnlock()
	if worker == nil {
		return false
	}
	queued := queuedSegment{payload: append([]byte(nil), segment...), enqueuedAt: time.Now()}
	blockedAt := time.Now()
	select {
	case worker.sendQueue <- queued:
		blocked := time.Since(blockedAt)
		if blocked >= laneSendRetryInterval {
			worker.metrics.AddHotMonotonic(telemetry.KCPSendBlockedSecondsTotal, blocked.Seconds())
		}
		return true
	case <-worker.done:
		return false
	case <-l.parent.closed:
		return false
	}
}

func (l *kcpLane) inputSegment(segment []byte) {
	l.mu.Lock()
	l.observeKCPInput(segment)
	if l.kcp.Input(segment, kcp.IKCP_PACKET_REGULAR, true) < 0 {
		l.mu.Unlock()
		return
	}
	messages := make([][]byte, 0, 2)
	for {
		size := l.kcp.PeekSize()
		if size <= 0 {
			break
		}
		if size > len(l.recvBuf) {
			l.recvBuf = make([]byte, size)
		}
		n := l.kcp.Recv(l.recvBuf)
		if n <= 0 {
			break
		}
		messages = append(messages, append([]byte(nil), l.recvBuf[:n]...))
	}
	l.mu.Unlock()
	l.parent.touch()
	for _, message := range messages {
		l.parent.deliver(l.id, message)
	}
}

func (t *ParasiteTunnel) deliver(_ uint16, message []byte) {
	connID, sequence, frame, ok := decodeLaneFrame(message)
	if !ok {
		t.recordEvent("lane_frame_rejected", "lane", "wire", nil)
		return
	}
	t.deliverMu.Lock()
	defer t.deliverMu.Unlock()
	_, msgType, validFrame := relayFrameIdentity(frame)
	if validFrame && unorderedRelayMessage(msgType) {
		t.deliverUnorderedLocked(connID, sequence, frame)
		return
	}
	state := t.receiveFlows[connID]
	if state == nil {
		state = &receiveFlowState{pending: make(map[uint64][]byte)}
		t.receiveFlows[connID] = state
	}
	if sequence < state.nextSequence {
		return
	}
	if sequence > state.nextSequence {
		if sequence-state.nextSequence > laneReorderFrameLimit || len(state.pending) >= laneReorderFrameLimit {
			t.recordEvent("lane_reorder_overflow", "lane", "sequence_gap", nil)
			go t.Close()
			return
		}
		if state.gapSince.IsZero() {
			state.gapSince = time.Now()
		}
		if _, exists := state.pending[sequence]; !exists {
			state.pending[sequence] = append([]byte(nil), frame...)
		}
		return
	}
	t.deliverFrameLocked(connID, frame)
	state.nextSequence++
	for {
		next, exists := state.pending[state.nextSequence]
		if !exists {
			break
		}
		delete(state.pending, state.nextSequence)
		t.deliverFrameLocked(connID, next)
		state.nextSequence++
	}
	if len(state.pending) == 0 {
		state.gapSince = time.Time{}
	} else {
		state.gapSince = time.Now()
	}
}

// UDP datagrams may be striped over all four independent calls. They are
// delivered immediately because UDP and QUIC tolerate reordering, while the
// sequence map still suppresses KCP duplicates and delays a terminal close
// until every preceding datagram was either received or timed out.
func (t *ParasiteTunnel) deliverUnorderedLocked(connID uint32, sequence uint64, frame []byte) {
	state := t.receiveFlows[connID]
	if state == nil {
		state = &receiveFlowState{pending: make(map[uint64][]byte), unordered: true}
		t.receiveFlows[connID] = state
	}
	if !state.unordered || sequence < state.nextSequence {
		return
	}
	if sequence > state.nextSequence {
		if sequence-state.nextSequence > laneReorderFrameLimit || len(state.pending) >= laneReorderFrameLimit {
			t.recordEvent("lane_udp_reorder_overflow", "lane", "sequence_gap", nil)
			delete(t.receiveFlows, connID)
			return
		}
		if _, exists := state.pending[sequence]; exists {
			return
		}
		state.pending[sequence] = nil
		if state.gapSince.IsZero() {
			state.gapSince = time.Now()
		}
		t.deliverFrameLocked(connID, frame)
		return
	}
	t.deliverFrameLocked(connID, frame)
	state.nextSequence++
	for {
		pending, exists := state.pending[state.nextSequence]
		if !exists {
			break
		}
		delete(state.pending, state.nextSequence)
		if pending != nil {
			t.deliverFrameLocked(connID, pending)
		}
		state.nextSequence++
	}
	if len(state.pending) == 0 {
		state.gapSince = time.Time{}
	}
}

func (t *ParasiteTunnel) deliverFrameLocked(connID uint32, frame []byte) {
	if t.handleTelemetryMessage(frame) {
		return
	}
	_, msgType, _ := relayFrameIdentity(frame)
	if connID != calltunnel.ControlConnID {
		t.markProgress()
	}
	t.callbackMu.RLock()
	callback := t.onData
	t.callbackMu.RUnlock()
	if callback != nil {
		callback(frame)
	}
	if connID != calltunnel.ControlConnID && terminalRelayMessage(msgType) {
		delete(t.receiveFlows, connID)
		t.releaseSendFlow(connID)
	}
}

func frameIdentity(frame []byte) (uint32, byte) {
	connID, msgType, ok := relayFrameIdentity(frame)
	if !ok {
		connID = calltunnel.ControlConnID
		msgType = 0
	}
	return connID, msgType
}

func (t *ParasiteTunnel) sendFlow(connID uint32) *sendFlowState {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	state := t.sendFlows[connID]
	if state == nil {
		state = &sendFlowState{}
		t.sendFlows[connID] = state
	}
	return state
}

func (state *sendFlowState) initialize(msgType byte) {
	if state.initialized {
		return
	}
	state.initialized = true
	state.unordered = unorderedRelayMessage(msgType)
}

func (state *sendFlowState) preferredLane() *uint16 {
	if state.unordered || !state.laneAssigned {
		return nil
	}
	return &state.laneID
}

func (t *ParasiteTunnel) preferredControlLane() *uint16 {
	if !t.controlLaneAssigned {
		return nil
	}
	return &t.controlLaneID
}

func (t *ParasiteTunnel) commitFlowFrame(connID uint32, msgType byte, state *sendFlowState, lane *kcpLane) {
	if !state.unordered && !state.laneAssigned {
		state.laneID = lane.id
		state.laneAssigned = true
	}
	state.nextSequence++
	bit := uint8(1 << lane.id)
	if state.laneMask&bit == 0 {
		state.laneMask |= bit
		lane.flowCount.Add(1)
	}
	if terminalRelayMessage(msgType) {
		t.releaseSendFlowState(connID, state)
	}
}

func (t *ParasiteTunnel) releaseSendFlow(connID uint32) {
	t.sendMu.Lock()
	state := t.sendFlows[connID]
	if state != nil {
		delete(t.sendFlows, connID)
	}
	t.sendMu.Unlock()
	if state != nil {
		state.mu.Lock()
		state.closed = true
		t.releaseFlowLanes(state)
		state.mu.Unlock()
	}
}

func (t *ParasiteTunnel) releaseSendFlowState(connID uint32, state *sendFlowState) {
	t.sendMu.Lock()
	owned := t.sendFlows[connID] == state
	if owned {
		delete(t.sendFlows, connID)
	}
	t.sendMu.Unlock()
	if owned {
		state.closed = true
		t.releaseFlowLanes(state)
	}
}

func (t *ParasiteTunnel) releaseFlowLanes(state *sendFlowState) {
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		if state.laneMask&(1<<laneID) != 0 {
			t.lanes[laneID].flowCount.Add(-1)
		}
	}
}

type laneCandidate struct {
	lane  *kcpLane
	score float64
}

func (t *ParasiteTunnel) sendEncoded(encoded []byte, wait bool, preferred *uint16) (*kcpLane, bool) {
	select {
	case <-t.closed:
		return nil, false
	default:
	}
	required := kcpSegmentsForPayload(len(encoded))
	if required < 1 || required > laneKCPMaxFragments {
		t.recordEvent("lane_send_rejected", "kcp", "frame_too_large", nil)
		if wait {
			t.closeAsync()
		}
		return nil, false
	}
	if lane := t.trySendEncoded(encoded, required, preferred); lane != nil {
		return lane, true
	}
	if !wait {
		return nil, false
	}
	blockedAt := time.Now()
	ticker := time.NewTicker(laneSendRetryInterval)
	timer := time.NewTimer(t.sendStallTimeout)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-t.closed:
			return nil, false
		case <-ticker.C:
			if lane := t.trySendEncoded(encoded, required, preferred); lane != nil {
				lane.metrics.AddHotMonotonic(telemetry.KCPSendBlockedSecondsTotal, time.Since(blockedAt).Seconds())
				return lane, true
			}
		case <-timer.C:
			t.metrics.AddHotMonotonic(telemetry.KCPSendBlockedSecondsTotal, time.Since(blockedAt).Seconds())
			t.recordEvent("lane_send_stalled", "kcp", "pending_timeout", nil)
			workerID, recovery := t.recoverStalledLane(preferred)
			switch recovery {
			case laneRecoveryStarted:
				t.recordEvent("lane_send_recovery", "lane", "worker_recycle", &workerID)
				blockedAt = time.Now()
				timer.Reset(t.sendStallTimeout)
				continue
			case laneRecoveryInProgress:
				// Other blocked relay flows join the same session-wide recovery.
				// They must not recycle the remaining healthy lanes in parallel.
				blockedAt = time.Now()
				timer.Reset(t.sendStallTimeout)
				continue
			case laneRecoveryTimedOut:
				t.recordEvent("lane_send_recovery_failed", "lane", "timeout", &workerID)
			default:
				t.recordEvent("lane_send_recovery_failed", "lane", "unavailable", nil)
			}
			t.closeAsync()
			return nil, false
		}
	}
}

func kcpSegmentsForPayload(size int) int {
	if size <= 0 {
		return 0
	}
	mss := laneKCPMTU - kcpHeaderSize
	return (size + mss - 1) / mss
}

func (t *ParasiteTunnel) trySendEncoded(encoded []byte, required int, preferred *uint16) *kcpLane {
	// Ordered relay flows normally stay on one KCP lane. Keep that fast path
	// while it has reasonable headroom, but do not let one saturated VK call
	// cap a flow while the other three independent calls are idle.
	if preferred != nil && int(*preferred) < LaneCount {
		if t.trySendEncodedOnLane(t.lanes[*preferred], encoded, required, laneKCPPreferredPending) {
			return t.lanes[*preferred]
		}
	}

	start := int((t.nextLane.Add(1) - 1) % LaneCount)
	candidates := make([]laneCandidate, 0, LaneCount)
	for offset := 0; offset < LaneCount; offset++ {
		laneID := (start + offset) % LaneCount
		lane := t.lanes[laneID]
		active, queueDepth := lane.workerState()
		if !active {
			continue
		}
		if !lane.mu.TryLock() {
			continue
		}
		waitSnd := lane.kcp.WaitSnd()
		rtt := lane.kcpSRTTMS
		lane.mu.Unlock()
		if waitSnd+required > laneKCPMaxPending || queueDepth+required > laneSendQueueDepth {
			continue
		}
		score := float64(waitSnd*4+queueDepth*2) + rtt/10 + float64(lane.flowCount.Load())
		candidates = append(candidates, laneCandidate{lane: lane, score: score})
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].score < candidates[j].score })
	for _, candidate := range candidates {
		lane := candidate.lane
		if t.trySendEncodedOnLane(lane, encoded, required, laneKCPMaxPending) {
			return lane
		}
	}
	return nil
}

func (t *ParasiteTunnel) trySendEncodedOnLane(lane *kcpLane, encoded []byte, required int, pendingLimit int) bool {
	active, queueDepth := lane.workerState()
	if !active || queueDepth+required > laneSendQueueDepth || !lane.mu.TryLock() {
		return false
	}
	defer lane.mu.Unlock()
	if lane.kcp.WaitSnd()+required > pendingLimit {
		return false
	}
	// The lane update loop flushes within 10 ms. Calling Update synchronously
	// here lets a blocked TURN write hold both the KCP mutex and RelayBridge's
	// per-flow send path, preventing the stall timer from recovering the lane.
	return lane.kcp.Send(encoded) >= 0
}

func (t *ParasiteTunnel) recoverStalledLane(preferred *uint16) (uint16, laneRecoveryResult) {
	t.recoveryMu.Lock()
	defer t.recoveryMu.Unlock()
	if t.recoveryActive {
		if time.Now().After(t.recoveryDeadline) {
			return t.recoveryLane, laneRecoveryTimedOut
		}
		return t.recoveryLane, laneRecoveryInProgress
	}

	var selected *laneWorker
	if preferred != nil && int(*preferred) < LaneCount {
		lane := t.lanes[*preferred]
		lane.workerMu.RLock()
		selected = lane.worker
		lane.workerMu.RUnlock()
	}

	selectedPressure := -1
	for _, lane := range t.lanes {
		if selected != nil {
			break
		}
		lane.workerMu.RLock()
		worker := lane.worker
		queueDepth := 0
		if worker != nil {
			queueDepth = len(worker.sendQueue)
		}
		lane.workerMu.RUnlock()
		if worker == nil {
			continue
		}
		pressure := queueDepth * 2
		if lane.mu.TryLock() {
			pressure += lane.kcp.WaitSnd() * 4
			lane.mu.Unlock()
		} else {
			// A busy KCP mutex is itself evidence that this lane is the one
			// blocking output; prefer it over an idle candidate.
			pressure += laneKCPMaxPending * 4
		}
		if pressure > selectedPressure {
			selected = worker
			selectedPressure = pressure
		}
	}
	if selected == nil {
		return 0, laneRecoveryUnavailable
	}
	workerID := selected.id
	t.recoveryActive = true
	t.recoveryLane = workerID
	t.recoveryStartedAt = time.Now()
	t.recoveryDeadline = time.Now().Add(3 * t.sendStallTimeout)
	t.removeWorker(selected)
	return workerID, laneRecoveryStarted
}

func (t *ParasiteTunnel) completeLaneRecovery(laneID uint16) {
	t.recoveryMu.Lock()
	recovered := t.recoveryActive && t.recoveryLane == laneID
	if recovered {
		t.recoveryActive = false
		t.recoveryDeadline = time.Time{}
		t.recoveryStartedAt = time.Time{}
	}
	t.recoveryMu.Unlock()
	if recovered {
		t.recordEvent("lane_send_recovered", "lane", "worker_attached", &laneID)
	}
}

func (l *kcpLane) workerState() (bool, int) {
	l.workerMu.RLock()
	defer l.workerMu.RUnlock()
	worker := l.worker
	if worker == nil {
		return false, 0
	}
	return true, len(worker.sendQueue)
}

func unorderedRelayMessage(msgType byte) bool {
	return msgType == calltunnel.MsgUDP || msgType == calltunnel.MsgUDPReply
}

func relayFrameIdentity(frame []byte) (uint32, byte, bool) {
	if len(frame) < 9 {
		return 0, 0, false
	}
	length := int(binary.BigEndian.Uint32(frame[:4]))
	if length < 5 || length+4 != len(frame) {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(frame[4:8]), frame[8], true
}

func encodeLaneFrame(connID uint32, sequence uint64, frame []byte) []byte {
	encoded := make([]byte, 24+len(frame))
	copy(encoded[:8], laneFrameMagic[:])
	binary.BigEndian.PutUint32(encoded[8:12], connID)
	binary.BigEndian.PutUint64(encoded[12:20], sequence)
	binary.BigEndian.PutUint32(encoded[20:24], uint32(len(frame)))
	copy(encoded[24:], frame)
	return encoded
}

func decodeLaneFrame(encoded []byte) (uint32, uint64, []byte, bool) {
	if len(encoded) < 24 || !bytes.Equal(encoded[:8], laneFrameMagic[:]) {
		return 0, 0, nil, false
	}
	length := int(binary.BigEndian.Uint32(encoded[20:24]))
	if length < 0 || length != len(encoded)-24 {
		return 0, 0, nil, false
	}
	return binary.BigEndian.Uint32(encoded[8:12]), binary.BigEndian.Uint64(encoded[12:20]), encoded[24:], true
}

func terminalRelayMessage(msgType byte) bool {
	return msgType == calltunnel.MsgClose || msgType == calltunnel.MsgConnectErr
}

func (t *ParasiteTunnel) removeWorker(worker *laneWorker) {
	lane := worker.lane
	lane.workerMu.Lock()
	if lane.worker == worker {
		lane.worker = nil
	}
	lane.workerMu.Unlock()
	worker.metrics.Set(telemetry.WorkerActive, 0)
	worker.metrics.Set(telemetry.WorkerSendQueueDepth, 0)
	worker.close()
	t.touch()
}

func (t *ParasiteTunnel) ActiveWorkers() int {
	active := 0
	for _, lane := range t.lanes {
		lane.workerMu.RLock()
		if lane.worker != nil {
			active++
		}
		lane.workerMu.RUnlock()
	}
	return active
}

func (t *ParasiteTunnel) WorkerEpoch(id uint16) (uint64, bool) {
	if id >= LaneCount {
		return 0, false
	}
	lane := t.lanes[id]
	lane.workerMu.RLock()
	defer lane.workerMu.RUnlock()
	if lane.worker == nil {
		return 0, false
	}
	return lane.worker.epoch, true
}

func (t *ParasiteTunnel) TelemetryValues() map[telemetry.Metric]float64 {
	t.metrics.Set(telemetry.KCPMTUBytes, laneKCPMTU)
	t.metrics.Set(telemetry.KCPSendWindowSegments, LaneCount*laneKCPSendWindow)
	t.metrics.Set(telemetry.KCPReceiveWindowSegments, LaneCount*laneKCPReceiveWindow)
	t.metrics.Set(telemetry.KCPMaxPendingSegments, LaneCount*laneKCPMaxPending)
	t.metrics.Set(telemetry.KCPUpdateIntervalMS, float64(laneKCPUpdateInterval/time.Millisecond))
	t.metrics.Set(telemetry.KCPFastResend, laneKCPFastResend)
	t.metrics.Set(telemetry.KCPCongestionControl, 0)
	t.metrics.Set(telemetry.WorkerSendQueueCapacity, LaneCount*laneSendQueueDepth)
	t.metrics.Set(telemetry.WorkerHeartbeatIntervalMS, float64(workerHeartbeatInterval/time.Millisecond))
	t.metrics.Set(telemetry.WorkerLivenessTimeoutMS, float64(workerLivenessTimeout/time.Millisecond))
	t.metrics.Set(telemetry.OuterRTPPayloadType, rtpPayloadTypeVideo)
	waitSnd := 0
	maxRTT := 0.0
	maxRTO := 200.0
	queueDepth := 0
	for _, lane := range t.lanes {
		lane.mu.Lock()
		laneWait := lane.kcp.WaitSnd()
		waitSnd += laneWait
		rtt := lane.kcpSRTTMS
		rttVar := lane.kcpRTTVARMS
		rto := 200.0
		if rtt > 0 {
			rto = max(30, rtt+max(10, 4*lane.kcpRTTVARMS))
		}
		lane.metrics.Set(telemetry.KCPWaitSnd, float64(laneWait))
		lane.metrics.Set(telemetry.KCPRTTMS, rtt)
		lane.metrics.Set(telemetry.KCPRTOMS, rto)
		lane.metrics.Set(telemetry.KCPRTTVarMS, rttVar)
		lane.metrics.Set(telemetry.KCPInflightSegments, float64(len(lane.kcpSent)))
		lane.metrics.Set(telemetry.KCPSendWindowSegments, laneKCPSendWindow)
		lane.metrics.Set(telemetry.KCPReceiveWindowSegments, laneKCPReceiveWindow)
		lane.metrics.Set(telemetry.KCPMaxPendingSegments, laneKCPMaxPending)
		lane.metrics.Set(telemetry.LaneCount, 1)
		lane.metrics.Set(telemetry.LaneFlowCount, float64(lane.flowCount.Load()))
		// Zero means that no fixed-rate pre-KCP pacer is installed. New data is
		// admitted only while the lane's KCP and physical output queues have
		// capacity, so telemetry can distinguish an unpaced lane from a low cap.
		lane.metrics.Set(telemetry.LaneAdmissionRateBPS, 0)
		lane.metrics.Set(telemetry.OuterRTPPayloadType, rtpPayloadTypeVideo)
		lane.mu.Unlock()
		maxRTT = max(maxRTT, rtt)
		maxRTO = max(maxRTO, rto)
		lane.workerMu.RLock()
		if lane.worker != nil {
			queueDepth += len(lane.worker.sendQueue)
		}
		lane.workerMu.RUnlock()
	}
	t.metrics.Set(telemetry.KCPWaitSnd, float64(waitSnd))
	t.metrics.Set(telemetry.KCPRTTMS, maxRTT)
	t.metrics.Set(telemetry.KCPRTOMS, maxRTO)
	t.metrics.Set(telemetry.WorkerActive, float64(t.ActiveWorkers()))
	t.metrics.Set(telemetry.WorkerSendQueueDepth, float64(queueDepth))
	t.metrics.Set(telemetry.LaneCount, LaneCount)
	t.sendMu.Lock()
	t.metrics.Set(telemetry.LaneFlowCount, float64(len(t.sendFlows)))
	t.sendMu.Unlock()
	values := make(map[telemetry.Metric]float64, len(telemetry.TunnelMetrics))
	for _, metric := range telemetry.TunnelMetrics {
		values[metric] = t.metrics.Value(metric)
	}
	return values
}

type workerTelemetrySnapshot struct {
	id      uint16
	metrics map[string]any
}

func (t *ParasiteTunnel) telemetryWorkerSnapshots(metrics []telemetry.Metric) []workerTelemetrySnapshot {
	result := make([]workerTelemetrySnapshot, 0, LaneCount)
	for _, lane := range t.lanes {
		lane.workerMu.RLock()
		worker := lane.worker
		queueDepth := 0.0
		if worker != nil {
			queueDepth = float64(len(worker.sendQueue))
		}
		lane.workerMu.RUnlock()
		lane.metrics.Set(telemetry.WorkerActive, boolFloat(worker != nil))
		lane.metrics.Set(telemetry.WorkerSendQueueDepth, queueDepth)
		result = append(result, workerTelemetrySnapshot{id: lane.id, metrics: lane.metrics.Snapshot(metrics)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (t *ParasiteTunnel) LastActivity() time.Time {
	return time.Unix(0, t.lastActivity.Load())
}

func (t *ParasiteTunnel) LastProgress() time.Time {
	return time.Unix(0, t.lastProgress.Load())
}

func (t *ParasiteTunnel) Done() <-chan struct{} {
	return t.closed
}

func (t *ParasiteTunnel) touch() {
	t.lastActivity.Store(time.Now().UnixNano())
}

func (t *ParasiteTunnel) markProgress() {
	t.lastProgress.Store(time.Now().UnixNano())
}

func (t *ParasiteTunnel) recordEvent(event, stage, reason string, workerID *uint16) {
	record := telemetry.Event{
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
		Event:     event,
		Stage:     stage,
		Reason:    reason,
		WorkerID:  workerID,
	}
	t.metrics.RecordEvent(event, stage, reason, workerID)
	t.telemetryMu.RLock()
	handler := t.onTelemetryEvent
	t.telemetryMu.RUnlock()
	if handler != nil {
		handler(record)
	}
}

func (t *ParasiteTunnel) Close() error {
	t.closeOnce.Do(func() {
		t.recoveryMu.Lock()
		recoveryActive := t.recoveryActive
		recoveryLane := t.recoveryLane
		t.recoveryActive = false
		t.recoveryDeadline = time.Time{}
		t.recoveryStartedAt = time.Time{}
		t.recoveryMu.Unlock()
		if recoveryActive {
			t.recordEvent("lane_send_recovery_escalated", "session", "session_close", &recoveryLane)
		}
		close(t.closed)
		for _, lane := range t.lanes {
			lane.workerMu.Lock()
			worker := lane.worker
			lane.worker = nil
			lane.workerMu.Unlock()
			if worker != nil {
				worker.close()
			}
		}
		t.callbackMu.RLock()
		callback := t.onClose
		t.callbackMu.RUnlock()
		if callback != nil {
			callback()
		}
	})
	return nil
}

// closeAsync is used by terminal conditions discovered while SendData owns a
// per-flow lock. Closing inline would invoke RelayBridge's close callback and
// could re-enter SendData for the same flow before that lock is released.
func (t *ParasiteTunnel) closeAsync() {
	go func() { _ = t.Close() }()
}

func (l *kcpLane) updateLoop() {
	ticker := time.NewTicker(laneKCPUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			l.kcp.Update()
			l.mu.Unlock()
		case <-l.parent.closed:
			return
		}
	}
}

func (t *ParasiteTunnel) reorderWatchLoop() {
	ticker := time.NewTicker(laneReorderCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			t.expireReorderGaps(now)
		case <-t.closed:
			return
		}
	}
}

func (t *ParasiteTunnel) expireReorderGaps(now time.Time) bool {
	expired := false
	t.deliverMu.Lock()
	for connID, state := range t.receiveFlows {
		if !state.gapSince.IsZero() && now.Sub(state.gapSince) >= t.reorderGapTimeout {
			if state.unordered {
				for _, pending := range state.pending {
					_, msgType, ok := relayFrameIdentity(pending)
					if ok && terminalRelayMessage(msgType) {
						t.deliverFrameLocked(connID, pending)
						break
					}
				}
				delete(t.receiveFlows, connID)
				t.recordEvent("lane_udp_reorder_timeout", "lane", "sequence_gap", nil)
				continue
			}
			expired = true
			break
		}
	}
	t.deliverMu.Unlock()
	if expired {
		t.recordEvent("lane_reorder_timeout", "lane", "sequence_gap", nil)
		_ = t.Close()
	}
	return expired
}
