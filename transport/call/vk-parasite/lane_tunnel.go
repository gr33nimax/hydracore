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
	LaneCount             = 4
	laneKCPMTU            = 1000
	laneKCPSendWindow     = 128
	laneKCPReceiveWindow  = 128
	laneKCPMaxPending     = 256
	laneKCPUpdateInterval = 10 * time.Millisecond
	laneKCPFastResend     = 2
	laneKCPReceiveBuffer  = 32 * 1024
	laneSendQueueDepth    = 256
	laneReorderFrameLimit = 4096
	workerHeartbeatInterval = 15 * time.Second
	workerLivenessTimeout   = 60 * time.Second
	workerStaleReplacement  = 2 * workerHeartbeatInterval
)

var workerHeartbeat = [8]byte{'H', 'C', 'V', 'K', 'H', 'B', 2, 0}
var laneFrameMagic = [8]byte{'H', 'C', 'V', 'K', 'L', 'N', 4, 0}

type queuedSegment struct {
	payload    []byte
	enqueuedAt time.Time
}

type kcpSentSegment struct {
	sentAt        time.Time
	retransmitted bool
}

type sendFlowState struct {
	nextSequence uint64
	laneMask     uint8
}

type receiveFlowState struct {
	nextSequence uint64
	pending      map[uint64][]byte
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
	controlSendSequence uint64
	receiveFlows        map[uint32]*receiveFlowState
	nextLane            atomic.Uint32

	callbackMu sync.RWMutex
	deliverMu  sync.Mutex
	onData     func([]byte)
	onClose    func()

	lastActivity atomic.Int64
	metrics      *telemetry.Accumulator

	telemetryMu             sync.RWMutex
	onTelemetryControl      func(time.Duration)
	onTelemetryClientRecord func([]byte)
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
	tunnel.lastActivity.Store(time.Now().UnixNano())
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
			if size > 0 {
				lane.observeKCPOutput(buffer[:size])
				lane.dispatchSegment(buffer[:size])
			}
		})
		lane.kcp.NoDelay(1, int(laneKCPUpdateInterval/time.Millisecond), laneKCPFastResend, 1)
		lane.kcp.WndSize(laneKCPSendWindow, laneKCPReceiveWindow)
		lane.kcp.SetMtu(laneKCPMTU)
		tunnel.lanes[index] = lane
	}
	go tunnel.updateLoop()
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
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	lane, encoded, connID, msgType := t.prepareFrame(frame)
	blockedAt := time.Time{}
	for {
		select {
		case <-t.closed:
			return
		default:
		}
		lane.mu.Lock()
		if lane.kcp.WaitSnd() < laneKCPMaxPending {
			if lane.kcp.Send(encoded) >= 0 {
				lane.kcp.Update()
				lane.mu.Unlock()
				if !blockedAt.IsZero() {
					lane.metrics.AddHotMonotonic(telemetry.KCPSendBlockedSecondsTotal, time.Since(blockedAt).Seconds())
				}
				t.touch()
				t.commitFrame(connID, msgType, lane)
				return
			}
		}
		lane.mu.Unlock()
		if blockedAt.IsZero() {
			blockedAt = time.Now()
		}
		time.Sleep(2 * time.Millisecond)
	}
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
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	lane, encoded, connID, msgType := t.prepareFrame(frame)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	if lane.kcp.WaitSnd() >= laneKCPMaxPending || lane.kcp.Send(encoded) < 0 {
		return false
	}
	lane.kcp.Update()
	if activity {
		t.touch()
	}
	t.commitFrame(connID, msgType, lane)
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
			if _, err := w.conn.Write(segment.payload); err != nil {
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
			if _, err := w.conn.Write(workerHeartbeat[:]); err != nil {
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

func (l *kcpLane) dispatchSegment(segment []byte) {
	l.workerMu.RLock()
	worker := l.worker
	l.workerMu.RUnlock()
	if worker == nil {
		l.parent.metrics.AddHot(telemetry.WorkerNoAvailableDropsTotal, 1)
		return
	}
	queued := queuedSegment{payload: append([]byte(nil), segment...), enqueuedAt: time.Now()}
	select {
	case worker.sendQueue <- queued:
	case <-worker.done:
		l.parent.metrics.AddHot(telemetry.WorkerNoAvailableDropsTotal, 1)
	default:
		worker.metrics.AddHot(telemetry.WorkerSendQueueDropsTotal, 1)
		l.parent.metrics.AddHot(telemetry.WorkerNoAvailableDropsTotal, 1)
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
		t.metrics.RecordEvent("lane_frame_rejected", "lane", "wire", nil)
		return
	}
	t.deliverMu.Lock()
	defer t.deliverMu.Unlock()
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
			t.metrics.RecordEvent("lane_reorder_overflow", "lane", "sequence_gap", nil)
			go t.Close()
			return
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
}

func (t *ParasiteTunnel) deliverFrameLocked(connID uint32, frame []byte) {
	if t.handleTelemetryMessage(frame) {
		return
	}
	_, msgType, _ := relayFrameIdentity(frame)
	t.callbackMu.RLock()
	callback := t.onData
	t.callbackMu.RUnlock()
	if callback != nil {
		callback(frame)
	}
	if connID != calltunnel.ControlConnID && terminalRelayMessage(msgType) {
		delete(t.receiveFlows, connID)
	}
}

func (t *ParasiteTunnel) prepareFrame(frame []byte) (*kcpLane, []byte, uint32, byte) {
	connID, msgType, ok := relayFrameIdentity(frame)
	if !ok {
		connID = calltunnel.ControlConnID
		msgType = 0
	}
	lane := t.bestLane()
	sequence := t.controlSendSequence
	if connID != calltunnel.ControlConnID {
		state := t.sendFlows[connID]
		if state == nil {
			state = &sendFlowState{}
			t.sendFlows[connID] = state
		}
		sequence = state.nextSequence
	}
	return lane, encodeLaneFrame(connID, sequence, frame), connID, msgType
}

func (t *ParasiteTunnel) commitFrame(connID uint32, msgType byte, lane *kcpLane) {
	if connID == calltunnel.ControlConnID {
		t.controlSendSequence++
		return
	}
	state := t.sendFlows[connID]
	if state == nil {
		return
	}
	state.nextSequence++
	bit := uint8(1 << lane.id)
	if state.laneMask&bit == 0 {
		state.laneMask |= bit
		lane.flowCount.Add(1)
	}
	if terminalRelayMessage(msgType) {
		delete(t.sendFlows, connID)
		for laneID := uint16(0); laneID < LaneCount; laneID++ {
			if state.laneMask&(1<<laneID) != 0 {
				t.lanes[laneID].flowCount.Add(-1)
			}
		}
	}
}

func (t *ParasiteTunnel) bestLane() *kcpLane {
	start := int((t.nextLane.Add(1) - 1) % LaneCount)
	best := t.lanes[start]
	bestScore := best.score()
	for offset := 1; offset < LaneCount; offset++ {
		lane := t.lanes[(start+offset)%LaneCount]
		if score := lane.score(); score < bestScore {
			best = lane
			bestScore = score
		}
	}
	return best
}

func (l *kcpLane) score() float64 {
	l.workerMu.RLock()
	worker := l.worker
	queueDepth := 0
	if worker != nil {
		queueDepth = len(worker.sendQueue)
	}
	l.workerMu.RUnlock()
	activePenalty := 0.0
	if worker == nil {
		activePenalty = 1e9
	}
	l.mu.Lock()
	waitSnd := l.kcp.WaitSnd()
	rtt := l.kcpSRTTMS
	l.mu.Unlock()
	return activePenalty + float64(waitSnd*4+queueDepth*2) + rtt/10 + float64(l.flowCount.Load())
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
	waitSnd := 0
	maxRTT := 0.0
	maxRTO := 200.0
	queueDepth := 0
	for _, lane := range t.lanes {
		lane.mu.Lock()
		laneWait := lane.kcp.WaitSnd()
		waitSnd += laneWait
		rtt := lane.kcpSRTTMS
		rto := 200.0
		if rtt > 0 {
			rto = max(30, rtt+max(10, 4*lane.kcpRTTVARMS))
		}
		lane.metrics.Set(telemetry.KCPWaitSnd, float64(laneWait))
		lane.metrics.Set(telemetry.KCPRTTMS, rtt)
		lane.metrics.Set(telemetry.KCPRTOMS, rto)
		lane.metrics.Set(telemetry.KCPSendWindowSegments, laneKCPSendWindow)
		lane.metrics.Set(telemetry.KCPReceiveWindowSegments, laneKCPReceiveWindow)
		lane.metrics.Set(telemetry.KCPMaxPendingSegments, laneKCPMaxPending)
		lane.metrics.Set(telemetry.LaneCount, 1)
		lane.metrics.Set(telemetry.LaneFlowCount, float64(lane.flowCount.Load()))
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

func (t *ParasiteTunnel) touch() {
	t.lastActivity.Store(time.Now().UnixNano())
}

func (t *ParasiteTunnel) Close() error {
	t.closeOnce.Do(func() {
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

func (t *ParasiteTunnel) updateLoop() {
	ticker := time.NewTicker(laneKCPUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, lane := range t.lanes {
				lane.mu.Lock()
				lane.kcp.Update()
				lane.mu.Unlock()
			}
		case <-t.closed:
			return
		}
	}
}
