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
	laneKCPControlReserve    = 8
	laneKCPInitialAdmission  = 64
	laneKCPMinimumAdmission  = 48
	laneKCPMaximumAdmission  = laneKCPSendWindow
	laneKCPOutputBacklog     = laneKCPSendWindow
	laneKCPMaxFragments      = 255
	laneKCPUpdateInterval    = 10 * time.Millisecond
	laneKCPFastResend        = 2
	laneKCPNoCongestion      = 1
	laneKCPReceiveBuffer     = 32 * 1024
	laneSendQueueDepth       = 96
	laneReorderFrameLimit    = 4096
	laneSendRetryInterval    = 2 * time.Millisecond
	laneDeliverySampleWindow = 500 * time.Millisecond
	laneDeliveryWindowGain   = 3.0
	laneSendStallTimeout     = 4 * time.Second
	laneAckStallTimeout       = 4 * time.Second
	laneAckStallMaximum       = 12 * time.Second
	defaultLaneRecoveryGrace = 6 * time.Second
	laneReorderGapTimeout    = 15 * time.Second
	laneReorderCheckInterval = time.Second
	workerHeartbeatInterval  = 5 * time.Second
	workerLivenessTimeout    = 20 * time.Second
	workerWriteTimeout       = 5 * time.Second
	workerRecycleWriteTimeout = 500 * time.Millisecond
	workerStaleReplacement   = 2 * workerHeartbeatInterval
)

var workerHeartbeat = [8]byte{'H', 'C', 'V', 'K', 'H', 'B', 2, 0}
var workerRecycle = [8]byte{'H', 'C', 'V', 'K', 'R', 'C', 1, 0}
var workerRecycleControl = [8]byte{'H', 'C', 'V', 'K', 'R', 'C', 2, 0}
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
	laneOwner    atomic.Uint32
	abortStarted atomic.Bool
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
	closed       bool
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
	writeMu     sync.Mutex
	lastInbound atomic.Int64
}

func (w *laneWorker) close() {
	w.closeOnce.Do(func() {
		close(w.done)
		_ = w.conn.Close()
	})
}

type kcpLane struct {
	id            uint16
	parent        *ParasiteTunnel
	mu            sync.Mutex
	kcp           *kcp.KCP
	recvBuf       []byte
	outputPending []queuedSegment
	outputReady   chan struct{}

	workerMu sync.RWMutex
	worker   *laneWorker

	metrics     *telemetry.Accumulator
	kcpSent     map[uint32]kcpSentSegment
	kcpSRTTMS   float64
	kcpRTTVARMS float64
	kcpLastUNA  uint32
	kcpHasUNA   bool
	flowCount   atomic.Int64

	admissionWindow       int
	deliveryRateBPS       float64
	deliverySampleAt      time.Time
	deliveryAckedSegments uint64
	deliveryOutSegments   uint64
	deliveryRetrans       uint64
	pressureSince         time.Time
	lastAckProgress       atomic.Int64
	availabilityEpoch     atomic.Uint64
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
	laneRecoveryGrace time.Duration
	reorderGapTimeout time.Duration
	recoveryMu        sync.Mutex
	recoveryActive    bool
	recoveryLane      uint16
	recoveryDeadline  time.Time
	recoveryStartedAt time.Time
	recoveryCooldown  time.Duration
	recoveryReadyAt   time.Time

	telemetryMu             sync.RWMutex
	telemetryCollectionMu   sync.Mutex
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
	tunnel.laneRecoveryGrace = defaultLaneRecoveryGrace
	tunnel.recoveryCooldown = laneSendStallTimeout
	tunnel.reorderGapTimeout = laneReorderGapTimeout
	for index := 0; index < LaneCount; index++ {
		lane := &kcpLane{
			id:               uint16(index),
			parent:           tunnel,
			recvBuf:          make([]byte, laneKCPReceiveBuffer),
			outputReady:      make(chan struct{}, 1),
			metrics:          telemetry.NewAccumulator(),
			kcpSent:          make(map[uint32]kcpSentSegment),
			admissionWindow:  laneKCPInitialAdmission,
			deliverySampleAt: time.Now(),
		}
		lane.metrics.SetCounterParent(metrics)
		lane.lastAckProgress.Store(time.Now().UnixNano())
		laneID := lane.id
		lane.kcp = kcp.NewKCP(laneConversation(seed, laneID), func(buffer []byte, size int) {
			if size > 0 {
				lane.stageSegment(buffer[:size])
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
		go lane.outputLoop()
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
		lane, sent := t.sendEncoded(encoded, true, t.preferredControlLane(), true)
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
	lane, sent := t.sendEncoded(encoded, true, state.preferredLane(), priorityRelayMessage(msgType))
	if !sent {
		if !state.unordered && state.abortStarted.CompareAndSwap(false, true) {
			go t.finishOrderedFlowAbort(connID, "lane_flow_send_timeout")
		}
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
		lane, sent := t.sendEncoded(encoded, false, t.preferredControlLane(), true)
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
	lane, sent := t.sendEncoded(encoded, false, state.preferredLane(), priorityRelayMessage(msgType))
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
	t.telemetryCollectionMu.Lock()
	wasActive := t.metrics.CollectionActive()
	t.metrics.SetCollectionActive(active)
	for _, lane := range t.lanes {
		lane.metrics.SetCollectionActive(active)
		if active && !wasActive {
			lane.mu.Lock()
			lane.kcpSent = make(map[uint32]kcpSentSegment)
			lane.kcpSRTTMS = 0
			lane.kcpRTTVARMS = 0
			lane.kcpLastUNA = 0
			lane.kcpHasUNA = false
			lane.deliverySampleAt = time.Now()
			lane.deliveryAckedSegments = 0
			lane.deliveryOutSegments = 0
			lane.deliveryRetrans = 0
			lane.mu.Unlock()
		}
	}
	t.telemetryCollectionMu.Unlock()
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
	lane.availabilityEpoch.Add(1)
	lane.workerMu.Unlock()
	lane.mu.Lock()
	lane.pressureSince = time.Time{}
	lane.lastAckProgress.Store(time.Now().UnixNano())
	lane.mu.Unlock()
	select {
	case lane.outputReady <- struct{}{}:
	default:
	}
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
		if bytes.Equal(buffer[:n], workerRecycle[:]) {
			workerID := w.id
			w.metrics.RecordEvent("lane_peer_recycle", "lane", "peer_request", &workerID)
			w.parent.removeWorker(w)
			return
		}
		if workerID, epoch, ok := decodeWorkerRecycleControl(buffer[:n]); ok {
			w.parent.recyclePeerWorker(workerID, epoch)
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
			w.metrics.Set(telemetry.WorkerWriteLatencyMS, float64(time.Since(started))/float64(time.Millisecond))
			delay := started.Sub(segment.enqueuedAt)
			w.metrics.Set(telemetry.WorkerOutputQueueDelayMS, float64(delay)/float64(time.Millisecond))
			if delay >= 2*laneKCPUpdateInterval {
				w.metrics.AddHot(telemetry.WorkerOutputQueueLateTotal, 1)
				w.metrics.AddHotMonotonic(telemetry.KCPSendBlockedSecondsTotal, delay.Seconds())
			}
			// Record KCP transmission at the successful physical write, not when
			// kcp.Update merely made the segment ready. This keeps RTT and retry
			// attribution aligned with TURN/DTLS instead of post-KCP queue time.
			w.lane.mu.Lock()
			w.lane.observeKCPOutput(segment.payload)
			w.lane.mu.Unlock()
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
	return w.writeWithTimeout(payload, workerWriteTimeout)
}

func (w *laneWorker) writeWithTimeout(payload []byte, timeout time.Duration) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err := w.conn.Write(payload)
	_ = w.conn.SetWriteDeadline(time.Time{})
	return err
}

func (w *laneWorker) writeRecycleControl(payload []byte) bool {
	deadline := time.Now().Add(workerRecycleWriteTimeout)
	for !w.writeMu.TryLock() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(laneSendRetryInterval)
	}
	defer w.writeMu.Unlock()
	_ = w.conn.SetWriteDeadline(deadline)
	n, err := w.conn.Write(payload)
	_ = w.conn.SetWriteDeadline(time.Time{})
	return err == nil && n == len(payload)
}

func encodeWorkerRecycleControl(workerID uint16, epoch uint64) []byte {
	payload := make([]byte, 18)
	copy(payload[:8], workerRecycleControl[:])
	binary.BigEndian.PutUint16(payload[8:10], workerID)
	binary.BigEndian.PutUint64(payload[10:18], epoch)
	return payload
}

func decodeWorkerRecycleControl(payload []byte) (uint16, uint64, bool) {
	if len(payload) != 18 || !bytes.Equal(payload[:8], workerRecycleControl[:]) {
		return 0, 0, false
	}
	workerID := binary.BigEndian.Uint16(payload[8:10])
	if workerID >= LaneCount {
		return 0, 0, false
	}
	return workerID, binary.BigEndian.Uint64(payload[10:18]), true
}

// requestPeerWorkerRecycle routes the generation-fenced recycle request over a
// different, healthy VK call first. The affected DTLS writer is precisely the
// resource that may be wedged, so using it as the only control path leaves the
// remote half alive until its much slower liveness timeout.
func (t *ParasiteTunnel) requestPeerWorkerRecycle(target *laneWorker) bool {
	payload := encodeWorkerRecycleControl(target.id, target.epoch)
	type candidate struct {
		worker *laneWorker
		depth  int
	}
	candidates := make([]candidate, 0, LaneCount-1)
	for _, lane := range t.lanes {
		if lane.id == target.id {
			continue
		}
		lane.workerMu.RLock()
		worker := lane.worker
		if worker != nil {
			candidates = append(candidates, candidate{worker: worker, depth: len(worker.sendQueue)})
		}
		lane.workerMu.RUnlock()
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].depth < candidates[j].depth })
	for _, current := range candidates {
		if current.worker.writeRecycleControl(payload) {
			workerID := target.id
			t.recordEvent("lane_peer_recycle_requested", "lane", "alternate_worker", &workerID)
			return true
		}
	}
	if target.writeRecycleControl(payload) {
		workerID := target.id
		t.recordEvent("lane_peer_recycle_requested", "lane", "target_worker", &workerID)
		return true
	}
	workerID := target.id
	t.recordEvent("lane_peer_recycle_unavailable", "lane", "control_write_timeout", &workerID)
	return false
}

func (t *ParasiteTunnel) recyclePeerWorker(workerID uint16, epoch uint64) {
	lane := t.lanes[workerID]
	lane.workerMu.RLock()
	worker := lane.worker
	stale := worker != nil && worker.epoch <= epoch
	lane.workerMu.RUnlock()
	if !stale {
		return
	}
	t.recordEvent("lane_peer_recycle", "lane", "peer_request", &workerID)
	t.removeWorker(worker)
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

// stageSegment is called by kcp-go while lane.mu is held. It must therefore
// never wait for TURN/DTLS output: doing so blocks Input on the same mutex and
// creates a self-amplifying ACK starvation/retransmission loop. The dedicated
// output pump is the only consumer and may block without stopping KCP input.
func (l *kcpLane) stageSegment(segment []byte) {
	l.outputPending = append(l.outputPending, queuedSegment{
		payload:    append([]byte(nil), segment...),
		enqueuedAt: time.Now(),
	})
	select {
	case l.outputReady <- struct{}{}:
	default:
	}
}

func (l *kcpLane) outputLoop() {
	retry := time.NewTicker(laneSendRetryInterval)
	defer retry.Stop()
	for {
		select {
		case <-l.outputReady:
		case <-retry.C:
		case <-l.parent.closed:
			return
		}
		for {
			l.mu.Lock()
			if len(l.outputPending) == 0 {
				l.mu.Unlock()
				break
			}
			segment := l.outputPending[0]
			l.mu.Unlock()

			l.workerMu.RLock()
			worker := l.worker
			l.workerMu.RUnlock()
			if worker == nil {
				break
			}
			select {
			case worker.sendQueue <- segment:
				l.mu.Lock()
				if len(l.outputPending) > 0 {
					l.outputPending[0] = queuedSegment{}
					l.outputPending = l.outputPending[1:]
				}
				l.mu.Unlock()
			case <-worker.done:
				// Keep the segment staged for the replacement worker. KCP framing
				// belongs to the logical lane, not to one TURN allocation.
				continue
			case <-l.parent.closed:
				return
			}
		}
	}
}

func (l *kcpLane) inputSegment(segment []byte) {
	lockStarted := time.Now()
	l.mu.Lock()
	if waited := time.Since(lockStarted); waited >= laneSendRetryInterval {
		l.metrics.AddHotMonotonic(telemetry.KCPMutexBlockedSecondsTotal, waited.Seconds())
	}
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
	if state.closed {
		return
	}
	if sequence < state.nextSequence {
		return
	}
	if sequence > state.nextSequence {
		if sequence-state.nextSequence > laneReorderFrameLimit || len(state.pending) >= laneReorderFrameLimit {
			state.closed = true
			state.pending = nil
			state.gapSince = time.Time{}
			go t.finishOrderedFlowAbort(connID, "lane_reorder_overflow")
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
	lane := t.lanes[t.controlLaneID]
	if active, queueDepth := lane.workerState(); !active || queueDepth >= laneSendQueueDepth/2 {
		return nil
	}
	if !lane.mu.TryLock() {
		return nil
	}
	healthy := len(lane.outputPending) < laneKCPOutputBacklog/2 &&
		lane.kcp.WaitSnd() < lane.admissionLimitLocked(true)
	lane.mu.Unlock()
	if !healthy {
		return nil
	}
	return &t.controlLaneID
}

func (t *ParasiteTunnel) commitFlowFrame(connID uint32, msgType byte, state *sendFlowState, lane *kcpLane) {
	if !state.unordered && !state.laneAssigned {
		state.laneID = lane.id
		state.laneOwner.Store(uint32(lane.id) + 1)
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

func (t *ParasiteTunnel) sendEncoded(encoded []byte, wait bool, preferred *uint16, priority bool) (*kcpLane, bool) {
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
	if lane := t.trySendEncoded(encoded, required, preferred, priority); lane != nil {
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
			if lane := t.trySendEncoded(encoded, required, preferred, priority); lane != nil {
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

func (t *ParasiteTunnel) trySendEncoded(encoded []byte, required int, preferred *uint16, priority bool) *kcpLane {
	// An ordered relay flow is owned by exactly one KCP loss domain. Spilling
	// one TCP byte stream across independent calls converts a loss on either
	// call into cross-lane head-of-line blocking and can no longer be recovered
	// safely without an explicit migration fence.
	if preferred != nil && int(*preferred) < LaneCount {
		if t.trySendEncodedOnLane(t.lanes[*preferred], encoded, required, priority) {
			return t.lanes[*preferred]
		}
		return nil
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
		pendingLimit := lane.admissionLimitLocked(priority)
		outputDepth := len(lane.outputPending)
		lane.mu.Unlock()
		if waitSnd+required > pendingLimit || outputDepth+queueDepth+required > laneKCPOutputBacklog+laneSendQueueDepth {
			continue
		}
		score := float64(waitSnd*4+queueDepth*2) + rtt/10 + float64(lane.flowCount.Load())
		candidates = append(candidates, laneCandidate{lane: lane, score: score})
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].score < candidates[j].score })
	for _, candidate := range candidates {
		lane := candidate.lane
		if t.trySendEncodedOnLane(lane, encoded, required, priority) {
			return lane
		}
	}
	return nil
}

func (t *ParasiteTunnel) trySendEncodedOnLane(lane *kcpLane, encoded []byte, required int, priority bool) bool {
	active, queueDepth := lane.workerState()
	if !active || !lane.mu.TryLock() {
		return false
	}
	defer lane.mu.Unlock()
	if len(lane.outputPending)+queueDepth+required > laneKCPOutputBacklog+laneSendQueueDepth {
		return false
	}
	pendingLimit := lane.admissionLimitLocked(priority)
	if lane.kcp.WaitSnd()+required > pendingLimit {
		return false
	}
	// The lane update loop flushes within 10 ms. Calling Update synchronously
	// here lets a blocked TURN write hold both the KCP mutex and RelayBridge's
	// per-flow send path, preventing the stall timer from recovering the lane.
	return lane.kcp.Send(encoded) >= 0
}

func (l *kcpLane) admissionLimitLocked(priority bool) int {
	limit := l.admissionWindow
	if limit < laneKCPMinimumAdmission {
		limit = laneKCPMinimumAdmission
	}
	if limit > laneKCPMaximumAdmission {
		limit = laneKCPMaximumAdmission
	}
	if !priority {
		limit -= laneKCPControlReserve
		if limit < 1 {
			limit = 1
		}
	}
	return limit
}

func (l *kcpLane) ackStallTimeoutLocked() time.Duration {
	timeout := laneAckStallTimeout
	// A high-latency TURN path needs several measured retransmission windows
	// before lack of ACK progress is evidence of a dead worker. The four-second
	// floor still reacts quickly on normal 50-100 ms paths, while the cap avoids
	// recreating the former 20-second dead connection.
	if adaptive := 8 * l.estimatedKCPRTO(); adaptive > timeout {
		timeout = adaptive
	}
	if timeout > laneAckStallMaximum {
		return laneAckStallMaximum
	}
	return timeout
}

func (t *ParasiteTunnel) recoverStalledLane(preferred *uint16) (uint16, laneRecoveryResult) {
	t.recoveryMu.Lock()
	now := time.Now()
	if t.recoveryActive {
		if now.After(t.recoveryDeadline) {
			workerID := t.recoveryLane
			t.recoveryActive = false
			t.recoveryDeadline = time.Time{}
			t.recoveryStartedAt = time.Time{}
			t.recoveryMu.Unlock()
			t.abortLaneFlows(workerID, "lane_recovery_timeout")
			return workerID, laneRecoveryTimedOut
		}
		workerID := t.recoveryLane
		t.recoveryMu.Unlock()
		return workerID, laneRecoveryInProgress
	}
	// Senders that were already waiting when a lane recovered retain their own
	// stall timers. Without a cooldown, those stale timers immediately recycle
	// the fresh worker even though its KCP backlog has only just begun draining.
	if now.Before(t.recoveryReadyAt) {
		workerID := t.recoveryLane
		t.recoveryMu.Unlock()
		return workerID, laneRecoveryInProgress
	}

	var selected *laneWorker
	if preferred != nil && int(*preferred) < LaneCount {
		lane := t.lanes[*preferred]
		lane.workerMu.RLock()
		selected = lane.worker
		lane.workerMu.RUnlock()
		if selected == nil {
			t.recoveryMu.Unlock()
			return *preferred, laneRecoveryUnavailable
		}
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
		t.recoveryMu.Unlock()
		return 0, laneRecoveryUnavailable
	}
	workerID := selected.id
	t.recoveryActive = true
	t.recoveryLane = workerID
	t.recoveryStartedAt = now
	t.recoveryDeadline = now.Add(3 * t.sendStallTimeout)
	t.recoveryMu.Unlock()
	t.requestPeerWorkerRecycle(selected)
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
		t.recoveryReadyAt = time.Now().Add(t.recoveryCooldown)
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

func priorityRelayMessage(msgType byte) bool {
	switch msgType {
	case calltunnel.MsgData, calltunnel.MsgUDP, calltunnel.MsgUDPReply:
		return false
	default:
		return true
	}
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
	removed := false
	availabilityEpoch := uint64(0)
	if lane.worker == worker {
		lane.worker = nil
		removed = true
		availabilityEpoch = lane.availabilityEpoch.Add(1)
	}
	lane.workerMu.Unlock()
	worker.metrics.Set(telemetry.WorkerActive, 0)
	worker.metrics.Set(telemetry.WorkerSendQueueDepth, 0)
	worker.close()
	t.touch()
	if removed {
		workerID := worker.id
		t.recordEvent("lane_worker_detached", "lane", "transport_closed", &workerID)
		go t.abortLaneFlowsIfUnavailable(workerID, availabilityEpoch)
	}
}

func (t *ParasiteTunnel) abortLaneFlowsIfUnavailable(laneID uint16, availabilityEpoch uint64) {
	timer := time.NewTimer(t.laneRecoveryGrace)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.closed:
		return
	}
	lane := t.lanes[laneID]
	lane.workerMu.RLock()
	unavailable := lane.worker == nil && lane.availabilityEpoch.Load() == availabilityEpoch
	lane.workerMu.RUnlock()
	if !unavailable {
		return
	}
	t.abortLaneFlows(laneID, "lane_recovery_timeout")
}

func (t *ParasiteTunnel) abortLaneFlows(laneID uint16, reason string) {
	connIDs := make([]uint32, 0)
	laneOwner := uint32(laneID) + 1
	t.sendMu.Lock()
	for connID, state := range t.sendFlows {
		if state.laneOwner.Load() == laneOwner && state.abortStarted.CompareAndSwap(false, true) {
			connIDs = append(connIDs, connID)
		}
	}
	t.sendMu.Unlock()
	if len(connIDs) == 0 {
		return
	}
	t.recordEvent("lane_flows_aborted", "flow", reason, &laneID)
	for _, connID := range connIDs {
		t.finishOrderedFlowAbort(connID, "lane_flow_aborted")
	}
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
	t.metrics.Set(telemetry.KCPMaxPendingSegments, LaneCount*laneKCPMaximumAdmission)
	t.metrics.Set(telemetry.KCPUpdateIntervalMS, float64(laneKCPUpdateInterval/time.Millisecond))
	t.metrics.Set(telemetry.KCPFastResend, laneKCPFastResend)
	t.metrics.Set(telemetry.KCPCongestionControl, 0)
	t.metrics.Set(telemetry.WorkerSendQueueCapacity, LaneCount*laneSendQueueDepth)
	t.metrics.Set(telemetry.KCPOutputQueueCapacity, LaneCount*laneKCPOutputBacklog)
	t.metrics.Set(telemetry.WorkerHeartbeatIntervalMS, float64(workerHeartbeatInterval/time.Millisecond))
	t.metrics.Set(telemetry.WorkerLivenessTimeoutMS, float64(workerLivenessTimeout/time.Millisecond))
	t.metrics.Set(telemetry.OuterRTPPayloadType, rtpPayloadTypeVideo)
	waitSnd := 0
	maxRTT := 0.0
	maxRTO := 200.0
	queueDepth := 0
	outputDepth := 0
	admissionWindow := 0
	admissionRate := 0.0
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
		lane.metrics.Set(telemetry.KCPMaxPendingSegments, laneKCPMaximumAdmission)
		lane.metrics.Set(telemetry.LaneCount, 1)
		lane.metrics.Set(telemetry.LaneFlowCount, float64(lane.flowCount.Load()))
		lane.metrics.Set(telemetry.LaneAdmissionRateBPS, lane.deliveryRateBPS)
		lane.metrics.Set(telemetry.LaneAdmissionWindowSegments, float64(lane.admissionWindow))
		lane.metrics.Set(telemetry.KCPOutputQueueDepth, float64(len(lane.outputPending)))
		lane.metrics.Set(telemetry.KCPOutputQueueCapacity, laneKCPOutputBacklog)
		outputDepth += len(lane.outputPending)
		admissionWindow += lane.admissionWindow
		admissionRate += lane.deliveryRateBPS
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
	t.metrics.Set(telemetry.KCPOutputQueueDepth, float64(outputDepth))
	t.metrics.Set(telemetry.LaneAdmissionWindowSegments, float64(admissionWindow))
	t.metrics.Set(telemetry.LaneAdmissionRateBPS, admissionRate)
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
		lane.mu.Lock()
		outputDepth := float64(len(lane.outputPending))
		admissionWindow := float64(lane.admissionWindow)
		deliveryRate := lane.deliveryRateBPS
		lane.mu.Unlock()
		lane.metrics.Set(telemetry.WorkerActive, boolFloat(worker != nil))
		lane.metrics.Set(telemetry.WorkerSendQueueDepth, queueDepth)
		lane.metrics.Set(telemetry.KCPOutputQueueDepth, outputDepth)
		lane.metrics.Set(telemetry.KCPOutputQueueCapacity, laneKCPOutputBacklog)
		lane.metrics.Set(telemetry.LaneAdmissionWindowSegments, admissionWindow)
		lane.metrics.Set(telemetry.LaneAdmissionRateBPS, deliveryRate)
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
		t.recoveryReadyAt = time.Time{}
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
			now := time.Now()
			lockStarted := time.Now()
			l.mu.Lock()
			if waited := time.Since(lockStarted); waited >= laneSendRetryInterval {
				l.metrics.AddHotMonotonic(telemetry.KCPMutexBlockedSecondsTotal, waited.Seconds())
			}
			// Do not let kcp-go start another transmission burst while the
			// previous one is still waiting for the physical writer. Input keeps
			// running, so ACK progress can drain WaitSnd instead of being blocked
			// behind the saturated TURN path.
			outputDepth := len(l.outputPending)
			waitSnd := l.kcp.WaitSnd()
			if outputDepth < laneKCPOutputBacklog {
				l.kcp.Update()
			} else {
				l.metrics.AddHot(telemetry.KCPUpdateBackpressureTotal, 1)
			}
			pressure := outputDepth >= 3*laneKCPOutputBacklog/4 || waitSnd >= l.admissionLimitLocked(false)
			if pressure {
				if l.pressureSince.IsZero() {
					l.pressureSince = now
				}
			} else {
				l.pressureSince = time.Time{}
			}
			ackStallTimeout := l.ackStallTimeoutLocked()
			ackStalled := pressure && !l.pressureSince.IsZero() &&
				now.Sub(l.pressureSince) >= ackStallTimeout &&
				now.Sub(time.Unix(0, l.lastAckProgress.Load())) >= ackStallTimeout
			l.mu.Unlock()
			if ackStalled {
				workerID, recovery := l.parent.recoverStalledLane(&l.id)
				if recovery == laneRecoveryStarted {
					l.parent.recordEvent("lane_send_recovery", "lane", "ack_progress_timeout", &workerID)
				}
			}
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
	expired := make([]uint32, 0, 1)
	t.deliverMu.Lock()
	for connID, state := range t.receiveFlows {
		if state.closed || state.gapSince.IsZero() || now.Sub(state.gapSince) < t.reorderGapTimeout {
			continue
		}
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
		state.closed = true
		state.pending = nil
		state.gapSince = time.Time{}
		expired = append(expired, connID)
	}
	t.deliverMu.Unlock()
	for _, connID := range expired {
		t.finishOrderedFlowAbort(connID, "lane_reorder_timeout")
	}
	return len(expired) > 0
}

func (t *ParasiteTunnel) finishOrderedFlowAbort(connID uint32, event string) {
	t.metrics.AddHot(telemetry.FlowReorderAbortTotal, 1)
	t.recordEvent(event, "flow", "sequence_gap", nil)
	// Report a remote close only to the local RelayBridge. The affected TCP
	// connection is recreated by the proxy/application, while unrelated flows
	// and the other three calls remain usable.
	frame := calltunnel.EncodeFrame(connID, calltunnel.MsgClose, nil)
	t.callbackMu.RLock()
	callback := t.onData
	t.callbackMu.RUnlock()
	if callback != nil {
		callback(frame)
	}
	// A concurrent writer may still own the flow mutex while waiting for lane
	// recovery. Its bookkeeping must not delay delivery of the local close.
	go t.releaseSendFlow(connID)
}
