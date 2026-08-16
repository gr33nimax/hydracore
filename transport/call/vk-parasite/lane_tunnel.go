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

	HC "github.com/sagernet/sing-box/common/hydracore"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/logger"
	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	LaneCount                = 4
	laneKCPMTU               = 1000
	laneKCPSendWindow        = 512
	laneKCPReceiveWindow     = 512
	laneKCPMaxPending        = 1024
	laneKCPControlReserve    = 8
	laneKCPInitialAdmission  = 16
	laneKCPMinimumAdmission  = 8
	laneKCPMaximumAdmission  = 512
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
	lanePacingInitialBPS      = 1_500_000 / 8
	lanePacingMinimumBPS      = 256_000 / 8
	lanePacingMaximumBPS      = 25_000_000 / 8
	lanePacingBucketSegments  = 4
	lanePacingStartupGain     = 1.25
	lanePacingDecrease        = 0.80
	lanePacingSteadyGain      = 1.10
	lanePacingProbeGain       = 1.125
	lanePacingProbeInterval   = 8 * time.Second
	lanePacingProbeDuration   = time.Second
	laneResetRetryInterval    = 250 * time.Millisecond
	laneResetMinimumDeadline  = 4 * time.Second
	laneResetMaximumDeadline  = 12 * time.Second
	laneProbeInterval         = 250 * time.Millisecond
	laneProbeDeadline         = 10 * time.Second
	laneFrameHeaderSize       = 32
	laneSendStallTimeout     = 4 * time.Second
	laneAckStallTimeout       = 4 * time.Second
	laneAckStallMaximum       = 12 * time.Second
	defaultLaneRecoveryGrace = 6 * time.Second
	laneReorderGapTimeout    = 15 * time.Second
	laneReorderCheckInterval = time.Second
	flowReplayPerFlowLimit   = 512 * 1024
	flowReplaySessionLimit   = 8 * 1024 * 1024
	udpFlowletMaximumDwell   = 15 * time.Millisecond
	udpFlowletMaximumBytes   = 64 * 1024
	workerHeartbeatInterval  = 5 * time.Second
	workerLivenessTimeout    = 20 * time.Second
	workerWriteTimeout       = 5 * time.Second
	workerRecycleWriteTimeout = 500 * time.Millisecond
	workerStaleReplacement   = 2 * workerHeartbeatInterval
)

var workerHeartbeat = [8]byte{'H', 'C', 'V', 'K', 'H', 'B', 2, 0}
var laneResetControlMagic = [8]byte{'H', 'C', 'V', 'K', 'R', 'S', 9, 0}
var laneFrameMagic = [8]byte{'H', 'C', 'V', 'K', 'L', 'N', 9, 0}
var laneProbeMagic = [8]byte{'H', 'C', 'V', 'K', 'P', 'R', 9, 0}

type laneState uint8

const (
	laneStateActive laneState = iota
	laneStateQuarantined
	laneStateResetting
	laneStateProbing
)

const (
	laneResetPrepare byte = 1
	laneResetACK     byte = 2
	laneResetCommit  byte = 3
	laneResetSuggest byte = 4
	laneProbeRequest byte = 1
	laneProbeACK     byte = 2
)

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
	replay       []flowReplayFrame
	replayBytes  int
	flowletStarted time.Time
	flowletBytes   int
}

type flowReplayFrame struct {
	sequence uint64
	frame    []byte
}

type receiveFlowState struct {
	nextSequence uint64
	pending      map[uint64][]byte
	commitSequence uint64
	commitLane     uint16
	commitPending  bool
	gapSince     time.Time
	unordered    bool
	closed       bool
}

type laneWorker struct {
	id          uint16
	epoch       uint64
	generation  uint64
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
	generation  uint64
	state       laneState
	flowCount   atomic.Int64

	admissionWindow       int
	deliveryRateBPS       float64
	deliverySampleAt      time.Time
	deliveryAckedSegments uint64
	deliveryOutSegments   uint64
	deliveryRetrans       uint64
	deliveryDemanded      bool
	applicationLimited    bool
	minRTTMS              float64
	pacingRateBPS         float64
	pacingTokens          float64
	pacingLastRefill      time.Time
	pacingStartup         bool
	pacingNextProbe       time.Time
	pacingProbeUntil      time.Time
	previousOutputDepth   int
	resetInFlight         bool
	pendingResetGeneration uint64
	resetAck              chan uint64
	resetStartedAt        time.Time
	probeStartedAt        time.Time
	probeLastSentAt       time.Time
	probeReceived         bool
	probeAcked            bool
	pressureSince         time.Time
	lastAckProgress       atomic.Int64
	availabilityEpoch     atomic.Uint64
}

type ParasiteTunnel struct {
	logger logger.ContextLogger
	seed   uint32
	lanes  [LaneCount]*kcpLane

	sendMu              sync.Mutex
	sendFlows           map[uint32]*sendFlowState
	controlSendMu       sync.Mutex
	controlSendSequence uint64
	controlLaneID       uint16
	controlLaneAssigned bool
	receiveFlows        map[uint32]*receiveFlowState
	nextLane            atomic.Uint32
	replayBytes         atomic.Int64
	migrationAcks       sync.Map

	callbackMu sync.RWMutex
	deliverMu  sync.Mutex
	onData     func([]byte)
	onClose    func()

	lastActivity          atomic.Int64
	lastProgress          atomic.Int64
	lastAggregateProgress atomic.Int64
	lastApplicationDemand atomic.Int64
	fullyAttached         atomic.Bool
	metrics               *telemetry.Accumulator

	sendStallTimeout  time.Duration
	laneRecoveryGrace time.Duration
	reorderGapTimeout time.Duration
	recoveryMu        sync.Mutex
	recoveryActive    bool
	recoveryLane      uint16
	recoveryDeadline  time.Time
	recoveryStartedAt  time.Time
	recoveryCooldown    time.Duration
	recoveryReadyAt     time.Time
	recoveryDeferred    uint8
	recoveryPending     uint8
	recoverySuggestedAt [LaneCount]time.Time
	recoveryCoordinator atomic.Bool
	sessionReplaceOnce  sync.Once

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
		seed:    seed,
		sendFlows:    make(map[uint32]*sendFlowState),
		receiveFlows: make(map[uint32]*receiveFlowState),
		metrics: metrics,
		closed:  make(chan struct{}),
	}
	now := time.Now().UnixNano()
	tunnel.lastActivity.Store(now)
	tunnel.lastProgress.Store(now)
	tunnel.lastAggregateProgress.Store(now)
	tunnel.recoveryCoordinator.Store(true)
	tunnel.sendStallTimeout = laneSendStallTimeout
	tunnel.laneRecoveryGrace = defaultLaneRecoveryGrace
	tunnel.recoveryCooldown = laneSendStallTimeout
	tunnel.reorderGapTimeout = laneReorderGapTimeout
	for index := 0; index < LaneCount; index++ {
		laneNow := time.Now()
		lane := &kcpLane{
			id:               uint16(index),
			parent:           tunnel,
			recvBuf:          make([]byte, laneKCPReceiveBuffer),
			outputReady:      make(chan struct{}, 1),
			metrics:          telemetry.NewAccumulator(),
			kcpSent:          make(map[uint32]kcpSentSegment),
			generation:       1,
			state:            laneStateActive,
			admissionWindow:  laneKCPInitialAdmission,
			deliverySampleAt: laneNow,
			minRTTMS:         0,
			pacingRateBPS:    lanePacingInitialBPS,
			pacingTokens:     float64(lanePacingBucketSegments * (laneKCPMTU - kcpHeaderSize)),
			pacingLastRefill: laneNow,
			pacingStartup:    true,
			pacingNextProbe:  laneNow.Add(lanePacingProbeInterval),
		}
		lane.applicationLimited = true
		lane.metrics.SetCounterParent(metrics)
		lane.lastAckProgress.Store(time.Now().UnixNano())
		lane.resetKCPLocked(1)
		tunnel.lanes[index] = lane
		go lane.updateLoop()
		go lane.outputLoop()
	}
	go tunnel.reorderWatchLoop()
	go tunnel.noProgressWatchLoop()
	return tunnel, nil
}

func laneConversation(seed uint32, laneID uint16) uint32 {
	return laneConversationGeneration(seed, laneID, 1)
}

func laneConversationGeneration(seed uint32, laneID uint16, generation uint64) uint32 {
	value := seed + 0x9e3779b9*uint32(laneID+1)
	value ^= uint32(generation)
	value ^= uint32(generation>>32) * 0x85ebca6b
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

func (l *kcpLane) resetKCPLocked(generation uint64) {
	l.generation = generation
	l.kcp = kcp.NewKCP(laneConversationGeneration(l.parent.seed, l.id, generation), func(buffer []byte, size int) {
		if size > 0 {
			l.stageSegment(buffer[:size])
		}
	})
	// KCP's own congestion controller remains disabled. Admission into KCP is
	// ACK-clocked below; delaying output after KCP has started its retransmission
	// clock would manufacture false RTOs.
	l.kcp.NoDelay(1, int(laneKCPUpdateInterval/time.Millisecond), laneKCPFastResend, laneKCPNoCongestion)
	l.kcp.WndSize(laneKCPSendWindow, laneKCPReceiveWindow)
	l.kcp.SetMtu(laneKCPMTU)
}

func (t *ParasiteTunnel) SendData(frame []byte) {
	if len(frame) == 0 {
		return
	}
	connID, msgType := frameIdentity(frame)
	if connID == calltunnel.ControlConnID {
		t.controlSendMu.Lock()
		defer t.controlSendMu.Unlock()
		encoded := encodeLaneFrameGeneration(0, connID, t.controlSendSequence, frame)
		lane, sent := t.sendEncoded(encoded, true, t.preferredControlLane(), true)
		if sent {
			t.controlLaneID = lane.id
			t.controlLaneAssigned = true
			t.controlSendSequence++
			t.touch()
		}
		return
	}
	if !priorityRelayMessage(msgType) {
		t.markApplicationDemand()
	}
	state := t.sendFlow(connID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return
	}
	state.initialize(msgType)
	for _, fragment := range fragmentTCPRelayFrame(frame, msgType) {
		if !state.unordered && !t.waitReplayCapacityLocked(state, len(fragment)) {
			if state.abortStarted.CompareAndSwap(false, true) {
				go t.finishOrderedFlowAbort(connID, "lane_flow_replay_backpressure")
			}
			return
		}
		sequence := state.nextSequence
		encoded := encodeLaneFrameGeneration(0, connID, sequence, fragment)
		lane, sent := t.sendEncoded(encoded, true, state.preferredLane(), priorityRelayFrame(fragment, msgType))
		if !sent {
			if !state.unordered && state.abortStarted.CompareAndSwap(false, true) {
				go t.finishOrderedFlowAbort(connID, "lane_flow_send_timeout")
			}
			return
		}
		t.touch()
		t.markProgress()
		t.commitFlowFrame(connID, msgType, state, lane, sequence, fragment)
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
	connID, msgType := frameIdentity(frame)
	if connID == calltunnel.ControlConnID {
		if !t.controlSendMu.TryLock() {
			return false
		}
		defer t.controlSendMu.Unlock()
		encoded := encodeLaneFrameGeneration(0, connID, t.controlSendSequence, frame)
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
	for _, fragment := range fragmentTCPRelayFrame(frame, msgType) {
		if !state.unordered && !t.replayCapacityAvailable(state, len(fragment)) {
			return false
		}
		sequence := state.nextSequence
		encoded := encodeLaneFrameGeneration(0, connID, sequence, fragment)
		lane, sent := t.sendEncoded(encoded, false, state.preferredLane(), priorityRelayFrame(fragment, msgType))
		if !sent {
			return false
		}
		if activity {
			t.touch()
		}
		t.markProgress()
		t.commitFlowFrame(connID, msgType, state, lane, sequence, fragment)
	}
	return true
}

func fragmentTCPRelayFrame(frame []byte, msgType byte) [][]byte {
	if msgType != calltunnel.MsgData || len(frame) <= 9 {
		return [][]byte{frame}
	}
	maximumPayload := 4*(laneKCPMTU-kcpHeaderSize) - laneFrameHeaderSize - 9
	if len(frame)-9 <= maximumPayload {
		return [][]byte{frame}
	}
	connID := binary.BigEndian.Uint32(frame[4:8])
	payload := frame[9:]
	fragments := make([][]byte, 0, (len(payload)+maximumPayload-1)/maximumPayload)
	for len(payload) > 0 {
		size := min(maximumPayload, len(payload))
		fragments = append(fragments, calltunnel.EncodeFrame(connID, calltunnel.MsgData, payload[:size]))
		payload = payload[size:]
	}
	return fragments
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

// SetRecoveryCoordinator selects the endpoint which is allowed to initiate a
// generation reset. The peer only suggests a lane, preventing the two ends
// from quarantining different lanes at the same time.
func (t *ParasiteTunnel) SetRecoveryCoordinator(coordinator bool) {
	t.recoveryCoordinator.Store(coordinator)
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
	return t.AddWorkerGenerationEpoch(id, t.LaneGeneration(id), epoch, conn)
}

func (t *ParasiteTunnel) AddWorkerGenerationEpoch(id uint16, generation, epoch uint64, conn net.Conn) (<-chan struct{}, error) {
	worker, err := t.reserveWorkerGeneration(id, generation, epoch, conn)
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
	return t.AttachWorkerGenerationEpoch(id, t.LaneGeneration(id), epoch, conn, beforeStart)
}

func (t *ParasiteTunnel) AttachWorkerGenerationEpoch(id uint16, generation, epoch uint64, conn net.Conn, beforeStart func() error) (<-chan struct{}, error) {
	worker, err := t.reserveWorkerGeneration(id, generation, epoch, conn)
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
	return t.reserveWorkerGeneration(id, t.LaneGeneration(id), epoch, conn)
}

func (t *ParasiteTunnel) reserveWorkerGeneration(id uint16, generation, epoch uint64, conn net.Conn) (*laneWorker, error) {
	select {
	case <-t.closed:
		return nil, errors.New("call vk_parasite: session already closed")
	default:
	}
	if id >= LaneCount {
		return nil, errors.New("call vk_parasite: invalid lane id")
	}
	lane := t.lanes[id]
	lane.mu.Lock()
	if generation == 0 || generation != lane.generation || (lane.state != laneStateActive && lane.state != laneStateProbing) {
		lane.mu.Unlock()
		return nil, errors.New("call vk_parasite: stale lane generation")
	}
	lane.mu.Unlock()
	worker := &laneWorker{
		id:        id,
		epoch:     epoch,
		generation: generation,
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
	if t.ActiveWorkers() == LaneCount {
		t.fullyAttached.Store(true)
	}
	lane.mu.Lock()
	lane.pressureSince = time.Time{}
	lane.lastAckProgress.Store(time.Now().UnixNano())
	lane.mu.Unlock()
	select {
	case lane.outputReady <- struct{}{}:
	default:
	}
	worker.metrics.Set(telemetry.WorkerActive, 1)
	if replaced != nil {
		replaced.close()
	}
	t.touch()
	lane.mu.Lock()
	probing := lane.state == laneStateProbing
	lane.mu.Unlock()
	if probing {
		go t.sendLaneProbe(id)
	}
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
		if kind, laneID, generation, ok := decodeLaneResetControl(buffer[:n]); ok {
			w.parent.handleLaneResetControl(kind, laneID, generation)
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

func encodeLaneResetControl(kind byte, laneID uint16, generation uint64) []byte {
	payload := make([]byte, 19)
	copy(payload[:8], laneResetControlMagic[:])
	payload[8] = kind
	binary.BigEndian.PutUint16(payload[9:11], laneID)
	binary.BigEndian.PutUint64(payload[11:19], generation)
	return payload
}

func decodeLaneResetControl(payload []byte) (byte, uint16, uint64, bool) {
	if len(payload) != 19 || !bytes.Equal(payload[:8], laneResetControlMagic[:]) {
		return 0, 0, 0, false
	}
	kind := payload[8]
	laneID := binary.BigEndian.Uint16(payload[9:11])
	generation := binary.BigEndian.Uint64(payload[11:19])
	if laneID >= LaneCount || generation == 0 || kind < laneResetPrepare || kind > laneResetSuggest {
		return 0, 0, 0, false
	}
	return kind, laneID, generation, true
}

func (t *ParasiteTunnel) broadcastLaneResetControl(kind byte, laneID uint16, generation uint64) int {
	payload := encodeLaneResetControl(kind, laneID, generation)
	type candidate struct {
		worker *laneWorker
		depth  int
	}
	candidates := make([]candidate, 0, LaneCount)
	var targetFallback *laneWorker
	for _, lane := range t.lanes {
		lane.mu.Lock()
		healthy := lane.state == laneStateActive || lane.state == laneStateProbing
		lane.mu.Unlock()
		lane.workerMu.RLock()
		worker := lane.worker
		if worker != nil && healthy {
			candidates = append(candidates, candidate{worker: worker, depth: len(worker.sendQueue)})
		} else if worker != nil && lane.id == laneID {
			targetFallback = worker
		}
		lane.workerMu.RUnlock()
	}
	if len(candidates) == 0 && targetFallback != nil {
		candidates = append(candidates, candidate{worker: targetFallback, depth: len(targetFallback.sendQueue)})
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].depth < candidates[j].depth })
	results := make(chan bool, len(candidates))
	for _, current := range candidates {
		go func(worker *laneWorker) { results <- worker.writeRecycleControl(payload) }(current.worker)
	}
	sent := 0
	for range candidates {
		if <-results {
			sent++
		}
	}
	return sent
}

func (t *ParasiteTunnel) handleLaneResetControl(kind byte, laneID uint16, generation uint64) {
	lane := t.lanes[laneID]
	switch kind {
	case laneResetSuggest:
		if !t.recoveryCoordinator.Load() {
			return
		}
		lane.mu.Lock()
		valid := lane.state == laneStateActive && generation == lane.generation+1
		lane.mu.Unlock()
		if valid {
			go t.recoverStalledLane(&laneID)
		}
	case laneResetPrepare:
		lane.mu.Lock()
		current := lane.generation
		newPrepare := false
		if generation > current+1 {
			lane.mu.Unlock()
			t.replaceSession("lane_generation_jump", &laneID)
			return
		}
		if generation == current+1 {
			newPrepare = true
			lane.state = laneStateQuarantined
			lane.pendingResetGeneration = generation
			if lane.resetStartedAt.IsZero() {
				lane.resetStartedAt = time.Now()
			}
			lane.metrics.AddHot(telemetry.LaneResetRequestTotal, 1)
		}
		lane.mu.Unlock()
		t.recoveryMu.Lock()
		t.recoverySuggestedAt[laneID] = time.Time{}
		t.recoveryMu.Unlock()
		if newPrepare {
			t.migrateLaneFlows(laneID, "peer_generation_reset")
		}
		if generation >= current {
			t.broadcastLaneResetControl(laneResetACK, laneID, generation)
			lane.metrics.AddHot(telemetry.LaneResetAckTotal, 1)
		}
		if t.quarantinedLaneCount() >= 3 {
			t.replaceSession("three_quarantined_lanes", &laneID)
		}
	case laneResetACK:
		lane.mu.Lock()
		ack := lane.resetAck
		valid := lane.resetInFlight && generation == lane.pendingResetGeneration
		lane.mu.Unlock()
		if valid && ack != nil {
			select {
			case ack <- generation:
			default:
			}
			lane.metrics.AddHot(telemetry.LaneResetAckTotal, 1)
		}
	case laneResetCommit:
		lane.mu.Lock()
		current := lane.generation
		valid := generation == current+1 && (lane.pendingResetGeneration == 0 || lane.pendingResetGeneration == generation)
		lane.mu.Unlock()
		if valid {
			lane.metrics.AddHot(telemetry.LaneResetCommitTotal, 1)
			t.resetLaneGeneration(laneID, generation, "peer_commit")
		}
	}
}

// Kept as a narrow compatibility shim for callers that still express recovery
// in worker epochs. Wire v7 always turns that request into a generation reset.
func (t *ParasiteTunnel) recyclePeerWorker(workerID uint16, epoch uint64) {
	if workerID >= LaneCount {
		return
	}
	lane := t.lanes[workerID]
	lane.workerMu.RLock()
	worker := lane.worker
	valid := worker != nil && worker.epoch <= epoch
	lane.workerMu.RUnlock()
	if valid {
		t.recoverStalledLane(&workerID)
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
	expectedConversation := laneConversationGeneration(l.parent.seed, l.id, l.generation)
	if len(segment) < 4 || binary.LittleEndian.Uint32(segment[:4]) != expectedConversation {
		l.metrics.AddHot(telemetry.LaneStaleGenerationDropsTotal, 1)
		l.mu.Unlock()
		return
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
	l.parent.maybeCompleteLaneProbe(l.id)
	for _, message := range messages {
		l.parent.deliver(l.id, message)
	}
}

func (t *ParasiteTunnel) deliver(laneID uint16, message []byte) {
	generation, connID, sequence, frame, ok := decodeLaneFrameGeneration(message)
	if !ok {
		t.recordEvent("lane_frame_rejected", "lane", "wire", nil)
		return
	}
	lane := t.lanes[laneID]
	lane.mu.Lock()
	currentGeneration := lane.generation
	lane.mu.Unlock()
	if generation != currentGeneration {
		lane.metrics.AddHot(telemetry.LaneStaleGenerationDropsTotal, 1)
		return
	}
	if t.handleLaneProbe(laneID, generation, frame) {
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
		t.sendFlowProgress(connID, state.nextSequence)
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
	t.sendFlowProgress(connID, state.nextSequence)
	t.ackFlowCommitIfReady(connID, state)
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
	if t.handleFlowControlMessage(frame) {
		return
	}
	_, msgType, _ := relayFrameIdentity(frame)
	if connID != calltunnel.ControlConnID {
		t.markProgress()
		t.markAggregateProgress()
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
	if !state.laneAssigned {
		return nil
	}
	if state.unordered && (time.Since(state.flowletStarted) >= udpFlowletMaximumDwell || state.flowletBytes >= udpFlowletMaximumBytes) {
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
	healthy := lane.state == laneStateActive &&
		len(lane.outputPending) < laneKCPOutputBacklog/2 &&
		lane.kcp.WaitSnd() < lane.admissionLimitLocked(true)
	lane.mu.Unlock()
	if !healthy {
		return nil
	}
	return &t.controlLaneID
}

func (t *ParasiteTunnel) commitFlowFrame(connID uint32, msgType byte, state *sendFlowState, lane *kcpLane, sequence uint64, frame []byte) {
	if !state.unordered && !state.laneAssigned {
		state.laneID = lane.id
		state.laneOwner.Store(uint32(lane.id) + 1)
		state.laneAssigned = true
	}
	if state.unordered {
		if !state.laneAssigned || state.laneID != lane.id || time.Since(state.flowletStarted) >= udpFlowletMaximumDwell || state.flowletBytes >= udpFlowletMaximumBytes {
			state.laneID = lane.id
			state.laneAssigned = true
			state.flowletStarted = time.Now()
			state.flowletBytes = 0
		}
		state.flowletBytes += len(frame)
	}
	if !state.unordered {
		copy := append([]byte(nil), frame...)
		state.replay = append(state.replay, flowReplayFrame{sequence: sequence, frame: copy})
		state.replayBytes += len(copy)
		t.replayBytes.Add(int64(len(copy)))
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
	if state.replayBytes > 0 {
		t.replayBytes.Add(-int64(state.replayBytes))
		state.replayBytes = 0
		state.replay = nil
	}
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		if state.laneMask&(1<<laneID) != 0 {
			t.lanes[laneID].flowCount.Add(-1)
		}
	}
}

func (t *ParasiteTunnel) replayCapacityAvailable(state *sendFlowState, size int) bool {
	return state.replayBytes+size <= flowReplayPerFlowLimit &&
		t.replayBytes.Load()+int64(size) <= flowReplaySessionLimit
}

func (t *ParasiteTunnel) waitReplayCapacityLocked(state *sendFlowState, size int) bool {
	deadline := time.Now().Add(t.sendStallTimeout)
	for !t.replayCapacityAvailable(state, size) {
		state.mu.Unlock()
		timer := time.NewTimer(laneSendRetryInterval)
		select {
		case <-timer.C:
		case <-t.closed:
			timer.Stop()
			state.mu.Lock()
			return false
		}
		state.mu.Lock()
		if state.closed || time.Now().After(deadline) {
			return false
		}
	}
	return true
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
				if preferred != nil {
					return nil, false
				}
				blockedAt = time.Now()
				timer.Reset(t.sendStallTimeout)
				continue
			case laneRecoveryInProgress:
				if preferred != nil {
					return nil, false
				}
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
		if lane.state != laneStateActive {
			lane.mu.Unlock()
			continue
		}
		waitSnd := lane.kcp.WaitSnd()
		rtt := lane.kcpSRTTMS
		pendingLimit := lane.admissionLimitLocked(priority)
		outputDepth := len(lane.outputPending)
		if !priority && (waitSnd+required > pendingLimit || outputDepth+queueDepth+required > laneKCPOutputBacklog+laneSendQueueDepth) {
			lane.deliveryDemanded = true
		}
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
	if lane.state != laneStateActive {
		return false
	}
	if len(lane.outputPending)+queueDepth+required > laneKCPOutputBacklog+laneSendQueueDepth {
		if !priority {
			lane.deliveryDemanded = true
		}
		return false
	}
	pendingLimit := lane.admissionLimitLocked(priority)
	if lane.kcp.WaitSnd()+required > pendingLimit {
		if !priority {
			lane.deliveryDemanded = true
		}
		return false
	}
	if !lane.admitPacingLocked(len(encoded), priority, time.Now()) {
		return false
	}
	encoded = bindLaneGeneration(encoded, lane.generation)
	// The lane update loop flushes within 10 ms. Calling Update synchronously
	// here lets a blocked TURN write hold both the KCP mutex and RelayBridge's
	// per-flow send path, preventing the stall timer from recovering the lane.
	return lane.kcp.Send(encoded) >= 0
}

func bindLaneGeneration(encoded []byte, generation uint64) []byte {
	if len(encoded) < laneFrameHeaderSize || !bytes.Equal(encoded[:8], laneFrameMagic[:]) || binary.BigEndian.Uint64(encoded[8:16]) != 0 {
		return encoded
	}
	bound := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint64(bound[8:16], generation)
	return bound
}

func (l *kcpLane) admitPacingLocked(size int, priority bool, now time.Time) bool {
	if priority {
		return true
	}
	if l.pacingLastRefill.IsZero() {
		l.pacingLastRefill = now
	}
	elapsed := now.Sub(l.pacingLastRefill).Seconds()
	if elapsed > 0 {
		bucket := float64(lanePacingBucketSegments * (laneKCPMTU - kcpHeaderSize))
		l.pacingTokens = min(bucket, l.pacingTokens+elapsed*l.pacingRateBPS)
		l.pacingLastRefill = now
	}
	if l.pacingTokens < float64(size) {
		l.deliveryDemanded = true
		l.metrics.AddHot(telemetry.LaneTokenStarvationTotal, 1)
		return false
	}
	l.pacingTokens -= float64(size)
	return true
}

func (l *kcpLane) admissionLimitLocked(priority bool) int {
	limit := l.admissionWindow
	if limit < laneKCPMinimumAdmission {
		limit = laneKCPMinimumAdmission
	}
	if limit > laneKCPMaximumAdmission {
		limit = laneKCPMaximumAdmission
	}
	if priority {
		limit = min(laneKCPMaximumAdmission, limit+laneKCPControlReserve)
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
	var selected *kcpLane
	hasPreferred := preferred != nil && int(*preferred) < LaneCount
	if hasPreferred {
		selected = t.lanes[*preferred]
		active, _ := selected.workerState()
		if !active {
			selected.mu.Lock()
			state := selected.state
			selected.mu.Unlock()
			if state != laneStateActive {
				return selected.id, laneRecoveryInProgress
			}
		}
	}

	selectedPressure := -1
	for _, lane := range t.lanes {
		if hasPreferred {
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
			selected = lane
			selectedPressure = pressure
		}
	}
	if selected == nil {
		return 0, laneRecoveryUnavailable
	}
	workerID := selected.id
	selected.mu.Lock()
	state := selected.state
	selected.mu.Unlock()
	if state != laneStateActive {
		return workerID, laneRecoveryInProgress
	}
	if !t.recoveryCoordinator.Load() {
		if t.suggestLaneReset(workerID) {
			return workerID, laneRecoveryInProgress
		}
		return workerID, laneRecoveryUnavailable
	}
	now := time.Now()
	deferredReason := ""
	deferredDuplicate := false
	recordDeferred := false
	deferredDelay := time.Duration(0)
	t.recoveryMu.Lock()
	switch {
	case t.recoveryActive:
		deferredReason = "recovery_active"
		deferredDuplicate = t.recoveryLane == workerID
	case now.Before(t.recoveryReadyAt):
		deferredReason = "recovery_cooldown"
		deferredDelay = time.Until(t.recoveryReadyAt)
	default:
		t.recoveryActive = true
		t.recoveryLane = workerID
		t.recoveryStartedAt = now
		t.recoveryDeadline = now.Add(t.laneResetHandshakeDeadline(workerID) + t.laneRecoveryGrace)
		t.recoveryPending &^= uint8(1 << workerID)
		t.recoveryDeferred = t.recoveryPending
	}
	if deferredReason != "" && !deferredDuplicate && t.recoveryDeferred&uint8(1<<workerID) == 0 {
		t.recoveryDeferred |= uint8(1 << workerID)
		t.recoveryPending |= uint8(1 << workerID)
		recordDeferred = true
	}
	t.recoveryMu.Unlock()
	if deferredReason != "" {
		if recordDeferred {
			selected.metrics.AddHot(telemetry.LaneRecoveryDeferredTotal, 1)
			t.recordEvent("lane_send_recovery_deferred", "lane", deferredReason, &workerID)
			if deferredDelay > 0 {
				t.scheduleLaneRecovery(workerID, deferredDelay)
			}
		}
		return workerID, laneRecoveryInProgress
	}
	if !t.initiateLaneReset(workerID, "ack_progress_timeout") {
		selected.mu.Lock()
		stillActive := selected.state == laneStateActive && !selected.resetInFlight
		selected.mu.Unlock()
		if stillActive {
			t.recoveryMu.Lock()
			if t.recoveryActive && t.recoveryLane == workerID {
				t.recoveryActive = false
				t.recoveryDeadline = time.Time{}
				t.recoveryStartedAt = time.Time{}
			}
			t.recoveryMu.Unlock()
		}
		return workerID, laneRecoveryInProgress
	}
	return workerID, laneRecoveryStarted
}

func (t *ParasiteTunnel) completeLaneRecovery(laneID uint16) {
	t.recoveryMu.Lock()
	recovered := t.recoveryActive && t.recoveryLane == laneID
	var nextLane uint16
	hasNext := false
	delay := time.Duration(0)
	if recovered {
		t.recoveryActive = false
		t.recoveryDeadline = time.Time{}
		t.recoveryStartedAt = time.Time{}
		t.recoveryReadyAt = time.Now().Add(t.recoveryCooldown)
		t.recoveryDeferred = t.recoveryPending
		for candidate := uint16(0); candidate < LaneCount; candidate++ {
			if t.recoveryPending&uint8(1<<candidate) != 0 {
				nextLane = candidate
				hasNext = true
				delay = time.Until(t.recoveryReadyAt)
				break
			}
		}
	}
	t.recoveryMu.Unlock()
	if recovered {
		t.recordEvent("lane_send_recovered", "lane", "probe_ack_progress", &laneID)
	}
	if hasNext {
		t.scheduleLaneRecovery(nextLane, delay)
	}
}

func (t *ParasiteTunnel) scheduleLaneRecovery(laneID uint16, delay time.Duration) {
	go func() {
		timer := time.NewTimer(max(delay, 0))
		defer timer.Stop()
		select {
		case <-timer.C:
			t.recoverStalledLane(&laneID)
		case <-t.closed:
		}
	}()
}

func (t *ParasiteTunnel) initiateLaneReset(laneID uint16, reason string) bool {
	if laneID >= LaneCount {
		return false
	}
	if !t.recoveryCoordinator.Load() {
		return t.suggestLaneReset(laneID)
	}
	lane := t.lanes[laneID]
	lane.mu.Lock()
	if lane.state != laneStateActive || lane.resetInFlight {
		lane.mu.Unlock()
		return false
	}
	lane.state = laneStateQuarantined
	lane.resetInFlight = true
	lane.pendingResetGeneration = lane.generation + 1
	lane.resetAck = make(chan uint64, 1)
	lane.resetStartedAt = time.Now()
	generation := lane.pendingResetGeneration
	ack := lane.resetAck
	lane.metrics.AddHot(telemetry.LaneResetRequestTotal, 1)
	lane.mu.Unlock()
	t.migrateLaneFlows(laneID, reason)
	t.recordEvent("lane_reset_requested", "lane", reason, &laneID)
	go t.coordinateLaneReset(laneID, generation, ack, t.laneResetHandshakeDeadline(laneID))
	return true
}

func (t *ParasiteTunnel) suggestLaneReset(laneID uint16) bool {
	if laneID >= LaneCount {
		return false
	}
	lane := t.lanes[laneID]
	lane.mu.Lock()
	if lane.state != laneStateActive {
		lane.mu.Unlock()
		return false
	}
	generation := lane.generation + 1
	lane.mu.Unlock()
	now := time.Now()
	t.recoveryMu.Lock()
	last := t.recoverySuggestedAt[laneID]
	if !last.IsZero() && now.Sub(last) < laneResetRetryInterval {
		t.recoveryMu.Unlock()
		return true
	}
	first := last.IsZero()
	t.recoverySuggestedAt[laneID] = now
	t.recoveryMu.Unlock()
	if first {
		lane.metrics.AddHot(telemetry.LaneRecoveryDeferredTotal, 1)
		t.recordEvent("lane_send_recovery_deferred", "lane", "peer_coordinator", &laneID)
	}
	return t.broadcastLaneResetControl(laneResetSuggest, laneID, generation) > 0
}

func (t *ParasiteTunnel) laneResetHandshakeDeadline(laneID uint16) time.Duration {
	if laneID >= LaneCount {
		return laneResetMinimumDeadline
	}
	lane := t.lanes[laneID]
	lane.mu.Lock()
	deadline := 8 * lane.estimatedKCPRTO()
	lane.mu.Unlock()
	if deadline < laneResetMinimumDeadline {
		return laneResetMinimumDeadline
	}
	if deadline > laneResetMaximumDeadline {
		return laneResetMaximumDeadline
	}
	return deadline
}

func (t *ParasiteTunnel) coordinateLaneReset(laneID uint16, generation uint64, ack <-chan uint64, handshakeDeadline time.Duration) {
	deadline := time.NewTimer(handshakeDeadline)
	retry := time.NewTicker(laneResetRetryInterval)
	defer deadline.Stop()
	defer retry.Stop()
	attempt := 0
	for {
		if attempt > 0 {
			t.lanes[laneID].metrics.AddHot(telemetry.LaneResetRetryTotal, 1)
		}
		t.broadcastLaneResetControl(laneResetPrepare, laneID, generation)
		attempt++
		select {
		case acknowledged := <-ack:
			if acknowledged != generation {
				continue
			}
			lane := t.lanes[laneID]
			lane.mu.Lock()
			lane.state = laneStateResetting
			lane.mu.Unlock()
			for commitAttempt := 0; commitAttempt < 4; commitAttempt++ {
				t.broadcastLaneResetControl(laneResetCommit, laneID, generation)
				lane.metrics.AddHot(telemetry.LaneResetCommitTotal, 1)
				if commitAttempt == 0 {
					t.resetLaneGeneration(laneID, generation, "local_commit")
				}
				if commitAttempt < 3 {
					select {
					case <-time.After(laneResetRetryInterval):
					case <-t.closed:
						return
					}
				}
			}
			return
		case <-retry.C:
			continue
		case <-deadline.C:
			t.recordEvent("lane_reset_failed", "lane", "handshake_timeout", &laneID)
			return
		case <-t.closed:
			return
		}
	}
}

func (t *ParasiteTunnel) resetLaneGeneration(laneID uint16, generation uint64, reason string) {
	lane := t.lanes[laneID]
	lane.mu.Lock()
	if generation <= lane.generation {
		lane.mu.Unlock()
		return
	}
	lane.state = laneStateResetting
	lane.outputPending = nil
	lane.kcpSent = make(map[uint32]kcpSentSegment)
	lane.kcpSRTTMS = 0
	lane.kcpRTTVARMS = 0
	lane.kcpLastUNA = 0
	lane.kcpHasUNA = false
	lane.deliveryRateBPS = 0
	lane.deliverySampleAt = time.Now()
	lane.deliveryAckedSegments = 0
	lane.deliveryOutSegments = 0
	lane.deliveryRetrans = 0
	lane.deliveryDemanded = false
	lane.applicationLimited = true
	lane.minRTTMS = 0
	lane.admissionWindow = laneKCPInitialAdmission
	lane.pacingRateBPS = lanePacingInitialBPS
	lane.pacingTokens = float64(lanePacingBucketSegments * (laneKCPMTU - kcpHeaderSize))
	lane.pacingLastRefill = time.Now()
	lane.pacingStartup = true
	lane.pacingNextProbe = time.Now().Add(lanePacingProbeInterval)
	lane.pacingProbeUntil = time.Time{}
	lane.previousOutputDepth = 0
	lane.pressureSince = time.Time{}
	lane.probeStartedAt = time.Now()
	lane.probeLastSentAt = time.Time{}
	lane.probeReceived = false
	lane.probeAcked = false
	lane.pendingResetGeneration = 0
	lane.resetInFlight = false
	lane.resetAck = nil
	lane.resetKCPLocked(generation)
	lane.state = laneStateProbing
	lane.lastAckProgress.Store(time.Now().UnixNano())
	lane.mu.Unlock()
	lane.workerMu.RLock()
	worker := lane.worker
	lane.workerMu.RUnlock()
	if worker != nil {
		t.removeWorker(worker)
	}
	t.recordEvent("lane_reset_committed", "lane", reason, &laneID)
	go t.awaitLaneProbe(laneID, generation)
}

func (t *ParasiteTunnel) awaitLaneProbe(laneID uint16, generation uint64) {
	timer := time.NewTimer(laneProbeDeadline)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.closed:
		return
	}
	lane := t.lanes[laneID]
	lane.mu.Lock()
	failed := lane.generation == generation && lane.state == laneStateProbing
	lane.mu.Unlock()
	if failed {
		t.recordEvent("lane_reset_failed", "lane", "probe_timeout", &laneID)
	}
}

func (t *ParasiteTunnel) quarantinedLaneCount() int {
	count := 0
	for _, lane := range t.lanes {
		lane.mu.Lock()
		if lane.state != laneStateActive {
			count++
		}
		lane.mu.Unlock()
	}
	return count
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

func encodeLaneProbe(kind byte, generation uint64) []byte {
	payload := make([]byte, 17)
	copy(payload[:8], laneProbeMagic[:])
	payload[8] = kind
	binary.BigEndian.PutUint64(payload[9:17], generation)
	return payload
}

func decodeLaneProbe(payload []byte) (byte, uint64, bool) {
	if len(payload) != 17 || !bytes.Equal(payload[:8], laneProbeMagic[:]) {
		return 0, 0, false
	}
	kind := payload[8]
	generation := binary.BigEndian.Uint64(payload[9:17])
	return kind, generation, generation > 0 && (kind == laneProbeRequest || kind == laneProbeACK)
}

func (t *ParasiteTunnel) sendLaneProbe(laneID uint16) {
	if laneID >= LaneCount {
		return
	}
	lane := t.lanes[laneID]
	active, queueDepth := lane.workerState()
	if !active {
		return
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	if lane.state != laneStateProbing || queueDepth+len(lane.outputPending) >= laneSendQueueDepth+laneKCPOutputBacklog {
		return
	}
	now := time.Now()
	if !lane.probeLastSentAt.IsZero() && now.Sub(lane.probeLastSentAt) < laneProbeInterval {
		return
	}
	frame := encodeLaneFrameGeneration(lane.generation, calltunnel.ControlConnID, 0, encodeLaneProbe(laneProbeRequest, lane.generation))
	if lane.kcp.Send(frame) >= 0 {
		lane.probeLastSentAt = now
		lane.metrics.Set(telemetry.LaneProbeResult, 0)
	}
}

func (t *ParasiteTunnel) handleLaneProbe(laneID uint16, generation uint64, payload []byte) bool {
	kind, probeGeneration, ok := decodeLaneProbe(payload)
	if !ok {
		return false
	}
	lane := t.lanes[laneID]
	if probeGeneration != generation {
		lane.metrics.AddHot(telemetry.LaneStaleGenerationDropsTotal, 1)
		return true
	}
	lane.mu.Lock()
	if lane.state != laneStateProbing || lane.generation != generation {
		lane.mu.Unlock()
		return true
	}
	if kind == laneProbeRequest {
		lane.probeReceived = true
		ackFrame := encodeLaneFrameGeneration(generation, calltunnel.ControlConnID, 0, encodeLaneProbe(laneProbeACK, generation))
		_ = lane.kcp.Send(ackFrame)
	} else {
		lane.probeAcked = true
	}
	lane.mu.Unlock()
	t.maybeCompleteLaneProbe(laneID)
	return true
}

func (t *ParasiteTunnel) maybeCompleteLaneProbe(laneID uint16) {
	lane := t.lanes[laneID]
	lane.mu.Lock()
	ackProgress := time.Unix(0, lane.lastAckProgress.Load())
	complete := lane.state == laneStateProbing && lane.probeReceived && lane.probeAcked && ackProgress.After(lane.probeStartedAt)
	if complete {
		lane.state = laneStateActive
		lane.metrics.Set(telemetry.LaneProbeResult, 1)
		if !lane.resetStartedAt.IsZero() {
			lane.metrics.Set(telemetry.LaneResetDurationMS, float64(time.Since(lane.resetStartedAt))/float64(time.Millisecond))
		}
		lane.resetStartedAt = time.Time{}
	}
	lane.mu.Unlock()
	if complete {
		t.recordEvent("lane_probe_succeeded", "lane", "bidirectional_ack_progress", &laneID)
		t.completeLaneRecovery(laneID)
	}
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

func priorityRelayFrame(frame []byte, msgType byte) bool {
	return priorityRelayMessage(msgType) || len(frame) <= 256
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
	return encodeLaneFrameGeneration(1, connID, sequence, frame)
}

func encodeLaneFrameGeneration(generation uint64, connID uint32, sequence uint64, frame []byte) []byte {
	encoded := make([]byte, laneFrameHeaderSize+len(frame))
	copy(encoded[:8], laneFrameMagic[:])
	binary.BigEndian.PutUint64(encoded[8:16], generation)
	binary.BigEndian.PutUint32(encoded[16:20], connID)
	binary.BigEndian.PutUint64(encoded[20:28], sequence)
	binary.BigEndian.PutUint32(encoded[28:32], uint32(len(frame)))
	copy(encoded[laneFrameHeaderSize:], frame)
	return encoded
}

func decodeLaneFrame(encoded []byte) (uint32, uint64, []byte, bool) {
	_, connID, sequence, frame, ok := decodeLaneFrameGeneration(encoded)
	return connID, sequence, frame, ok
}

func decodeLaneFrameGeneration(encoded []byte) (uint64, uint32, uint64, []byte, bool) {
	if len(encoded) < laneFrameHeaderSize || !bytes.Equal(encoded[:8], laneFrameMagic[:]) {
		return 0, 0, 0, nil, false
	}
	generation := binary.BigEndian.Uint64(encoded[8:16])
	if generation == 0 {
		return 0, 0, 0, nil, false
	}
	length := int(binary.BigEndian.Uint32(encoded[28:32]))
	if length < 0 || length != len(encoded)-laneFrameHeaderSize {
		return 0, 0, 0, nil, false
	}
	return generation, binary.BigEndian.Uint32(encoded[16:20]), binary.BigEndian.Uint64(encoded[20:28]), encoded[laneFrameHeaderSize:], true
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
		// Physical detach is not a logical lane failure. maintainWorker first
		// attempts a same-generation hot swap; only a bounded lack of a new
		// worker escalates to the wire-v9 generation recovery handshake.
		go t.recoverLaneIfUnavailable(workerID, availabilityEpoch)
	}
}

func (t *ParasiteTunnel) recoverLaneIfUnavailable(laneID uint16, availabilityEpoch uint64) {
	timer := time.NewTimer(t.laneResetHandshakeDeadline(laneID))
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
	t.recoverStalledLane(&laneID)
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

func (t *ParasiteTunnel) workerReadyAfter(id uint16, previousEpoch uint64) bool {
	if id >= LaneCount {
		return false
	}
	lane := t.lanes[id]
	lane.workerMu.RLock()
	defer lane.workerMu.RUnlock()
	worker := lane.worker
	readyEpoch := worker != nil && worker.epoch > previousEpoch
	if !readyEpoch {
		return false
	}
	lane.mu.Lock()
	active := lane.state == laneStateActive
	lane.mu.Unlock()
	return active
}

func (t *ParasiteTunnel) LaneGeneration(id uint16) uint64 {
	if id >= LaneCount {
		return 0
	}
	lane := t.lanes[id]
	lane.mu.Lock()
	generation := lane.generation
	lane.mu.Unlock()
	return generation
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
	deliveredRate := 0.0
	minimumRTT := 0.0
	maxGeneration := uint64(0)
	maxLaneState := laneStateActive
	maxAckAge := 0.0
	applicationLimited := 0
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
		lane.metrics.Set(telemetry.LaneAdmissionRateBPS, lane.pacingRateBPS)
		lane.metrics.Set(telemetry.LaneAdmissionWindowSegments, float64(lane.admissionWindow))
		lane.metrics.Set(telemetry.LaneGeneration, float64(lane.generation))
		lane.metrics.Set(telemetry.LaneState, float64(lane.state))
		lane.metrics.Set(telemetry.LanePacingRateBPS, lane.pacingRateBPS)
		lane.metrics.Set(telemetry.LaneDeliveredRateBPS, lane.deliveryRateBPS)
		lane.metrics.Set(telemetry.LaneMinRTTMS, lane.minRTTMS)
		lane.metrics.Set(telemetry.LaneInflightLimitSegments, float64(lane.admissionWindow))
		lane.metrics.Set(telemetry.LaneApplicationLimited, boolFloat(lane.applicationLimited))
		lane.metrics.Set(telemetry.LaneAckAgeSeconds, time.Since(time.Unix(0, lane.lastAckProgress.Load())).Seconds())
		lane.metrics.Set(telemetry.KCPOutputQueueDepth, float64(len(lane.outputPending)))
		lane.metrics.Set(telemetry.KCPOutputQueueCapacity, laneKCPOutputBacklog)
		outputDepth += len(lane.outputPending)
		admissionWindow += lane.admissionWindow
		admissionRate += lane.pacingRateBPS
		deliveredRate += lane.deliveryRateBPS
		if lane.minRTTMS > 0 && (minimumRTT == 0 || lane.minRTTMS < minimumRTT) {
			minimumRTT = lane.minRTTMS
		}
		maxGeneration = max(maxGeneration, lane.generation)
		maxLaneState = max(maxLaneState, lane.state)
		maxAckAge = max(maxAckAge, time.Since(time.Unix(0, lane.lastAckProgress.Load())).Seconds())
		if lane.applicationLimited {
			applicationLimited++
		}
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
	t.metrics.Set(telemetry.LaneGeneration, float64(maxGeneration))
	t.metrics.Set(telemetry.LaneState, float64(maxLaneState))
	t.metrics.Set(telemetry.LanePacingRateBPS, admissionRate)
	t.metrics.Set(telemetry.LaneDeliveredRateBPS, deliveredRate)
	t.metrics.Set(telemetry.LaneMinRTTMS, minimumRTT)
	t.metrics.Set(telemetry.LaneInflightLimitSegments, float64(admissionWindow))
	t.metrics.Set(telemetry.LaneApplicationLimited, float64(applicationLimited)/float64(LaneCount))
	t.metrics.Set(telemetry.LaneAckAgeSeconds, maxAckAge)
	t.metrics.Set(telemetry.LaneCount, LaneCount)
	t.metrics.Set(telemetry.AggregateProgressAgeSeconds, time.Since(time.Unix(0, t.lastAggregateProgress.Load())).Seconds())
	t.metrics.Set(telemetry.QuarantinedLanes, float64(t.quarantinedLaneCount()))
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
		pacingRate := lane.pacingRateBPS
		generation := lane.generation
		state := lane.state
		minRTT := lane.minRTTMS
		applicationLimited := lane.applicationLimited
		ackAge := time.Since(time.Unix(0, lane.lastAckProgress.Load())).Seconds()
		lane.mu.Unlock()
		lane.metrics.Set(telemetry.WorkerActive, boolFloat(worker != nil))
		lane.metrics.Set(telemetry.WorkerSendQueueDepth, queueDepth)
		lane.metrics.Set(telemetry.KCPOutputQueueDepth, outputDepth)
		lane.metrics.Set(telemetry.KCPOutputQueueCapacity, laneKCPOutputBacklog)
		lane.metrics.Set(telemetry.LaneAdmissionWindowSegments, admissionWindow)
		lane.metrics.Set(telemetry.LaneAdmissionRateBPS, pacingRate)
		lane.metrics.Set(telemetry.LaneGeneration, float64(generation))
		lane.metrics.Set(telemetry.LaneState, float64(state))
		lane.metrics.Set(telemetry.LanePacingRateBPS, pacingRate)
		lane.metrics.Set(telemetry.LaneDeliveredRateBPS, deliveryRate)
		lane.metrics.Set(telemetry.LaneMinRTTMS, minRTT)
		lane.metrics.Set(telemetry.LaneInflightLimitSegments, admissionWindow)
		lane.metrics.Set(telemetry.LaneApplicationLimited, boolFloat(applicationLimited))
		lane.metrics.Set(telemetry.LaneAckAgeSeconds, ackAge)
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

func (t *ParasiteTunnel) transportHealthSnapshot() HC.TransportHealthSnapshot {
	now := time.Now()
	lastInbound := int64(0)
	for _, lane := range t.lanes {
		lane.workerMu.RLock()
		worker := lane.worker
		if worker != nil {
			lastInbound = max(lastInbound, worker.lastInbound.Load())
		}
		lane.workerMu.RUnlock()
	}
	lastDemand := t.lastApplicationDemand.Load()
	return HC.TransportHealthSnapshot{
		ActiveLanes:             int32(t.ActiveWorkers()),
		TotalLanes:              LaneCount,
		Demand:                  lastDemand != 0 && now.Sub(time.Unix(0, lastDemand)) <= transportFailureLimit,
		LastProgressAt:          time.Unix(0, t.lastProgress.Load()).UnixMilli(),
		LastAggregateProgressAt: time.Unix(0, t.lastAggregateProgress.Load()).UnixMilli(),
		LastInboundAt:           time.Unix(0, lastInbound).UnixMilli(),
		ObservedAt:              now.UnixMilli(),
	}
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

func (t *ParasiteTunnel) markAggregateProgress() {
	t.lastAggregateProgress.Store(time.Now().UnixNano())
}

func (t *ParasiteTunnel) markApplicationDemand() {
	t.lastApplicationDemand.Store(time.Now().UnixNano())
}

func (t *ParasiteTunnel) hasFreshApplicationDemand(now time.Time, window time.Duration) bool {
	lastDemand := t.lastApplicationDemand.Load()
	return lastDemand != 0 && now.Sub(time.Unix(0, lastDemand)) <= window
}

func (t *ParasiteTunnel) replaceSession(reason string, laneID *uint16) {
	t.sessionReplaceOnce.Do(func() {
		t.metrics.AddHot(telemetry.SessionReplacementTotal, 1)
		t.recordEvent("session_replacement", "session", reason, laneID)
		t.closeAsync()
	})
}

func (t *ParasiteTunnel) noProgressWatchLoop() {
	ticker := time.NewTicker(laneDeliverySampleWindow)
	defer ticker.Stop()
	var pendingSince time.Time
	for {
		select {
		case now := <-ticker.C:
			pending, medianRTO := t.pendingDataAndMedianRTO()
			age := now.Sub(time.Unix(0, t.lastAggregateProgress.Load()))
			if age < 0 {
				age = 0
			}
			t.metrics.Set(telemetry.AggregateProgressAgeSeconds, age.Seconds())
			t.metrics.Set(telemetry.QuarantinedLanes, float64(t.quarantinedLaneCount()))
			if !pending {
				pendingSince = time.Time{}
				continue
			}
			if pendingSince.IsZero() {
				pendingSince = now
				continue
			}
			threshold := sessionNoProgressThreshold(medianRTO)
			if !t.hasFreshApplicationDemand(now, threshold) {
				pendingSince = time.Time{}
				continue
			}
			progressReference := time.Unix(0, t.lastAggregateProgress.Load())
			if pendingSince.After(progressReference) {
				progressReference = pendingSince
			}
			if now.Sub(progressReference) >= threshold {
				t.replaceSession("aggregate_no_progress", nil)
				return
			}
		case <-t.closed:
			return
		}
	}
}

func sessionNoProgressThreshold(medianRTO time.Duration) time.Duration {
	return 30 * time.Second
}

func (t *ParasiteTunnel) pendingDataAndMedianRTO() (bool, time.Duration) {
	pending := t.metrics.Value(telemetry.RelayQueueDepth) > 0
	rtos := make([]float64, 0, LaneCount)
	for _, lane := range t.lanes {
		lane.mu.Lock()
		pending = pending || lane.kcp.WaitSnd() > 0 || len(lane.outputPending) > 0
		rtos = append(rtos, float64(lane.estimatedKCPRTO()))
		lane.mu.Unlock()
		lane.workerMu.RLock()
		if lane.worker != nil && len(lane.worker.sendQueue) > 0 {
			pending = true
		}
		lane.workerMu.RUnlock()
	}
	sort.Float64s(rtos)
	if len(rtos) == 0 {
		return pending, 200 * time.Millisecond
	}
	return pending, time.Duration(rtos[len(rtos)/2])
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
		t.recoveryDeferred = 0
		t.recoveryPending = 0
		t.recoverySuggestedAt = [LaneCount]time.Time{}
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
			state := l.state
			probeDue := state == laneStateProbing && (l.probeLastSentAt.IsZero() || now.Sub(l.probeLastSentAt) >= laneProbeInterval)
			if outputDepth < laneKCPOutputBacklog {
				l.kcp.Update()
			} else {
				l.metrics.AddHot(telemetry.KCPUpdateBackpressureTotal, 1)
			}
			pressure := state == laneStateActive && (outputDepth >= 3*laneKCPOutputBacklog/4 || waitSnd >= l.admissionLimitLocked(false))
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
			if probeDue {
				l.parent.sendLaneProbe(l.id)
			}
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
