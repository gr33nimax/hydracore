package multiuser

import (
	"bytes"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	pooledKCPMTU            = 1000
	pooledKCPWindow         = 512
	pooledKCPUpdateInterval = 10 * time.Millisecond
	pooledKCPReceiveBuffer  = 32 * 1024
	pooledKCPMaxPending     = pooledKCPWindow * 4
	workerSendQueueDepth    = 512
	workerControlQueueDepth = 64
	workerHeartbeatInterval = 15 * time.Second
	workerLivenessTimeout   = 60 * time.Second
	workerStaleReplacement  = 2 * workerHeartbeatInterval
)

var workerHeartbeat = [8]byte{'H', 'C', 'V', 'K', 'H', 'B', 1, 0}

type pooledWorker struct {
	id          uint16
	epoch       uint64
	conn        net.Conn
	parent      *PooledTunnel
	metrics     *telemetry.Accumulator
	sendQueue   chan queuedSegment
	controlQueue chan queuedSegment
	done        chan struct{}
	closeOnce   sync.Once
	lastInbound atomic.Int64
	pacingRateBPS atomic.Uint64
	nextPacedSend time.Time
}

func (w *pooledWorker) close() {
	w.closeOnce.Do(func() {
		close(w.done)
		_ = w.conn.Close()
	})
}

type PooledTunnel struct {
	logger  logger.ContextLogger
	kcp     *kcp.KCP
	kcpMu   sync.Mutex
	recvBuf []byte

	workersMu         sync.RWMutex
	workers           map[uint16]*pooledWorker
	workerIDs         []uint16
	nextWorker        atomic.Uint32
	maxWorkers        int
	heartbeatInterval time.Duration
	livenessTimeout   time.Duration
	staleReplacement  time.Duration
	multipath          multipathConfig
	scheduler          *multipathScheduler

	callbackMu sync.RWMutex
	onData     func([]byte)
	onClose    func()

	lastActivity atomic.Int64
	metrics      *telemetry.Accumulator
	workerMetrics map[uint16]*telemetry.Accumulator
	kcpSent      map[uint32]kcpSentSegment
	kcpSRTTMS    float64
	kcpRTTVARMS  float64

	telemetryMu             sync.RWMutex
	onTelemetryControl      func(time.Duration)
	onTelemetryClientRecord func([]byte)
	closed                  chan struct{}
	closeOnce               sync.Once
}

type kcpSentSegment struct {
	sentAt        time.Time
	retransmitted bool
}

func NewPooledTunnel(conv uint32, maxWorkers int, log logger.ContextLogger) (*PooledTunnel, error) {
	return newPooledTunnel(conv, maxWorkers, log, nil)
}

func newPooledTunnel(conv uint32, maxWorkers int, log logger.ContextLogger, metrics *telemetry.Accumulator) (*PooledTunnel, error) {
	return newPooledTunnelWithProfile(conv, maxWorkers, MultipathProfileLegacy, log, metrics)
}

func NewPooledTunnelWithProfile(conv uint32, maxWorkers int, profile MultipathProfile, log logger.ContextLogger) (*PooledTunnel, error) {
	return newPooledTunnelWithProfile(conv, maxWorkers, profile, log, nil)
}

func newPooledTunnelWithProfile(conv uint32, maxWorkers int, profile MultipathProfile, log logger.ContextLogger, metrics *telemetry.Accumulator) (*PooledTunnel, error) {
	if conv == 0 {
		return nil, errors.New("call multi_user: KCP conversation must not be zero")
	}
	if maxWorkers <= 0 || maxWorkers > HardMaxWorkers {
		return nil, errors.New("call multi_user: invalid worker limit")
	}
	if metrics == nil {
		metrics = telemetry.NewAccumulator()
	}
	multipath, err := multipathConfigFor(profile)
	if err != nil {
		return nil, err
	}
	tunnel := &PooledTunnel{
		logger:            log,
		workers:           make(map[uint16]*pooledWorker),
		maxWorkers:        maxWorkers,
		heartbeatInterval: workerHeartbeatInterval,
		livenessTimeout:   workerLivenessTimeout,
		staleReplacement:  workerStaleReplacement,
		multipath:          multipath,
		recvBuf:           make([]byte, pooledKCPReceiveBuffer),
		metrics:           metrics,
		workerMetrics:     make(map[uint16]*telemetry.Accumulator),
		kcpSent:           make(map[uint32]kcpSentSegment),
		closed:            make(chan struct{}),
	}
	tunnel.scheduler = newMultipathScheduler(multipath)
	tunnel.lastActivity.Store(time.Now().UnixNano())
	tunnel.kcp = kcp.NewKCP(conv, func(buffer []byte, size int) {
		if size <= 0 {
			return
		}
		tunnel.observeKCPOutput(buffer[:size])
		tunnel.dispatchSegment(buffer[:size])
	})
	tunnel.kcp.NoDelay(1, 10, multipath.fastResend, multipath.congestion)
	tunnel.kcp.WndSize(pooledKCPWindow, pooledKCPWindow)
	tunnel.kcp.SetMtu(pooledKCPMTU)
	go tunnel.updateLoop()
	return tunnel, nil
}

func (t *PooledTunnel) SendData(frame []byte) {
	if len(frame) == 0 {
		return
	}
	blockedAt := time.Time{}
	for {
		select {
		case <-t.closed:
			return
		default:
		}
		t.kcpMu.Lock()
		if t.kcp.WaitSnd() < pooledKCPMaxPending {
			t.kcp.Send(frame)
			t.kcp.Update()
			t.kcpMu.Unlock()
			if !blockedAt.IsZero() {
				t.metrics.AddHotMonotonic(telemetry.KCPSendBlockedSecondsTotal, time.Since(blockedAt).Seconds())
			}
			t.touch()
			return
		}
		t.kcpMu.Unlock()
		if blockedAt.IsZero() {
			blockedAt = time.Now()
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (t *PooledTunnel) trySendData(frame []byte) bool {
	return t.trySendDataWithActivity(frame, true)
}

func (t *PooledTunnel) trySendControlData(frame []byte) bool {
	return t.trySendDataWithActivity(frame, false)
}

func (t *PooledTunnel) trySendDataWithActivity(frame []byte, activity bool) bool {
	if len(frame) == 0 {
		return false
	}
	select {
	case <-t.closed:
		return false
	default:
	}
	t.kcpMu.Lock()
	defer t.kcpMu.Unlock()
	if t.kcp.WaitSnd() >= pooledKCPMaxPending {
		return false
	}
	if t.kcp.Send(frame) < 0 {
		return false
	}
	t.kcp.Update()
	if activity {
		t.touch()
	}
	return true
}

func (t *PooledTunnel) SetOnData(callback func([]byte)) {
	t.callbackMu.Lock()
	t.onData = callback
	t.callbackMu.Unlock()
}

func (t *PooledTunnel) SetOnClose(callback func()) {
	t.callbackMu.Lock()
	t.onClose = callback
	t.callbackMu.Unlock()
}

func (t *PooledTunnel) Reconfigure(_, _ int) {}

func (t *PooledTunnel) SetTelemetryCounterParent(parent *telemetry.Accumulator) {
	t.metrics.SetCounterParent(parent)
}

func (t *PooledTunnel) SetTelemetryCollectionActive(active bool) {
	wasActive := t.metrics.CollectionActive()
	t.metrics.SetCollectionActive(active)
	if active && !wasActive {
		t.kcpMu.Lock()
		t.kcpSent = make(map[uint32]kcpSentSegment)
		t.kcpSRTTMS = 0
		t.kcpRTTVARMS = 0
		t.kcpMu.Unlock()
	}
	t.workersMu.RLock()
	for _, metrics := range t.workerMetrics {
		metrics.SetCollectionActive(active)
	}
	t.workersMu.RUnlock()
}

func (t *PooledTunnel) telemetryWorker(id uint16) *telemetry.Accumulator {
	t.workersMu.Lock()
	defer t.workersMu.Unlock()
	metrics := t.workerMetrics[id]
	if metrics == nil {
		metrics = telemetry.NewAccumulator()
		metrics.SetCounterParent(t.metrics)
		metrics.SetCollectionActive(t.metrics.CollectionActive())
		t.workerMetrics[id] = metrics
	}
	return metrics
}

func (t *PooledTunnel) SetTelemetryControlHandler(handler func(time.Duration)) {
	t.telemetryMu.Lock()
	t.onTelemetryControl = handler
	t.telemetryMu.Unlock()
}

func (t *PooledTunnel) SetTelemetryClientRecordHandler(handler func([]byte)) {
	t.telemetryMu.Lock()
	t.onTelemetryClientRecord = handler
	t.telemetryMu.Unlock()
}

func (t *PooledTunnel) AddWorker(id uint16, conn net.Conn) (<-chan struct{}, error) {
	return t.AddWorkerEpoch(id, 0, conn)
}

func (t *PooledTunnel) AddWorkerEpoch(id uint16, epoch uint64, conn net.Conn) (<-chan struct{}, error) {
	worker, err := t.reserveWorker(id, epoch, conn)
	if err != nil {
		return nil, err
	}
	t.startWorker(worker)
	return worker.done, nil
}

// AttachWorker reserves the worker identity, runs beforeStart (the server auth
// ACK), and only then lets worker goroutines read or write DTLS application
// records. This prevents a queued KCP segment from racing ahead of the ACK.
func (t *PooledTunnel) AttachWorker(id uint16, conn net.Conn, beforeStart func() error) (<-chan struct{}, error) {
	return t.AttachWorkerEpoch(id, 0, conn, beforeStart)
}

func (t *PooledTunnel) AttachWorkerEpoch(id uint16, epoch uint64, conn net.Conn, beforeStart func() error) (<-chan struct{}, error) {
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

func (t *PooledTunnel) reserveWorker(id uint16, epoch uint64, conn net.Conn) (*pooledWorker, error) {
	select {
	case <-t.closed:
		return nil, errors.New("call multi_user: session already closed")
	default:
	}
	worker := &pooledWorker{
		id:        id,
		epoch:     epoch,
		conn:      conn,
		parent:    t,
		metrics:   t.telemetryWorker(id),
		sendQueue: make(chan queuedSegment, workerSendQueueDepth),
		controlQueue: make(chan queuedSegment, workerControlQueueDepth),
		done:      make(chan struct{}),
	}
	worker.lastInbound.Store(time.Now().UnixNano())
	t.workersMu.Lock()
	replaced := t.workers[id]
	if replaced != nil && epoch > 0 && epoch <= replaced.epoch {
		t.workersMu.Unlock()
		return nil, errors.New("call multi_user: stale worker epoch")
	}
	if replaced != nil && epoch == 0 && time.Since(time.Unix(0, replaced.lastInbound.Load())) < t.staleReplacement {
		t.workersMu.Unlock()
		return nil, errors.New("call multi_user: duplicate active worker attach")
	}
	if replaced == nil && len(t.workers) >= t.maxWorkers {
		t.workersMu.Unlock()
		return nil, errors.New("call multi_user: worker limit reached")
	}
	t.workers[id] = worker
	if replaced == nil {
		t.workerIDs = append(t.workerIDs, id)
	}
	t.workersMu.Unlock()
	t.scheduler.registerWorker(worker)
	worker.metrics.Set(telemetry.WorkerActive, 1)
	if replaced != nil {
		t.scheduler.removeWorker(replaced)
		replaced.close()
	}
	t.touch()
	return worker, nil
}

func (t *PooledTunnel) DropWorker(id uint16) {
	t.workersMu.RLock()
	worker := t.workers[id]
	t.workersMu.RUnlock()
	if worker != nil {
		t.removeWorker(worker)
	}
}

func (t *PooledTunnel) startWorker(worker *pooledWorker) {
	go worker.readLoop()
	go worker.writeLoop()
	go worker.watchdogLoop()
}

func (w *pooledWorker) readLoop() {
	buffer := make([]byte, pooledKCPMTU+128)
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
		w.parent.inputSegment(buffer[:n])
	}
}

func (w *pooledWorker) writeLoop() {
	ticker := time.NewTicker(w.parent.heartbeatInterval)
	defer ticker.Stop()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var pending *queuedSegment
	var pendingReady time.Time
	for {
		if pending != nil {
			wait := time.Until(pendingReady)
			if wait <= 0 {
				if !w.writeQueuedSegment(*pending) {
					return
				}
				pending = nil
				continue
			}
			timer.Reset(wait)
			select {
			case segment := <-w.controlQueue:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if !w.writeQueuedSegment(segment) {
					return
				}
			case <-timer.C:
				if !w.writeQueuedSegment(*pending) {
					return
				}
				pending = nil
			case <-ticker.C:
				if !w.writeHeartbeat() {
					return
				}
			case <-w.done:
				return
			case <-w.parent.closed:
				w.parent.removeWorker(w)
				return
			}
			continue
		}
		select {
		case segment := <-w.controlQueue:
			if !w.writeQueuedSegment(segment) {
				return
			}
			continue
		default:
		}
		select {
		case segment := <-w.sendQueue:
			pending = &segment
			pendingReady = w.reservePacing(len(segment.payload), time.Now())
		case segment := <-w.controlQueue:
			if !w.writeQueuedSegment(segment) {
				return
			}
		case <-ticker.C:
			if !w.writeHeartbeat() {
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

func (w *pooledWorker) reservePacing(size int, now time.Time) time.Time {
	rate := w.pacingRateBPS.Load()
	if rate == 0 || size <= 0 {
		return now
	}
	spacing := time.Duration(float64(size*8) / float64(rate) * float64(time.Second))
	burst := time.Duration(float64(w.parent.multipath.burstBytes*8) / float64(rate) * float64(time.Second))
	if w.nextPacedSend.IsZero() || w.nextPacedSend.Before(now.Add(-burst)) {
		w.nextPacedSend = now.Add(-burst)
	}
	readyAt := w.nextPacedSend
	w.nextPacedSend = w.nextPacedSend.Add(spacing)
	if readyAt.After(now) {
		return readyAt
	}
	return now
}

func (w *pooledWorker) writeQueuedSegment(segment queuedSegment) bool {
	if _, err := w.conn.Write(segment.payload); err != nil {
		w.parent.removeWorker(w)
		return false
	}
	if w.pacingRateBPS.Load() > 0 {
		waited := time.Since(segment.enqueuedAt)
		if waited > 0 {
			w.metrics.AddHotMonotonic(telemetry.WorkerPacingWaitSecondsTotal, waited.Seconds())
		}
		w.metrics.AddHot(telemetry.WorkerPacingPacketsTotal, 1)
	}
	w.parent.touch()
	return true
}

func (w *pooledWorker) writeHeartbeat() bool {
	if _, err := w.conn.Write(workerHeartbeat[:]); err != nil {
		w.parent.removeWorker(w)
		return false
	}
	return true
}

func (w *pooledWorker) watchdogLoop() {
	ticker := time.NewTicker(w.parent.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			lastInbound := time.Unix(0, w.lastInbound.Load())
			if now.Sub(lastInbound) >= w.parent.livenessTimeout {
				w.metrics.Add(telemetry.WorkerLivenessExpiredTotal, 1)
				workerID := w.id
				w.metrics.RecordEvent("worker_liveness_expired", "worker", "timeout", &workerID)
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

func (t *PooledTunnel) dispatchSegment(segment []byte) {
	t.workersMu.RLock()
	workerCount := len(t.workerIDs)
	if workerCount == 0 {
		t.workersMu.RUnlock()
		t.metrics.AddHot(telemetry.WorkerNoAvailableDropsTotal, 1)
		return
	}
	start := int((t.nextWorker.Add(1) - 1) % uint32(workerCount))
	workers := make([]*pooledWorker, 0, workerCount)
	for offset := 0; offset < workerCount; offset++ {
		id := t.workerIDs[(start+offset)%workerCount]
		if worker := t.workers[id]; worker != nil {
			workers = append(workers, worker)
		}
	}
	t.workersMu.RUnlock()
	if t.scheduler.adaptive() {
		workers = t.scheduler.rankWorkers(workers, segment, time.Now())
	}
	queued := queuedSegment{payload: append([]byte(nil), segment...), enqueuedAt: time.Now()}
	control := t.scheduler.adaptive() && len(kcpPushSequences(segment)) == 0
	for attempts := 0; attempts < len(workers); attempts++ {
		if !t.scheduler.adaptive() {
			best := attempts
			for index := attempts + 1; index < len(workers); index++ {
				if len(workers[index].sendQueue)+len(workers[index].controlQueue) < len(workers[best].sendQueue)+len(workers[best].controlQueue) {
					best = index
				}
			}
			workers[attempts], workers[best] = workers[best], workers[attempts]
		}
		worker := workers[attempts]
		queue := worker.sendQueue
		if control {
			queue = worker.controlQueue
		}
		select {
		case queue <- queued:
			t.scheduler.commitOutput(segment, worker, queued.enqueuedAt)
			return
		case <-worker.done:
		default:
		}
	}
	// This counter represents one KCP segment that could not be queued
	// anywhere. The previous implementation incremented it once per full
	// worker, overstating actual transport loss by up to workerCount.
	workers[0].metrics.AddHot(telemetry.WorkerSendQueueDropsTotal, 1)
	t.metrics.AddHot(telemetry.WorkerNoAvailableDropsTotal, 1)
}

func (t *PooledTunnel) inputSegment(segment []byte) {
	t.kcpMu.Lock()
	t.scheduler.observeInput(segment, time.Now())
	t.observeKCPInput(segment)
	t.kcp.Input(segment, kcp.IKCP_PACKET_REGULAR, true)
	messages := make([][]byte, 0, 2)
	for {
		size := t.kcp.PeekSize()
		if size <= 0 {
			break
		}
		if size > len(t.recvBuf) {
			t.recvBuf = make([]byte, size)
		}
		n := t.kcp.Recv(t.recvBuf)
		if n <= 0 {
			break
		}
		messages = append(messages, append([]byte(nil), t.recvBuf[:n]...))
	}
	t.kcpMu.Unlock()
	t.touch()
	t.callbackMu.RLock()
	callback := t.onData
	t.callbackMu.RUnlock()
	if callback != nil {
		for _, message := range messages {
			if t.handleTelemetryMessage(message) {
				continue
			}
			callback(message)
		}
	} else {
		for _, message := range messages {
			t.handleTelemetryMessage(message)
		}
	}
}

func (t *PooledTunnel) removeWorker(worker *pooledWorker) {
	t.workersMu.Lock()
	current, exists := t.workers[worker.id]
	if !exists || current != worker {
		t.workersMu.Unlock()
		worker.close()
		return
	}
	delete(t.workers, worker.id)
	for index, id := range t.workerIDs {
		if id == worker.id {
			t.workerIDs = append(t.workerIDs[:index], t.workerIDs[index+1:]...)
			break
		}
	}
	t.workersMu.Unlock()
	t.scheduler.removeWorker(worker)
	worker.metrics.Set(telemetry.WorkerActive, 0)
	worker.metrics.Set(telemetry.WorkerSendQueueDepth, 0)
	worker.close()
	t.touch()
}

func (t *PooledTunnel) ActiveWorkers() int {
	t.workersMu.RLock()
	defer t.workersMu.RUnlock()
	return len(t.workers)
}

func (t *PooledTunnel) TelemetryValues() map[telemetry.Metric]float64 {
	t.metrics.Set(telemetry.KCPMTUBytes, pooledKCPMTU)
	t.metrics.Set(telemetry.KCPSendWindowSegments, pooledKCPWindow)
	t.metrics.Set(telemetry.KCPReceiveWindowSegments, pooledKCPWindow)
	t.metrics.Set(telemetry.KCPMaxPendingSegments, pooledKCPMaxPending)
	t.metrics.Set(telemetry.KCPUpdateIntervalMS, float64(pooledKCPUpdateInterval/time.Millisecond))
	t.metrics.Set(telemetry.KCPFastResend, float64(t.multipath.fastResend))
	t.metrics.Set(telemetry.KCPCongestionControl, float64(1-t.multipath.congestion))
	if t.multipath.profile == MultipathProfileAdaptive {
		t.metrics.Set(telemetry.WorkerSendQueueCapacity, workerSendQueueDepth+workerControlQueueDepth)
		t.metrics.Set(telemetry.MultipathProfile, 1)
	} else {
		t.metrics.Set(telemetry.WorkerSendQueueCapacity, workerSendQueueDepth)
		t.metrics.Set(telemetry.MultipathProfile, 0)
	}
	t.metrics.Set(telemetry.MultipathChunkPackets, float64(t.multipath.chunkPackets))
	t.metrics.Set(telemetry.MultipathChunkDwellMS, float64(t.multipath.chunkDwell/time.Millisecond))
	t.metrics.Set(telemetry.WorkerHeartbeatIntervalMS, float64(t.heartbeatInterval/time.Millisecond))
	t.metrics.Set(telemetry.WorkerLivenessTimeoutMS, float64(t.livenessTimeout/time.Millisecond))
	t.kcpMu.Lock()
	t.metrics.Set(telemetry.KCPWaitSnd, float64(t.kcp.WaitSnd()))
	t.metrics.Set(telemetry.KCPRTTMS, t.kcpSRTTMS)
	rto := 200.0
	if t.kcpSRTTMS > 0 {
		rto = t.kcpSRTTMS + max(10, 4*t.kcpRTTVARMS)
		if rto < 30 {
			rto = 30
		}
	}
	t.metrics.Set(telemetry.KCPRTOMS, rto)
	t.kcpMu.Unlock()
	t.workersMu.RLock()
	t.metrics.Set(telemetry.WorkerActive, float64(len(t.workers)))
	queueDepth := 0
	for _, worker := range t.workers {
		queueDepth += len(worker.sendQueue) + len(worker.controlQueue)
		t.scheduler.publishWorkerMetrics(worker)
	}
	t.workersMu.RUnlock()
	t.metrics.Set(telemetry.WorkerSendQueueDepth, float64(queueDepth))
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

func (t *PooledTunnel) telemetryWorkerSnapshots(metrics []telemetry.Metric) []workerTelemetrySnapshot {
	t.workersMu.RLock()
	defer t.workersMu.RUnlock()
	result := make([]workerTelemetrySnapshot, 0, len(t.workerMetrics))
	for id, accumulator := range t.workerMetrics {
		active := 0.0
		queueDepth := 0.0
		if worker := t.workers[id]; worker != nil {
			active = 1
			queueDepth = float64(len(worker.sendQueue) + len(worker.controlQueue))
			t.scheduler.publishWorkerMetrics(worker)
		}
		accumulator.Set(telemetry.WorkerActive, active)
		accumulator.Set(telemetry.WorkerSendQueueDepth, queueDepth)
		result = append(result, workerTelemetrySnapshot{
			id:      id,
			metrics: accumulator.Snapshot(metrics),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result
}

func (t *PooledTunnel) LastActivity() time.Time {
	return time.Unix(0, t.lastActivity.Load())
}

func (t *PooledTunnel) touch() {
	t.lastActivity.Store(time.Now().UnixNano())
}

func (t *PooledTunnel) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
		t.workersMu.Lock()
		workers := make([]*pooledWorker, 0, len(t.workers))
		for _, worker := range t.workers {
			workers = append(workers, worker)
		}
		t.workers = make(map[uint16]*pooledWorker)
		t.workerIDs = nil
		t.workersMu.Unlock()
		for _, worker := range workers {
			worker.close()
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

func (t *PooledTunnel) updateLoop() {
	ticker := time.NewTicker(pooledKCPUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.kcpMu.Lock()
			t.kcp.Update()
			t.kcpMu.Unlock()
		case <-t.closed:
			return
		}
	}
}
