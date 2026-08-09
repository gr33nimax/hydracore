package multiuser

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/logger"
	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	pooledKCPMTU            = 1000
	pooledKCPWindow         = 512
	pooledKCPUpdateInterval = 10 * time.Millisecond
	pooledKCPReceiveBuffer  = 32 * 1024
	pooledKCPMaxPending     = pooledKCPWindow * 4
	workerSendQueueDepth    = 256
	workerHeartbeatInterval = 15 * time.Second
	workerLivenessTimeout   = 60 * time.Second
	workerStaleReplacement  = 2 * workerHeartbeatInterval
)

var workerHeartbeat = [8]byte{'H', 'C', 'V', 'K', 'H', 'B', 1, 0}

type pooledWorker struct {
	id          uint16
	conn        net.Conn
	parent      *PooledTunnel
	sendQueue   chan []byte
	done        chan struct{}
	closeOnce   sync.Once
	lastInbound atomic.Int64
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

	callbackMu sync.RWMutex
	onData     func([]byte)
	onClose    func()

	lastActivity atomic.Int64
	closed       chan struct{}
	closeOnce    sync.Once
}

func NewPooledTunnel(conv uint32, maxWorkers int, log logger.ContextLogger) (*PooledTunnel, error) {
	if conv == 0 {
		return nil, errors.New("call multi_user: KCP conversation must not be zero")
	}
	if maxWorkers <= 0 || maxWorkers > HardMaxWorkers {
		return nil, errors.New("call multi_user: invalid worker limit")
	}
	tunnel := &PooledTunnel{
		logger:            log,
		workers:           make(map[uint16]*pooledWorker),
		maxWorkers:        maxWorkers,
		heartbeatInterval: workerHeartbeatInterval,
		livenessTimeout:   workerLivenessTimeout,
		staleReplacement:  workerStaleReplacement,
		recvBuf:           make([]byte, pooledKCPReceiveBuffer),
		closed:            make(chan struct{}),
	}
	tunnel.lastActivity.Store(time.Now().UnixNano())
	tunnel.kcp = kcp.NewKCP(conv, func(buffer []byte, size int) {
		if size <= 0 {
			return
		}
		tunnel.dispatchSegment(buffer[:size])
	})
	tunnel.kcp.NoDelay(1, 10, 2, 1)
	tunnel.kcp.WndSize(pooledKCPWindow, pooledKCPWindow)
	tunnel.kcp.SetMtu(pooledKCPMTU)
	go tunnel.updateLoop()
	return tunnel, nil
}

func (t *PooledTunnel) SendData(frame []byte) {
	if len(frame) == 0 {
		return
	}
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
			t.touch()
			return
		}
		t.kcpMu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
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

func (t *PooledTunnel) AddWorker(id uint16, conn net.Conn) (<-chan struct{}, error) {
	worker, err := t.reserveWorker(id, conn)
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
	worker, err := t.reserveWorker(id, conn)
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

func (t *PooledTunnel) reserveWorker(id uint16, conn net.Conn) (*pooledWorker, error) {
	select {
	case <-t.closed:
		return nil, errors.New("call multi_user: session already closed")
	default:
	}
	worker := &pooledWorker{
		id:        id,
		conn:      conn,
		parent:    t,
		sendQueue: make(chan []byte, workerSendQueueDepth),
		done:      make(chan struct{}),
	}
	worker.lastInbound.Store(time.Now().UnixNano())
	t.workersMu.Lock()
	replaced := t.workers[id]
	if replaced != nil && time.Since(time.Unix(0, replaced.lastInbound.Load())) < t.staleReplacement {
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
	if replaced != nil {
		replaced.close()
	}
	t.touch()
	return worker, nil
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
	for {
		select {
		case segment := <-w.sendQueue:
			if _, err := w.conn.Write(segment); err != nil {
				w.parent.removeWorker(w)
				return
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

func (w *pooledWorker) watchdogLoop() {
	ticker := time.NewTicker(w.parent.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			lastInbound := time.Unix(0, w.lastInbound.Load())
			if now.Sub(lastInbound) >= w.parent.livenessTimeout {
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
	for _, worker := range workers {
		copySegment := append([]byte(nil), segment...)
		select {
		case worker.sendQueue <- copySegment:
			return
		default:
		}
	}
}

func (t *PooledTunnel) inputSegment(segment []byte) {
	t.kcpMu.Lock()
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
			callback(message)
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
	worker.close()
	t.touch()
}

func (t *PooledTunnel) ActiveWorkers() int {
	t.workersMu.RLock()
	defer t.workersMu.RUnlock()
	return len(t.workers)
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
