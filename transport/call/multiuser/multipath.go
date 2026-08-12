package multiuser

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

type MultipathProfile string

const (
	MultipathProfileLegacy   MultipathProfile = "legacy"
	MultipathProfileAdaptive MultipathProfile = "adaptive"
)

const (
	adaptiveChunkPackets  = 16
	adaptiveChunkDwell    = 16 * time.Millisecond
	adaptiveSentRetention = 2 * time.Minute
)

type multipathConfig struct {
	profile      MultipathProfile
	fastResend   int
	congestion   int
	chunkPackets int
	chunkDwell   time.Duration
}

func normalizeMultipathProfile(profile MultipathProfile) (MultipathProfile, error) {
	switch profile {
	case "", MultipathProfileLegacy:
		return MultipathProfileLegacy, nil
	case MultipathProfileAdaptive:
		return MultipathProfileAdaptive, nil
	default:
		return "", errors.New("call multi_user: multipath_profile must be legacy or adaptive")
	}
}

func multipathConfigFor(profile MultipathProfile) (multipathConfig, error) {
	profile, err := normalizeMultipathProfile(profile)
	if err != nil {
		return multipathConfig{}, err
	}
	config := multipathConfig{
		profile:    profile,
		fastResend: 2,
		congestion: 1,
	}
	if profile == MultipathProfileAdaptive {
		config.fastResend = 4
		config.congestion = 0
		config.chunkPackets = adaptiveChunkPackets
		config.chunkDwell = adaptiveChunkDwell
	}
	return config, nil
}

type queuedSegment struct {
	payload    []byte
	enqueuedAt time.Time
}

type multipathSentSegment struct {
	workerID      uint16
	assignedAt    time.Time
	sentAt        time.Time
	size          int
	retransmitted bool
}

type multipathPathState struct {
	worker        *pooledWorker
	metrics       *telemetry.Accumulator
	rttMS         float64
	retryPressure float64
	lastChunk     uint64
}

type multipathScheduler struct {
	mu sync.Mutex

	config multipathConfig
	paths  map[uint16]*multipathPathState
	sent   map[uint32]multipathSentSegment

	chunkWorker    uint16
	chunkRemaining int
	chunkUntil     time.Time
	chunkSet       bool
	chunkOrdinal   uint64
	lastPrune      time.Time
}

func newMultipathScheduler(config multipathConfig) *multipathScheduler {
	return &multipathScheduler{
		config: config,
		paths:  make(map[uint16]*multipathPathState),
		sent:   make(map[uint32]multipathSentSegment),
	}
}

func (s *multipathScheduler) adaptive() bool {
	return s != nil && s.config.profile == MultipathProfileAdaptive
}

func (s *multipathScheduler) registerWorker(worker *pooledWorker) {
	if !s.adaptive() || worker == nil {
		return
	}
	s.mu.Lock()
	state := &multipathPathState{
		worker:  worker,
		metrics: worker.metrics,
	}
	s.paths[worker.id] = state
	s.mu.Unlock()
}

func (s *multipathScheduler) removeWorker(worker *pooledWorker) {
	if !s.adaptive() || worker == nil {
		return
	}
	s.mu.Lock()
	if state := s.paths[worker.id]; state != nil && state.worker == worker {
		state.worker = nil
	}
	for sequence, sent := range s.sent {
		if sent.workerID == worker.id {
			sent.retransmitted = true
			s.sent[sequence] = sent
		}
	}
	if s.chunkSet && s.chunkWorker == worker.id {
		s.chunkSet = false
	}
	s.mu.Unlock()
}

func (s *multipathScheduler) rankWorkers(workers []*pooledWorker, packet []byte, now time.Time) []*pooledWorker {
	if !s.adaptive() || len(workers) < 2 {
		return workers
	}
	pushSequences := kcpPushSequences(packet)
	s.mu.Lock()
	defer s.mu.Unlock()

	var previousWorker uint16
	var hasPrevious bool
	for _, sequence := range pushSequences {
		if previous, exists := s.sent[sequence]; exists {
			previousWorker = previous.workerID
			hasPrevious = true
			break
		}
	}

	preferred := uint16(0)
	hasPreferred := false
	if len(pushSequences) > 0 && !hasPrevious && s.chunkSet &&
		s.chunkRemaining > 0 && now.Before(s.chunkUntil) && workerPresent(workers, s.chunkWorker) {
		preferred = s.chunkWorker
		hasPreferred = true
		s.chunkRemaining--
	}
	if !hasPreferred {
		ranked := append([]*pooledWorker(nil), workers...)
		sort.SliceStable(ranked, func(i, j int) bool {
			left := s.paths[ranked[i].id]
			right := s.paths[ranked[j].id]
			if hasPrevious {
				leftPrevious := ranked[i].id == previousWorker
				rightPrevious := ranked[j].id == previousWorker
				if leftPrevious != rightPrevious {
					return !leftPrevious
				}
			}
			leftScore := multipathPathScore(left, ranked[i])
			rightScore := multipathPathScore(right, ranked[j])
			if len(pushSequences) > 0 && !hasPrevious && left != nil && right != nil &&
				left.retryPressure < 0.12 && right.retryPressure < 0.12 && left.lastChunk != right.lastChunk {
				return left.lastChunk < right.lastChunk
			}
			return leftScore < rightScore
		})
		workers = ranked
		if len(workers) > 0 {
			preferred = workers[0].id
			hasPreferred = true
		}
		if hasPreferred && len(pushSequences) > 0 && !hasPrevious {
			s.chunkOrdinal++
			if state := s.paths[preferred]; state != nil {
				state.lastChunk = s.chunkOrdinal
			}
			s.chunkWorker = preferred
			s.chunkRemaining = max(0, s.config.chunkPackets-len(pushSequences))
			s.chunkUntil = now.Add(s.config.chunkDwell)
			s.chunkSet = true
		}
	}
	if hasPreferred {
		moveWorkerFirst(workers, preferred)
	}
	return workers
}

func (s *multipathScheduler) assignOutput(packet []byte, worker *pooledWorker, now time.Time) {
	if !s.adaptive() || worker == nil {
		return
	}
	sequences := kcpPushSequences(packet)
	if len(sequences) == 0 {
		return
	}
	s.mu.Lock()
	s.assignOutputLocked(packet, sequences, worker, now)
	s.mu.Unlock()
}

func (s *multipathScheduler) enqueueOutput(
	packet []byte,
	worker *pooledWorker,
	queued queuedSegment,
	queue chan queuedSegment,
) bool {
	if !s.adaptive() || worker == nil {
		return false
	}
	sequences := kcpPushSequences(packet)
	if len(sequences) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case queue <- queued:
		s.assignOutputLocked(packet, sequences, worker, queued.enqueuedAt)
		return true
	case <-worker.done:
		return false
	default:
		return false
	}
}

func (s *multipathScheduler) assignOutputLocked(
	packet []byte,
	sequences []uint32,
	worker *pooledWorker,
	now time.Time,
) {
	for _, sequence := range sequences {
		segmentSize := kcpSequenceSize(packet, sequence)
		if previous, exists := s.sent[sequence]; exists {
			if state := s.paths[previous.workerID]; state != nil {
				state.retryPressure = state.retryPressure*0.9375 + 0.0625
				state.metrics.AddHot(telemetry.WorkerPathRetransSegmentsTotal, 1)
			}
			if previous.workerID != worker.id {
				worker.metrics.AddHot(telemetry.WorkerPathSwitchesTotal, 1)
			}
			previous.workerID = worker.id
			previous.assignedAt = now
			previous.sentAt = time.Time{}
			previous.retransmitted = true
			if segmentSize > 0 {
				previous.size = segmentSize
			}
			s.sent[sequence] = previous
			continue
		}
		s.sent[sequence] = multipathSentSegment{
			workerID:   worker.id,
			assignedAt: now,
			size:       segmentSize,
		}
	}
	if s.lastPrune.IsZero() || now.Sub(s.lastPrune) >= time.Minute {
		cutoff := now.Add(-adaptiveSentRetention)
		for sequence, sent := range s.sent {
			if sent.assignedAt.Before(cutoff) {
				delete(s.sent, sequence)
			}
		}
		s.lastPrune = now
	}
}

func (s *multipathScheduler) commitWrite(packet []byte, worker *pooledWorker, now time.Time) {
	if !s.adaptive() || worker == nil {
		return
	}
	sequences := kcpPushSequences(packet)
	if len(sequences) == 0 {
		return
	}
	s.mu.Lock()
	for _, sequence := range sequences {
		sent, exists := s.sent[sequence]
		if !exists || sent.workerID != worker.id || !sent.sentAt.IsZero() {
			continue
		}
		sent.sentAt = now
		s.sent[sequence] = sent
	}
	s.mu.Unlock()
}

func (s *multipathScheduler) observeInput(packet []byte, now time.Time) {
	if !s.adaptive() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	forEachKCPSegment(packet, func(command byte, sequence uint32, _ int) {
		if command != kcpCommandACK {
			return
		}
		sent, exists := s.sent[sequence]
		if !exists {
			return
		}
		delete(s.sent, sequence)
		state := s.paths[sent.workerID]
		if state == nil {
			return
		}
		state.retryPressure *= 0.9375
		if sent.retransmitted || sent.sentAt.IsZero() {
			return
		}
		if state.worker != nil && sent.size > 0 {
			state.worker.metrics.AddHot(telemetry.WorkerPathAckedBytesTotal, uint64(sent.size))
		}
		rttMS := float64(now.Sub(sent.sentAt)) / float64(time.Millisecond)
		if rttMS < 0 {
			return
		}
		if state.rttMS == 0 {
			state.rttMS = rttMS
		} else {
			state.rttMS += (rttMS - state.rttMS) / 8
		}
	})
}

func (s *multipathScheduler) publishWorkerMetrics(worker *pooledWorker) {
	if !s.adaptive() || worker == nil {
		return
	}
	s.mu.Lock()
	state := s.paths[worker.id]
	if state != nil {
		worker.metrics.Set(telemetry.WorkerPathRTTMS, state.rttMS)
		worker.metrics.Set(telemetry.WorkerPathRetryRatio, state.retryPressure)
		// Compatibility alias for the first adaptive telemetry schema. This is
		// KCP retry pressure, not an estimate of physical TURN packet loss.
		worker.metrics.Set(telemetry.WorkerPathLossRatio, state.retryPressure)
	}
	s.mu.Unlock()
}

func multipathPathScore(state *multipathPathState, worker *pooledWorker) float64 {
	queue := float64(len(worker.sendQueue) + 4*len(worker.controlQueue))
	if state == nil {
		return queue * 20
	}
	return queue*20 + state.retryPressure*250 + state.rttMS/10
}

func workerPresent(workers []*pooledWorker, id uint16) bool {
	for _, worker := range workers {
		if worker.id == id {
			return true
		}
	}
	return false
}

func moveWorkerFirst(workers []*pooledWorker, id uint16) {
	for index, worker := range workers {
		if worker.id == id {
			copy(workers[1:index+1], workers[:index])
			workers[0] = worker
			return
		}
	}
}

func kcpPushSequences(packet []byte) []uint32 {
	sequences := make([]uint32, 0, 1)
	forEachKCPSegment(packet, func(command byte, sequence uint32, _ int) {
		if command == kcpCommandPush {
			sequences = append(sequences, sequence)
		}
	})
	return sequences
}

func kcpSequenceSize(packet []byte, target uint32) int {
	size := 0
	forEachKCPSegment(packet, func(command byte, sequence uint32, segmentSize int) {
		if command == kcpCommandPush && sequence == target {
			size = segmentSize
		}
	})
	return size
}
