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
	adaptiveChunkPackets       = 16
	adaptiveChunkDwell         = 16 * time.Millisecond
	adaptiveInitialRateBPS     = 8_000_000
	adaptiveMinimumRateBPS     = 1_500_000
	adaptiveMaximumRateBPS     = 32_000_000
	adaptivePacingBurstBytes   = 8 * 1024
	adaptiveRateIncreasePeriod = 200 * time.Millisecond
	adaptiveSentRetention      = 2 * time.Minute
)

type multipathConfig struct {
	profile        MultipathProfile
	fastResend     int
	congestion     int
	chunkPackets   int
	chunkDwell     time.Duration
	initialRateBPS uint64
	minimumRateBPS uint64
	maximumRateBPS uint64
	burstBytes     int
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
		config.chunkPackets = adaptiveChunkPackets
		config.chunkDwell = adaptiveChunkDwell
		config.initialRateBPS = adaptiveInitialRateBPS
		config.minimumRateBPS = adaptiveMinimumRateBPS
		config.maximumRateBPS = adaptiveMaximumRateBPS
		config.burstBytes = adaptivePacingBurstBytes
	}
	return config, nil
}

type queuedSegment struct {
	payload    []byte
	enqueuedAt time.Time
}

type multipathSentSegment struct {
	workerID      uint16
	sentAt        time.Time
	size          int
	retransmitted bool
}

type multipathPathState struct {
	worker       *pooledWorker
	metrics      *telemetry.Accumulator
	rateBPS      uint64
	rttMS        float64
	lossRatio    float64
	lastIncrease time.Time
	lastChunk    uint64
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
		rateBPS: s.config.initialRateBPS,
	}
	s.paths[worker.id] = state
	worker.pacingRateBPS.Store(state.rateBPS)
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
				left.lossRatio < 0.12 && right.lossRatio < 0.12 && left.lastChunk != right.lastChunk {
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

func (s *multipathScheduler) commitOutput(packet []byte, worker *pooledWorker, now time.Time) {
	if !s.adaptive() || worker == nil {
		return
	}
	sequences := kcpPushSequences(packet)
	if len(sequences) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sequence := range sequences {
		segmentSize := kcpSequenceSize(packet, sequence)
		if previous, exists := s.sent[sequence]; exists {
			if state := s.paths[previous.workerID]; state != nil {
				state.lossRatio = state.lossRatio*0.875 + 0.125
				state.rateBPS = max(s.config.minimumRateBPS, state.rateBPS*85/100)
				if state.worker != nil {
					state.worker.pacingRateBPS.Store(state.rateBPS)
				}
				state.metrics.AddHot(telemetry.WorkerPathRetransSegmentsTotal, 1)
			}
			if previous.workerID != worker.id {
				worker.metrics.AddHot(telemetry.WorkerPathSwitchesTotal, 1)
			}
			previous.workerID = worker.id
			previous.sentAt = now
			previous.retransmitted = true
			if segmentSize > 0 {
				previous.size = segmentSize
			}
			s.sent[sequence] = previous
			continue
		}
		s.sent[sequence] = multipathSentSegment{
			workerID: worker.id,
			sentAt:   now,
			size:     segmentSize,
		}
	}
	if s.lastPrune.IsZero() || now.Sub(s.lastPrune) >= time.Minute {
		cutoff := now.Add(-adaptiveSentRetention)
		for sequence, sent := range s.sent {
			if sent.sentAt.Before(cutoff) {
				delete(s.sent, sequence)
			}
		}
		s.lastPrune = now
	}
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
		state.lossRatio *= 0.98
		if state.worker != nil && sent.size > 0 {
			state.worker.metrics.AddHot(telemetry.WorkerPathAckedBytesTotal, uint64(sent.size))
		}
		if sent.retransmitted {
			return
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
		if now.Sub(state.lastIncrease) >= adaptiveRateIncreasePeriod && state.lossRatio < 0.05 {
			state.rateBPS = min(s.config.maximumRateBPS, state.rateBPS+max(uint64(128_000), state.rateBPS/20))
			state.lastIncrease = now
			if state.worker != nil {
				state.worker.pacingRateBPS.Store(state.rateBPS)
			}
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
		worker.metrics.Set(telemetry.WorkerPacingRateBPS, float64(state.rateBPS))
		worker.metrics.Set(telemetry.WorkerPathRTTMS, state.rttMS)
		worker.metrics.Set(telemetry.WorkerPathLossRatio, state.lossRatio)
	}
	s.mu.Unlock()
}

func multipathPathScore(state *multipathPathState, worker *pooledWorker) float64 {
	queue := float64(len(worker.sendQueue) + 4*len(worker.controlQueue))
	if state == nil {
		return queue * 20
	}
	ratePenalty := 0.0
	if state.rateBPS > 0 {
		ratePenalty = float64(adaptiveInitialRateBPS) / float64(state.rateBPS)
	}
	return queue*20 + state.lossRatio*1000 + state.rttMS/5 + ratePenalty
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
