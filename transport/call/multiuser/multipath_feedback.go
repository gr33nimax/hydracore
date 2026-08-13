package multiuser

import (
	"bytes"
	"encoding/binary"
	"sort"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

const (
	multipathControlVersion          = 1
	multipathControlData        byte = 1
	multipathControlFeedback    byte = 2
	multipathFeedbackInterval        = 10 * time.Millisecond
	multipathFeedbackWindow          = 64
	multipathReorderingWindow        = 8
	multipathMinimumLossDelay        = 50 * time.Millisecond
	multipathProbeTimeout            = 2 * time.Second
	multipathProbePruneInterval      = 250 * time.Millisecond
	multipathOwnerRetention          = 30 * time.Second
	adaptiveControlCopies            = 2
)

var multipathControlMagic = [8]byte{'H', 'C', 'V', 'K', 'M', 'P', 'X', 1}

type multipathControlPart struct {
	payload      []byte
	preferred   uint16
	hasPreferred bool
}

func (s *multipathScheduler) rankControlWorkers(
	workers []*pooledWorker,
	preferred uint16,
	hasPreferred bool,
	now time.Time,
) []*pooledWorker {
	if !s.adaptive() || len(workers) < 2 {
		return workers
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ranked := append([]*pooledWorker(nil), workers...)
	sort.SliceStable(ranked, func(i, j int) bool {
		if hasPreferred {
			leftPreferred := ranked[i].id == preferred
			rightPreferred := ranked[j].id == preferred
			if leftPreferred != rightPreferred {
				return leftPreferred
			}
		}
		left := s.paths[ranked[i].id]
		right := s.paths[ranked[j].id]
		leftScore := multipathPathScore(left, ranked[i])
		rightScore := multipathPathScore(right, ranked[j])
		if left != nil && left.feedbackSeen && now.Sub(left.feedbackAt) > multipathOwnerRetention {
			leftScore += 100
		}
		if right != nil && right.feedbackSeen && now.Sub(right.feedbackAt) > multipathOwnerRetention {
			rightScore += 100
		}
		return leftScore < rightScore
	})
	return ranked
}

func (s *multipathScheduler) observeInboundPath(
	worker *pooledWorker,
	packet []byte,
	probeSequence uint32,
	now time.Time,
) {
	if !s.adaptive() || worker == nil {
		return
	}
	sequences := kcpPushSequences(packet)
	if len(sequences) == 0 {
		return
	}
	s.mu.Lock()
	state := s.paths[worker.id]
	if state != nil && state.worker == worker {
		for _, sequence := range sequences {
			s.receivedOwners[sequence] = multipathReceivedOwner{workerID: worker.id, seenAt: now}
		}
		if probeSequence != 0 {
			observeMultipathReceiveSequence(state, probeSequence)
		}
	}
	if s.lastOwnerPrune.IsZero() || now.Sub(s.lastOwnerPrune) >= time.Second {
		cutoff := now.Add(-multipathOwnerRetention)
		for sequence, owner := range s.receivedOwners {
			if owner.seenAt.Before(cutoff) {
				delete(s.receivedOwners, sequence)
			}
		}
		s.lastOwnerPrune = now
	}
	s.mu.Unlock()
}

func observeMultipathReceiveSequence(state *multipathPathState, sequence uint32) {
	state.receiveDirty = true
	if !state.receiveHas {
		state.receiveHas = true
		state.receiveLatest = sequence
		state.receiveMask = 1
		return
	}
	if kcpSequenceAfter(sequence, state.receiveLatest) {
		shift := uint32(sequence - state.receiveLatest)
		if shift >= multipathFeedbackWindow {
			state.receiveMask = 1
		} else {
			state.receiveMask = state.receiveMask<<shift | 1
		}
		state.receiveLatest = sequence
		return
	}
	behind := uint32(state.receiveLatest - sequence)
	if behind < multipathFeedbackWindow {
		state.receiveMask |= uint64(1) << behind
	}
}

func (s *multipathScheduler) nextControlFrame(worker *pooledWorker, now time.Time) []byte {
	if !s.adaptive() || worker == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.paths[worker.id]
	if state == nil || state.worker != worker {
		return nil
	}
	feedbackLimited := !state.lastFeedback.IsZero() &&
		now.Sub(state.lastFeedback) < multipathFeedbackInterval
	if !state.receiveHas || !state.receiveDirty || feedbackLimited {
		return nil
	}
	state.receiveDirty = false
	state.lastFeedback = now
	state.metrics.AddHot(telemetry.WorkerPathFeedbackRecordsTotal, 1)
	return encodeMultipathFeedback(worker.id, state.receiveLatest, state.receiveMask)
}

func (s *multipathScheduler) consumeControlFrame(worker *pooledWorker, frame []byte, now time.Time) bool {
	_, workerID, latest, mask, ok := decodeMultipathControl(frame)
	if !ok {
		return false
	}
	if !s.adaptive() || worker == nil || workerID != worker.id {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.paths[worker.id]
	if state == nil || state.worker != worker {
		return true
	}
	state.metrics.Set(telemetry.WorkerPathFeedbackCapable, 1)
	s.applyPathFeedbackLocked(state, latest, mask, now)
	return true
}

func (s *multipathScheduler) applyPathFeedbackLocked(
	state *multipathPathState,
	latest uint32,
	mask uint64,
	now time.Time,
) {
	acked := 0
	lost := 0
	ackedBytes := 0
	lossDelay := multipathMinimumLossDelay
	if state.rttMS > 0 {
		lossDelay = max(lossDelay, time.Duration(2*state.rttMS*float64(time.Millisecond)))
	}
	for sequence, probe := range state.probes {
		if kcpSequenceAfter(sequence, latest) {
			continue
		}
		behind := uint32(latest - sequence)
		delivered := behind < multipathFeedbackWindow && mask&(uint64(1)<<behind) != 0
		if !delivered && behind < multipathReorderingWindow {
			continue
		}
		if !delivered && now.Sub(probe.sentAt) < lossDelay {
			continue
		}
		delete(state.probes, sequence)
		if delivered {
			acked++
			ackedBytes += probe.size
			if !probe.sentAt.IsZero() {
				rttMS := float64(now.Sub(probe.sentAt)) / float64(time.Millisecond)
				if rttMS >= 0 {
					if state.rttMS == 0 {
						state.rttMS = rttMS
					} else {
						state.rttMS += (rttMS - state.rttMS) / 8
					}
				}
			}
			continue
		}
		lost++
	}
	observed := acked + lost
	if observed == 0 {
		return
	}
	state.feedbackSeen = true
	state.feedbackAt = now
	s.recordPathResultLocked(state, acked, lost, ackedBytes, now)
}

func (s *multipathScheduler) expirePathProbesLocked(state *multipathPathState, now time.Time) {
	if state == nil || (!state.lastProbePrune.IsZero() && now.Sub(state.lastProbePrune) < multipathProbePruneInterval) {
		return
	}
	state.lastProbePrune = now
	timeout := multipathProbeTimeout
	if state.rttMS > 0 {
		timeout = max(timeout, time.Duration(4*state.rttMS*float64(time.Millisecond)))
	}
	lost := 0
	for sequence, probe := range state.probes {
		if !probe.sentAt.IsZero() && now.Sub(probe.sentAt) >= timeout {
			delete(state.probes, sequence)
			lost++
		}
	}
	if lost > 0 {
		s.recordPathResultLocked(state, 0, lost, 0, now)
	}
}

func (s *multipathScheduler) recordPathResultLocked(
	state *multipathPathState,
	acked int,
	lost int,
	ackedBytes int,
	now time.Time,
) {
	observed := acked + lost
	if observed == 0 {
		return
	}
	sampleLoss := float64(lost) / float64(observed)
	if !state.lossSeen {
		state.lossSeen = true
		state.feedbackLoss = sampleLoss
	} else {
		state.feedbackLoss += (sampleLoss - state.feedbackLoss) / 8
	}
	state.metrics.AddHot(telemetry.WorkerPathFeedbackAckedPacketsTotal, uint64(acked))
	state.metrics.AddHot(telemetry.WorkerPathFeedbackLostPacketsTotal, uint64(lost))
	if ackedBytes > 0 {
		state.metrics.AddHot(telemetry.WorkerPathAckedBytesTotal, uint64(ackedBytes))
		s.observeDeliveryLocked(state, ackedBytes, now)
	}
	if lost > 0 && state.feedbackLoss >= adaptiveFeedbackBackoffThreshold {
		s.backoffPathLocked(state, now)
	}
	if acked > 0 && state.feedbackLoss <= adaptiveFeedbackHealthyThreshold && state.window < adaptiveMaxPathWindow {
		state.window = min(adaptiveMaxPathWindow, state.window+float64(acked)/max(1, state.window))
	}
}

func (s *multipathScheduler) partitionOutput(packet []byte) ([]byte, []multipathControlPart, bool) {
	if !s.adaptive() {
		return packet, nil, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var push []byte
	var generic []byte
	groups := make(map[uint16][]byte)
	order := make([]uint16, 0, 4)
	valid := forEachRawKCPSegment(packet, func(command byte, sequence uint32, raw []byte) {
		switch command {
		case kcpCommandPush:
			push = append(push, raw...)
		case kcpCommandACK:
			owner, exists := s.receivedOwners[sequence]
			if !exists {
				generic = append(generic, raw...)
				return
			}
			if _, exists = groups[owner.workerID]; !exists {
				order = append(order, owner.workerID)
			}
			groups[owner.workerID] = append(groups[owner.workerID], raw...)
		default:
			generic = append(generic, raw...)
		}
	})
	if !valid {
		return packet, nil, false
	}
	controls := make([]multipathControlPart, 0, len(order)+1)
	for _, workerID := range order {
		controls = append(controls, multipathControlPart{
			payload:       groups[workerID],
			preferred:     workerID,
			hasPreferred: true,
		})
	}
	if len(generic) > 0 {
		controls = append(controls, multipathControlPart{payload: generic})
	}
	return push, controls, true
}

func forEachRawKCPSegment(packet []byte, callback func(command byte, sequence uint32, raw []byte)) bool {
	for len(packet) > 0 {
		if len(packet) < kcpHeaderSize {
			return false
		}
		payloadLength := binary.LittleEndian.Uint32(packet[20:24])
		if payloadLength > uint32(len(packet)-kcpHeaderSize) {
			return false
		}
		segmentLength := kcpHeaderSize + int(payloadLength)
		callback(packet[4], binary.LittleEndian.Uint32(packet[12:16]), packet[:segmentLength])
		packet = packet[segmentLength:]
	}
	return true
}

func encodeMultipathFeedback(workerID uint16, latest uint32, mask uint64) []byte {
	frame := make([]byte, 24)
	copy(frame, multipathControlMagic[:])
	frame[8] = multipathControlFeedback
	frame[9] = multipathControlVersion
	binary.BigEndian.PutUint16(frame[10:12], workerID)
	binary.BigEndian.PutUint32(frame[12:16], latest)
	binary.BigEndian.PutUint64(frame[16:24], mask)
	return frame
}

func encodeMultipathData(workerID uint16, probeSequence uint32, packet []byte) []byte {
	frame := make([]byte, 16+len(packet))
	copy(frame, multipathControlMagic[:])
	frame[8] = multipathControlData
	frame[9] = multipathControlVersion
	binary.BigEndian.PutUint16(frame[10:12], workerID)
	binary.BigEndian.PutUint32(frame[12:16], probeSequence)
	copy(frame[16:], packet)
	return frame
}

func decodeMultipathData(frame []byte, workerID uint16) ([]byte, uint32, bool) {
	if len(frame) <= 16 || !bytes.Equal(frame[:8], multipathControlMagic[:]) ||
		frame[8] != multipathControlData || frame[9] != multipathControlVersion ||
		binary.BigEndian.Uint16(frame[10:12]) != workerID {
		return nil, 0, false
	}
	return frame[16:], binary.BigEndian.Uint32(frame[12:16]), true
}

func decodeMultipathControl(frame []byte) (kind byte, workerID uint16, latest uint32, mask uint64, ok bool) {
	if len(frame) != 24 || !bytes.Equal(frame[:8], multipathControlMagic[:]) || frame[9] != multipathControlVersion {
		return 0, 0, 0, 0, false
	}
	kind = frame[8]
	workerID = binary.BigEndian.Uint16(frame[10:12])
	if kind != multipathControlFeedback {
		return 0, 0, 0, 0, false
	}
	return kind, workerID, binary.BigEndian.Uint32(frame[12:16]), binary.BigEndian.Uint64(frame[16:24]), true
}
