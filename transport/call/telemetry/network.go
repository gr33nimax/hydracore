package telemetry

import (
	"encoding/binary"
	"math"
	"time"
)

const rtpHeaderSize = 12

const (
	maximumTrackedRTPStreams = 512
	networkJitterIdleReset    = time.Second
)

type sequenceState struct {
	initialized bool
	maxSequence uint16
	cycles      uint64
	base        uint64
	highest     uint64
	bitmap      uint64
	received    uint64
	lastArrival time.Time
	meanGapMS   float64
	jitterMS    float64
}

func (a *Accumulator) ObserveOuterPacket(wire []byte, arrival time.Time) {
	if a == nil || !a.CollectionActive() || len(wire) < rtpHeaderSize || wire[0]>>6 != 2 {
		return
	}
	ssrc := binary.BigEndian.Uint32(wire[8:12])
	sequence := binary.BigEndian.Uint16(wire[2:4])
	a.networkMu.Lock()
	stream := a.networkStreams[ssrc]
	if stream == nil {
		if len(a.networkStreams) >= maximumTrackedRTPStreams {
			var oldestSSRC uint32
			var oldest time.Time
			for candidateSSRC, candidate := range a.networkStreams {
				if oldest.IsZero() || candidate.lastArrival.Before(oldest) {
					oldestSSRC = candidateSSRC
					oldest = candidate.lastArrival
				}
			}
			delete(a.networkStreams, oldestSSRC)
		}
		stream = new(sequenceState)
		a.networkStreams[ssrc] = stream
	}
	duplicate, reordered := stream.observeSequence(sequence)
	stream.observeArrival(arrival)
	var expected, received uint64
	var jitterSum float64
	var jitterCount int
	for _, current := range a.networkStreams {
		if current.initialized {
			expected += current.highest - current.base + 1
			received += current.received
			jitterSum += current.jitterMS
			jitterCount++
		}
	}
	a.networkMu.Unlock()
	if duplicate {
		a.Add(OuterDuplicatePacketsTotal, 1)
	}
	if reordered {
		a.Add(OuterReorderedPacketsTotal, 1)
	}
	lost := uint64(0)
	if expected > received {
		lost = expected - received
	}
	if expected > 0 {
		a.Set(NetworkLossRatio, float64(lost)/float64(expected))
	}
	if jitterCount > 0 {
		a.Set(NetworkJitterMS, jitterSum/float64(jitterCount))
	}
}

func (s *sequenceState) observeSequence(sequence uint16) (duplicate bool, reordered bool) {
	if !s.initialized {
		s.initialized = true
		s.maxSequence = sequence
		s.base = uint64(sequence)
		s.highest = uint64(sequence)
		s.bitmap = 1
		s.received = 1
		return false, false
	}
	previousMaximum := s.maxSequence
	if sequence < previousMaximum && previousMaximum-sequence > 0x8000 {
		s.cycles += 1 << 16
		s.maxSequence = sequence
	} else if sequence > previousMaximum && sequence-previousMaximum < 0x8000 {
		s.maxSequence = sequence
	}
	extended := s.cycles + uint64(sequence)
	if sequence > previousMaximum && sequence-previousMaximum > 0x8000 && s.cycles >= 1<<16 {
		extended -= 1 << 16
	}
	if extended > s.highest {
		shift := extended - s.highest
		if shift >= 64 {
			s.bitmap = 1
		} else {
			s.bitmap = s.bitmap<<shift | 1
		}
		s.highest = extended
		s.received++
		return false, false
	}
	behind := s.highest - extended
	if behind >= 64 {
		return false, true
	}
	mask := uint64(1) << behind
	if s.bitmap&mask != 0 {
		return true, false
	}
	s.bitmap |= mask
	s.received++
	return false, behind > 0
}

func (s *sequenceState) observeArrival(arrival time.Time) {
	if s.lastArrival.IsZero() {
		s.lastArrival = arrival
		return
	}
	gapMS := float64(arrival.Sub(s.lastArrival)) / float64(time.Millisecond)
	s.lastArrival = arrival
	if gapMS < 0 {
		return
	}
	if gapMS >= float64(networkJitterIdleReset)/float64(time.Millisecond) {
		s.meanGapMS = 0
		s.jitterMS = 0
		return
	}
	if s.meanGapMS == 0 {
		s.meanGapMS = gapMS
		return
	}
	deviation := math.Abs(gapMS - s.meanGapMS)
	s.meanGapMS += (gapMS - s.meanGapMS) / 16
	s.jitterMS += (deviation - s.jitterMS) / 16
}
