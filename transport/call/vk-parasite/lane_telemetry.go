package vkparasite

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
)

const (
	kcpCommandPush = 81
	kcpCommandACK  = 82
	kcpHeaderSize  = 24
)

func (l *kcpLane) observeKCPOutput(packet []byte) {
	now := time.Now()
	forEachKCPSegment(packet, func(command byte, sequence, timestamp uint32, size int) {
		l.metrics.AddHot(telemetry.KCPOutSegmentsTotal, 1)
		l.metrics.AddHot(telemetry.KCPOutBytesTotal, uint64(size))
		if command != kcpCommandPush {
			return
		}
		l.deliveryOutSegments++
		if previous, exists := l.kcpSent[sequence]; exists {
			l.deliveryRetrans++
			if isEstimatedRTO(now.Sub(previous.lastSentAt), l.estimatedKCPRTO()) {
				l.metrics.AddHot(telemetry.KCPRTORetransEstimateSegmentsTotal, 1)
				l.metrics.AddHot(telemetry.KCPRTORetransEstimateBytesTotal, uint64(size))
			} else {
				l.metrics.AddHot(telemetry.KCPFastRetransEstimateSegmentsTotal, 1)
				l.metrics.AddHot(telemetry.KCPFastRetransEstimateBytesTotal, uint64(size))
			}
			previous.lastSentAt = now
			previous.attempts = append(previous.attempts, kcpSendAttempt{timestamp: timestamp, sentAt: now})
			l.kcpSent[sequence] = previous
			l.metrics.AddHot(telemetry.KCPRetransSegmentsTotal, 1)
			l.metrics.AddHot(telemetry.KCPRetransBytesTotal, uint64(size))
			return
		}
		l.kcpSent[sequence] = kcpSentSegment{
			lastSentAt: now,
			attempts:   []kcpSendAttempt{{timestamp: timestamp, sentAt: now}},
		}
	})
}

func (l *kcpLane) estimatedKCPRTO() time.Duration {
	if l.kcpSRTTMS <= 0 {
		return 200 * time.Millisecond
	}
	rtoMS := l.kcpSRTTMS + 4*l.kcpRTTVARMS
	if rtoMS < 30 {
		rtoMS = 30
	}
	return time.Duration(rtoMS * float64(time.Millisecond))
}

func isEstimatedRTO(elapsed, rto time.Duration) bool {
	// kcp-go does not expose the internal retransmission reason. Classify the
	// callback conservatively from the lane's measured RTO and keep the metric
	// name explicit that this is an estimate.
	return elapsed >= rto*3/4
}

func (l *kcpLane) observeKCPInput(packet []byte) {
	now := time.Now()
	ackedProgress := 0
	var cumulativeACK uint32
	hasCumulativeACK := false
	type acknowledgement struct{ sequence, timestamp uint32 }
	acknowledgements := make([]acknowledgement, 0, 1)
	forEachKCPSegmentHeader(packet, func(command byte, sequence, una, timestamp uint32, _ int) {
		if !hasCumulativeACK || kcpSequenceAfter(una, cumulativeACK) {
			cumulativeACK = una
			hasCumulativeACK = true
		}
		if command != kcpCommandACK {
			return
		}
		acknowledgements = append(acknowledgements, acknowledgement{sequence, timestamp})
	})
	for _, ack := range acknowledgements {
		l.metrics.AddHot(telemetry.KCPAckSegmentsTotal, 1)
		sent, exists := l.kcpSent[ack.sequence]
		if !exists {
			continue
		}
		delete(l.kcpSent, ack.sequence)
		l.metrics.AddHot(telemetry.KCPAckProgressSegmentsTotal, 1)
		ackedProgress++
		for _, attempt := range sent.attempts {
			if attempt.timestamp == ack.timestamp {
				l.updateKCPRTT(float64(now.Sub(attempt.sentAt)) / float64(time.Millisecond))
				break
			}
		}
	}
	if hasCumulativeACK {
		pruned := l.pruneKCPSentBefore(cumulativeACK)
		l.metrics.AddHot(telemetry.KCPAckProgressSegmentsTotal, uint64(pruned))
		ackedProgress += pruned
	}
	if ackedProgress > 0 {
		l.lastAckProgress.Store(now.UnixNano())
		if l.parent != nil {
			l.parent.markAggregateProgress()
		}
		l.updateDeliveryController(now, ackedProgress)
	}
}

func (l *kcpLane) updateDeliveryController(now time.Time, ackedSegments int) {
	if ackedSegments <= 0 {
		return
	}
	if l.deliverySampleAt.IsZero() {
		l.deliverySampleAt = now
	}
	// Do not interpret the first ACK after an idle period as a low-rate path.
	// Start a fresh delivery epoch instead.
	if idle := now.Sub(l.deliverySampleAt); idle > 5*time.Second {
		l.deliverySampleAt = now
		l.deliveryAckedSegments = uint64(ackedSegments)
		l.deliveryOutSegments = 0
		l.deliveryRetrans = 0
		l.deliveryDemanded = false
		l.applicationLimited = true
		l.metrics.Set(telemetry.LaneApplicationLimited, 1)
		return
	}
	l.deliveryAckedSegments += uint64(ackedSegments)
	elapsed := now.Sub(l.deliverySampleAt)
	if elapsed < laneDeliverySampleWindow {
		return
	}
	mss := float64(laneKCPMTU - kcpHeaderSize)
	instantRate := float64(l.deliveryAckedSegments) * mss / elapsed.Seconds()
	applicationLimited := !l.deliveryDemanded
	l.applicationLimited = applicationLimited
	l.metrics.Set(telemetry.LaneApplicationLimited, boolFloat(applicationLimited))
	retryRatio := 0.0
	if l.deliveryOutSegments > 0 {
		retryRatio = float64(l.deliveryRetrans) / float64(l.deliveryOutSegments)
	}
	queueDepth := len(l.outputPending)
	queueGrowing := queueDepth > l.previousOutputDepth && queueDepth >= lanePacingBucketSegments
	rttInflated := l.minRTTMS > 0 && l.kcpSRTTMS > 2*l.minRTTMS
	severeRetry := retryRatio > 0.15
	congestionSignal := !applicationLimited && (severeRetry || rttInflated && queueGrowing)
	if !applicationLimited {
		if l.deliveryRateBPS == 0 {
			l.deliveryRateBPS = instantRate
		} else {
			l.deliveryRateBPS = 0.75*l.deliveryRateBPS + 0.25*instantRate
		}
		if l.deliveryCapacityBPS == 0 || instantRate > l.deliveryCapacityBPS {
			l.deliveryCapacityBPS = instantRate
		} else {
			l.deliveryCapacityBPS = max(instantRate, 0.98*l.deliveryCapacityBPS)
		}
		if congestionSignal {
			l.congestionSamples++
		} else {
			l.congestionSamples = 0
		}
		congested := l.congestionSamples >= laneCongestionSamples
		switch {
		case congested:
			l.pacingRateBPS *= lanePacingDecrease
			l.deliveryCapacityBPS = max(instantRate, lanePacingDecrease*l.deliveryCapacityBPS)
			l.pacingStartup = false
			l.pacingProbeUntil = time.Time{}
			l.pacingNextProbe = now.Add(lanePacingProbeInterval)
			l.congestionSamples = 0
		case l.pacingStartup && instantRate >= 0.55*l.pacingRateBPS && retryRatio < 0.15:
			l.pacingRateBPS *= lanePacingStartupGain
			if l.pacingRateBPS >= lanePacingMaximumBPS {
				l.pacingStartup = false
				l.pacingNextProbe = now.Add(lanePacingProbeInterval)
			}
		case !l.pacingStartup && !l.pacingProbeUntil.IsZero() && !now.Before(l.pacingProbeUntil):
			l.pacingRateBPS = lanePacingSteadyGain * l.deliveryCapacityBPS
			l.pacingProbeUntil = time.Time{}
		case !l.pacingStartup && l.pacingProbeUntil.IsZero() && !now.Before(l.pacingNextProbe):
			l.pacingRateBPS *= lanePacingProbeGain
			l.pacingProbeUntil = now.Add(lanePacingProbeDuration)
			l.pacingNextProbe = now.Add(lanePacingProbeInterval)
		case !l.pacingStartup:
			targetRate := lanePacingSteadyGain * l.deliveryCapacityBPS
			l.pacingRateBPS = 0.75*l.pacingRateBPS + 0.25*targetRate
		}
		l.pacingRateBPS = min(float64(lanePacingMaximumBPS), max(float64(lanePacingMinimumBPS), l.pacingRateBPS))
		windowRTTMS := max(80, l.minRTTMS)
		if l.minRTTMS <= 0 {
			windowRTTMS = 80
		}
		windowRate := max(l.pacingRateBPS, l.deliveryCapacityBPS)
		target := int(math.Ceil(1.5 * windowRate * (windowRTTMS / 1000) / mss))
		l.admissionWindow = min(laneKCPMaximumAdmission, max(laneKCPMinimumAdmission, target))
	}
	l.previousOutputDepth = queueDepth
	l.deliverySampleAt = now
	l.deliveryAckedSegments = 0
	l.deliveryOutSegments = 0
	l.deliveryRetrans = 0
	l.deliveryDemanded = false
}

func (l *kcpLane) updateKCPRTT(rtt float64) {
	if rtt < 0 {
		return
	}
	l.metrics.AddHot(telemetry.KCPRTTSamplesTotal, 1)
	if l.minRTTMS == 0 || rtt < l.minRTTMS {
		l.minRTTMS = rtt
	}
	if l.kcpSRTTMS == 0 {
		l.kcpSRTTMS = rtt
		l.kcpRTTVARMS = rtt / 2
		return
	}
	delta := rtt - l.kcpSRTTMS
	l.kcpSRTTMS += delta / 8
	if delta < 0 {
		delta = -delta
	}
	l.kcpRTTVARMS += (delta - l.kcpRTTVARMS) / 4
}

func (l *kcpLane) pruneKCPSentBefore(una uint32) int {
	removed := 0
	if !l.kcpHasUNA {
		removed = l.pruneKCPSentFallback(una)
		l.kcpLastUNA = una
		l.kcpHasUNA = true
		return removed
	}
	distance := uint32(una - l.kcpLastUNA)
	if distance == 0 || !kcpSequenceAfter(una, l.kcpLastUNA) {
		return 0
	}
	if distance > laneKCPMaxPending*2 {
		removed = l.pruneKCPSentFallback(una)
	} else {
		for sequence := l.kcpLastUNA; sequence != una; sequence++ {
			if _, exists := l.kcpSent[sequence]; exists {
				delete(l.kcpSent, sequence)
				removed++
			}
		}
	}
	l.kcpLastUNA = una
	return removed
}

func (l *kcpLane) pruneKCPSentFallback(una uint32) int {
	removed := 0
	for sequence := range l.kcpSent {
		if kcpSequenceBefore(sequence, una) {
			delete(l.kcpSent, sequence)
			removed++
		}
	}
	return removed
}

func forEachKCPSegment(packet []byte, callback func(command byte, sequence, timestamp uint32, size int)) {
	forEachKCPSegmentHeader(packet, func(command byte, sequence, _ uint32, timestamp uint32, size int) {
		callback(command, sequence, timestamp, size)
	})
}

func forEachKCPSegmentHeader(packet []byte, callback func(command byte, sequence, una, timestamp uint32, size int)) {
	for len(packet) >= kcpHeaderSize {
		length := int(binary.LittleEndian.Uint32(packet[20:24]))
		if length < 0 || kcpHeaderSize+length > len(packet) {
			return
		}
		segmentSize := kcpHeaderSize + length
		callback(
			packet[4],
			binary.LittleEndian.Uint32(packet[12:16]),
			binary.LittleEndian.Uint32(packet[16:20]),
			binary.LittleEndian.Uint32(packet[8:12]),
			segmentSize,
		)
		packet = packet[segmentSize:]
	}
}

func kcpSequenceBefore(sequence, reference uint32) bool {
	return int32(sequence-reference) < 0
}

func kcpSequenceAfter(sequence, reference uint32) bool {
	return int32(sequence-reference) > 0
}

func (t *ParasiteTunnel) handleTelemetryMessage(message []byte) bool {
	if len(message) < 9 {
		return false
	}
	frameLength := int(binary.BigEndian.Uint32(message[:4]))
	if frameLength < 5 || frameLength+4 != len(message) || binary.BigEndian.Uint32(message[4:8]) != calltunnel.ControlConnID {
		return false
	}
	payload := message[9:]
	switch message[8] {
	case calltunnel.MsgTelemetryControl:
		if len(payload) != 3 || payload[0] != telemetry.SchemaVersion {
			return true
		}
		lease := time.Duration(binary.BigEndian.Uint16(payload[1:])) * time.Second
		t.telemetryMu.RLock()
		handler := t.onTelemetryControl
		t.telemetryMu.RUnlock()
		if handler != nil {
			handler(lease)
		}
		return true
	case calltunnel.MsgTelemetryRecord:
		if len(payload) == 0 || len(payload) > telemetry.MaximumRecordLen {
			return true
		}
		t.telemetryMu.RLock()
		handler := t.onTelemetryClientRecord
		t.telemetryMu.RUnlock()
		if handler != nil {
			handler(append([]byte(nil), payload...))
		}
		return true
	default:
		return false
	}
}

func (t *ParasiteTunnel) RequestClientTelemetry(lease time.Duration) bool {
	seconds := int(lease / time.Second)
	if seconds < 2 {
		seconds = 2
	}
	if seconds > 120 {
		seconds = 120
	}
	payload := []byte{telemetry.SchemaVersion, 0, 0}
	binary.BigEndian.PutUint16(payload[1:], uint16(seconds))
	return t.trySendControlData(calltunnel.EncodeFrame(calltunnel.ControlConnID, calltunnel.MsgTelemetryControl, payload))
}

func (t *ParasiteTunnel) SendClientTelemetry(record []byte) bool {
	if len(record) == 0 || len(record) > telemetry.MaximumRecordLen {
		return false
	}
	return t.trySendData(calltunnel.EncodeFrame(calltunnel.ControlConnID, calltunnel.MsgTelemetryRecord, record))
}

func (t *ParasiteTunnel) RelaySetActive(tcp, udp int) {
	t.metrics.Set(telemetry.RelayTCPActive, float64(tcp))
	t.metrics.Set(telemetry.RelayUDPActive, float64(udp))
}

func (t *ParasiteTunnel) RelayAddBytes(bytes uint64) {
	t.metrics.AddHot(telemetry.RelayBytesTotal, bytes)
	if bytes > 0 {
		t.markAggregateProgress()
	}
}

func (t *ParasiteTunnel) RelayQueueDelta(bytes int) {
	t.metrics.AddHotGauge(telemetry.RelayQueueDepth, float64(bytes))
}

func (t *ParasiteTunnel) RelayResetQueue() {
	t.metrics.Set(telemetry.RelayQueueDepth, 0)
}

func (t *ParasiteTunnel) RelayQueueDrop() {
	t.metrics.Add(telemetry.RelayQueueDropsTotal, 1)
}

func (t *ParasiteTunnel) RelayConnectFailure() {
	t.metrics.Add(telemetry.RelayConnectFailureTotal, 1)
}
