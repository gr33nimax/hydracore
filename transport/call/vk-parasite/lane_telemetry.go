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
			l.deliveryRetransBytes += uint64(size)
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
			size:       size,
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
		l.ackedBytesTotal += uint64(sent.size)
		l.metrics.AddHot(telemetry.KCPAckedBytesTotal, uint64(sent.size))
		l.metrics.AddHot(telemetry.KCPAckProgressSegmentsTotal, 1)
		ackedProgress++
		// Karn's algorithm: an ACK for a retransmitted segment is ambiguous and
		// must not feed the RTT/RTO estimator, even when its echoed timestamp
		// happens to match one of the attempts.
		if len(sent.attempts) == 1 && sent.attempts[0].timestamp == ack.timestamp {
			l.updateKCPRTT(float64(now.Sub(sent.attempts[0].sentAt)) / float64(time.Millisecond))
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
		l.deliveryRetransBytes = 0
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
	queueDepth := len(l.outputPending)
	waitSnd := 0
	if l.kcp != nil {
		waitSnd = l.kcp.WaitSnd()
	}
	// Token/admission rejection is not the only proof of demand. A lane with
	// a material KCP or writer backlog is network-limited even if the caller did
	// not hit a limiter during this sample; treating it as application-limited
	// would disable congestion backoff precisely while the path is saturated.
	backlogged := queueDepth >= lanePacingBucketSegments || waitSnd >= lanePacingBucketSegments
	applicationLimited := !l.deliveryDemanded && !backlogged
	l.applicationLimited = applicationLimited
	l.metrics.Set(telemetry.LaneApplicationLimited, boolFloat(applicationLimited))
	// Byte-granular window deltas drive the marginal-goodput probe evaluation.
	// The pacing knob position is not the offered load: admission spends
	// tokens only when the application actually supplies data, and the steady
	// branch blends probe bumps down within the probe itself, so the probe
	// compares measured bytes instead.
	ackedDelta := l.ackedBytesTotal - l.lastAckedBytes
	admittedDelta := l.admittedBytesTotal - l.lastAdmittedBytes
	l.lastAckedBytes = l.ackedBytesTotal
	l.lastAdmittedBytes = l.admittedBytesTotal
	l.windowAckedBytes[0] = l.windowAckedBytes[1]
	l.windowAckedBytes[1] = ackedDelta
	l.windowAdmittedBytes[0] = l.windowAdmittedBytes[1]
	l.windowAdmittedBytes[1] = admittedDelta
	demand := l.deliveryDemanded || backlogged
	l.windowDemandBits = (l.windowDemandBits << 1 | boolBit(demand)) & 0b11
	if !l.pacingProbeUntil.IsZero() {
		l.probeWindows++
		l.probeAckedBytes += ackedDelta
		l.probeAdmittedBytes += admittedDelta
		if demand {
			l.probeDemandWindows++
		}
	}
	retryRatio := 0.0
	if l.deliveryOutSegments > 0 {
		retryRatio = float64(l.deliveryRetrans) / float64(l.deliveryOutSegments)
	}
	// Compensation reacts to the smoothed retry ratio, not to a single window:
	// one lossy burst must not swing the offered rate, and one clean window
	// on a degraded path must not restore it.
	if l.retryRatioSmooth == 0 {
		l.retryRatioSmooth = retryRatio
	} else {
		l.retryRatioSmooth += laneRetryRatioSmoothing * (retryRatio - l.retryRatioSmooth)
	}
	instantRetxRate := float64(l.deliveryRetransBytes) / elapsed.Seconds()
	if l.retxRateBPS == 0 {
		l.retxRateBPS = instantRetxRate
	} else {
		l.retxRateBPS += laneRetransmitRateSmoothing * (instantRetxRate - l.retxRateBPS)
	}
	queueGrowing := queueDepth > l.previousOutputDepth && queueDepth >= lanePacingBucketSegments
	queuePressured := queueDepth >= 3*laneKCPOutputBacklog/4
	windowPressure := waitSnd >= max(lanePacingBucketSegments, 3*l.admissionLimitLocked(false)/4)
	rttInflated := l.minRTTMS > 0 && l.kcpSRTTMS > 2*l.minRTTMS
	severeRetry := retryRatio > 0.15
	previousDeliveryRate := l.deliveryRateBPS
	deliveryCollapsed := previousDeliveryRate > 0 && instantRate < 0.70*previousDeliveryRate
	transportPressure := queueGrowing || queuePressured || windowPressure
	// VK media paths can sustain useful throughput with double-digit random or
	// policer loss. Retransmissions alone therefore describe a lossy path, not
	// congestion. Back off only when delay or collapsing delivery is accompanied
	// by persistent transport pressure.
	congestionSignal := !applicationLimited && transportPressure && (rttInflated || (severeRetry && deliveryCollapsed))
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
		case l.pacingStartup && !congestionSignal && instantRate >= 0.55*l.pacingRateBPS:
			l.pacingRateBPS *= lanePacingStartupGain
			if l.pacingRateBPS >= lanePacingMaximumBPS {
				l.pacingStartup = false
				l.pacingNextProbe = now.Add(lanePacingProbeInterval)
			}
		case l.pacingStartup:
			// Delivery stopped following the startup ramp. Enter the measured
			// loss-aware steady state instead of remaining forever at the last
			// startup step or treating the retry ratio alone as congestion.
			l.pacingRateBPS = l.steadyPacingTargetLocked()
			l.pacingStartup = false
			l.pacingNextProbe = now.Add(lanePacingProbeInterval)
		case !l.pacingStartup && !l.pacingProbeUntil.IsZero() && !now.Before(l.pacingProbeUntil):
			l.evaluateProbeLocked(now)
		case !l.pacingStartup && l.pacingProbeUntil.IsZero() && !now.Before(l.pacingNextProbe):
			if l.windowDemandBits != 0b11 {
				l.pacingNextProbe = now.Add(lanePacingProbeInterval)
				break
			}
			// A probe is a marginal-goodput experiment. The baseline is the
			// measured byte rate of the previous two delivery windows, not
			// the pacing knob: the steady branch blends probe bumps down
			// within the probe itself, and admission never spends tokens the
			// application does not supply.
			l.probeBaselinePacing = l.pacingRateBPS
			l.probeBaselineAckedBPS = float64(l.windowAckedBytes[0]+l.windowAckedBytes[1]) / (2 * laneDeliverySampleWindow.Seconds())
			l.probeBaselineAdmittedBPS = float64(l.windowAdmittedBytes[0]+l.windowAdmittedBytes[1]) / (2 * laneDeliverySampleWindow.Seconds())
			l.probeBaselineDemandOK = true
			l.probeBaselineRetrySmooth = l.retryRatioSmooth
			// A previous probe may have been aborted without evaluation
			// (congestion backoff or a generation reset clears pacingProbeUntil
			// without running evaluateProbeLocked). Start the experiment from
			// clean accumulators.
			l.probeAckedBytes = 0
			l.probeAdmittedBytes = 0
			l.probeWindows = 0
			l.probeDemandWindows = 0
			l.pacingRateBPS *= lanePacingProbeGain
			l.pacingProbeUntil = now.Add(lanePacingProbeDuration)
			l.pacingNextProbe = now.Add(lanePacingProbeInterval)
		case !l.pacingStartup:
			targetRate := l.steadyPacingTargetLocked()
			l.pacingRateBPS = 0.75*l.pacingRateBPS + 0.25*targetRate
		}
		if l.parent != nil && !applicationLimited && l.retryRatioSmooth > 0.30 {
			medianRetry := l.parent.medianActiveRetryRatio(l.id)
			if medianRetry >= 0 && l.retryRatioSmooth > 3*max(0.05, medianRetry) {
				l.degradedLossSamples++
				if l.degradedLossSamples >= 5 {
					l.degradedLossSamples = 0
					go l.parent.recoverStalledLaneWithReason(&l.id, "lane_quality")
				}
			} else {
				l.degradedLossSamples = 0
			}
		} else if applicationLimited {
			l.degradedLossSamples = 0
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
	l.deliveryRetransBytes = 0
	l.deliveryDemanded = false
}

func boolBit(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

// lossCompensationLocked converts the smoothed retransmission ratio into an
// over-offering factor for the measured delivery capacity. The ceiling starts
// conservative and is raised only after a pacing probe has proven that extra
// offered rate becomes extra delivered goodput; unproven compensation feeds
// the retransmission plateau (offered ~= 1.65x delivered) instead of useful
// throughput. The defensive floor treats an uninitialized ceiling as the
// default instead of collapsing the target to zero.
func (l *kcpLane) lossCompensationLocked() float64 {
	ceiling := l.compensationCeiling
	if ceiling <= 0 {
		ceiling = laneCompensationInitialCeiling
	}
	compensation := 1 / max(0.67, 1-l.retryRatioSmooth)
	return min(ceiling, compensation)
}

func (l *kcpLane) steadyPacingTargetLocked() float64 {
	return lanePacingSteadyGain * l.lossCompensationLocked() * l.deliveryCapacityBPS
}

// evaluateProbeLocked closes a pacing probe by comparing delivered goodput
// gain against the measured offered gain. A path which converts the probe
// into delivery keeps the elevated rate and earns a higher compensation
// ceiling; a path which converts it mostly into retransmissions is rolled
// back, gets a lower ceiling after two consecutive harmful probes and a
// doubling cooldown. Inconclusive probes (no demand, no measurable baseline,
// or no clear signal) change nothing.
func (l *kcpLane) evaluateProbeLocked(now time.Time) {
	l.pacingProbeUntil = time.Time{}
	baselinePacing := l.probeBaselinePacing
	baselineAcked := l.probeBaselineAckedBPS
	baselineAdmitted := l.probeBaselineAdmittedBPS
	baselineDemand := l.probeBaselineDemandOK
	baselineRetry := l.probeBaselineRetrySmooth
	probeAcked := l.probeAckedBytes
	probeAdmitted := l.probeAdmittedBytes
	probeWindows := l.probeWindows
	probeDemandWindows := l.probeDemandWindows
	l.probeBaselinePacing = 0
	l.probeBaselineAckedBPS = 0
	l.probeBaselineAdmittedBPS = 0
	l.probeBaselineDemandOK = false
	l.probeBaselineRetrySmooth = 0
	l.probeAckedBytes = 0
	l.probeAdmittedBytes = 0
	l.probeWindows = 0
	l.probeDemandWindows = 0
	laneID := l.id

	if now.Sub(l.probeLastVerdictAt) > laneProbeStreakExpiry {
		l.probeHarmfulStreak = 0
	}
	l.probeLastVerdictAt = now

	valid := baselineDemand && probeWindows >= 2 && probeDemandWindows >= 1 &&
		baselineAcked >= float64(laneProbeMinWindowBytes)/laneDeliverySampleWindow.Seconds() &&
		baselineAdmitted > 0 && probeAdmitted > 0 && probeAcked > 0
	if !valid {
		l.metrics.RecordEvent("lane_pacing_probe_inconclusive", "pacing", "invalid_baseline", &laneID)
		l.pacingRateBPS = l.steadyPacingTargetLocked()
		return
	}
	probeAckedBPS := float64(probeAcked) / (float64(probeWindows) * laneDeliverySampleWindow.Seconds())
	probeAdmittedBPS := float64(probeAdmitted) / (float64(probeWindows) * laneDeliverySampleWindow.Seconds())
	deliveredGain := probeAckedBPS/baselineAcked - 1
	offeredGain := probeAdmittedBPS/baselineAdmitted - 1
	switch {
	case offeredGain > 0 && deliveredGain >= lanePacingProbeUsefulRatio*offeredGain:
		l.probeHarmfulStreak = 0
		l.probeCooldownShift = 0
		l.compensationCeiling = min(laneCompensationMaximum, l.compensationCeiling+laneCompensationStepUp)
		l.metrics.RecordEvent("lane_pacing_probe_useful", "pacing", "marginal_goodput", &laneID)
		l.pacingRateBPS = l.steadyPacingTargetLocked()
	case offeredGain > 0 && deliveredGain <= lanePacingProbeHarmfulRatio*offeredGain && l.retryRatioSmooth >= baselineRetry:
		l.probeHarmfulStreak++
		if l.probeHarmfulStreak >= laneProbeHarmfulStreakLimit {
			l.compensationCeiling = max(laneCompensationMinimum, l.compensationCeiling-laneCompensationStepDown)
			l.probeCooldownShift = min(l.probeCooldownShift+1, laneProbeCooldownMaxShift)
			l.probeHarmfulStreak = 0
			target := l.steadyPacingTargetLocked()
			if baselinePacing > 0 && target > baselinePacing {
				target = baselinePacing
			}
			l.pacingRateBPS = target
			l.pacingNextProbe = now.Add(lanePacingProbeInterval << l.probeCooldownShift)
		} else {
			l.pacingRateBPS = l.steadyPacingTargetLocked()
		}
		l.metrics.RecordEvent("lane_pacing_probe_harmful", "pacing", "marginal_goodput", &laneID)
	default:
		l.metrics.RecordEvent("lane_pacing_probe_inconclusive", "pacing", "no_signal", &laneID)
		l.pacingRateBPS = l.steadyPacingTargetLocked()
	}
}

// newDataBudgetBPSLocked is the pacing rate available for fresh application
// data after the retransmission debt: the smoothed KCP retransmit rate is
// subtracted so retransmits consume a share of the measured path instead of
// inflating the total offered load. The floor sits deliberately below
// lanePacingMinimumBPS, otherwise the debt becomes a no-op exactly when the
// target is already clamped at its floor during a retransmission storm; it
// keeps only the ACK clock, probes and small interactive flows alive.
func (l *kcpLane) newDataBudgetBPSLocked() float64 {
	return max(laneNewDataFloorBPS, l.pacingRateBPS-l.retxRateBPS)
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
	var removedBytes uint64
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
			if segment, exists := l.kcpSent[sequence]; exists {
				removedBytes += uint64(segment.size)
				delete(l.kcpSent, sequence)
				removed++
			}
		}
		l.recordAckedPruneBytes(removedBytes)
	}
	l.kcpLastUNA = una
	return removed
}

func (l *kcpLane) pruneKCPSentFallback(una uint32) int {
	removed := 0
	var removedBytes uint64
	for sequence, segment := range l.kcpSent {
		if kcpSequenceBefore(sequence, una) {
			removedBytes += uint64(segment.size)
			delete(l.kcpSent, sequence)
			removed++
		}
	}
	l.recordAckedPruneBytes(removedBytes)
	return removed
}

// recordAckedPruneBytes folds cumulative-UNA pruning into the uniquely-acked
// byte accounting. Without it, byte-accurate delivery would be blind to every
// ACK that arrives as an aggregate window instead of a per-segment record.
func (l *kcpLane) recordAckedPruneBytes(bytes uint64) {
	if bytes == 0 {
		return
	}
	l.ackedBytesTotal += bytes
	l.metrics.AddHot(telemetry.KCPAckedBytesTotal, bytes)
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
