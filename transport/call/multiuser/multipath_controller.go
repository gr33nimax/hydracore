package multiuser

import (
	"math"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

const (
	adaptiveInitialPathWindow      = 48.0
	adaptiveMinPathWindow          = 24.0
	adaptiveMaxPathWindow          = 192.0
	adaptiveFeedbackHealthyThreshold = 0.03
	adaptiveFeedbackBackoffThreshold = 0.10
	adaptiveWindowBackoffFactor    = 0.90
	adaptiveMinBackoffInterval     = 250 * time.Millisecond
	adaptiveQueueBackoffDelay      = 10 * time.Millisecond
	adaptiveDeliverySampleInterval = 100 * time.Millisecond
	adaptiveDeliveryStale          = 2 * time.Second
)

func (s *multipathScheduler) sendWindow() int {
	if !s.adaptive() {
		return pooledKCPWindow
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	window := 0
	for _, state := range s.paths {
		if state.worker != nil {
			window += int(math.Floor(state.window))
		}
	}
	if window == 0 {
		return pooledKCPWindow
	}
	return max(1, min(pooledKCPWindow, window))
}

func (s *multipathScheduler) pendingLimit() int {
	if !s.adaptive() {
		return pooledKCPMaxPending
	}
	return max(256, min(pooledKCPMaxPending, s.sendWindow()*2))
}

func (s *multipathScheduler) pathHasHeadroomLocked(state *multipathPathState) bool {
	return state != nil && state.worker != nil && state.inflight < int(math.Ceil(state.window))
}

func (s *multipathScheduler) releaseInflightLocked(state *multipathPathState) {
	if state.inflight > 0 {
		state.inflight--
	}
}

func (s *multipathScheduler) acknowledgeLocked(sequence uint32, _ time.Time, _ bool) {
	sent, exists := s.sent[sequence]
	if !exists {
		return
	}
	delete(s.sent, sequence)
	state := s.paths[sent.worker.id]
	if state == nil || state.worker != sent.worker {
		return
	}
	s.releaseInflightLocked(state)
	state.retryPressure *= 0.9375
	// A KCP ACK confirms delivery for the shared conversation, not for the
	// physical worker that carried the original PUSH.  Only the mandatory
	// per-worker feedback channel is allowed to update path RTT, delivery rate,
	// loss, or congestion windows.
}

func (s *multipathScheduler) acknowledgeBeforeLocked(una uint32, now time.Time) {
	if !s.hasUNA {
		s.releaseCumulativeACKFallbackLocked(una, now)
		s.lastUNA = una
		s.hasUNA = true
		return
	}
	distance := uint32(una - s.lastUNA)
	if distance == 0 || !kcpSequenceAfter(una, s.lastUNA) {
		return
	}
	if distance > pooledKCPMaxPending*2 {
		s.releaseCumulativeACKFallbackLocked(una, now)
	} else {
		for sequence := s.lastUNA; sequence != una; sequence++ {
			s.acknowledgeLocked(sequence, now, false)
		}
	}
	s.lastUNA = una
}

func (s *multipathScheduler) releaseCumulativeACKFallbackLocked(una uint32, now time.Time) {
	for sequence := range s.sent {
		if kcpSequenceBefore(sequence, una) {
			s.acknowledgeLocked(sequence, now, false)
		}
	}
}

func (s *multipathScheduler) observeDeliveryLocked(state *multipathPathState, size int, now time.Time) {
	if !state.lastAck.IsZero() && now.Sub(state.lastAck) > adaptiveDeliveryStale {
		state.deliveryBPS = 0
		state.deliverySince = now
		state.deliveryBytes = 0
	}
	state.lastAck = now
	if state.deliverySince.IsZero() {
		state.deliverySince = now
	}
	state.deliveryBytes += size
	elapsed := now.Sub(state.deliverySince)
	if elapsed < adaptiveDeliverySampleInterval {
		return
	}
	sample := float64(state.deliveryBytes*8) / elapsed.Seconds()
	if state.deliveryBPS == 0 {
		state.deliveryBPS = sample
	} else {
		state.deliveryBPS += (sample - state.deliveryBPS) / 4
	}
	state.deliverySince = now
	state.deliveryBytes = 0
}

func (s *multipathScheduler) observeQueueDelay(worker *pooledWorker, packet []byte, delay time.Duration, now time.Time) {
	if !s.adaptive() || worker == nil || delay < adaptiveQueueBackoffDelay || len(kcpPushSequences(packet)) == 0 {
		return
	}
	s.mu.Lock()
	if state := s.paths[worker.id]; state != nil && state.worker == worker {
		s.backoffPathLocked(state, now)
	}
	s.mu.Unlock()
}

func (s *multipathScheduler) backoffPathLocked(state *multipathPathState, now time.Time) {
	interval := adaptiveMinBackoffInterval
	if pathRTT := time.Duration(state.rttMS * float64(time.Millisecond)); pathRTT > interval {
		interval = pathRTT
	}
	if !state.lastBackoff.IsZero() && now.Sub(state.lastBackoff) < interval {
		return
	}
	state.lastBackoff = now
	floor := max(adaptiveMinPathWindow, float64(state.inflight+4))
	if state.deliveryBPS > 0 && !state.lastAck.IsZero() && now.Sub(state.lastAck) <= adaptiveDeliveryStale {
		rtt := max(state.rttMS, float64(adaptiveMinBackoffInterval/time.Millisecond))
		bdpSegments := state.deliveryBPS * (rtt / 1000) / 8 / float64(pooledKCPMTU-kcpHeaderSize)
		floor = min(adaptiveMaxPathWindow, max(floor, bdpSegments))
	}
	next := max(floor, state.window*adaptiveWindowBackoffFactor)
	if next >= state.window {
		return
	}
	state.window = next
	state.metrics.AddHot(telemetry.WorkerPathBackoffTotal, 1)
}
