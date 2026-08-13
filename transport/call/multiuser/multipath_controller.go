package multiuser

import (
	"math"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

const (
	adaptiveInitialPathWindow      = 40.0
	adaptiveMinPathWindow          = 8.0
	adaptiveMaxPathWindow          = 128.0
	adaptiveRetryHealthyThreshold  = 0.08
	adaptiveRetryBackoffThreshold  = 0.15
	adaptiveWindowBackoffFactor    = 0.80
	adaptiveMinBackoffInterval     = 100 * time.Millisecond
	adaptiveQueueBackoffDelay      = 20 * time.Millisecond
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
	return max(128, min(pooledKCPMaxPending, s.sendWindow()*4))
}

func (s *multipathScheduler) pathHasHeadroomLocked(state *multipathPathState) bool {
	return state != nil && state.worker != nil && state.inflight < int(math.Ceil(state.window))
}

func (s *multipathScheduler) releaseInflightLocked(state *multipathPathState) {
	if state.inflight > 0 {
		state.inflight--
	}
}

func (s *multipathScheduler) acknowledgeLocked(sequence uint32, now time.Time, exact bool) {
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
	if sent.size > 0 {
		state.worker.metrics.AddHot(telemetry.WorkerPathAckedBytesTotal, uint64(sent.size))
		s.observeDeliveryLocked(state, sent.size, now)
	}
	if sent.retransmitted || sent.sentAt.IsZero() {
		return
	}
	if exact {
		rttMS := float64(now.Sub(sent.sentAt)) / float64(time.Millisecond)
		if rttMS >= 0 {
			if state.rttMS == 0 {
				state.rttMS = rttMS
			} else {
				state.rttMS += (rttMS - state.rttMS) / 8
			}
		}
	}
	if state.retryPressure <= adaptiveRetryHealthyThreshold && state.window < adaptiveMaxPathWindow {
		state.window = min(adaptiveMaxPathWindow, state.window+1/state.window)
	}
}

func (s *multipathScheduler) acknowledgeBeforeLocked(una uint32, now time.Time) {
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
	floor := adaptiveMinPathWindow
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
