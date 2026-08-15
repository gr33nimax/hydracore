package vkparasite

import (
	"sync"
	"time"
)

const (
	// Four physical VK/TURN calls sustain roughly 2-3 Mbit/s each in the
	// instrumented runs. Start close to that measured operating point instead
	// of inheriting the eight-lane aggregate ramp, then let retransmission
	// pressure tune each call independently.
	laneAdmissionInitialRate = 320_000.0
	laneAdmissionMinimumRate = 96_000.0
	laneAdmissionMaximumRate = 800_000.0
	laneAdmissionBurstBytes  = 64 * 1024
	laneAdmissionTunePeriod  = time.Second
)

// laneAdmission controls new application bytes before KCP starts its
// retransmission timer. KCP acknowledgements and retransmissions never pass
// through this limiter, and control frames bypass it, so recovery and flow
// control cannot deadlock behind bulk traffic.
type laneAdmission struct {
	mu sync.Mutex

	rate       float64
	tokens     float64
	lastRefill time.Time
	lastTune   time.Time
	outBytes   uint64
	retxBytes  uint64
}

func newLaneAdmission() *laneAdmission {
	now := time.Now()
	return &laneAdmission{
		rate:       laneAdmissionInitialRate,
		tokens:     laneAdmissionBurstBytes,
		lastRefill: now,
		lastTune:   now,
	}
}

func (a *laneAdmission) ready(now time.Time, size int, priority bool) bool {
	if priority {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillLocked(now)
	return a.tokens >= a.tokenCost(size)
}

func (a *laneAdmission) take(now time.Time, size int, priority bool) bool {
	if priority {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillLocked(now)
	cost := a.tokenCost(size)
	if a.tokens < cost {
		return false
	}
	a.tokens -= cost
	return true
}

func (a *laneAdmission) observeOutput(size int, retransmission bool) {
	if size <= 0 {
		return
	}
	a.mu.Lock()
	a.outBytes += uint64(size)
	if retransmission {
		a.retxBytes += uint64(size)
	}
	a.mu.Unlock()
}

func (a *laneAdmission) tune(now time.Time, waitSnd int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillLocked(now)
	if now.Sub(a.lastTune) < laneAdmissionTunePeriod {
		return
	}
	outBytes := a.outBytes
	retxBytes := a.retxBytes
	a.outBytes = 0
	a.retxBytes = 0
	a.lastTune = now
	if outBytes < 32*1024 {
		return
	}
	ratio := float64(retxBytes) / float64(outBytes)
	switch {
	case ratio >= 0.12:
		a.rate *= 0.65
	case ratio >= 0.05:
		a.rate *= 0.82
	case ratio <= 0.015 && waitSnd < laneKCPSendWindow/2:
		a.rate *= 1.08
	}
	if a.rate < laneAdmissionMinimumRate {
		a.rate = laneAdmissionMinimumRate
	}
	if a.rate > laneAdmissionMaximumRate {
		a.rate = laneAdmissionMaximumRate
	}
	if a.tokens > laneAdmissionBurstBytes {
		a.tokens = laneAdmissionBurstBytes
	}
}

func (a *laneAdmission) rateBytesPerSecond() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.rate
}

func (a *laneAdmission) refillLocked(now time.Time) {
	if !now.After(a.lastRefill) {
		return
	}
	a.tokens += now.Sub(a.lastRefill).Seconds() * a.rate
	if a.tokens > laneAdmissionBurstBytes {
		a.tokens = laneAdmissionBurstBytes
	}
	a.lastRefill = now
}

func (*laneAdmission) tokenCost(size int) float64 {
	if size <= 0 {
		return 0
	}
	if size > laneAdmissionBurstBytes {
		return laneAdmissionBurstBytes
	}
	return float64(size)
}
