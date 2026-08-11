package telemetry

import "time"

const maximumBufferedEvents = 128

type Event struct {
	Timestamp float64
	Event     string
	Stage     string
	Reason    string
	WorkerID  *uint16
}

func (a *Accumulator) RecordEvent(event, stage, reason string, workerID *uint16) {
	if a == nil {
		return
	}
	record := Event{
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
		Event:     event,
		Stage:     stage,
		Reason:    reason,
		WorkerID:  workerID,
	}
	a.eventsMu.Lock()
	if len(a.events) == maximumBufferedEvents {
		copy(a.events, a.events[1:])
		a.events = a.events[:maximumBufferedEvents-1]
	}
	a.events = append(a.events, record)
	a.eventsMu.Unlock()
}

func (a *Accumulator) DrainEvents(limit int) []Event {
	if a == nil || limit <= 0 {
		return nil
	}
	a.eventsMu.Lock()
	defer a.eventsMu.Unlock()
	cutoff := float64(time.Now().Add(-5*time.Minute).UnixNano()) / 1e9
	firstCurrent := 0
	for firstCurrent < len(a.events) && a.events[firstCurrent].Timestamp < cutoff {
		firstCurrent++
	}
	if firstCurrent > 0 {
		copy(a.events, a.events[firstCurrent:])
		a.events = a.events[:len(a.events)-firstCurrent]
	}
	if limit > len(a.events) {
		limit = len(a.events)
	}
	result := append([]Event(nil), a.events[:limit]...)
	copy(a.events, a.events[limit:])
	a.events = a.events[:len(a.events)-limit]
	return result
}
