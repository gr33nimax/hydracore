package vkparasite

import (
	"encoding/binary"
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
	forEachKCPSegment(packet, func(command byte, sequence uint32, size int) {
		l.metrics.AddHot(telemetry.KCPOutSegmentsTotal, 1)
		l.metrics.AddHot(telemetry.KCPOutBytesTotal, uint64(size))
		if command != kcpCommandPush {
			return
		}
		if previous, exists := l.kcpSent[sequence]; exists {
			previous.retransmitted = true
			l.kcpSent[sequence] = previous
			l.metrics.AddHot(telemetry.KCPRetransSegmentsTotal, 1)
			l.metrics.AddHot(telemetry.KCPRetransBytesTotal, uint64(size))
			return
		}
		l.kcpSent[sequence] = kcpSentSegment{sentAt: now}
	})
}

func (l *kcpLane) observeKCPInput(packet []byte) {
	now := time.Now()
	var cumulativeACK uint32
	hasCumulativeACK := false
	acknowledgements := make([]uint32, 0, 1)
	forEachKCPSegmentHeader(packet, func(command byte, sequence, una uint32, _ int) {
		if !hasCumulativeACK || kcpSequenceAfter(una, cumulativeACK) {
			cumulativeACK = una
			hasCumulativeACK = true
		}
		if command != kcpCommandACK {
			return
		}
		acknowledgements = append(acknowledgements, sequence)
	})
	for _, sequence := range acknowledgements {
		sent, exists := l.kcpSent[sequence]
		if !exists {
			continue
		}
		delete(l.kcpSent, sequence)
		if sent.retransmitted {
			continue
		}
		rtt := float64(now.Sub(sent.sentAt)) / float64(time.Millisecond)
		if rtt < 0 {
			continue
		}
		if l.kcpSRTTMS == 0 {
			l.kcpSRTTMS = rtt
			l.kcpRTTVARMS = rtt / 2
			continue
		}
		delta := rtt - l.kcpSRTTMS
		l.kcpSRTTMS += delta / 8
		if delta < 0 {
			delta = -delta
		}
		l.kcpRTTVARMS += (delta - l.kcpRTTVARMS) / 4
	}
	if hasCumulativeACK {
		l.pruneKCPSentBefore(cumulativeACK)
	}
}

func (l *kcpLane) pruneKCPSentBefore(una uint32) {
	if !l.kcpHasUNA {
		l.pruneKCPSentFallback(una)
		l.kcpLastUNA = una
		l.kcpHasUNA = true
		return
	}
	distance := uint32(una - l.kcpLastUNA)
	if distance == 0 || !kcpSequenceAfter(una, l.kcpLastUNA) {
		return
	}
	if distance > laneKCPMaxPending*2 {
		l.pruneKCPSentFallback(una)
	} else {
		for sequence := l.kcpLastUNA; sequence != una; sequence++ {
			delete(l.kcpSent, sequence)
		}
	}
	l.kcpLastUNA = una
}

func (l *kcpLane) pruneKCPSentFallback(una uint32) {
	for sequence := range l.kcpSent {
		if kcpSequenceBefore(sequence, una) {
			delete(l.kcpSent, sequence)
		}
	}
}

func forEachKCPSegment(packet []byte, callback func(command byte, sequence uint32, size int)) {
	forEachKCPSegmentHeader(packet, func(command byte, sequence, _ uint32, size int) {
		callback(command, sequence, size)
	})
}

func forEachKCPSegmentHeader(packet []byte, callback func(command byte, sequence, una uint32, size int)) {
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
