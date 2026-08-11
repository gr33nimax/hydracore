package multiuser

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

func (t *PooledTunnel) observeKCPOutput(packet []byte) {
	if !t.metrics.CollectionActive() {
		return
	}
	now := time.Now()
	forEachKCPSegment(packet, func(command byte, sequence uint32) {
		t.metrics.AddHot(telemetry.KCPOutSegmentsTotal, 1)
		if command != kcpCommandPush {
			return
		}
		if previous, exists := t.kcpSent[sequence]; exists {
			previous.retransmitted = true
			t.kcpSent[sequence] = previous
			t.metrics.AddHot(telemetry.KCPRetransSegmentsTotal, 1)
			return
		}
		t.kcpSent[sequence] = kcpSentSegment{sentAt: now}
	})
}

func (t *PooledTunnel) observeKCPInput(packet []byte) {
	if !t.metrics.CollectionActive() {
		return
	}
	now := time.Now()
	forEachKCPSegment(packet, func(command byte, sequence uint32) {
		if command != kcpCommandACK {
			return
		}
		sent, exists := t.kcpSent[sequence]
		if !exists {
			return
		}
		delete(t.kcpSent, sequence)
		if sent.retransmitted {
			return
		}
		rtt := float64(now.Sub(sent.sentAt)) / float64(time.Millisecond)
		if rtt < 0 {
			return
		}
		if t.kcpSRTTMS == 0 {
			t.kcpSRTTMS = rtt
			t.kcpRTTVARMS = rtt / 2
			return
		}
		delta := rtt - t.kcpSRTTMS
		t.kcpSRTTMS += delta / 8
		if delta < 0 {
			delta = -delta
		}
		t.kcpRTTVARMS += (delta - t.kcpRTTVARMS) / 4
	})
}

func forEachKCPSegment(packet []byte, callback func(command byte, sequence uint32)) {
	for len(packet) >= kcpHeaderSize {
		length := int(binary.LittleEndian.Uint32(packet[20:24]))
		if length < 0 || kcpHeaderSize+length > len(packet) {
			return
		}
		callback(packet[4], binary.LittleEndian.Uint32(packet[12:16]))
		packet = packet[kcpHeaderSize+length:]
	}
}

func (t *PooledTunnel) handleTelemetryMessage(message []byte) bool {
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

func (t *PooledTunnel) RequestClientTelemetry(lease time.Duration) bool {
	seconds := int(lease / time.Second)
	if seconds < 2 {
		seconds = 2
	}
	if seconds > 30 {
		seconds = 30
	}
	payload := []byte{telemetry.SchemaVersion, 0, 0}
	binary.BigEndian.PutUint16(payload[1:], uint16(seconds))
	return t.trySendData(calltunnel.EncodeFrame(calltunnel.ControlConnID, calltunnel.MsgTelemetryControl, payload))
}

func (t *PooledTunnel) SendClientTelemetry(record []byte) bool {
	if len(record) == 0 || len(record) > telemetry.MaximumRecordLen {
		return false
	}
	return t.trySendData(calltunnel.EncodeFrame(calltunnel.ControlConnID, calltunnel.MsgTelemetryRecord, record))
}

func (t *PooledTunnel) RelaySetActive(tcp, udp int) {
	t.metrics.Set(telemetry.RelayTCPActive, float64(tcp))
	t.metrics.Set(telemetry.RelayUDPActive, float64(udp))
}

func (t *PooledTunnel) RelayAddBytes(bytes uint64) {
	t.metrics.AddHot(telemetry.RelayBytesTotal, bytes)
}

func (t *PooledTunnel) RelayQueueDelta(bytes int) {
	t.metrics.AddHotGauge(telemetry.RelayQueueDepth, float64(bytes))
}

func (t *PooledTunnel) RelayResetQueue() {
	t.metrics.Set(telemetry.RelayQueueDepth, 0)
}

func (t *PooledTunnel) RelayQueueDrop() {
	t.metrics.Add(telemetry.RelayQueueDropsTotal, 1)
}

func (t *PooledTunnel) RelayConnectFailure() {
	t.metrics.Add(telemetry.RelayConnectFailureTotal, 1)
}
