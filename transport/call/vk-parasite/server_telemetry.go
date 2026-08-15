package vkparasite

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
)

const serverTelemetryInterval = 2 * time.Second

const (
	clientTelemetryWindow     = 2 * time.Second
	clientTelemetryMaxRecords = 256
	clientTelemetryMaxBytes   = 1024 * 1024
)

type serverTelemetry struct {
	server         *Server
	metrics        *telemetry.Accumulator
	sink           *telemetry.Sink
	interval       time.Duration
	logger         logger.ContextLogger
	processSampler telemetry.ProcessSampler
	started        atomic.Bool
	sequence       atomic.Uint64
	sinkRotations  uint64
	generation     string
	done           chan struct{}
}

func newServerTelemetry(server *Server, options ServerOptions, log logger.ContextLogger) *serverTelemetry {
	interval := options.TelemetryInterval
	if interval == 0 {
		interval = serverTelemetryInterval
	}
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	return &serverTelemetry{
		server:  server,
		metrics: telemetry.NewAccumulator(),
		sink: telemetry.NewSink(telemetry.SinkConfig{
			StateDirectory: options.TelemetryStateDirectory,
			OutputPath:     options.TelemetryOutputPath,
		}),
		interval: interval,
		logger:     log,
		generation: serverTelemetryGeneration(),
		done:       make(chan struct{}),
	}
}

func (t *serverTelemetry) start() {
	if t.started.CompareAndSwap(false, true) {
		go t.run()
	}
}

func (t *serverTelemetry) run() {
	defer close(t.done)
	t.emit()
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.emit()
		case <-t.server.ctx.Done():
			return
		}
	}
}

func (t *serverTelemetry) emit() {
	sessions := t.server.telemetrySessions()
	active, _, err := t.sink.Sync()
	if err != nil {
		t.setCollectionActive(sessions, false)
		t.logger.Warn("call telemetry: native sink unavailable: ", err)
		return
	}
	if !active {
		t.setCollectionActive(sessions, false)
		return
	}
	t.setCollectionActive(sessions, true)
	sequence := t.sequence.Add(1)
	rotations := t.sink.Rotations()
	if rotations > t.sinkRotations {
		t.metrics.Add(telemetry.TelemetrySinkRotationsTotal, rotations-t.sinkRotations)
		t.sinkRotations = rotations
	}

	telemetry.SampleServerRuntime(t.metrics)
	t.processSampler.Sample(t.metrics)
	t.metrics.Set(telemetry.HandshakePending, float64(len(t.server.pending)))
	t.metrics.Set(telemetry.SessionActive, float64(len(sessions)))
	peerDepth, peerCapacity := t.server.peerQueueStats()
	t.metrics.Set(telemetry.PeerReadQueueDepth, float64(peerDepth))
	t.metrics.Set(telemetry.PeerReadQueueCapacity, float64(peerCapacity))
	t.metrics.Set(telemetry.UDPIngressQueueDepth, float64(t.server.ingressDepth.Load()))
	t.metrics.Set(telemetry.UDPIngressQueueCapacity, float64(t.server.options.IngressQueuePackets))
	t.metrics.Set(telemetry.UDPIngressWorkers, float64(t.server.options.IngressWorkers))
	t.metrics.Set(telemetry.TelemetrySequence, float64(sequence))
	metrics := t.metrics.Snapshot(serverSnapshotMetrics())
	for _, session := range sessions {
		values := session.tunnel.TelemetryValues()
		mergeTunnelMetrics(metrics, values)
		t.emitSession(session, sequence)
	}
	serverRecord := telemetry.Snapshot("server", "", t.generation, metrics)
	if err = t.sink.Write(serverRecord); err != nil {
		t.logger.Warn("call telemetry: write server snapshot: ", err)
		return
	}
	for _, session := range sessions {
		if session.tunnel.ActiveWorkers() == 0 {
			continue
		}
		if !session.tunnel.RequestClientTelemetry(120 * time.Second) {
			session.tunnel.metrics.Add(telemetry.TelemetryControlDropsTotal, 1)
		}
	}
}

func (t *serverTelemetry) emitSession(session *serverSession, sequence uint64) {
	identity := hex.EncodeToString(session.id[:])
	workers := session.tunnel.telemetryWorkerSnapshots(serverWorkerSnapshotMetrics())
	mergeWorkerNetworkGauges(session.tunnel.metrics, workers)
	peerDepth := 0.0
	peerCapacity := 0.0
	for _, worker := range workers {
		if metricNumber(worker.metrics[telemetry.Name(telemetry.WorkerActive)]) == 0 {
			continue
		}
		peerDepth += metricNumber(worker.metrics[telemetry.Name(telemetry.PeerReadQueueDepth)])
		peerCapacity += metricNumber(worker.metrics[telemetry.Name(telemetry.PeerReadQueueCapacity)])
	}
	session.tunnel.metrics.Set(telemetry.PeerReadQueueDepth, peerDepth)
	metrics := session.tunnel.metrics.Snapshot(serverSessionSnapshotMetrics())
	now := time.Now()
	metrics[telemetry.Name(telemetry.SessionActive)] = 1.0
	metrics[telemetry.Name(telemetry.SessionAgeSeconds)] = max(0, now.Sub(session.createdAt).Seconds())
	metrics[telemetry.Name(telemetry.SessionIdleSeconds)] = max(0, now.Sub(session.tunnel.LastActivity()).Seconds())
	metrics[telemetry.Name(telemetry.WorkerDesired)] = float64(session.expected)
	metrics[telemetry.Name(telemetry.PeerReadQueueCapacity)] = peerCapacity
	metrics[telemetry.Name(telemetry.TelemetrySequence)] = sequence
	if err := t.sink.Write(telemetry.Snapshot("server", session.user, identity, metrics)); err != nil {
		t.logger.Warn("call telemetry: write server session snapshot: ", err)
		return
	}
	for _, worker := range workers {
		worker.metrics[telemetry.Name(telemetry.TelemetrySequence)] = sequence
		workerID := worker.id
		record := telemetry.Snapshot("server", session.user, identity, worker.metrics)
		record.WorkerID = &workerID
		if err := t.sink.Write(record); err != nil {
			t.logger.Warn("call telemetry: write server worker snapshot: ", err)
			return
		}
	}
}

func (t *serverTelemetry) setCollectionActive(sessions []*serverSession, active bool) {
	t.metrics.SetCollectionActive(active)
	for _, session := range sessions {
		session.tunnel.SetTelemetryCollectionActive(active)
	}
}

func serverSnapshotMetrics() []telemetry.Metric {
	metrics := append([]telemetry.Metric(nil), telemetry.ServerRequired...)
	return append(metrics,
		telemetry.OuterReorderedPacketsTotal,
		telemetry.OuterDuplicatePacketsTotal,
		telemetry.NetworkLossRatio,
		telemetry.NetworkJitterMS,
		telemetry.RuntimeCPUPercent,
		telemetry.RuntimeRSSBytes,
		telemetry.RuntimeThermalState,
		telemetry.RuntimeThermalAvailable,
	)
}

func serverSessionSnapshotMetrics() []telemetry.Metric {
	metrics := append([]telemetry.Metric(nil), telemetry.TunnelMetrics...)
	return append(metrics,
		telemetry.SessionActive,
		telemetry.SessionAgeSeconds,
		telemetry.SessionIdleSeconds,
		telemetry.WorkerDesired,
		telemetry.WorkerAttachSuccessTotal,
		telemetry.WorkerAttachFailureTotal,
		telemetry.OuterPacketsInTotal,
		telemetry.OuterPacketsOutTotal,
		telemetry.OuterBytesInTotal,
		telemetry.OuterBytesOutTotal,
		telemetry.OuterPayloadBytesInTotal,
		telemetry.OuterPayloadBytesOutTotal,
		telemetry.OuterOverheadBytesInTotal,
		telemetry.OuterOverheadBytesOutTotal,
		telemetry.OuterAuthFailuresTotal,
		telemetry.OuterWrapFailuresTotal,
		telemetry.OuterReorderedPacketsTotal,
		telemetry.OuterDuplicatePacketsTotal,
		telemetry.PeerReadQueueDepth,
		telemetry.PeerReadQueueCapacity,
		telemetry.PeerReadQueueDropsTotal,
		telemetry.NetworkLossRatio,
		telemetry.NetworkJitterMS,
		telemetry.TelemetrySequence,
		telemetry.TelemetryControlDropsTotal,
		telemetry.TelemetryRecordDropsTotal,
	)
}

func serverWorkerSnapshotMetrics() []telemetry.Metric {
	return []telemetry.Metric{
		telemetry.WorkerActive,
		telemetry.WorkerAttachSuccessTotal,
		telemetry.WorkerAttachFailureTotal,
		telemetry.WorkerSendQueueDepth,
		telemetry.WorkerSendQueueDropsTotal,
		telemetry.WorkerLivenessExpiredTotal,
		telemetry.LaneCount,
		telemetry.LaneFlowCount,
		telemetry.KCPWaitSnd,
		telemetry.KCPOutSegmentsTotal,
		telemetry.KCPRetransSegmentsTotal,
		telemetry.KCPOutBytesTotal,
		telemetry.KCPRetransBytesTotal,
		telemetry.KCPFastRetransEstimateSegmentsTotal,
		telemetry.KCPFastRetransEstimateBytesTotal,
		telemetry.KCPRTORetransEstimateSegmentsTotal,
		telemetry.KCPRTORetransEstimateBytesTotal,
		telemetry.KCPRTTMS,
		telemetry.KCPRTOMS,
		telemetry.KCPRTTVarMS,
		telemetry.KCPRTTSamplesTotal,
		telemetry.KCPAckSegmentsTotal,
		telemetry.KCPAckProgressSegmentsTotal,
		telemetry.KCPInflightSegments,
		telemetry.KCPOutputQueueDepth,
		telemetry.KCPOutputQueueCapacity,
		telemetry.KCPUpdateBackpressureTotal,
		telemetry.KCPMutexBlockedSecondsTotal,
		telemetry.WorkerOutputQueueDelayMS,
		telemetry.WorkerOutputQueueLateTotal,
		telemetry.WorkerWriteLatencyMS,
		telemetry.LaneAdmissionRateBPS,
		telemetry.LaneAdmissionWindowSegments,
		telemetry.FlowReorderAbortTotal,
		telemetry.PeerReadQueueDepth,
		telemetry.PeerReadQueueCapacity,
		telemetry.PeerReadQueueDropsTotal,
		telemetry.OuterPacketsInTotal,
		telemetry.OuterPacketsOutTotal,
		telemetry.OuterBytesInTotal,
		telemetry.OuterBytesOutTotal,
		telemetry.OuterPayloadBytesInTotal,
		telemetry.OuterPayloadBytesOutTotal,
		telemetry.OuterOverheadBytesInTotal,
		telemetry.OuterOverheadBytesOutTotal,
		telemetry.OuterAuthFailuresTotal,
		telemetry.OuterWrapFailuresTotal,
		telemetry.OuterReorderedPacketsTotal,
		telemetry.OuterDuplicatePacketsTotal,
		telemetry.NetworkLossRatio,
		telemetry.NetworkJitterMS,
		telemetry.TelemetrySequence,
	}
}

func (s *Server) peerQueueStats() (depth int, capacity int) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	for _, peer := range s.peers {
		depth += len(peer.readQueue)
		capacity += cap(peer.readQueue)
	}
	return
}

func (s *Server) telemetrySessions() []*serverSession {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	sessions := make([]*serverSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func mergeTunnelMetrics(target map[string]any, values map[telemetry.Metric]float64) {
	for metric, value := range values {
		name := telemetry.Name(metric)
		current := metricNumber(target[name])
		if telemetry.IsCounter(metric) || metric == telemetry.KCPSendBlockedSecondsTotal || metric == telemetry.KCPMutexBlockedSecondsTotal {
			continue
		}
		switch metric {
		case telemetry.KCPWaitSnd,
			telemetry.KCPInflightSegments,
			telemetry.KCPOutputQueueDepth,
			telemetry.LaneAdmissionRateBPS,
			telemetry.LaneAdmissionWindowSegments,
			telemetry.RelayTCPActive,
			telemetry.RelayUDPActive,
			telemetry.RelayQueueDepth,
			telemetry.WorkerActive,
			telemetry.WorkerSendQueueDepth:
			target[name] = current + value
		default:
			if value > current {
				target[name] = value
			}
		}
	}
}

func serverTelemetryGeneration() string {
	var identifier [8]byte
	if _, err := rand.Read(identifier[:]); err == nil {
		return "server-" + hex.EncodeToString(identifier[:])
	}
	return "server-" + strconv.FormatInt(time.Now().UnixNano(), 16)
}

func metricNumber(value any) float64 {
	switch typed := value.(type) {
	case uint64:
		return float64(typed)
	case int:
		return float64(typed)
	case float64:
		return typed
	default:
		return 0
	}
}

func (t *serverTelemetry) clientRecord(session *serverSession, payload []byte) {
	receivedAt := time.Now()
	if !session.allowClientTelemetry(len(payload), receivedAt) {
		session.tunnel.metrics.Add(telemetry.TelemetryRecordDropsTotal, 1)
		return
	}
	record, err := telemetry.DecodeClientRecord(payload)
	if err != nil {
		t.event("client_record_rejected", "telemetry", "invalid", session.user, session.id, nil)
		return
	}
	if record.WorkerID != nil && *record.WorkerID >= session.expected {
		t.event("client_record_rejected", "telemetry", "worker_id", session.user, session.id, nil)
		return
	}
	record.Timestamp = float64(receivedAt.UnixNano()) / 1e9
	record.User = session.user
	record.SessionID = hex.EncodeToString(session.id[:])
	if err = t.sink.Write(record); err != nil {
		t.logger.Warn("call telemetry: write client record: ", err)
	}
}

func (s *serverSession) allowClientTelemetry(size int, now time.Time) bool {
	s.telemetryMu.Lock()
	defer s.telemetryMu.Unlock()
	if s.telemetryWindow.IsZero() || now.Sub(s.telemetryWindow) >= clientTelemetryWindow {
		s.telemetryWindow = now
		s.telemetryRecords = 0
		s.telemetryBytes = 0
	}
	if size <= 0 || s.telemetryRecords >= clientTelemetryMaxRecords || s.telemetryBytes+size > clientTelemetryMaxBytes {
		return false
	}
	s.telemetryRecords++
	s.telemetryBytes += size
	return true
}

func (t *serverTelemetry) event(event, stage, reason, user string, sessionID [16]byte, workerID *uint16) {
	if !t.sink.Active() {
		return
	}
	identity := t.generation
	if sessionID != ([16]byte{}) {
		identity = hex.EncodeToString(sessionID[:])
	}
	record := telemetry.EventRecord("server", user, identity, telemetry.Event{
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
		Event:     event,
		Stage:     stage,
		Reason:    reason,
		WorkerID:  workerID,
	})
	if err := t.sink.Write(record); err != nil {
		t.logger.Warn("call telemetry: write server event: ", err)
	}
}

func (t *serverTelemetry) close() error {
	if t.started.Load() {
		<-t.done
	}
	return t.sink.Close()
}
