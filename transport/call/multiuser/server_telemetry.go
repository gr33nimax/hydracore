package multiuser

import (
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
)

const serverTelemetryInterval = 2 * time.Second

const (
	clientTelemetryWindow     = 2 * time.Second
	clientTelemetryMaxRecords = 64
	clientTelemetryMaxBytes   = 256 * 1024
)

type serverTelemetry struct {
	server         *Server
	metrics        *telemetry.Accumulator
	sink           *telemetry.Sink
	interval       time.Duration
	logger         logger.ContextLogger
	processSampler telemetry.ProcessSampler
	started        atomic.Bool
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
		logger:   log,
		done:     make(chan struct{}),
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

	telemetry.SampleServerRuntime(t.metrics)
	t.processSampler.Sample(t.metrics)
	t.metrics.Set(telemetry.HandshakePending, float64(len(t.server.pending)))
	t.metrics.Set(telemetry.SessionActive, float64(len(sessions)))
	t.metrics.Set(telemetry.PeerReadQueueDepth, float64(t.server.peerQueueDepth()))
	metrics := t.metrics.Snapshot(serverSnapshotMetrics())
	for _, session := range sessions {
		mergeTunnelMetrics(metrics, session.tunnel.TelemetryValues())
	}
	serverRecord := telemetry.Snapshot("server", "", "server", metrics)
	if err = t.sink.Write(serverRecord); err != nil {
		t.logger.Warn("call telemetry: write server snapshot: ", err)
		return
	}
	lease := 3 * t.interval
	if lease < 6*time.Second {
		lease = 6 * time.Second
	}
	for _, session := range sessions {
		session.tunnel.RequestClientTelemetry(lease)
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

func (s *Server) peerQueueDepth() int {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	depth := 0
	for _, peer := range s.peers {
		depth += len(peer.readQueue)
	}
	return depth
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
		if telemetry.IsCounter(metric) || metric == telemetry.KCPSendBlockedSecondsTotal {
			continue
		}
		if metric == telemetry.KCPRTTMS || metric == telemetry.KCPRTOMS {
			if value > current {
				target[name] = value
			}
			continue
		}
		target[name] = current + value
	}
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
	identity := ""
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
