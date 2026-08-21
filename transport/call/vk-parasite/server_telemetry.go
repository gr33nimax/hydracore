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
		interval:   interval,
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
	serverRecord := telemetry.Snapshot("server", "", t.generation, metrics)
	if err = t.sink.Write(serverRecord); err != nil {
		t.logger.Warn("call telemetry: write server snapshot: ", err)
		return
	}
	for _, session := range sessions {
		t.emitSession(session, sequence)
	}
}

func (t *serverTelemetry) emitSession(session *serverSession, sequence uint64) {
	identity := hex.EncodeToString(session.id[:])
	metrics := make(map[string]any)
	now := time.Now()
	metrics[telemetry.Name(telemetry.SessionActive)] = 1.0
	metrics[telemetry.Name(telemetry.SessionAgeSeconds)] = max(0, now.Sub(session.createdAt).Seconds())
	metrics[telemetry.Name(telemetry.WorkerDesired)] = float64(session.expected)
	if session.relay != nil {
		metrics[telemetry.Name(telemetry.WorkerActive)] = float64(session.relay.ActivePaths())
		metrics[telemetry.Name(telemetry.QUICConnCount)] = float64(session.relay.ActivePaths())
	}
	metrics[telemetry.Name(telemetry.TelemetrySequence)] = sequence
	if err := t.sink.Write(telemetry.Snapshot("server", session.user, identity, metrics)); err != nil {
		t.logger.Warn("call telemetry: write server session snapshot: ", err)
		return
	}
}

func (t *serverTelemetry) setCollectionActive(sessions []*serverSession, active bool) {
	t.metrics.SetCollectionActive(active)
}

func (t *serverTelemetry) event(name, stage, reason, user string, sessionID [16]byte, workerID *uint16) {
	if !t.metrics.CollectionActive() {
		return
	}
	identity := ""
	if sessionID != ([16]byte{}) {
		identity = hex.EncodeToString(sessionID[:])
	}
	event := telemetry.Event{
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
		Event:     name,
		Stage:     stage,
		Reason:    reason,
		WorkerID:  workerID,
	}
	record := telemetry.EventRecord("server", user, identity, event)
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
		telemetry.QUICConnCount,
		telemetry.QUICStreamsActive,
		telemetry.QUICStreamsOpenedTotal,
		telemetry.QUICRTTMS,
		telemetry.QUICRTTVarMS,
		telemetry.QUICPacketsLostTotal,
		telemetry.QUICBytesRetransTotal,
		telemetry.QUICCongestionWindowBytes,
		telemetry.QUICDatagramsSentTotal,
		telemetry.QUICDatagramsDroppedTotal,
		telemetry.PathReplacementsTotal,
	)
}

func serverTelemetryGeneration() string {
	var randomBytes [8]byte
	if _, err := rand.Read(randomBytes[:]); err == nil {
		return hex.EncodeToString(randomBytes[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 16)
}
