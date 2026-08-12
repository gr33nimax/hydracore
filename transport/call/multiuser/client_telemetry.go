package multiuser

import (
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

const (
	clientTelemetryInterval = 2 * time.Second
	clientEventsPerInterval  = 16
)

func (c *Client) enableTelemetry(lease time.Duration) {
	if lease < 2*time.Second {
		lease = 2 * time.Second
	}
	if lease > 120*time.Second {
		lease = 120 * time.Second
	}
	c.telemetryLease.Store(time.Now().Add(lease).UnixNano())
	if c.telemetryLeaseExpired.Swap(false) {
		c.metrics.RecordEvent("telemetry_lease_resumed", "telemetry", "control_received", nil)
	}
	c.tunnel.SetTelemetryCollectionActive(true)
}

func (c *Client) telemetryLoop() {
	ticker := time.NewTicker(clientTelemetryInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			if now.UnixNano() >= c.telemetryLease.Load() {
				if c.telemetryLease.Load() > 0 && c.telemetryLeaseExpired.CompareAndSwap(false, true) {
					c.metrics.Add(telemetry.TelemetryLeaseExpiredTotal, 1)
					c.tunnel.SetTelemetryCollectionActive(false)
				}
				continue
			}
			c.emitTelemetry()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) emitTelemetry() {
	sequence := c.telemetrySequence.Add(1)
	c.metrics.Set(telemetry.WorkerDesired, float64(c.options.Workers))
	c.metrics.Set(telemetry.WorkerActive, float64(c.tunnel.ActiveWorkers()))
	c.processSampler.Sample(c.metrics)
	c.tunnel.TelemetryValues()
	workers := c.tunnel.telemetryWorkerSnapshots(clientWorkerSnapshotMetrics())
	mergeWorkerNetworkGauges(c.metrics, workers)
	for _, event := range c.metrics.DrainEvents(clientEventsPerInterval) {
		record := telemetry.EventRecord("client", "", "", event)
		c.sendTelemetryRecord(record)
	}
	metrics := c.metrics.Snapshot(clientSnapshotMetrics())
	metrics[telemetry.Name(telemetry.TelemetrySequence)] = sequence
	record := telemetry.Snapshot("client", "", "", metrics)
	c.sendTelemetryRecord(record)
	for _, worker := range workers {
		worker.metrics[telemetry.Name(telemetry.TelemetrySequence)] = sequence
		for _, event := range c.tunnel.telemetryWorker(worker.id).DrainEvents(clientEventsPerInterval) {
			if event.WorkerID == nil {
				workerID := worker.id
				event.WorkerID = &workerID
			}
			c.sendTelemetryRecord(telemetry.EventRecord("client", "", "", event))
		}
		workerID := worker.id
		workerRecord := telemetry.Snapshot("client", "", "", worker.metrics)
		workerRecord.WorkerID = &workerID
		c.sendTelemetryRecord(workerRecord)
	}
}

func mergeWorkerNetworkGauges(
	metrics *telemetry.Accumulator,
	workers []workerTelemetrySnapshot,
) {
	maxLoss := 0.0
	maxJitter := 0.0
	for _, worker := range workers {
		maxLoss = max(maxLoss, metricNumber(worker.metrics[telemetry.Name(telemetry.NetworkLossRatio)]))
		maxJitter = max(maxJitter, metricNumber(worker.metrics[telemetry.Name(telemetry.NetworkJitterMS)]))
	}
	metrics.Set(telemetry.NetworkLossRatio, maxLoss)
	metrics.Set(telemetry.NetworkJitterMS, maxJitter)
}

func (c *Client) sendTelemetryRecord(record telemetry.Record) {
	payload, err := telemetry.Marshal(record)
	if err != nil || !c.tunnel.SendClientTelemetry(payload) {
		c.metrics.Add(telemetry.TelemetryRecordDropsTotal, 1)
	}
}

func clientSnapshotMetrics() []telemetry.Metric {
	metrics := append([]telemetry.Metric(nil), telemetry.ClientRequired...)
	return append(metrics,
		telemetry.OuterWrapFailuresTotal,
		telemetry.OuterReorderedPacketsTotal,
		telemetry.OuterDuplicatePacketsTotal,
		telemetry.RuntimeThermalAvailable,
		telemetry.WorkerNoAvailableDropsTotal,
	)
}

func clientWorkerSnapshotMetrics() []telemetry.Metric {
	return []telemetry.Metric{
		telemetry.VKAuthSuccessTotal,
		telemetry.VKAuthFailureTotal,
		telemetry.VKAuthLatencyMS,
		telemetry.VKAuthAnonymTokenLatencyMS,
		telemetry.VKCallPreviewLatencyMS,
		telemetry.VKAnonymCallTokenLatencyMS,
		telemetry.VKAnonymLoginLatencyMS,
		telemetry.VKJoinConversationLatencyMS,
		telemetry.VKCredentialRequestTotal,
		telemetry.VKCredentialFetchTotal,
		telemetry.VKCredentialCacheHitTotal,
		telemetry.TURNAllocateSuccessTotal,
		telemetry.TURNAllocateFailureTotal,
		telemetry.TURNAllocateLatencyMS,
		telemetry.TURNEndpointsTriedTotal,
		telemetry.TURNEndpointCount,
		telemetry.TURNSelectedEndpointOrdinal,
		telemetry.DTLSHandshakeSuccessTotal,
		telemetry.DTLSHandshakeFailureTotal,
		telemetry.DTLSHandshakeLatencyMS,
		telemetry.InnerAuthSuccessTotal,
		telemetry.InnerAuthFailureTotal,
		telemetry.InnerAuthLatencyMS,
		telemetry.WorkerActive,
		telemetry.WorkerReconnectTotal,
		telemetry.WorkerReconnectBackoffMS,
		telemetry.WorkerSendQueueDepth,
		telemetry.WorkerSendQueueDropsTotal,
		telemetry.WorkerLivenessExpiredTotal,
		telemetry.WorkerPacingRateBPS,
		telemetry.WorkerPacingWaitSecondsTotal,
		telemetry.WorkerPacingPacketsTotal,
		telemetry.WorkerPathRTTMS,
		telemetry.WorkerPathLossRatio,
		telemetry.WorkerPathAckedBytesTotal,
		telemetry.WorkerPathRetransSegmentsTotal,
		telemetry.WorkerPathSwitchesTotal,
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
