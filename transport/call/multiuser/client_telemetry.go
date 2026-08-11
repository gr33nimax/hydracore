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
	if lease > 30*time.Second {
		lease = 30 * time.Second
	}
	c.telemetryLease.Store(time.Now().Add(lease).UnixNano())
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
				c.tunnel.SetTelemetryCollectionActive(false)
				continue
			}
			c.emitTelemetry()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) emitTelemetry() {
	c.metrics.Set(telemetry.WorkerDesired, float64(c.options.Workers))
	c.metrics.Set(telemetry.WorkerActive, float64(c.tunnel.ActiveWorkers()))
	c.processSampler.Sample(c.metrics)
	c.tunnel.TelemetryValues()
	for _, event := range c.metrics.DrainEvents(clientEventsPerInterval) {
		record := telemetry.EventRecord("client", "", "", event)
		payload, err := telemetry.Marshal(record)
		if err == nil {
			c.tunnel.SendClientTelemetry(payload)
		}
	}
	metrics := c.metrics.Snapshot(clientSnapshotMetrics())
	record := telemetry.Snapshot("client", "", "", metrics)
	payload, err := telemetry.Marshal(record)
	if err == nil {
		c.tunnel.SendClientTelemetry(payload)
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
