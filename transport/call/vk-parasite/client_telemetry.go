package vkparasite

import (
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
)

// ai-generated: client telemetry collector for vk_parasite QUICRelay transport
const (
	clientTelemetryInterval     = 2 * time.Second
	clientEventsPerInterval     = 16
	clientTelemetryEventBacklog = 64
	clientTelemetryFlushBudget  = 16
)

func (c *Client) enableTelemetry(lease time.Duration) {
	if lease < 2*time.Second {
		lease = 2 * time.Second
	}
	if lease > 120*time.Second {
		lease = 120 * time.Second
	}
	c.metrics.SetCollectionActive(true)
}

func (c *Client) telemetryLoop() {
	ticker := time.NewTicker(clientTelemetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			c.emitTelemetry()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) emitTelemetry() {
	if c.relay == nil {
		return
	}
	c.metrics.Set(telemetry.WorkerDesired, float64(c.options.Workers))
	c.metrics.Set(telemetry.WorkerActive, float64(c.relay.ActivePaths()))
	c.metrics.Set(telemetry.QUICConnCount, float64(c.relay.ActivePaths()))
}

func clientSnapshotMetrics() []telemetry.Metric {
	metrics := append([]telemetry.Metric(nil), telemetry.ClientRequired...)
	return append(metrics,
		telemetry.OuterReorderedPacketsTotal,
		telemetry.OuterDuplicatePacketsTotal,
		telemetry.NetworkLossRatio,
		telemetry.NetworkJitterMS,
		telemetry.NetworkHandoverTotal,
		telemetry.NetworkChangeTotal,
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
