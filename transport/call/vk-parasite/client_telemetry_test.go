package vkparasite

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func TestClientTelemetryBacklogRetriesEventsAndCoalescesSnapshots(t *testing.T) {
	t.Parallel()
	clientTunnel, err := NewParasiteTunnel(0x77889901, logger.NOP())
	require.NoError(t, err)
	serverTunnel, err := NewParasiteTunnel(0x77889901, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = clientTunnel.Close()
		_ = serverTunnel.Close()
	})
	metrics := telemetry.NewAccumulator()
	client := &Client{
		options: ClientOptions{Workers: LaneCount},
		tunnel: clientTunnel,
		metrics: metrics,
		telemetrySnapshots: make(map[int][]byte, LaneCount+1),
	}

	client.sendTelemetryRecord(telemetry.EventRecord("client", "", "", telemetry.Event{
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
		Event: "lane_send_recovery",
		Stage: "lane",
		Reason: "test",
	}))
	workerID := uint16(0)
	first := telemetry.Snapshot("client", "", "", map[string]any{"telemetry_sequence": 1})
	first.WorkerID = &workerID
	client.sendTelemetryRecord(first)
	latest := telemetry.Snapshot("client", "", "", map[string]any{"telemetry_sequence": 2})
	latest.WorkerID = &workerID
	client.sendTelemetryRecord(latest)

	client.flushTelemetryRecords()
	require.Equal(t, 2.0, metrics.Value(telemetry.TelemetryPendingRecords))
	require.Zero(t, metrics.Value(telemetry.TelemetryRecordDropsTotal))

	received := make(chan []byte, 2)
	serverTunnel.SetTelemetryClientRecordHandler(func(payload []byte) {
		received <- payload
	})
	connectTestLanes(t, clientTunnel, serverTunnel)
	client.flushTelemetryRecords()

	receive := func() telemetry.Record {
		select {
		case payload := <-received:
			record, decodeErr := telemetry.DecodeClientRecord(payload)
			require.NoError(t, decodeErr)
			return record
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for buffered telemetry")
			return telemetry.Record{}
		}
	}
	firstReceived := receive()
	secondReceived := receive()
	require.Equal(t, "event", firstReceived.Kind)
	require.Equal(t, "snapshot", secondReceived.Kind)
	require.Equal(t, float64(2), secondReceived.Metrics["telemetry_sequence"])
	require.Zero(t, metrics.Value(telemetry.TelemetryPendingRecords))
}
