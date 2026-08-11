package multiuser

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func TestServerTelemetryWritesUltimateCompatibleSnapshot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the native VPS sink relies on POSIX rename semantics")
	}
	t.Parallel()
	stateDirectory := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "run", "calls-telemetry.jsonl")
	sessionID := "20260811T120000Z-deadbeef"
	writeTelemetryJSON(t, filepath.Join(stateDirectory, "active.json"), map[string]any{"session_id": sessionID})
	writeTelemetryJSON(t, filepath.Join(stateDirectory, sessionID+".json"), map[string]any{
		"session_id": sessionID,
		"stopped_at": 0,
	})
	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword:             "outer-secret",
		Users:                    []ServerUser{{Name: "alice", Password: "user-secret"}},
		SessionHandler:           func(SessionInfo, *PooledTunnel) error { return nil },
		TelemetryStateDirectory: stateDirectory,
		TelemetryOutputPath:     outputPath,
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	server.telemetry.emit()
	request := authRequest{
		SessionID: [16]byte{1}, Conv: 0x12345678, WorkerTotal: 1,
		User: "alice", Password: "user-secret",
	}
	session, _, err := server.getOrCreateSession(request)
	require.NoError(t, err)
	server.releaseSessionAttach(session)
	clientRecord := telemetry.Snapshot("client", "", "", telemetry.NewAccumulator().Snapshot(telemetry.ClientRequired))
	clientRecord.Timestamp = 1
	clientPayload, err := telemetry.Marshal(clientRecord)
	require.NoError(t, err)
	server.telemetry.clientRecord(session, clientPayload)
	invalidWorkerID := uint16(2)
	clientEvent := telemetry.EventRecord("client", "", "", telemetry.Event{
		Timestamp: 1,
		Event:     "worker_reconnect",
		Stage:     "worker",
		Reason:    "transport",
		WorkerID:  &invalidWorkerID,
	})
	clientPayload, err = telemetry.Marshal(clientEvent)
	require.NoError(t, err)
	server.telemetry.clientRecord(session, clientPayload)

	file, err := os.Open(outputPath)
	require.NoError(t, err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	require.True(t, scanner.Scan())
	var record telemetry.Record
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
	require.Equal(t, "server", record.Scope)
	for _, metric := range telemetry.ServerRequired {
		require.Contains(t, record.Metrics, telemetry.Name(metric))
	}
	require.True(t, scanner.Scan())
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
	require.Equal(t, "client", record.Scope)
	require.Equal(t, "alice", record.User)
	require.NotEmpty(t, record.SessionID)
	require.Greater(t, record.Timestamp, float64(1))
	for _, metric := range telemetry.ClientRequired {
		require.Contains(t, record.Metrics, telemetry.Name(metric))
	}
	require.True(t, scanner.Scan())
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
	require.Equal(t, "server", record.Scope)
	require.Equal(t, "client_record_rejected", record.Event)
	require.Equal(t, "worker_id", record.Reason)
}

func TestServerTelemetrySeparatesProcessSessionAndWorkerSnapshots(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the native VPS sink relies on POSIX rename semantics")
	}
	stateDirectory := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "run", "calls-telemetry.jsonl")
	sessionID := "20260811T120000Z-deadbeef"
	writeTelemetryJSON(t, filepath.Join(stateDirectory, "active.json"), map[string]any{"session_id": sessionID})
	writeTelemetryJSON(t, filepath.Join(stateDirectory, sessionID+".json"), map[string]any{
		"session_id": sessionID,
		"stopped_at": 0,
	})
	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword:             "outer-secret",
		Users:                    []ServerUser{{Name: "alice", Password: "user-secret"}},
		SessionHandler:           func(SessionInfo, *PooledTunnel) error { return nil },
		TelemetryStateDirectory: stateDirectory,
		TelemetryOutputPath:     outputPath,
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	request := authRequest{
		SessionID: [16]byte{1}, Conv: 0x12345678, WorkerTotal: 1,
		User: "alice", Password: "user-secret",
	}
	session, _, err := server.getOrCreateSession(request)
	require.NoError(t, err)
	server.releaseSessionAttach(session)
	worker := session.tunnel.telemetryWorker(0)
	worker.Add(telemetry.OuterBytesOutTotal, 1234)

	server.telemetry.emit()

	file, err := os.Open(outputPath)
	require.NoError(t, err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	records := make([]telemetry.Record, 0, 3)
	for scanner.Scan() {
		var record telemetry.Record
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		records = append(records, record)
	}
	require.NoError(t, scanner.Err())
	require.Len(t, records, 3)
	require.Equal(t, "alice", records[0].User)
	require.Nil(t, records[0].WorkerID)
	require.Equal(t, float64(1234), metricNumber(records[0].Metrics["outer_bytes_out_total"]))
	require.Equal(t, "alice", records[1].User)
	require.NotNil(t, records[1].WorkerID)
	require.Equal(t, uint16(0), *records[1].WorkerID)
	require.Equal(t, float64(1234), metricNumber(records[1].Metrics["outer_bytes_out_total"]))
	require.Equal(t, "server", records[2].SessionID)
	require.Empty(t, records[2].User)
}

func TestTelemetryControlDoesNotKeepAnUnattachedSessionAlive(t *testing.T) {
	tunnel, err := NewPooledTunnel(1, 1, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	before := tunnel.LastActivity()

	require.True(t, tunnel.RequestClientTelemetry(30*time.Second))

	require.Equal(t, before, tunnel.LastActivity())
}

func writeTelemetryJSON(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, payload, 0o600))
}
