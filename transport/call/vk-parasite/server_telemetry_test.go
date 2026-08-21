package vkparasite

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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
		ObfsPassword:            "outer-secret",
		Users:                   []ServerUser{{Name: "alice", Password: "user-secret"}},
		SessionHandler:          func(SessionInfo, *QUICRelay) error { return nil },
		TelemetryStateDirectory: stateDirectory,
		TelemetryOutputPath:     outputPath,
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	server.telemetry.emit()
	request := authRequest{
		SessionID: [16]byte{1}, Conv: 0x12345678, WorkerTotal: LaneCount, LaneGeneration: 1,
		User: "alice", Password: "user-secret",
	}
	session, _, err := server.getOrCreateSession(request)
	require.NoError(t, err)
	server.releaseSessionAttach(session)

	server.telemetry.emit()

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
}

func writeTelemetryJSON(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, payload, 0o600))
}
