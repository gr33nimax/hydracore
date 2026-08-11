package telemetry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinkFollowsUltimateSessionLifecycle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the native VPS sink relies on POSIX rename semantics")
	}
	t.Parallel()
	stateDirectory := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "run", "calls-telemetry.jsonl")
	sessionID := "20260811T120000Z-deadbeef"
	writeJSON(t, filepath.Join(stateDirectory, "active.json"), map[string]any{"session_id": sessionID})
	sessionPath := filepath.Join(stateDirectory, sessionID+".json")
	writeJSON(t, sessionPath, map[string]any{"session_id": sessionID, "stopped_at": 0})

	sink := NewSink(SinkConfig{StateDirectory: stateDirectory, OutputPath: outputPath})
	t.Cleanup(func() { _ = sink.Close() })
	active, changed, err := sink.Sync()
	require.NoError(t, err)
	require.True(t, active)
	require.True(t, changed)
	require.NoError(t, sink.Write(Snapshot("server", "", "server", NewAccumulator().Snapshot(ServerRequired))))
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Contains(t, string(content), `"scope":"server"`)
	require.Equal(t, os.FileMode(0o600), requireFileMode(t, outputPath))
	require.Equal(t, os.FileMode(0o600), requireFileMode(t, outputPath+".session"))

	// A core/service restart during the same Ultimate session must preserve the
	// not-yet-ingested tail instead of rotating the native stream.
	require.NoError(t, sink.Close())
	sink = NewSink(SinkConfig{StateDirectory: stateDirectory, OutputPath: outputPath})
	active, changed, err = sink.Sync()
	require.NoError(t, err)
	require.True(t, active)
	require.True(t, changed)
	content, err = os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Contains(t, string(content), `"scope":"server"`)

	nextSessionID := "20260811T130000Z-cafebabe"
	writeJSON(t, filepath.Join(stateDirectory, nextSessionID+".json"), map[string]any{
		"session_id": nextSessionID,
		"stopped_at": 0,
	})
	writeJSON(t, filepath.Join(stateDirectory, "active.json"), map[string]any{"session_id": nextSessionID})
	active, changed, err = sink.Sync()
	require.NoError(t, err)
	require.True(t, active)
	require.True(t, changed)
	content, err = os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Empty(t, content)

	nextSessionPath := filepath.Join(stateDirectory, nextSessionID+".json")
	writeJSON(t, nextSessionPath, map[string]any{"session_id": nextSessionID, "stopped_at": 1})
	active, changed, err = sink.Sync()
	require.NoError(t, err)
	require.False(t, active)
	require.True(t, changed)
}

func requireFileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}

func TestSinkRejectsSymlinkedActivePointer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation is not generally available to Windows test users")
	}
	t.Parallel()
	stateDirectory := t.TempDir()
	target := filepath.Join(t.TempDir(), "pointer.json")
	writeJSON(t, target, map[string]any{"session_id": "20260811T120000Z-deadbeef"})
	require.NoError(t, os.Symlink(target, filepath.Join(stateDirectory, "active.json")))
	sink := NewSink(SinkConfig{StateDirectory: stateDirectory, OutputPath: filepath.Join(t.TempDir(), "telemetry.jsonl")})
	active, _, err := sink.Sync()
	require.Error(t, err)
	require.False(t, active)
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, payload, 0o600))
}
