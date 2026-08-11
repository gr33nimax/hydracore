package telemetry

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequiredMetricContractMatchesHydraUltimate(t *testing.T) {
	t.Parallel()
	serverExpected := []string{
		"auth_success_total", "auth_failure_total",
		"dtls_handshake_success_total", "dtls_handshake_failure_total", "dtls_handshake_latency_ms",
		"handshake_pending", "handshake_rejected_total", "handshake_timeout_total", "handshake_latency_ms",
		"kcp_wait_snd", "kcp_out_segments_total", "kcp_retrans_segments_total", "kcp_rtt_ms", "kcp_rto_ms", "kcp_send_blocked_seconds_total",
		"outer_packets_in_total", "outer_packets_out_total", "outer_bytes_in_total", "outer_bytes_out_total", "outer_auth_failures_total", "outer_wrap_failures_total",
		"peer_read_queue_depth", "peer_read_queue_drops_total",
		"relay_tcp_active", "relay_udp_active", "relay_bytes_total", "relay_queue_depth", "relay_queue_drops_total", "relay_connect_failure_total",
		"runtime_goroutines", "runtime_heap_bytes", "runtime_gc_pause_seconds_total",
		"session_active", "session_created_total", "session_closed_total",
		"worker_active", "worker_attach_success_total", "worker_attach_failure_total", "worker_send_queue_depth",
		"worker_send_queue_drops_total", "worker_no_available_drops_total", "worker_liveness_expired_total",
	}
	clientExpected := []string{
		"vk_auth_success_total", "vk_auth_failure_total", "vk_auth_latency_ms", "vk_auth_anonym_token_latency_ms",
		"vk_call_preview_latency_ms", "vk_anonym_call_token_latency_ms", "vk_anonym_login_latency_ms", "vk_join_conversation_latency_ms",
		"turn_allocate_success_total", "turn_allocate_failure_total", "turn_allocate_latency_ms", "turn_endpoints_tried_total",
		"dtls_handshake_success_total", "dtls_handshake_failure_total", "dtls_handshake_latency_ms",
		"inner_auth_success_total", "inner_auth_failure_total", "inner_auth_latency_ms",
		"worker_desired", "worker_active", "worker_reconnect_total", "worker_reconnect_backoff_ms",
		"worker_send_queue_depth", "worker_send_queue_drops_total", "worker_liveness_expired_total",
		"kcp_wait_snd", "kcp_out_segments_total", "kcp_retrans_segments_total", "kcp_rtt_ms", "kcp_rto_ms", "kcp_send_blocked_seconds_total",
		"network_loss_ratio", "network_jitter_ms", "network_handover_total", "network_change_total",
		"outer_packets_in_total", "outer_packets_out_total", "outer_bytes_in_total", "outer_bytes_out_total", "outer_auth_failures_total",
		"runtime_cpu_percent", "runtime_rss_bytes", "runtime_thermal_state",
	}
	accumulator := NewAccumulator()
	require.ElementsMatch(t, serverExpected, metricNames(accumulator.Snapshot(ServerRequired)))
	require.ElementsMatch(t, clientExpected, metricNames(accumulator.Snapshot(ClientRequired)))
}

func TestClientRecordRejectsIdentityAndUnknownMetrics(t *testing.T) {
	t.Parallel()
	valid := Snapshot("client", "", "", NewAccumulator().Snapshot(ClientRequired))
	payload, err := Marshal(valid)
	require.NoError(t, err)
	decoded, err := DecodeClientRecord(payload)
	require.NoError(t, err)
	require.Equal(t, "client", decoded.Scope)

	valid.User = "forged@example.invalid"
	payload, err = json.Marshal(valid)
	require.NoError(t, err)
	_, err = DecodeClientRecord(payload)
	require.Error(t, err)

	valid.User = ""
	valid.Metrics["password"] = 1
	payload, err = json.Marshal(valid)
	require.NoError(t, err)
	_, err = DecodeClientRecord(payload)
	require.Error(t, err)
}

func metricNames(metrics map[string]any) []string {
	names := make([]string, 0, len(metrics))
	for name := range metrics {
		names = append(names, name)
	}
	return names
}
