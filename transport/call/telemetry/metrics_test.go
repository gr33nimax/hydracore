package telemetry

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// ai-generated: contract tests between HydraCore telemetry accumulator and HYDRA Ultimate expectations
func TestRequiredMetricContractMatchesHydraUltimate(t *testing.T) {
	t.Parallel()
	serverExpected := []string{
		"auth_success_total", "auth_failure_total",
		"dtls_handshake_success_total", "dtls_handshake_failure_total", "dtls_handshake_latency_ms",
		"handshake_pending", "handshake_rejected_total", "handshake_timeout_total", "handshake_latency_ms",
		"outer_packets_in_total", "outer_packets_out_total", "outer_bytes_in_total", "outer_bytes_out_total",
		"outer_payload_bytes_in_total", "outer_payload_bytes_out_total", "outer_overhead_bytes_in_total", "outer_overhead_bytes_out_total",
		"outer_auth_failures_total", "outer_wrap_failures_total",
		"peer_read_queue_depth", "peer_read_queue_capacity", "peer_read_queue_drops_total",
		"udp_ingress_queue_depth", "udp_ingress_queue_capacity", "udp_ingress_queue_drops_total", "udp_ingress_workers",
		"udp_socket_receive_buffer_bytes", "udp_socket_send_buffer_bytes",
		"relay_tcp_active", "relay_udp_active", "relay_bytes_total", "relay_queue_depth", "relay_queue_drops_total", "relay_connect_failure_total",
		"runtime_goroutines", "runtime_heap_bytes", "runtime_gc_pause_seconds_total",
		"session_active", "session_created_total", "session_closed_total",
		"worker_active", "worker_attach_success_total", "worker_attach_failure_total",
		"outer_rtp_payload_type",
		"aggregate_progress_age_seconds", "session_replacement_total",
		"telemetry_sequence", "telemetry_control_drops_total", "telemetry_record_drops_total", "telemetry_sink_rotations_total",
		"quic_conn_count", "quic_streams_active", "quic_streams_opened_total", "quic_rtt_ms", "quic_rtt_var_ms",
		"quic_packets_lost_total", "quic_bytes_retrans_total", "quic_congestion_window_bytes",
		"quic_datagrams_sent_total", "quic_datagrams_dropped_total", "path_replacements_total",
	}
	clientExpected := []string{
		"vk_auth_success_total", "vk_auth_failure_total", "vk_auth_latency_ms", "vk_auth_anonym_token_latency_ms",
		"vk_call_preview_latency_ms", "vk_anonym_call_token_latency_ms", "vk_anonym_login_latency_ms", "vk_join_conversation_latency_ms",
		"vk_credential_request_total", "vk_credential_fetch_total", "vk_credential_cache_hit_total",
		"turn_allocate_success_total", "turn_allocate_failure_total", "turn_allocate_latency_ms", "turn_endpoints_tried_total",
		"turn_endpoint_count", "turn_selected_endpoint_ordinal",
		"dtls_handshake_success_total", "dtls_handshake_failure_total", "dtls_handshake_latency_ms",
		"inner_auth_success_total", "inner_auth_failure_total", "inner_auth_latency_ms",
		"worker_desired", "worker_active", "worker_reconnect_total", "worker_reconnect_backoff_ms",
		"outer_rtp_payload_type",
		"aggregate_progress_age_seconds", "session_replacement_total",
		"network_loss_ratio", "network_jitter_ms", "network_handover_total", "network_change_total",
		"outer_packets_in_total", "outer_packets_out_total", "outer_bytes_in_total", "outer_bytes_out_total",
		"outer_payload_bytes_in_total", "outer_payload_bytes_out_total", "outer_overhead_bytes_in_total", "outer_overhead_bytes_out_total", "outer_auth_failures_total",
		"runtime_cpu_percent", "runtime_rss_bytes", "runtime_thermal_state",
		"telemetry_sequence", "telemetry_record_drops_total", "telemetry_lease_expired_total", "telemetry_pending_records", "telemetry_snapshot_coalesced_total",
		"quic_conn_count", "quic_streams_active", "quic_streams_opened_total", "quic_rtt_ms", "quic_rtt_var_ms",
		"quic_packets_lost_total", "quic_bytes_retrans_total", "quic_congestion_window_bytes",
		"quic_datagrams_sent_total", "quic_datagrams_dropped_total", "path_replacements_total",
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

func metricNames(snapshot map[string]any) []string {
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	return names
}
