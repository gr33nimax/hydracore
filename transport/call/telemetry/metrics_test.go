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
		"kcp_wait_snd", "kcp_out_segments_total", "kcp_retrans_segments_total", "kcp_out_bytes_total", "kcp_retrans_bytes_total", "kcp_rtt_ms", "kcp_rto_ms", "kcp_send_blocked_seconds_total",
		"kcp_mtu_bytes", "kcp_send_window_segments", "kcp_receive_window_segments", "kcp_max_pending_segments",
		"kcp_update_interval_ms", "kcp_fast_resend", "kcp_congestion_control",
		"outer_packets_in_total", "outer_packets_out_total", "outer_bytes_in_total", "outer_bytes_out_total",
		"outer_payload_bytes_in_total", "outer_payload_bytes_out_total", "outer_overhead_bytes_in_total", "outer_overhead_bytes_out_total",
		"outer_auth_failures_total", "outer_wrap_failures_total",
		"peer_read_queue_depth", "peer_read_queue_capacity", "peer_read_queue_drops_total",
		"udp_ingress_queue_depth", "udp_ingress_queue_capacity", "udp_ingress_queue_drops_total", "udp_ingress_workers",
		"udp_socket_receive_buffer_bytes", "udp_socket_send_buffer_bytes",
		"relay_tcp_active", "relay_udp_active", "relay_bytes_total", "relay_queue_depth", "relay_queue_drops_total", "relay_connect_failure_total",
		"runtime_goroutines", "runtime_heap_bytes", "runtime_gc_pause_seconds_total",
		"session_active", "session_created_total", "session_closed_total",
		"worker_active", "worker_attach_success_total", "worker_attach_failure_total", "worker_send_queue_depth",
		"worker_send_queue_drops_total", "worker_no_available_drops_total", "worker_liveness_expired_total",
		"multipath_profile", "multipath_chunk_packets", "multipath_chunk_dwell_ms",
		"worker_pacing_rate_bps", "worker_pacing_wait_seconds_total", "worker_pacing_packets_total",
		"worker_path_rtt_ms", "worker_path_delivery_rate_bps", "worker_path_window_segments", "worker_path_inflight_segments",
		"worker_path_loss_ratio", "worker_path_retry_ratio", "worker_path_acked_bytes_total", "worker_path_attempt_segments_total",
		"worker_path_retrans_segments_total", "worker_path_switches_total", "worker_path_backoff_total",
		"worker_path_feedback_capable", "worker_path_feedback_age_ms", "worker_path_feedback_records_total",
		"worker_path_feedback_acked_packets_total", "worker_path_feedback_lost_packets_total", "worker_path_control_copies_total",
		"worker_output_queue_delay_ms", "worker_output_queue_late_total",
		"telemetry_sequence", "telemetry_control_drops_total", "telemetry_record_drops_total", "telemetry_sink_rotations_total",
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
		"worker_send_queue_depth", "worker_send_queue_drops_total", "worker_liveness_expired_total",
		"worker_send_queue_capacity", "worker_heartbeat_interval_ms", "worker_liveness_timeout_ms",
		"multipath_profile", "multipath_chunk_packets", "multipath_chunk_dwell_ms",
		"worker_pacing_rate_bps", "worker_pacing_wait_seconds_total", "worker_pacing_packets_total",
		"worker_path_rtt_ms", "worker_path_delivery_rate_bps", "worker_path_window_segments", "worker_path_inflight_segments",
		"worker_path_loss_ratio", "worker_path_retry_ratio", "worker_path_acked_bytes_total", "worker_path_attempt_segments_total",
		"worker_path_retrans_segments_total", "worker_path_switches_total", "worker_path_backoff_total",
		"worker_path_feedback_capable", "worker_path_feedback_age_ms", "worker_path_feedback_records_total",
		"worker_path_feedback_acked_packets_total", "worker_path_feedback_lost_packets_total", "worker_path_control_copies_total",
		"worker_output_queue_delay_ms", "worker_output_queue_late_total",
		"kcp_wait_snd", "kcp_out_segments_total", "kcp_retrans_segments_total", "kcp_out_bytes_total", "kcp_retrans_bytes_total", "kcp_rtt_ms", "kcp_rto_ms", "kcp_send_blocked_seconds_total",
		"kcp_mtu_bytes", "kcp_send_window_segments", "kcp_receive_window_segments", "kcp_max_pending_segments",
		"kcp_update_interval_ms", "kcp_fast_resend", "kcp_congestion_control",
		"network_loss_ratio", "network_jitter_ms", "network_handover_total", "network_change_total",
		"outer_packets_in_total", "outer_packets_out_total", "outer_bytes_in_total", "outer_bytes_out_total",
		"outer_payload_bytes_in_total", "outer_payload_bytes_out_total", "outer_overhead_bytes_in_total", "outer_overhead_bytes_out_total", "outer_auth_failures_total",
		"runtime_cpu_percent", "runtime_rss_bytes", "runtime_thermal_state",
		"telemetry_sequence", "telemetry_record_drops_total", "telemetry_lease_expired_total",
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
