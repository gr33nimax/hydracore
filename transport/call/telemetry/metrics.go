package telemetry

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Metric uint16

const (
	AuthSuccessTotal Metric = iota
	AuthFailureTotal
	DTLSHandshakeSuccessTotal
	DTLSHandshakeFailureTotal
	DTLSHandshakeLatencyMS
	HandshakePending
	HandshakeRejectedTotal
	HandshakeTimeoutTotal
	HandshakeLatencyMS
	KCPWaitSnd
	KCPOutSegmentsTotal
	KCPRetransSegmentsTotal
	KCPOutBytesTotal
	KCPRetransBytesTotal
	KCPFastRetransEstimateSegmentsTotal
	KCPFastRetransEstimateBytesTotal
	KCPRTORetransEstimateSegmentsTotal
	KCPRTORetransEstimateBytesTotal
	KCPRTTMS
	KCPRTOMS
	KCPRTTVarMS
	KCPRTTSamplesTotal
	KCPAckSegmentsTotal
	KCPAckProgressSegmentsTotal
	KCPInflightSegments
	KCPSendBlockedSecondsTotal
	OuterPacketsInTotal
	OuterPacketsOutTotal
	OuterBytesInTotal
	OuterBytesOutTotal
	OuterPayloadBytesInTotal
	OuterPayloadBytesOutTotal
	OuterOverheadBytesInTotal
	OuterOverheadBytesOutTotal
	OuterAuthFailuresTotal
	OuterWrapFailuresTotal
	OuterReorderedPacketsTotal
	OuterDuplicatePacketsTotal
	PeerReadQueueDepth
	PeerReadQueueCapacity
	PeerReadQueueDropsTotal
	UDPIngressQueueDepth
	UDPIngressQueueCapacity
	UDPIngressQueueDropsTotal
	UDPIngressWorkers
	UDPSocketReceiveBufferBytes
	UDPSocketSendBufferBytes
	RelayTCPActive
	RelayUDPActive
	RelayBytesTotal
	RelayQueueDepth
	RelayQueueDropsTotal
	RelayConnectFailureTotal
	RuntimeGoroutines
	RuntimeHeapBytes
	RuntimeGCPauseSecondsTotal
	RuntimeCPUPercent
	RuntimeRSSBytes
	RuntimeThermalState
	RuntimeThermalAvailable
	SessionActive
	SessionCreatedTotal
	SessionClosedTotal
	WorkerDesired
	WorkerActive
	WorkerAttachSuccessTotal
	WorkerAttachFailureTotal
	WorkerReconnectTotal
	WorkerReconnectBackoffMS
	WorkerSendQueueDepth
	WorkerSendQueueDropsTotal
	WorkerNoAvailableDropsTotal
	WorkerLivenessExpiredTotal
	VKAuthSuccessTotal
	VKAuthFailureTotal
	VKAuthLatencyMS
	VKAuthAnonymTokenLatencyMS
	VKCallPreviewLatencyMS
	VKAnonymCallTokenLatencyMS
	VKAnonymLoginLatencyMS
	VKJoinConversationLatencyMS
	VKCredentialRequestTotal
	VKCredentialFetchTotal
	VKCredentialCacheHitTotal
	TURNAllocateSuccessTotal
	TURNAllocateFailureTotal
	TURNAllocateLatencyMS
	TURNEndpointsTriedTotal
	TURNEndpointCount
	TURNSelectedEndpointOrdinal
	InnerAuthSuccessTotal
	InnerAuthFailureTotal
	InnerAuthLatencyMS
	NetworkLossRatio
	NetworkJitterMS
	NetworkHandoverTotal
	NetworkChangeTotal
	SessionAgeSeconds
	SessionIdleSeconds
	TelemetrySequence
	TelemetryControlDropsTotal
	TelemetryRecordDropsTotal
	TelemetryLeaseExpiredTotal
	TelemetrySinkRotationsTotal
	TelemetryPendingRecords
	TelemetrySnapshotCoalescedTotal
	KCPMTUBytes
	KCPSendWindowSegments
	KCPReceiveWindowSegments
	KCPMaxPendingSegments
	KCPUpdateIntervalMS
	KCPFastResend
	KCPCongestionControl
	WorkerSendQueueCapacity
	WorkerHeartbeatIntervalMS
	WorkerLivenessTimeoutMS
	LaneCount
	LaneFlowCount
	WorkerOutputQueueDelayMS
	WorkerOutputQueueLateTotal
	LaneAdmissionRateBPS
	LaneAdmissionWindowSegments
	KCPOutputQueueDepth
	KCPOutputQueueCapacity
	KCPUpdateBackpressureTotal
	KCPMutexBlockedSecondsTotal
	WorkerWriteLatencyMS
	FlowReorderAbortTotal
	OuterRTPPayloadType
	LaneGeneration
	LaneState
	LanePacingRateBPS
	LaneDeliveredRateBPS
	LaneMinRTTMS
	LaneInflightLimitSegments
	LaneTokenStarvationTotal
	LaneApplicationLimited
	LaneRecoveryDeferredTotal
	LaneAckAgeSeconds
	LaneResetRequestTotal
	LaneResetRetryTotal
	LaneResetAckTotal
	LaneResetCommitTotal
	LaneResetDurationMS
	LaneStaleGenerationDropsTotal
	LaneProbeResult
	LaneAdmittedBytesTotal
	KCPAckedBytesTotal
	LaneRecoveryAttemptID
	LaneRecoveryLastOutcome
	AggregateProgressAgeSeconds
	QuarantinedLanes
	SessionReplacementTotal
	metricCount
)

type metricDescriptor struct {
	name    string
	counter bool
}

var metricDescriptors = [...]metricDescriptor{
	{name: "auth_success_total", counter: true},
	{name: "auth_failure_total", counter: true},
	{name: "dtls_handshake_success_total", counter: true},
	{name: "dtls_handshake_failure_total", counter: true},
	{name: "dtls_handshake_latency_ms"},
	{name: "handshake_pending"},
	{name: "handshake_rejected_total", counter: true},
	{name: "handshake_timeout_total", counter: true},
	{name: "handshake_latency_ms"},
	{name: "kcp_wait_snd"},
	{name: "kcp_out_segments_total", counter: true},
	{name: "kcp_retrans_segments_total", counter: true},
	{name: "kcp_out_bytes_total", counter: true},
	{name: "kcp_retrans_bytes_total", counter: true},
	{name: "kcp_fast_retrans_estimate_segments_total", counter: true},
	{name: "kcp_fast_retrans_estimate_bytes_total", counter: true},
	{name: "kcp_rto_retrans_estimate_segments_total", counter: true},
	{name: "kcp_rto_retrans_estimate_bytes_total", counter: true},
	{name: "kcp_rtt_ms"},
	{name: "kcp_rto_ms"},
	{name: "kcp_rttvar_ms"},
	{name: "kcp_rtt_samples_total", counter: true},
	{name: "kcp_ack_segments_total", counter: true},
	{name: "kcp_ack_progress_segments_total", counter: true},
	{name: "kcp_inflight_segments"},
	{name: "kcp_send_blocked_seconds_total"},
	{name: "outer_packets_in_total", counter: true},
	{name: "outer_packets_out_total", counter: true},
	{name: "outer_bytes_in_total", counter: true},
	{name: "outer_bytes_out_total", counter: true},
	{name: "outer_payload_bytes_in_total", counter: true},
	{name: "outer_payload_bytes_out_total", counter: true},
	{name: "outer_overhead_bytes_in_total", counter: true},
	{name: "outer_overhead_bytes_out_total", counter: true},
	{name: "outer_auth_failures_total", counter: true},
	{name: "outer_wrap_failures_total", counter: true},
	{name: "outer_reordered_packets_total", counter: true},
	{name: "outer_duplicate_packets_total", counter: true},
	{name: "peer_read_queue_depth"},
	{name: "peer_read_queue_capacity"},
	{name: "peer_read_queue_drops_total", counter: true},
	{name: "udp_ingress_queue_depth"},
	{name: "udp_ingress_queue_capacity"},
	{name: "udp_ingress_queue_drops_total", counter: true},
	{name: "udp_ingress_workers"},
	{name: "udp_socket_receive_buffer_bytes"},
	{name: "udp_socket_send_buffer_bytes"},
	{name: "relay_tcp_active"},
	{name: "relay_udp_active"},
	{name: "relay_bytes_total", counter: true},
	{name: "relay_queue_depth"},
	{name: "relay_queue_drops_total", counter: true},
	{name: "relay_connect_failure_total", counter: true},
	{name: "runtime_goroutines"},
	{name: "runtime_heap_bytes"},
	{name: "runtime_gc_pause_seconds_total"},
	{name: "runtime_cpu_percent"},
	{name: "runtime_rss_bytes"},
	{name: "runtime_thermal_state"},
	{name: "runtime_thermal_available"},
	{name: "session_active"},
	{name: "session_created_total", counter: true},
	{name: "session_closed_total", counter: true},
	{name: "worker_desired"},
	{name: "worker_active"},
	{name: "worker_attach_success_total", counter: true},
	{name: "worker_attach_failure_total", counter: true},
	{name: "worker_reconnect_total", counter: true},
	{name: "worker_reconnect_backoff_ms"},
	{name: "worker_send_queue_depth"},
	{name: "worker_send_queue_drops_total", counter: true},
	{name: "worker_no_available_drops_total", counter: true},
	{name: "worker_liveness_expired_total", counter: true},
	{name: "vk_auth_success_total", counter: true},
	{name: "vk_auth_failure_total", counter: true},
	{name: "vk_auth_latency_ms"},
	{name: "vk_auth_anonym_token_latency_ms"},
	{name: "vk_call_preview_latency_ms"},
	{name: "vk_anonym_call_token_latency_ms"},
	{name: "vk_anonym_login_latency_ms"},
	{name: "vk_join_conversation_latency_ms"},
	{name: "vk_credential_request_total", counter: true},
	{name: "vk_credential_fetch_total", counter: true},
	{name: "vk_credential_cache_hit_total", counter: true},
	{name: "turn_allocate_success_total", counter: true},
	{name: "turn_allocate_failure_total", counter: true},
	{name: "turn_allocate_latency_ms"},
	{name: "turn_endpoints_tried_total", counter: true},
	{name: "turn_endpoint_count"},
	{name: "turn_selected_endpoint_ordinal"},
	{name: "inner_auth_success_total", counter: true},
	{name: "inner_auth_failure_total", counter: true},
	{name: "inner_auth_latency_ms"},
	{name: "network_loss_ratio"},
	{name: "network_jitter_ms"},
	{name: "network_handover_total", counter: true},
	{name: "network_change_total", counter: true},
	{name: "session_age_seconds"},
	{name: "session_idle_seconds"},
	{name: "telemetry_sequence"},
	{name: "telemetry_control_drops_total", counter: true},
	{name: "telemetry_record_drops_total", counter: true},
	{name: "telemetry_lease_expired_total", counter: true},
	{name: "telemetry_sink_rotations_total", counter: true},
	{name: "telemetry_pending_records"},
	{name: "telemetry_snapshot_coalesced_total", counter: true},
	{name: "kcp_mtu_bytes"},
	{name: "kcp_send_window_segments"},
	{name: "kcp_receive_window_segments"},
	{name: "kcp_max_pending_segments"},
	{name: "kcp_update_interval_ms"},
	{name: "kcp_fast_resend"},
	{name: "kcp_congestion_control"},
	{name: "worker_send_queue_capacity"},
	{name: "worker_heartbeat_interval_ms"},
	{name: "worker_liveness_timeout_ms"},
	{name: "lane_count"},
	{name: "lane_flow_count"},
	{name: "worker_output_queue_delay_ms"},
	{name: "worker_output_queue_late_total", counter: true},
	{name: "lane_admission_bytes_per_second"},
	{name: "lane_admission_window_segments"},
	{name: "kcp_output_queue_depth"},
	{name: "kcp_output_queue_capacity"},
	{name: "kcp_update_backpressure_total", counter: true},
	{name: "kcp_mutex_blocked_seconds_total"},
	{name: "worker_write_latency_ms"},
	{name: "flow_reorder_abort_total", counter: true},
	{name: "outer_rtp_payload_type"},
	{name: "lane_generation"},
	{name: "lane_state"},
	{name: "lane_pacing_bytes_per_second"},
	{name: "lane_delivered_bytes_per_second"},
	{name: "lane_min_rtt_ms"},
	{name: "lane_inflight_limit_segments"},
	{name: "lane_token_starvation_total", counter: true},
	{name: "lane_application_limited"},
	{name: "lane_recovery_deferred_total", counter: true},
	{name: "lane_ack_age_seconds"},
	{name: "lane_reset_request_total", counter: true},
	{name: "lane_reset_retry_total", counter: true},
	{name: "lane_reset_ack_total", counter: true},
	{name: "lane_reset_commit_total", counter: true},
	{name: "lane_reset_duration_ms"},
	{name: "lane_stale_generation_drops_total", counter: true},
	{name: "lane_probe_result"},
	{name: "lane_admitted_bytes_total", counter: true},
	{name: "kcp_acked_bytes_total", counter: true},
	{name: "lane_recovery_attempt_id"},
	{name: "lane_recovery_last_outcome"},
	{name: "aggregate_progress_age_seconds"},
	{name: "quarantined_lanes"},
	{name: "session_replacement_total", counter: true},
}

var ServerRequired = []Metric{
	AuthSuccessTotal, AuthFailureTotal,
	DTLSHandshakeSuccessTotal, DTLSHandshakeFailureTotal, DTLSHandshakeLatencyMS,
	HandshakePending, HandshakeRejectedTotal, HandshakeTimeoutTotal, HandshakeLatencyMS,
	KCPWaitSnd, KCPOutSegmentsTotal, KCPRetransSegmentsTotal, KCPOutBytesTotal, KCPRetransBytesTotal,
	KCPFastRetransEstimateSegmentsTotal, KCPFastRetransEstimateBytesTotal, KCPRTORetransEstimateSegmentsTotal, KCPRTORetransEstimateBytesTotal,
	KCPRTTMS, KCPRTOMS, KCPRTTVarMS, KCPRTTSamplesTotal,
	KCPAckSegmentsTotal, KCPAckProgressSegmentsTotal, KCPInflightSegments, KCPSendBlockedSecondsTotal,
	KCPMTUBytes, KCPSendWindowSegments, KCPReceiveWindowSegments, KCPMaxPendingSegments,
	KCPUpdateIntervalMS, KCPFastResend, KCPCongestionControl,
	OuterPacketsInTotal, OuterPacketsOutTotal, OuterBytesInTotal, OuterBytesOutTotal,
	OuterPayloadBytesInTotal, OuterPayloadBytesOutTotal, OuterOverheadBytesInTotal, OuterOverheadBytesOutTotal,
	OuterAuthFailuresTotal, OuterWrapFailuresTotal,
	PeerReadQueueDepth, PeerReadQueueCapacity, PeerReadQueueDropsTotal,
	UDPIngressQueueDepth, UDPIngressQueueCapacity, UDPIngressQueueDropsTotal, UDPIngressWorkers,
	UDPSocketReceiveBufferBytes, UDPSocketSendBufferBytes,
	RelayTCPActive, RelayUDPActive, RelayBytesTotal, RelayQueueDepth, RelayQueueDropsTotal, RelayConnectFailureTotal,
	RuntimeGoroutines, RuntimeHeapBytes, RuntimeGCPauseSecondsTotal,
	SessionActive, SessionCreatedTotal, SessionClosedTotal,
	WorkerActive, WorkerAttachSuccessTotal, WorkerAttachFailureTotal, WorkerSendQueueDepth,
	WorkerSendQueueDropsTotal, WorkerNoAvailableDropsTotal, WorkerLivenessExpiredTotal,
	LaneCount, LaneFlowCount,
	WorkerOutputQueueDelayMS, WorkerOutputQueueLateTotal, WorkerWriteLatencyMS,
	LaneAdmissionRateBPS, LaneAdmissionWindowSegments,
	KCPOutputQueueDepth, KCPOutputQueueCapacity, KCPUpdateBackpressureTotal, KCPMutexBlockedSecondsTotal,
	FlowReorderAbortTotal, OuterRTPPayloadType,
	LaneGeneration, LaneState, LanePacingRateBPS, LaneDeliveredRateBPS, LaneMinRTTMS, LaneInflightLimitSegments,
	LaneTokenStarvationTotal, LaneApplicationLimited, LaneRecoveryDeferredTotal, LaneAckAgeSeconds, LaneResetRequestTotal, LaneResetRetryTotal, LaneResetAckTotal,
	LaneResetCommitTotal, LaneResetDurationMS, LaneStaleGenerationDropsTotal, LaneProbeResult,
	AggregateProgressAgeSeconds, QuarantinedLanes, SessionReplacementTotal,
	TelemetrySequence, TelemetryControlDropsTotal, TelemetryRecordDropsTotal, TelemetrySinkRotationsTotal,
}

var ClientRequired = []Metric{
	VKAuthSuccessTotal, VKAuthFailureTotal, VKAuthLatencyMS, VKAuthAnonymTokenLatencyMS,
	VKCallPreviewLatencyMS, VKAnonymCallTokenLatencyMS, VKAnonymLoginLatencyMS, VKJoinConversationLatencyMS,
	VKCredentialRequestTotal, VKCredentialFetchTotal, VKCredentialCacheHitTotal,
	TURNAllocateSuccessTotal, TURNAllocateFailureTotal, TURNAllocateLatencyMS, TURNEndpointsTriedTotal,
	TURNEndpointCount, TURNSelectedEndpointOrdinal,
	DTLSHandshakeSuccessTotal, DTLSHandshakeFailureTotal, DTLSHandshakeLatencyMS,
	InnerAuthSuccessTotal, InnerAuthFailureTotal, InnerAuthLatencyMS,
	WorkerDesired, WorkerActive, WorkerReconnectTotal, WorkerReconnectBackoffMS,
	WorkerSendQueueDepth, WorkerSendQueueDropsTotal, WorkerLivenessExpiredTotal,
	WorkerSendQueueCapacity, WorkerHeartbeatIntervalMS, WorkerLivenessTimeoutMS,
	LaneCount, LaneFlowCount,
	WorkerOutputQueueDelayMS, WorkerOutputQueueLateTotal, WorkerWriteLatencyMS,
	LaneAdmissionRateBPS, LaneAdmissionWindowSegments,
	KCPOutputQueueDepth, KCPOutputQueueCapacity, KCPUpdateBackpressureTotal, KCPMutexBlockedSecondsTotal,
	FlowReorderAbortTotal, OuterRTPPayloadType,
	LaneGeneration, LaneState, LanePacingRateBPS, LaneDeliveredRateBPS, LaneMinRTTMS, LaneInflightLimitSegments,
	LaneTokenStarvationTotal, LaneApplicationLimited, LaneRecoveryDeferredTotal, LaneAckAgeSeconds, LaneResetRequestTotal, LaneResetRetryTotal, LaneResetAckTotal,
	LaneResetCommitTotal, LaneResetDurationMS, LaneStaleGenerationDropsTotal, LaneProbeResult,
	AggregateProgressAgeSeconds, QuarantinedLanes, SessionReplacementTotal,
	KCPWaitSnd, KCPOutSegmentsTotal, KCPRetransSegmentsTotal, KCPOutBytesTotal, KCPRetransBytesTotal,
	KCPFastRetransEstimateSegmentsTotal, KCPFastRetransEstimateBytesTotal, KCPRTORetransEstimateSegmentsTotal, KCPRTORetransEstimateBytesTotal,
	KCPRTTMS, KCPRTOMS, KCPRTTVarMS, KCPRTTSamplesTotal,
	KCPAckSegmentsTotal, KCPAckProgressSegmentsTotal, KCPInflightSegments, KCPSendBlockedSecondsTotal,
	KCPMTUBytes, KCPSendWindowSegments, KCPReceiveWindowSegments, KCPMaxPendingSegments,
	KCPUpdateIntervalMS, KCPFastResend, KCPCongestionControl,
	NetworkLossRatio, NetworkJitterMS, NetworkHandoverTotal, NetworkChangeTotal,
	OuterPacketsInTotal, OuterPacketsOutTotal, OuterBytesInTotal, OuterBytesOutTotal,
	OuterPayloadBytesInTotal, OuterPayloadBytesOutTotal, OuterOverheadBytesInTotal, OuterOverheadBytesOutTotal,
	OuterAuthFailuresTotal,
	RuntimeCPUPercent, RuntimeRSSBytes, RuntimeThermalState,
	TelemetrySequence, TelemetryRecordDropsTotal, TelemetryLeaseExpiredTotal, TelemetryPendingRecords, TelemetrySnapshotCoalescedTotal,
}

var TunnelMetrics = []Metric{
	KCPWaitSnd, KCPOutSegmentsTotal, KCPRetransSegmentsTotal, KCPOutBytesTotal, KCPRetransBytesTotal,
	KCPFastRetransEstimateSegmentsTotal, KCPFastRetransEstimateBytesTotal, KCPRTORetransEstimateSegmentsTotal, KCPRTORetransEstimateBytesTotal,
	KCPRTTMS, KCPRTOMS, KCPRTTVarMS, KCPRTTSamplesTotal,
	KCPAckSegmentsTotal, KCPAckProgressSegmentsTotal, KCPInflightSegments, KCPSendBlockedSecondsTotal,
	KCPMTUBytes, KCPSendWindowSegments, KCPReceiveWindowSegments, KCPMaxPendingSegments,
	KCPUpdateIntervalMS, KCPFastResend, KCPCongestionControl,
	RelayTCPActive, RelayUDPActive, RelayBytesTotal, RelayQueueDepth, RelayQueueDropsTotal, RelayConnectFailureTotal,
	WorkerActive, WorkerSendQueueDepth, WorkerSendQueueDropsTotal, WorkerNoAvailableDropsTotal, WorkerLivenessExpiredTotal,
	WorkerSendQueueCapacity, WorkerHeartbeatIntervalMS, WorkerLivenessTimeoutMS,
	LaneCount, LaneFlowCount,
	WorkerOutputQueueDelayMS, WorkerOutputQueueLateTotal, WorkerWriteLatencyMS,
	LaneAdmissionRateBPS, LaneAdmissionWindowSegments,
	KCPOutputQueueDepth, KCPOutputQueueCapacity, KCPUpdateBackpressureTotal, KCPMutexBlockedSecondsTotal,
	FlowReorderAbortTotal, OuterRTPPayloadType,
	LaneGeneration, LaneState, LanePacingRateBPS, LaneDeliveredRateBPS, LaneMinRTTMS, LaneInflightLimitSegments,
	LaneTokenStarvationTotal, LaneApplicationLimited, LaneRecoveryDeferredTotal, LaneAckAgeSeconds, LaneResetRequestTotal, LaneResetRetryTotal, LaneResetAckTotal,
	LaneResetCommitTotal, LaneResetDurationMS, LaneStaleGenerationDropsTotal, LaneProbeResult,
	AggregateProgressAgeSeconds, QuarantinedLanes, SessionReplacementTotal,
}

type Accumulator struct {
	values [metricCount]atomic.Uint64
	active atomic.Bool
	parent atomic.Pointer[Accumulator]

	eventsMu sync.Mutex
	events   []Event

	networkMu      sync.Mutex
	networkStreams map[uint32]*sequenceState
}

func (a *Accumulator) SetCounterParent(parent *Accumulator) {
	if a != nil && parent != a {
		a.parent.Store(parent)
	}
}

func NewAccumulator() *Accumulator {
	return &Accumulator{networkStreams: make(map[uint32]*sequenceState)}
}

func Name(metric Metric) string {
	if metric >= metricCount {
		return ""
	}
	return metricDescriptors[metric].name
}

func IsCounter(metric Metric) bool {
	return metric < metricCount && metricDescriptors[metric].counter
}

func KnownMetric(name string) bool {
	for _, descriptor := range metricDescriptors {
		if descriptor.name == name {
			return true
		}
	}
	return false
}

func (a *Accumulator) Add(metric Metric, delta uint64) {
	if a == nil || metric >= metricCount || !IsCounter(metric) || delta == 0 {
		return
	}
	a.values[metric].Add(delta)
	if parent := a.parent.Load(); parent != nil {
		parent.Add(metric, delta)
	}
}

func (a *Accumulator) AddHot(metric Metric, delta uint64) {
	if a == nil || !a.CollectionActive() {
		return
	}
	a.Add(metric, delta)
}

func (a *Accumulator) Set(metric Metric, value float64) {
	if a == nil || metric >= metricCount || IsCounter(metric) || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	if value < 0 {
		value = 0
	}
	a.values[metric].Store(math.Float64bits(value))
}

func (a *Accumulator) AddGauge(metric Metric, delta float64) {
	if a == nil || metric >= metricCount || IsCounter(metric) || delta == 0 {
		return
	}
	value := &a.values[metric]
	for {
		oldBits := value.Load()
		updated := math.Float64frombits(oldBits) + delta
		if updated < 0 {
			updated = 0
		}
		if value.CompareAndSwap(oldBits, math.Float64bits(updated)) {
			return
		}
	}
}

func (a *Accumulator) AddHotGauge(metric Metric, delta float64) {
	if a == nil || !a.CollectionActive() {
		return
	}
	a.AddGauge(metric, delta)
}

func (a *Accumulator) AddMonotonic(metric Metric, delta float64) {
	if a == nil || metric >= metricCount || IsCounter(metric) || delta <= 0 || math.IsNaN(delta) || math.IsInf(delta, 0) {
		return
	}
	a.AddGauge(metric, delta)
	if parent := a.parent.Load(); parent != nil {
		parent.AddMonotonic(metric, delta)
	}
}

func (a *Accumulator) AddHotMonotonic(metric Metric, delta float64) {
	if a == nil || !a.CollectionActive() {
		return
	}
	a.AddMonotonic(metric, delta)
}

func (a *Accumulator) Value(metric Metric) float64 {
	if a == nil || metric >= metricCount {
		return 0
	}
	value := a.values[metric].Load()
	if IsCounter(metric) {
		return float64(value)
	}
	return math.Float64frombits(value)
}

func (a *Accumulator) Snapshot(metrics []Metric) map[string]any {
	result := make(map[string]any, len(metrics))
	for _, metric := range metrics {
		if IsCounter(metric) {
			result[Name(metric)] = a.values[metric].Load()
		} else {
			result[Name(metric)] = a.Value(metric)
		}
	}
	return result
}

func (a *Accumulator) SetCollectionActive(active bool) {
	if a == nil {
		return
	}
	wasActive := a.active.Swap(active)
	if active && !wasActive {
		a.networkMu.Lock()
		a.networkStreams = make(map[uint32]*sequenceState)
		a.networkMu.Unlock()
		a.Set(NetworkLossRatio, 0)
		a.Set(NetworkJitterMS, 0)
	}
}

func (a *Accumulator) CollectionActive() bool {
	return a != nil && a.active.Load()
}

type accumulatorContextKey struct{}

func ContextWithAccumulator(ctx context.Context, accumulator *Accumulator) context.Context {
	if accumulator == nil {
		return ctx
	}
	return context.WithValue(ctx, accumulatorContextKey{}, accumulator)
}

func FromContext(ctx context.Context) *Accumulator {
	if ctx == nil {
		return nil
	}
	accumulator, _ := ctx.Value(accumulatorContextKey{}).(*Accumulator)
	return accumulator
}

func LatencyMS(start time.Time) float64 {
	return float64(time.Since(start)) / float64(time.Millisecond)
}
