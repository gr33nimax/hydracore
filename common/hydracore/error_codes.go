package hydracore

const (
	ErrorCodeConfigInvalidPlan           = "config.invalid_plan"
	ErrorCodeConfigDigestMismatch        = "config.digest_mismatch"
	ErrorCodeConfigQuarantined           = "config.quarantined"
	ErrorCodeConfigStale                 = "config.stale"
	ErrorCodeRuntimeCancelled            = "runtime.cancelled"
	ErrorCodeRuntimeSuperseded           = "runtime.superseded"
	ErrorCodeRuntimeStartDeadline        = "runtime.start.deadline"
	ErrorCodeRuntimeStopUnconfirmed      = "runtime.stop.unconfirmed"
	ErrorCodeRuntimeCoreDied             = "runtime.core_died"
	ErrorCodeRuntimeIPCLost              = "runtime.ipc.lost"
	ErrorCodeRuntimeIPCBindFailed        = "runtime.ipc.bind_failed"
	ErrorCodeNetworkNoInterface          = "network.no_interface"
	ErrorCodeNetworkLost                 = "network.lost"
	ErrorCodeNetworkGenerationStale      = "network.generation_stale"
	ErrorCodeDNSBootstrapTimeout         = "dns.bootstrap.timeout"
	ErrorCodeDNSUpstreamTimeout          = "dns.upstream.timeout"
	ErrorCodeDNSUpstreamRefused          = "dns.upstream.refused"
	ErrorCodeDNSNoAnswer                 = "dns.no_answer"
	ErrorCodeVKCaptchaRequired           = "vk.captcha.required"
	ErrorCodeVKCaptchaTimeout            = "vk.captcha.timeout"
	ErrorCodeVKCaptchaCancelled          = "vk.captcha.cancelled"
	ErrorCodeVKCredentialsFlood          = "vk.credentials.flood"
	ErrorCodeVKCredentialsRejected       = "vk.credentials.rejected"
	ErrorCodeVKAuthTerminal              = "vk.auth.terminal"
	ErrorCodeTURNAllocateFailed          = "turn.allocate_failed"
	ErrorCodeTURNNoCandidate             = "turn.no_candidate"
	ErrorCodeDTLSHandshakeFailed         = "dtls.handshake_failed"
	ErrorCodeQUICDialFailed              = "quic.dial_failed"
	ErrorCodeQUICNoPaths                 = "quic.no_paths"
	ErrorCodeTransportLanesLost          = "transport.lanes_lost"
	ErrorCodeTransportRecoveryTimeout    = "transport.recovery.timeout"
	ErrorCodeProbeInvalidPlan            = "probe.invalid_plan"
	ErrorCodeProbeRequiresStoppedRuntime = "probe.requires_stopped_runtime"
	ErrorCodeProbeTimeout                = "probe.timeout"
	ErrorCodeProbeCancelled              = "probe.cancelled"
)

func AllErrorCodes() []string {
	return []string{
		ErrorCodeConfigInvalidPlan, ErrorCodeConfigDigestMismatch, ErrorCodeConfigQuarantined, ErrorCodeConfigStale,
		ErrorCodeRuntimeCancelled, ErrorCodeRuntimeSuperseded, ErrorCodeRuntimeStartDeadline, ErrorCodeRuntimeStopUnconfirmed,
		ErrorCodeRuntimeCoreDied, ErrorCodeRuntimeIPCLost, ErrorCodeRuntimeIPCBindFailed,
		ErrorCodeNetworkNoInterface, ErrorCodeNetworkLost, ErrorCodeNetworkGenerationStale,
		ErrorCodeDNSBootstrapTimeout, ErrorCodeDNSUpstreamTimeout, ErrorCodeDNSUpstreamRefused, ErrorCodeDNSNoAnswer,
		ErrorCodeVKCaptchaRequired, ErrorCodeVKCaptchaTimeout, ErrorCodeVKCaptchaCancelled, ErrorCodeVKCredentialsFlood,
		ErrorCodeVKCredentialsRejected, ErrorCodeVKAuthTerminal, ErrorCodeTURNAllocateFailed, ErrorCodeTURNNoCandidate,
		ErrorCodeDTLSHandshakeFailed, ErrorCodeQUICDialFailed, ErrorCodeQUICNoPaths, ErrorCodeTransportLanesLost,
		ErrorCodeTransportRecoveryTimeout, ErrorCodeProbeInvalidPlan, ErrorCodeProbeRequiresStoppedRuntime,
		ErrorCodeProbeTimeout, ErrorCodeProbeCancelled,
	}
}
