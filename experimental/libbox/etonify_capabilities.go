package libbox

import (
	"encoding/json"
	"time"

	"github.com/gr33nimax/hydra-wdtt/pkg/access"
	"github.com/gr33nimax/hydra-wdtt/pkg/workers"
	C "github.com/sagernet/sing-box/constant"
)

const hydraCoreAPIVersion = 1

// Kept for source-compatible in-package tests and downstream patches.
const etonifyAPIVersion = hydraCoreAPIVersion

type hydraCoreCapabilitySet struct {
	APIVersion                              int      `json:"api_version"`
	CoreID                                  string   `json:"core_id"`
	CoreName                                string   `json:"core_name"`
	CoreVersion                             string   `json:"core_version"`
	UpstreamProject                         string   `json:"upstream_project"`
	SupportsTargetedURLTest                 bool     `json:"supports_targeted_url_test"`
	SupportsPreconnectURLTest               bool     `json:"supports_preconnect_url_test"`
	SupportsGroupURLTestSessions            bool     `json:"supports_group_url_test_sessions"`
	SupportsStructuredProbeErrors           bool     `json:"supports_structured_probe_errors"`
	SupportsOutboundExternalInfo            bool     `json:"supports_outbound_external_info"`
	SupportsOutboundExternalInfoFallback    bool     `json:"supports_outbound_external_info_fallback"`
	SupportsMixedRoutingOutbound            bool     `json:"supports_mixed_routing_outbound"`
	SupportsURLTestTimeout                  bool     `json:"supports_url_test_timeout"`
	SupportsURLTestConcurrency              bool     `json:"supports_url_test_concurrency"`
	SupportsURLTestDeadline                 bool     `json:"supports_url_test_deadline"`
	SupportsURLTestForce                    bool     `json:"supports_url_test_force"`
	SupportsURLTestFailover                 bool     `json:"supports_url_test_failover"`
	SupportsURLTestUnavailableCheckInterval bool     `json:"supports_url_test_unavailable_check_interval"`
	SupportsURLTestMethod                   bool     `json:"supports_url_test_method"`
	SupportsURLTestInterruptDelayThreshold  bool     `json:"supports_url_test_interrupt_delay_threshold"`
	URLTestCompletionModel                  string   `json:"url_test_completion_model"`
	SupportsConfigCheck                     bool     `json:"supports_config_check"`
	SupportsCloseConnections                bool     `json:"supports_close_connections"`
	SupportsRealitySpiderX                  bool     `json:"supports_reality_spider_x"`
	SupportsXHTTP                           bool     `json:"supports_xhttp"`
	SupportsSplitHTTPAlias                  bool     `json:"supports_splithttp_alias"`
	XHTTPClientOnly                         bool     `json:"xhttp_client_only"`
	XHTTPProfile                            string   `json:"xhttp_profile"`
	XHTTPModes                              []string `json:"xhttp_modes"`
	XHTTPMaxPoolConnections                 int      `json:"xhttp_max_pool_connections"`
	XHTTPMaxPacketUploadBytes               int      `json:"xhttp_max_packet_upload_bytes"`
	SupportsXHTTPCloseAll                   bool     `json:"supports_xhttp_close_all"`
	SupportsVLESSEncryption                 bool     `json:"supports_vless_encryption"`
	VLESSEncryptionClientOnly               bool     `json:"vless_encryption_client_only"`
	VLESSEncryptionModes                    []string `json:"vless_encryption_modes"`
	VLESSEncryptionMaxRelays                int      `json:"vless_encryption_max_relays"`
	VLESSEncryptionHandshakeTimeoutMS       int      `json:"vless_encryption_handshake_timeout_ms"`
	TUNStacks                               []string `json:"tun_stacks"`
	SupportsWDTT                            bool     `json:"supports_wdtt"`
	SupportsWDTTCredentialBridge            bool     `json:"supports_wdtt_credential_bridge"`
	SupportsWDTTHotRotation                 bool     `json:"supports_wdtt_hot_rotation"`
	WDTTMinWorkers                          int      `json:"wdtt_min_workers"`
	WDTTRecommendedWorkers                  int      `json:"wdtt_recommended_workers"`
	WDTTMaxWorkers                          int      `json:"wdtt_max_workers"`
	WDTTLeaseTTLSeconds                     int      `json:"wdtt_lease_ttl_seconds"`
	WDTTLeaseRefreshAfterSeconds            int      `json:"wdtt_lease_refresh_after_seconds"`
	WDTTMaxHashes                           int      `json:"wdtt_max_hashes"`
	WDTTAuthModes                           []string `json:"wdtt_auth_modes"`
	WDTTObfsModes                           []string `json:"wdtt_obfs_modes"`
	RemotePolicyVersion                     int      `json:"remote_policy_version"`
	RemoteSafeTopLevelFields                []string `json:"remote_safe_top_level_fields"`
	RemoteSafeOutboundTypes                 []string `json:"remote_safe_outbound_types"`
	RemoteSafeEndpointTypes                 []string `json:"remote_safe_endpoint_types"`
	RemoteSafeDNSServerTypes                []string `json:"remote_safe_dns_server_types"`
	RemoteSafeProviderTypes                 []string `json:"remote_safe_provider_types"`
}

// Retain the old internal name for source compatibility with downstream tests.
type etonifyCapabilitySet = hydraCoreCapabilitySet

// HydraCoreCapabilities returns the versioned mobile integration contract.
//
// The JSON representation allows newer cores to add optional capabilities
// without forcing older clients to bind newly introduced Go types. A client
// must treat a missing or malformed response as the legacy capability set.
func HydraCoreCapabilities() string {
	capabilities := hydraCoreCapabilitySet{
		APIVersion:                           hydraCoreAPIVersion,
		CoreID:                               "io.hydrabox.hydracore",
		CoreName:                             "HydraCore",
		CoreVersion:                          C.Version,
		UpstreamProject:                      "etonify-core",
		SupportsTargetedURLTest:              true,
		SupportsPreconnectURLTest:            true,
		SupportsGroupURLTestSessions:         true,
		SupportsStructuredProbeErrors:        true,
		SupportsOutboundExternalInfo:         true,
		SupportsOutboundExternalInfoFallback: true,
		SupportsURLTestTimeout:               true,
		SupportsURLTestConcurrency:           true,
		SupportsURLTestDeadline:              true,
		SupportsURLTestForce:                 true,
		SupportsURLTestFailover:              true,
		URLTestCompletionModel:               "group_events",
		SupportsConfigCheck:                  true,
		SupportsCloseConnections:             true,
		SupportsRealitySpiderX:               true,
		SupportsXHTTP:                        true,
		SupportsSplitHTTPAlias:               true,
		XHTTPClientOnly:                      false,
		XHTTPProfile:                         "extended_mobile_v1",
		XHTTPModes:                           []string{"packet-up", "stream-up", "stream-one"},
		XHTTPMaxPoolConnections:              16,
		XHTTPMaxPacketUploadBytes:            256 * 1024,
		SupportsXHTTPCloseAll:                true,
		SupportsVLESSEncryption:              true,
		VLESSEncryptionClientOnly:            false,
		VLESSEncryptionModes:                 []string{"1rtt", "0rtt", "native", "xorpub", "random", "x25519", "mlkem768"},
		VLESSEncryptionMaxRelays:             8,
		VLESSEncryptionHandshakeTimeoutMS:    12_000,
		TUNStacks:                            []string{"system", "gvisor", "mixed"},
		SupportsWDTT:                         wdttIncluded,
		SupportsWDTTCredentialBridge:         wdttIncluded,
		SupportsWDTTHotRotation:              wdttIncluded,
		WDTTMinWorkers:                       workers.Minimum,
		WDTTRecommendedWorkers:               workers.Recommended,
		WDTTMaxWorkers:                       workers.Maximum,
		WDTTLeaseTTLSeconds:                  int(access.SessionTTL / time.Second),
		WDTTLeaseRefreshAfterSeconds:         int(access.SessionRefreshAfter / time.Second),
		WDTTMaxHashes:                        4,
		WDTTAuthModes:                        []string{"auto", "anonymous", "account"},
		WDTTObfsModes:                        []string{"audio", "video"},
		// Remote policy v2 retains the v1 executable leaf set and adds only the
		// bounded WDTT endpoint when this build includes it. A type that
		// embeds another typed outbound/endpoint, fetches executable provider
		// content, or opens a reverse/local service is deliberately omitted
		// until HydraCore enforces the same policy recursively at creation time.
		RemotePolicyVersion:      2,
		RemoteSafeTopLevelFields: []string{"$schema", "outbounds", "endpoints"},
		RemoteSafeOutboundTypes:  []string{"socks", "http", "vmess", "trojan", "naive", "shadowtls", "vless", "mieru", "anytls", "trusttunnel", "hysteria", "hysteria2", "tuic", "sudoku", "snell"},
		RemoteSafeEndpointTypes:  remoteSafeEndpointTypes(),
		RemoteSafeDNSServerTypes: []string{},
		RemoteSafeProviderTypes:  []string{},
	}
	content, err := json.Marshal(capabilities)
	if err != nil {
		return ""
	}
	return string(content)
}

func remoteSafeEndpointTypes() []string {
	types := []string{"wireguard"}
	if wdttIncluded {
		types = append(types, "wdtt")
	}
	return types
}

// EtonifyCapabilities is the deprecated compatibility entry point used by
// already-generated Etonify libbox bindings. New HydraBox bindings should call
// HydraCoreCapabilities. Both return the exact same attributed contract.
func EtonifyCapabilities() string {
	return HydraCoreCapabilities()
}
