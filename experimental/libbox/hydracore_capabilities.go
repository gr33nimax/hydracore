package libbox

import (
	"encoding/json"

	C "github.com/sagernet/sing-box/constant"
)

const hydraCoreAPIVersion = 2

type hydraCoreIdentity struct {
	CoreID      string `json:"core_id"`
	CoreName    string `json:"core_name"`
	CoreVersion string `json:"core_version"`
}

type hydraCoreFeatureSet struct {
	TargetedURLTest              bool `json:"targeted_url_test"`
	PreconnectURLTest            bool `json:"preconnect_url_test"`
	GroupURLTestSessions         bool `json:"group_url_test_sessions"`
	StructuredProbeErrors        bool `json:"structured_probe_errors"`
	OutboundExternalInfo         bool `json:"outbound_external_info"`
	OutboundExternalInfoFallback bool `json:"outbound_external_info_fallback"`
	ConfigValidation             bool `json:"config_validation"`
	RuntimeSnapshot              bool `json:"runtime_snapshot"`
	RuntimeEvents                bool `json:"runtime_events"`
	ManagedURLTestSessions       bool `json:"managed_url_test_sessions"`
	SubscriptionJWE              bool `json:"subscription_jwe"`
	XHTTP                        bool `json:"xhttp"`
	VLESSEncryption              bool `json:"vless_encryption"`
	Rmux                         bool `json:"rmux"`
	Call                         bool `json:"call"`
	AmneziaVersion               int  `json:"amnezia_version"`
}

type hydraCoreProtocolSet struct {
	Inbounds      []string `json:"inbounds"`
	Outbounds     []string `json:"outbounds"`
	Endpoints     []string `json:"endpoints"`
	CallPlatforms []string `json:"call_platforms"`
}

type hydraCoreRemotePolicy struct {
	Version             int      `json:"version"`
	SafeTopLevelFields  []string `json:"safe_top_level_fields"`
	SafeInboundTypes    []string `json:"safe_inbound_types"`
	SafeOutboundTypes   []string `json:"safe_outbound_types"`
	SafeEndpointTypes   []string `json:"safe_endpoint_types"`
	SafeDNSServerTypes  []string `json:"safe_dns_server_types"`
	SafeProviderTypes   []string `json:"safe_provider_types"`
	ReservedTagPrefixes []string `json:"reserved_tag_prefixes"`
}

type hydraCoreRuntimeContract struct {
	Version                    int `json:"version"`
	SnapshotSchemaVersion      int `json:"snapshot_schema_version"`
	MinimumEventIntervalMillis int `json:"minimum_event_interval_millis"`
	MaximumEventIntervalMillis int `json:"maximum_event_interval_millis"`
	RetainedURLTestSessions    int `json:"retained_url_test_sessions"`
}

type hydraCoreCapabilitySet struct {
	APIVersion             int                      `json:"api_version"`
	Identity               hydraCoreIdentity        `json:"identity"`
	Features               hydraCoreFeatureSet      `json:"features"`
	Protocols              hydraCoreProtocolSet     `json:"protocols"`
	TUNStacks              []string                 `json:"tun_stacks"`
	XHTTPModes             []string                 `json:"xhttp_modes"`
	VLESSEncryptionModes   []string                 `json:"vless_encryption_modes"`
	ValidationProfiles     []string                 `json:"validation_profiles"`
	SubscriptionContracts  []int                    `json:"subscription_contracts"`
	SubscriptionMediaTypes []string                 `json:"subscription_media_types"`
	RemotePolicy           hydraCoreRemotePolicy    `json:"remote_policy"`
	Runtime                hydraCoreRuntimeContract `json:"runtime"`
}

func HydraCoreCapabilities() string {
	safeInboundTypes := []string{}
	safeOutboundTypes := []string{
		"socks", "http", "vmess", "trojan", "naive", "shadowtls", "vless",
		"mieru", "anytls", "trusttunnel", "hysteria", "hysteria2", "tuic",
		"sudoku", "snell",
	}
	callPlatforms := []string{}
	if hydraCoreCallEnabled {
		safeInboundTypes = append(safeInboundTypes, "call")
		safeOutboundTypes = append(safeOutboundTypes, "call")
		callPlatforms = []string{"dion", "telemost", "vk", "wbstream"}
	}
	capabilities := hydraCoreCapabilitySet{
		APIVersion: hydraCoreAPIVersion,
		Identity: hydraCoreIdentity{
			CoreID:      "io.hydrabox.hydracore",
			CoreName:    "HydraCore",
			CoreVersion: C.Version,
		},
		Features: hydraCoreFeatureSet{
			TargetedURLTest:              true,
			PreconnectURLTest:            true,
			GroupURLTestSessions:         true,
			StructuredProbeErrors:        true,
			OutboundExternalInfo:         true,
			OutboundExternalInfoFallback: true,
			ConfigValidation:             true,
			RuntimeSnapshot:              true,
			RuntimeEvents:                true,
			ManagedURLTestSessions:       true,
			SubscriptionJWE:              true,
			XHTTP:                        true,
			VLESSEncryption:              true,
			Rmux:                         true,
			Call:                         hydraCoreCallEnabled,
			AmneziaVersion:               3,
		},
		Protocols: hydraCoreProtocolSet{
			Inbounds:      append([]string(nil), safeInboundTypes...),
			Outbounds:     append([]string(nil), safeOutboundTypes...),
			Endpoints:     []string{"wireguard"},
			CallPlatforms: callPlatforms,
		},
		TUNStacks:              []string{"system", "gvisor", "mixed"},
		XHTTPModes:             []string{"packet-up", "stream-up", "stream-one"},
		VLESSEncryptionModes:   []string{"1rtt", "0rtt", "native", "xorpub", "random", "x25519", "mlkem768"},
		ValidationProfiles:     []string{"local", "remote_v2"},
		SubscriptionContracts:  []int{2},
		SubscriptionMediaTypes: []string{"application/vnd.hydra.subscription+json", "application/jose+json"},
		RemotePolicy: hydraCoreRemotePolicy{
			Version:             2,
			SafeTopLevelFields:  []string{"$schema", "inbounds", "outbounds", "endpoints"},
			SafeInboundTypes:    safeInboundTypes,
			SafeOutboundTypes:   safeOutboundTypes,
			SafeEndpointTypes:   []string{"wireguard"},
			SafeDNSServerTypes:  []string{},
			SafeProviderTypes:   []string{},
			ReservedTagPrefixes: []string{"__hydra."},
		},
		Runtime: hydraCoreRuntimeContract{
			Version:                    1,
			SnapshotSchemaVersion:      1,
			MinimumEventIntervalMillis: 250,
			MaximumEventIntervalMillis: 30_000,
			RetainedURLTestSessions:    64,
		},
	}
	content, err := json.Marshal(capabilities)
	if err != nil {
		return ""
	}
	return string(content)
}
