package hydracore

import (
	"encoding/json"

	C "github.com/sagernet/sing-box/constant"
)

const APIVersion = 2

type Identity struct {
	CoreID      string `json:"core_id"`
	CoreName    string `json:"core_name"`
	CoreVersion string `json:"core_version"`
	Role        string `json:"role"`
}

type FeatureSet struct {
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
	CallVKParasite               bool `json:"call_vk_parasite"`
	CallVKParasiteClient         bool `json:"call_vk_parasite_client"`
	CallVKParasiteServer         bool `json:"call_vk_parasite_server"`
	CallVKTelemetry              bool `json:"call_vk_telemetry"`
	CallVKEightLaneKCP           bool `json:"call_vk_eight_lane_kcp"`
	CallVKFourLaneKCP            bool `json:"call_vk_four_lane_kcp"`
	CallVKPreKCPAdmission        bool `json:"call_vk_pre_kcp_admission"`
	CallVKRelayFlowControl       bool `json:"call_vk_relay_flow_control"`
	AmneziaVersion               int  `json:"amnezia_version"`
}

type ProtocolSet struct {
	Inbounds            []string          `json:"inbounds"`
	Outbounds           []string          `json:"outbounds"`
	Endpoints           []string          `json:"endpoints"`
	CallPlatforms       []string          `json:"call_platforms"`
	CallModes           []string          `json:"call_modes"`
	CallVKParasiteWire  WireCompatibility `json:"call_vk_parasite_wire"`
}

type WireCompatibility struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type RemotePolicy struct {
	Version             int      `json:"version"`
	SafeTopLevelFields  []string `json:"safe_top_level_fields"`
	SafeInboundTypes    []string `json:"safe_inbound_types"`
	SafeOutboundTypes   []string `json:"safe_outbound_types"`
	SafeEndpointTypes   []string `json:"safe_endpoint_types"`
	SafeDNSServerTypes  []string `json:"safe_dns_server_types"`
	SafeProviderTypes   []string `json:"safe_provider_types"`
	ReservedTagPrefixes []string `json:"reserved_tag_prefixes"`
}

type RuntimeContract struct {
	Version                    int `json:"version"`
	SnapshotSchemaVersion      int `json:"snapshot_schema_version"`
	MinimumEventIntervalMillis int `json:"minimum_event_interval_millis"`
	MaximumEventIntervalMillis int `json:"maximum_event_interval_millis"`
	RetainedURLTestSessions    int `json:"retained_url_test_sessions"`
}

type CapabilitySet struct {
	APIVersion             int             `json:"api_version"`
	Identity               Identity        `json:"identity"`
	Features               FeatureSet      `json:"features"`
	Protocols              ProtocolSet     `json:"protocols"`
	TUNStacks              []string        `json:"tun_stacks"`
	XHTTPModes             []string        `json:"xhttp_modes"`
	VLESSEncryptionModes   []string        `json:"vless_encryption_modes"`
	ValidationProfiles     []string        `json:"validation_profiles"`
	SubscriptionContracts  []int           `json:"subscription_contracts"`
	SubscriptionMediaTypes []string        `json:"subscription_media_types"`
	RemotePolicy           RemotePolicy    `json:"remote_policy"`
	Runtime                RuntimeContract `json:"runtime"`
}

func Capabilities() CapabilitySet {
	safeInboundTypes := []string{}
	safeOutboundTypes := []string{
		"socks", "http", "vmess", "trojan", "naive", "shadowtls", "vless",
		"mieru", "anytls", "trusttunnel", "hysteria", "hysteria2", "tuic",
		"sudoku", "snell",
	}
	if callServerEnabled {
		safeInboundTypes = append(safeInboundTypes, "call")
	}
	if callClientEnabled {
		safeOutboundTypes = append(safeOutboundTypes, "call")
	}
	return CapabilitySet{
		APIVersion: APIVersion,
		Identity: Identity{
			CoreID:      "io.hydrabox.hydracore",
			CoreName:    "HydraCore",
			CoreVersion: C.Version,
			Role:        distributionRole,
		},
		Features: FeatureSet{
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
			Call:                         callEnabled,
			CallVKParasite:               callEnabled,
			CallVKParasiteClient:         callClientEnabled,
			CallVKParasiteServer:         callServerEnabled,
			CallVKTelemetry:              callEnabled,
			CallVKEightLaneKCP:           false,
			CallVKFourLaneKCP:            callEnabled,
			CallVKPreKCPAdmission:        callEnabled,
			CallVKRelayFlowControl:       callEnabled,
			AmneziaVersion:               3,
		},
		Protocols: ProtocolSet{
			Inbounds:            append([]string(nil), safeInboundTypes...),
			Outbounds:           append([]string(nil), safeOutboundTypes...),
			Endpoints:           []string{"wireguard"},
			CallPlatforms:       append([]string(nil), callPlatforms...),
			CallModes:           append([]string(nil), callModes...),
			CallVKParasiteWire:  WireCompatibility{Min: callWireMin, Max: callWireMax},
		},
		TUNStacks:              []string{"system", "gvisor", "mixed"},
		XHTTPModes:             []string{"packet-up", "stream-up", "stream-one"},
		VLESSEncryptionModes:   []string{"1rtt", "0rtt", "native", "xorpub", "random", "x25519", "mlkem768"},
		ValidationProfiles:     []string{"local", "remote_v2"},
		SubscriptionContracts:  []int{2},
		SubscriptionMediaTypes: []string{"application/vnd.hydra.subscription+json", "application/jose+json"},
		RemotePolicy: RemotePolicy{
			Version:             2,
			SafeTopLevelFields:  []string{"$schema", "inbounds", "outbounds", "endpoints"},
			SafeInboundTypes:    safeInboundTypes,
			SafeOutboundTypes:   safeOutboundTypes,
			SafeEndpointTypes:   []string{"wireguard"},
			SafeDNSServerTypes:  []string{},
			SafeProviderTypes:   []string{},
			ReservedTagPrefixes: []string{"__hydra."},
		},
		Runtime: RuntimeContract{
			Version:                    1,
			SnapshotSchemaVersion:      1,
			MinimumEventIntervalMillis: 250,
			MaximumEventIntervalMillis: 30_000,
			RetainedURLTestSessions:    64,
		},
	}
}

func SupportsCallMode(mode string) bool {
	if mode != "vk_parasite" {
		mode = "p2p"
	}
	for _, supportedMode := range callModes {
		if supportedMode == mode {
			return true
		}
	}
	return false
}

func CapabilitiesJSON() string {
	content, err := json.Marshal(Capabilities())
	if err != nil {
		return ""
	}
	return string(content)
}
