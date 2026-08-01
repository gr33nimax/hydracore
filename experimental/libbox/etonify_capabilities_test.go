package libbox

import (
	"encoding/json"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/stretchr/testify/require"
)

func TestHydraCoreCapabilities(t *testing.T) {
	t.Parallel()

	content := HydraCoreCapabilities()
	var capabilities hydraCoreCapabilitySet
	require.NoError(t, json.Unmarshal([]byte(content), &capabilities))
	require.Equal(t, hydraCoreAPIVersion, capabilities.APIVersion)
	require.Equal(t, "io.hydrabox.hydracore", capabilities.CoreID)
	require.Equal(t, "HydraCore", capabilities.CoreName)
	require.Equal(t, C.Version, capabilities.CoreVersion)
	require.Equal(t, "etonify-core", capabilities.UpstreamProject)
	require.True(t, capabilities.SupportsTargetedURLTest)
	require.True(t, capabilities.SupportsPreconnectURLTest)
	require.True(t, capabilities.SupportsGroupURLTestSessions)
	require.True(t, capabilities.SupportsStructuredProbeErrors)
	require.True(t, capabilities.SupportsOutboundExternalInfo)
	require.True(t, capabilities.SupportsOutboundExternalInfoFallback)
	require.False(t, capabilities.SupportsMixedRoutingOutbound)
	require.True(t, capabilities.SupportsURLTestTimeout)
	require.True(t, capabilities.SupportsURLTestConcurrency)
	require.True(t, capabilities.SupportsURLTestDeadline)
	require.True(t, capabilities.SupportsURLTestForce)
	require.True(t, capabilities.SupportsURLTestFailover)
	require.False(t, capabilities.SupportsURLTestUnavailableCheckInterval)
	require.False(t, capabilities.SupportsURLTestMethod)
	require.False(t, capabilities.SupportsURLTestInterruptDelayThreshold)
	require.Equal(t, "group_events", capabilities.URLTestCompletionModel)
	require.True(t, capabilities.SupportsConfigCheck)
	require.True(t, capabilities.SupportsCloseConnections)
	require.True(t, capabilities.SupportsRealitySpiderX)
	require.True(t, capabilities.SupportsXHTTP)
	require.True(t, capabilities.SupportsSplitHTTPAlias)
	require.False(t, capabilities.XHTTPClientOnly)
	require.Equal(t, "extended_mobile_v1", capabilities.XHTTPProfile)
	require.Equal(t, []string{"packet-up", "stream-up", "stream-one"}, capabilities.XHTTPModes)
	require.Equal(t, 16, capabilities.XHTTPMaxPoolConnections)
	require.Equal(t, 256*1024, capabilities.XHTTPMaxPacketUploadBytes)
	require.True(t, capabilities.SupportsXHTTPCloseAll)
	require.True(t, capabilities.SupportsVLESSEncryption)
	require.False(t, capabilities.VLESSEncryptionClientOnly)
	require.Equal(t, []string{"1rtt", "0rtt", "native", "xorpub", "random", "x25519", "mlkem768"}, capabilities.VLESSEncryptionModes)
	require.Equal(t, 8, capabilities.VLESSEncryptionMaxRelays)
	require.Equal(t, 12_000, capabilities.VLESSEncryptionHandshakeTimeoutMS)
	require.Equal(t, []string{"system", "gvisor", "mixed"}, capabilities.TUNStacks)
	require.Equal(t, wdttIncluded, capabilities.SupportsWDTT)
	require.Equal(t, 36, capabilities.WDTTMaxWorkers)
	require.Equal(t, 4, capabilities.WDTTMaxHashes)
	require.Equal(t, []string{"anonymous"}, capabilities.WDTTAuthModes)
	require.Equal(t, []string{"audio", "video"}, capabilities.WDTTObfsModes)
	require.Equal(t, 2, capabilities.RemotePolicyVersion)
	require.Equal(t, []string{"$schema", "outbounds", "endpoints"}, capabilities.RemoteSafeTopLevelFields)
	require.Equal(t, []string{
		"socks", "http", "vmess", "trojan", "naive", "shadowtls", "vless", "mieru",
		"anytls", "trusttunnel", "hysteria", "hysteria2", "tuic", "sudoku", "snell",
	}, capabilities.RemoteSafeOutboundTypes)
	require.Equal(t, remoteSafeEndpointTypes(), capabilities.RemoteSafeEndpointTypes)
	require.Empty(t, capabilities.RemoteSafeDNSServerTypes)
	require.Empty(t, capabilities.RemoteSafeProviderTypes)

	for _, unsafeTopLevelField := range []string{"log", "dns", "ntp", "route", "providers", "inbounds", "services", "experimental"} {
		require.NotContains(t, capabilities.RemoteSafeTopLevelFields, unsafeTopLevelField)
	}
	for _, unsafeOutboundType := range []string{
		"direct", "block", "selector", "urltest", "fallback", "shadowsocks", "ssh", "masque", "openvpn",
		"tor", "parser", "bond", "failover", "bandwidth-limiter", "connection-limiter", "traffic-limiter", "rate-limiter",
	} {
		require.NotContains(t, capabilities.RemoteSafeOutboundTypes, unsafeOutboundType)
	}
	for _, unsafeEndpointType := range []string{"warp", "tailscale", "vpn-client", "vpn-server"} {
		require.NotContains(t, capabilities.RemoteSafeEndpointTypes, unsafeEndpointType)
	}
	require.Equal(t, content, EtonifyCapabilities())
}
