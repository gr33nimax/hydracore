package trafficontrol

import (
	"encoding/json"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func TestTrackerMetadataMarshalJSONIncludesAuthenticatedUser(t *testing.T) {
	t.Parallel()

	content, err := (TrackerMetadata{
		Metadata: adapter.InboundContext{
			Inbound:     "calls-vk-in",
			InboundType: "call",
			Network:     N.NetworkUDP,
			Source: M.Socksaddr{
				Addr: netip.MustParseAddr("192.0.2.10"),
				Port: 12345,
			},
			Destination: M.Socksaddr{
				Fqdn: "example.invalid",
				Port: 443,
			},
			User: "calls-user@example.invalid",
		},
		Upload:   new(atomic.Int64),
		Download: new(atomic.Int64),
	}).MarshalJSON()
	require.NoError(t, err)

	var payload struct {
		Metadata struct {
			User string `json:"user"`
		} `json:"metadata"`
	}
	require.NoError(t, json.Unmarshal(content, &payload))
	require.Equal(t, "calls-user@example.invalid", payload.Metadata.User)
}
