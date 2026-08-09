//go:build with_call

package libbox

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHydraSubscriptionV2AcceptsCallInboundAndOutbound(t *testing.T) {
	content := strings.Replace(validHydraSubscriptionJSON(),
		`"features":["rmux"]`,
		`"features":["rmux","call"]`,
		1,
	)
	content = strings.Replace(content,
		`"requested_permissions":["network.outbound"],
        "document":{"outbounds":[{"type":"socks","tag":"proxy-main","server":"origin.example.invalid","server_port":1080,"password":"do-not-expose"}]}`,
		`"requested_permissions":["network.outbound","network.inbound.call"],
        "document":{
          "inbounds":[{
            "type":"call","tag":"call-in","platform":"dion","mode":"creator",
            "read_buffer":32768,"max_buffered_amount":1048576,"memory_limit":8388608,
            "join_link":"https://call.example.invalid/room","cookies":[{"name":"session","value":"secret-cookie"}],
            "email":"user@example.invalid","password":"secret-password"
          }],
          "outbounds":[{
            "type":"call","tag":"proxy-main","platform":"dion","mode":"joiner",
            "read_buffer":32768,"max_buffered_amount":1048576,"memory_limit":8388608,
            "join_link":"https://call.example.invalid/room","cookies":[{"name":"session","value":"secret-cookie"}]
          }]
        }`,
		1,
	)
	result := decodeValidationResult(t, HydraCoreValidateSubscription(content))
	require.True(t, result.Valid, result.Diagnostics)
	inspection := HydraCoreInspectSubscription(content)
	require.Contains(t, inspection, "inbounds:call")
	require.Contains(t, inspection, "outbounds:call")
	require.NotContains(t, inspection, "secret-cookie")
	require.NotContains(t, inspection, "secret-password")
	require.NotContains(t, inspection, "call.example.invalid")
}

func TestHydraSubscriptionV2AcceptsVKMultiUserCall(t *testing.T) {
	content := strings.Replace(validHydraSubscriptionJSON(),
		`"features":["rmux"]`,
		`"features":["rmux","call"]`,
		1,
	)
	content = strings.Replace(content,
		`"requested_permissions":["network.outbound"],
        "document":{"outbounds":[{"type":"socks","tag":"proxy-main","server":"origin.example.invalid","server_port":1080,"password":"do-not-expose"}]}`,
		`"requested_permissions":["network.outbound","network.inbound.call"],
        "document":{
          "inbounds":[{
            "type":"call","tag":"call-vk-server","platform":"vk","mode":"multi_user",
            "listen":"0.0.0.0","listen_port":2443,"obfs_password":"shared-obfs-secret",
            "users":[{"name":"alice","password":"user-secret","max_sessions":1}],
            "max_sessions":32,"max_workers_per_session":4,"max_pending_handshakes":128,
            "handshake_timeout":"15s","session_idle_timeout":"5m"
          }],
          "outbounds":[{
            "type":"call","tag":"proxy-main","platform":"vk","mode":"multi_user",
            "server":"vpn.example.invalid","server_port":2443,
            "join_links":["https://vk.com/call/join/room-a","https://vk.com/call/join/room-b"],
            "user":"alice","password":"user-secret","obfs_password":"shared-obfs-secret",
            "workers":2,"worker_connect_timeout":"30s"
          }]
        }`,
		1,
	)
	result := decodeValidationResult(t, HydraCoreValidateSubscription(content))
	require.True(t, result.Valid, result.Diagnostics)
	inspection := HydraCoreInspectSubscription(content)
	require.Contains(t, inspection, "inbounds:call")
	require.Contains(t, inspection, "outbounds:call")
	require.NotContains(t, inspection, "shared-obfs-secret")
	require.NotContains(t, inspection, "user-secret")
	require.NotContains(t, inspection, "room-a")
}
