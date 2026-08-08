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
