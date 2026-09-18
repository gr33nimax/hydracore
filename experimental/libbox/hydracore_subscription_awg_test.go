//go:build with_wireguard && with_gvisor

package libbox

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A HydraBox profile can be a WireGuard endpoint rather than an outbound, and
// the AmneziaWG 3.1 fields must survive subscription validation untouched.
//
// The native validation of a WireGuard endpoint needs a tun stack, which is why
// this test lives behind the gVisor tag together with the endpoint gate.
func TestHydraCoreSubscriptionCarriesAWGEndpoint(t *testing.T) {
	content := strings.Replace(validHydraSubscriptionJSON(),
		`"requested_permissions":["network.outbound"]`,
		`"requested_permissions":["network.endpoint.wireguard"]`,
		1,
	)
	content = strings.Replace(content,
		`"document":{"outbounds":[{"type":"socks","tag":"proxy-main","server":"origin.example.invalid","server_port":1080,"password":"do-not-expose"}]}`,
		`"document":{"endpoints":[{"type":"wireguard","tag":"proxy-main","address":["10.0.0.2/32"],`+
			`"private_key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",`+
			`"peers":[{"address":"origin.example.invalid","port":51820,`+
			`"public_key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","allowed_ips":["0.0.0.0/0"]}],`+
			`"amnezia":{"jc":120,"s1":40,"s2":120,"s3":12,"s4":12,"h1":"1","h2":"2","h3":"3","h4":"4",`+
			`"i1":"<b 0xc70000000108>","header_protection_key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",`+
			`"content_padding_addition":"50-100","rekey_after_time":"100-140",`+
			`"random_trailers":true,"disable_cookies":false}}]}`,
		1,
	)
	content = strings.Replace(content,
		`"entrypoint":{"section":"outbounds","tag":"proxy-main"}`,
		`"entrypoint":{"section":"endpoints","tag":"proxy-main"}`,
		1,
	)

	validation := decodeValidationResult(t, HydraCoreValidateSubscription(content))
	require.True(t, validation.Valid, validation.Diagnostics)

	inspection := HydraCoreInspectSubscription(content)
	require.Contains(t, inspection, "endpoints:wireguard")
}
