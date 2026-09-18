package libbox

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func decodeValidationResult(t *testing.T, content string) hydraCoreValidationResult {
	t.Helper()
	var result hydraCoreValidationResult
	require.NoError(t, json.Unmarshal([]byte(content), &result))
	return result
}

func TestHydraCoreValidateConfigProfiles(t *testing.T) {
	validLocal := decodeValidationResult(t, HydraCoreValidateConfig(`{"outbounds":[{"type":"direct","tag":"direct"}]}`, "local"))
	require.True(t, validLocal.Valid)

	invalidProfile := decodeValidationResult(t, HydraCoreValidateConfig(`{}`, "future"))
	require.False(t, invalidProfile.Valid)
	require.Equal(t, "unknown_profile", invalidProfile.Diagnostics[0].Code)
}

func TestHydraCoreValidateRemoteV2(t *testing.T) {
	valid := decodeValidationResult(t, HydraCoreValidateConfig(`{
      "outbounds": [
        {"type":"trojan","tag":"proxy","server":"example.invalid","server_port":443,"password":"secret","detour":"transport"},
        {"type":"shadowtls","tag":"transport","server":"example.invalid","server_port":443,"version":3,"password":"secret","tls":{"enabled":true,"server_name":"example.invalid"}}
      ]
    }`, "remote_v2"))
	require.True(t, valid.Valid, valid.Diagnostics)

	tests := []struct {
		name   string
		config string
		code   string
	}{
		{"unsafe root", `{"services":[]}`, "unsafe_top_level_field"},
		{"unsafe outbound", `{"outbounds":[{"type":"direct","tag":"x"}]}`, "unsafe_outbound_type"},
		{"missing reference", `{"outbounds":[{"type":"trojan","tag":"x","server":"example.invalid","server_port":443,"password":"secret","detour":"missing"}]}`, "missing_reference"},
		{"nested missing reference", `{"outbounds":[{"type":"socks","tag":"x","server":"example.invalid","server_port":1080,"transport":{"outbound":"missing"}}]}`, "missing_reference"},
		{"reserved tag", `{"outbounds":[{"type":"socks","tag":"__hydra.internal","server":"example.invalid","server_port":1080}]}`, "reserved_tag"},
		{"local authority", `{"outbounds":[{"type":"socks","tag":"x","server":"example.invalid","server_port":1080,"bind_interface":"eth0"}]}`, "local_authority_field"},
		{"duplicate key", `{"outbounds":[],"outbounds":[]}`, "duplicate_json_key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := decodeValidationResult(t, HydraCoreValidateConfig(test.config, "remote_v2"))
			require.False(t, result.Valid)
			require.Equal(t, test.code, result.Diagnostics[0].Code)
			require.NotContains(t, result.Diagnostics[0].Message, "secret")
		})
	}
}

func TestTLSFragmentOutboundCompatibility(t *testing.T) {
	outbounds := map[string]string{
		"http":        `"server":"example.invalid","server_port":443`,
		"vmess":       `"server":"example.invalid","server_port":443,"uuid":"00000000-0000-0000-0000-000000000000","security":"auto"`,
		"trojan":      `"server":"example.invalid","server_port":443,"password":"secret"`,
		"hysteria":    `"server":"example.invalid","server_port":443,"auth_str":"secret","up_mbps":100,"down_mbps":100`,
		"hysteria2":   `"server":"example.invalid","server_port":443,"password":"secret"`,
		"tuic":        `"server":"example.invalid","server_port":443,"uuid":"00000000-0000-0000-0000-000000000000","password":"secret"`,
		"anytls":      `"server":"example.invalid","server_port":443,"password":"secret"`,
		"vless":       `"server":"example.invalid","server_port":443,"uuid":"00000000-0000-0000-0000-000000000000"`,
		"shadowtls":   `"server":"example.invalid","server_port":443,"version":3,"password":"secret"`,
		"trusttunnel": `"server":"example.invalid","server_port":443,"username":"user","password":"secret"`,
	}
	for mode, fields := range map[string]string{
		"control":  ``,
		"record":   `,"record_fragment":true`,
		"fragment": `,"fragment":true,"fragment_fallback_delay":"300ms"`,
	} {
		for outboundType, required := range outbounds {
			t.Run(mode+"/"+outboundType, func(t *testing.T) {
				config := fmt.Sprintf(`{"outbounds":[{"type":"%s","tag":"proxy",%s,"tls":{"enabled":true,"server_name":"example.invalid"%s}}]}`,
					outboundType,
					required,
					fields,
				)
				err := CheckConfig(config)
				// QUIC and the rest are compiled in only under their own build tags, and a narrower
				// test invocation is not evidence that a transport rejects this. The row is skipped
				// rather than asserted, and the skip is visible in the output; the full matrix runs
				// under the tags the client core actually ships with.
				if err != nil && strings.Contains(err.Error(), "not included in this build") {
					t.Skip("the outbound is not part of this build's tags")
				}
				require.NoError(t, err)
			})
		}
	}
}

// Naive refuses both fragmentation fields outright — the one type the bundled core rejects
// rather than silently ignores. This is why HydraBox stops emitting them there. If upstream
// ever lifts the restriction, this test starts failing and says to re-enable it deliberately
// instead of leaving dead capability on the table.
func TestNaiveFragmentIsRefusedByCore(t *testing.T) {
	for mode, fields := range map[string]string{
		"record":   `,"record_fragment":true`,
		"fragment": `,"fragment":true,"fragment_fallback_delay":"300ms"`,
	} {
		t.Run(mode, func(t *testing.T) {
			config := fmt.Sprintf(`{"outbounds":[{"type":"naive","tag":"proxy","server":"example.invalid","server_port":443,"username":"user","password":"secret","tls":{"enabled":true,"server_name":"example.invalid"%s}}]}`, fields)
			err := CheckConfig(config)
			// The naive outbound is compiled in only where the cronet stack is available, so a
			// desktop run cannot reach it. Skipping is narrower than weakening: the assertion
			// below still runs wherever the type actually exists.
			if err != nil && strings.Contains(err.Error(), "not included in this build") {
				t.Skip("the naive outbound is not part of this build's tags")
			}
			require.Error(t, err, "naive must refuse fragmentation rather than ignore it")
			require.Contains(t, err.Error(), "fragment is not supported on naive outbound")
		})
	}
}

func TestHydraCoreValidateConfigDoesNotEchoSecrets(t *testing.T) {
	secret := "do-not-echo-this-value"
	result := HydraCoreValidateConfig(`{"outbounds":[{"type":"trojan","tag":"x","password":"`+secret+`"}]}`, "remote_v2")
	require.False(t, strings.Contains(result, secret))
}
