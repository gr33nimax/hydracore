package libbox

import (
	"encoding/json"
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

func TestHydraCoreValidateConfigDoesNotEchoSecrets(t *testing.T) {
	secret := "do-not-echo-this-value"
	result := HydraCoreValidateConfig(`{"outbounds":[{"type":"trojan","tag":"x","password":"`+secret+`"}]}`, "remote_v2")
	require.False(t, strings.Contains(result, secret))
}
