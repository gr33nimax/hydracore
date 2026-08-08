package libbox

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validHydraSubscriptionJSON() string {
	return `{
      "api_version":"hydra.io/subscription/v2",
      "kind":"Subscription",
      "identity":{"issuer":"https://provider.example","id":"customer-main","channel":"stable","sequence":42},
      "validity":{"issued_at":"2026-08-08T12:00:00Z","not_before":"2026-08-08T11:55:00Z","expires_at":"2026-09-08T12:00:00Z"},
      "display":{"name":{"default":"Hydra","ru":"Гидра"},"homepage":"https://provider.example"},
      "requirements":{
        "core":{"id":"io.hydrabox.hydracore","api_version":2,"remote_policy":2,"features":["rmux"]},
        "client":{"subscription_contract":2}
      },
      "resources":[{
        "id":"primary",
        "format":"sing-box-json",
        "requested_permissions":["network.outbound"],
        "document":{"outbounds":[{"type":"socks","tag":"proxy-main","server":"origin.example.invalid","server_port":1080,"password":"do-not-expose"}]}
      }],
      "profiles":[{
        "id":"main","resource":"primary","name":{"default":"Main"},
        "entrypoint":{"section":"outbounds","tag":"proxy-main"},"enabled":true
      }],
      "default_profile":"main",
      "required_extensions":[],
      "extensions":{"io.hydra.labels/v1":{"tier":"private"}}
    }`
}

func TestHydraSubscriptionSchemasAreJSON(t *testing.T) {
	for _, schema := range []string{HydraCoreSubscriptionSchema(), HydraCoreSubscriptionJWESchema(), HydraCoreSubscriptionJWEPolicy()} {
		var value any
		require.NoError(t, json.Unmarshal([]byte(schema), &value))
	}
	require.Contains(t, HydraCoreSubscriptionSchema(), hydraSubscriptionAPIVersion)
	require.Contains(t, HydraCoreSubscriptionJWEPolicy(), "A256GCM")
}

func TestHydraCoreValidateAndInspectSubscription(t *testing.T) {
	validation := decodeValidationResult(t, HydraCoreValidateSubscription(validHydraSubscriptionJSON()))
	require.True(t, validation.Valid, validation.Diagnostics)

	inspectionContent := HydraCoreInspectSubscription(validHydraSubscriptionJSON())
	var inspection hydraSubscriptionInspection
	require.NoError(t, json.Unmarshal([]byte(inspectionContent), &inspection))
	require.True(t, inspection.Valid, inspection.Diagnostics)
	require.Equal(t, "customer-main", inspection.Identity.ID)
	require.Equal(t, []string{"outbounds:socks"}, inspection.Resources[0].Protocols)
	require.NotContains(t, inspectionContent, "origin.example.invalid")
	require.NotContains(t, inspectionContent, "do-not-expose")
}

func TestHydraCoreSubscriptionRejectsUnsafeContracts(t *testing.T) {
	tests := []struct {
		name string
		old  string
		new  string
		code string
	}{
		{"unknown field", `"kind":"Subscription",`, `"kind":"Subscription","unexpected":true,`, "invalid_subscription_shape"},
		{"permission mismatch", `"requested_permissions":["network.outbound"]`, `"requested_permissions":[]`, "permission_mismatch"},
		{"missing profile entrypoint", `"tag":"proxy-main"},"enabled"`, `"tag":"missing"},"enabled"`, "missing_profile_entrypoint"},
		{"unsupported core feature", `"features":["rmux"]`, `"features":["future-feature"]`, "unsupported_required_feature"},
		{"unsupported required extension", `"required_extensions":[]`, `"required_extensions":["io.hydra.labels/v1"]`, "unsupported_required_extension"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := strings.Replace(validHydraSubscriptionJSON(), test.old, test.new, 1)
			result := decodeValidationResult(t, HydraCoreValidateSubscription(content))
			require.False(t, result.Valid)
			require.Equal(t, test.code, result.Diagnostics[0].Code)
			require.NotContains(t, result.Diagnostics[0].Message, "do-not-expose")
		})
	}
}

func TestHydraCoreSubscriptionRejectsCrossResourceReferences(t *testing.T) {
	content := strings.Replace(validHydraSubscriptionJSON(),
		`"document":{"outbounds":[{"type":"socks","tag":"proxy-main","server":"origin.example.invalid","server_port":1080,"password":"do-not-expose"}]}`,
		`"document":{"outbounds":[{"type":"trojan","tag":"proxy-main","server":"origin.example.invalid","server_port":443,"password":"do-not-expose","detour":"other-resource"}]}`,
		1,
	)
	result := decodeValidationResult(t, HydraCoreValidateSubscription(content))
	require.False(t, result.Valid)
	require.Equal(t, "missing_reference", result.Diagnostics[0].Code)
}

func TestHydraCoreSubscriptionDoesNotEchoSecrets(t *testing.T) {
	secret := "subscription-secret-value"
	content := strings.Replace(validHydraSubscriptionJSON(), "do-not-expose", secret, 1)
	content = strings.Replace(content, `"server_port":1080`, `"server_port":"invalid"`, 1)
	validation := HydraCoreValidateSubscription(content)
	inspection := HydraCoreInspectSubscription(content)
	require.NotContains(t, validation, secret)
	require.NotContains(t, inspection, secret)
}

func TestHydraVersionRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		version      string
		versionRange string
		matches      bool
		valid        bool
	}{
		{"v1.13.16-extended-hydracore.1", ">=1.13.16-extended-hydracore.1 <1.14.0", true, true},
		{"1.13.15", ">=1.13.16 <2.0.0", false, true},
		{"1.13.16", "1.13.16", true, true},
		{"1.13.16", "^1.13.0", false, false},
		{"unknown", ">=1.0.0", false, false},
	}
	for _, test := range tests {
		matches, valid := hydraVersionMatchesRange(test.version, test.versionRange)
		require.Equal(t, test.valid, valid, test)
		require.Equal(t, test.matches, matches, test)
	}
}
