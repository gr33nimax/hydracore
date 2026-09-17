package libbox

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func hydraUpdateFixture(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return publicKey, privateKey
}

func hydraUpdateKeysJSON(t *testing.T, keyID string, publicKey ed25519.PublicKey) string {
	t.Helper()
	encoded, err := json.Marshal(hydraCoreUpdateKeys{
		Keys: []hydraCoreUpdateKey{{
			KeyID:     keyID,
			PublicKey: base64.RawURLEncoding.EncodeToString(publicKey),
		}},
	})
	require.NoError(t, err)
	return string(encoded)
}

func decodeUpdateResult(t *testing.T, content string) hydraCoreUpdateVerificationResult {
	t.Helper()
	var result hydraCoreUpdateVerificationResult
	require.NoError(t, json.Unmarshal([]byte(content), &result))
	return result
}

func TestHydraCoreVerifyUpdateManifestAcceptsAPinnedSignature(t *testing.T) {
	publicKey, privateKey := hydraUpdateFixture(t)
	manifest := []byte(`{"schema":1,"channel":"canary"}`)
	signature := ed25519.Sign(privateKey, manifest)

	result := decodeUpdateResult(t, HydraCoreVerifyUpdateManifest(
		base64.RawURLEncoding.EncodeToString(manifest),
		base64.RawURLEncoding.EncodeToString(signature),
		"update-2026-01",
		hydraUpdateKeysJSON(t, "update-2026-01", publicKey),
	))
	require.True(t, result.Valid, result.Diagnostics)
	require.Equal(t, "update-2026-01", result.KeyID)
	require.Empty(t, result.Diagnostics)
}

func TestHydraCoreVerifyUpdateManifestRefusesATamperedDocument(t *testing.T) {
	publicKey, privateKey := hydraUpdateFixture(t)
	manifest := []byte(`{"schema":1,"channel":"canary"}`)
	signature := ed25519.Sign(privateKey, manifest)

	// One byte of the document changes; the signature no longer covers it.
	tampered := append([]byte(nil), manifest...)
	tampered[len(tampered)-3] = 'x'

	result := decodeUpdateResult(t, HydraCoreVerifyUpdateManifest(
		base64.RawURLEncoding.EncodeToString(tampered),
		base64.RawURLEncoding.EncodeToString(signature),
		"update-2026-01",
		hydraUpdateKeysJSON(t, "update-2026-01", publicKey),
	))
	require.False(t, result.Valid)
	require.Equal(t, "signature_mismatch", result.Diagnostics[0].Code)
}

func TestHydraCoreVerifyUpdateManifestRefusesAKeyNobodyPinned(t *testing.T) {
	publicKey, privateKey := hydraUpdateFixture(t)
	_, otherPrivateKey := hydraUpdateFixture(t)
	manifest := []byte(`{"schema":1}`)

	// A valid signature by a key the client never pinned proves nothing.
	unpinned := decodeUpdateResult(t, HydraCoreVerifyUpdateManifest(
		base64.RawURLEncoding.EncodeToString(manifest),
		base64.RawURLEncoding.EncodeToString(ed25519.Sign(otherPrivateKey, manifest)),
		"update-2026-01",
		hydraUpdateKeysJSON(t, "update-2026-01", publicKey),
	))
	require.False(t, unpinned.Valid)
	require.Equal(t, "signature_mismatch", unpinned.Diagnostics[0].Code)

	unknown := decodeUpdateResult(t, HydraCoreVerifyUpdateManifest(
		base64.RawURLEncoding.EncodeToString(manifest),
		base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, manifest)),
		"update-2027-01",
		hydraUpdateKeysJSON(t, "update-2026-01", publicKey),
	))
	require.False(t, unknown.Valid)
	require.Equal(t, "unknown_key_id", unknown.Diagnostics[0].Code)
}

func TestHydraCoreVerifyUpdateManifestSurvivesMalformedInput(t *testing.T) {
	publicKey, _ := hydraUpdateFixture(t)
	keys := hydraUpdateKeysJSON(t, "update-2026-01", publicKey)
	manifest := base64.RawURLEncoding.EncodeToString([]byte(`{"schema":1}`))

	tests := []struct {
		name      string
		manifest  string
		signature string
		keyID     string
		code      string
	}{
		// A short signature must be refused, not verified: ed25519.Verify panics on the wrong size.
		{"short signature", manifest, base64.RawURLEncoding.EncodeToString([]byte("too short")), "update-2026-01", "invalid_signature"},
		{"empty signature", manifest, "", "update-2026-01", "invalid_signature"},
		{"empty manifest", "", base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), "update-2026-01", "empty_manifest"},
		{"not base64", "!!!not-base64!!!", base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), "update-2026-01", "invalid_encoding"},
		{"no key id", manifest, base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), "  ", "missing_key_id"},
		{"unreadable key list", manifest, base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), "update-2026-01", "invalid_key_list"},
		{"oversized manifest", base64.RawURLEncoding.EncodeToString(make([]byte, hydraUpdateMaxManifestBytes+1)), base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), "update-2026-01", "document_too_large"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keyList := keys
			if test.code == "invalid_key_list" {
				keyList = "not json"
			}
			result := decodeUpdateResult(t, HydraCoreVerifyUpdateManifest(test.manifest, test.signature, test.keyID, keyList))
			require.False(t, result.Valid)
			require.Equal(t, test.code, result.Diagnostics[0].Code)
		})
	}
}

func TestHydraCoreVerifyUpdateManifestReadsPaddingBothWays(t *testing.T) {
	publicKey, privateKey := hydraUpdateFixture(t)
	manifest := []byte(`{"schema":1,"channel":"stable"}`)
	signature := ed25519.Sign(privateKey, manifest)
	keys := hydraUpdateKeysJSON(t, "update-2026-01", publicKey)

	// One library pads base64url and another does not; it is the same document either way.
	for _, encode := range []func([]byte) string{
		base64.RawURLEncoding.EncodeToString,
		base64.URLEncoding.EncodeToString,
	} {
		result := decodeUpdateResult(t, HydraCoreVerifyUpdateManifest(
			encode(manifest),
			encode(signature),
			"update-2026-01",
			keys,
		))
		require.True(t, result.Valid, result.Diagnostics)
	}
	require.False(t, strings.Contains(keys, "PRIVATE"))
}
