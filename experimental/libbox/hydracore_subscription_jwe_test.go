package libbox

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHydraSubscriptionJWEKnownAnswerAndTamperDetection(t *testing.T) {
	key := make([]byte, 32)
	for index := range key {
		key[index] = byte(index)
	}
	iv := []byte{0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab}
	const plaintext = `{"api_version":"hydra.io/subscription/v2","kind":"Subscription","identity":{"issuer":"https://provider.example","id":"known-answer","sequence":1},"validity":{"issued_at":"2026-08-08T12:00:00Z"},"requirements":{"core":{"id":"io.hydrabox.hydracore","api_version":2,"remote_policy":2},"client":{"subscription_contract":2}},"resources":[{"id":"primary","format":"sing-box-json","requested_permissions":["network.outbound"],"document":{"outbounds":[{"type":"socks","tag":"proxy","server":"origin.example.invalid","server_port":1080,"password":"do-not-expose"}]}}],"profiles":[{"id":"main","resource":"primary","name":"Main","entrypoint":{"section":"outbounds","tag":"proxy"}}],"default_profile":"main"}`
	envelope := sealHydraSubscriptionTestJWE(t, []byte(plaintext), key, iv)
	const expectedEnvelope = `{"protected":"eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiaHlkcmEtc3Vic2NyaXB0aW9uK2p3ZSIsImN0eSI6ImFwcGxpY2F0aW9uL3ZuZC5oeWRyYS5zdWJzY3JpcHRpb24ranNvbiJ9","encrypted_key":"","iv":"oKGio6Slpqeoqaqr","ciphertext":"nTodXSyUdNoQFu68aVj6_BjVPWLzmSsDs31T5AzIB2iiAi6QwQ0lD32wJqNgFOfbfTkVPQCjeQwoLn83yx6nnJbV4Qpe0Y2UjcDFY62nv8m4btmG_-PxDpoFTaCetT3m4PJCz2bcof7mumsg6e3PBAY5BYzkF_BSQLiwidU7gTqgY5JT8THlZkI8kOh3aKvn9fWPHd-8k-iUucTXlbwWQrVratTSNSFioDAJEF_ggMxHHtCoOjKfDLiyRgNsskV1SRpuuwWgzoSBOC2qJ1BBl2iSxotQEb2a17p6jDnk4EUBPhxJwAmrXqLTOnEqciQNat6tTexwXKUK3cQ1E1Za4X5Qp6XV_o7CFMY66hNeM6NXfWMzwpj5GIVLNLmO2eFD72p82QJxCK-WDjxFg9vvMbZIvTplErHCEdRPl6fwyC_3rHajkQBHdParSTPMf_bOH-G8M-Y8Xp05be3oRBiiIjN-wyT0Cwork69ksRHMa-Onn2E_pxEgssF1OHWuk7aNdbvESJgr5XARN8obCZfz6XGCaO--Se5VRZvSqgMUHEelHGxqKtiR_04cTYrtZfNIYs664xjIuRSQZb_gh1BQ3CTLlNbeqfqHt-X8YMtEY7zr5cszQXv4Dg6xxlKiL86UJNqzdvR3e-fkUlEjwBMUS2m5mNEf77xniyM2zNqsdH43UqeTQDttaZdL1bfXpPcxZITp-Pr5G-YxlV3G9a-LIx4a3c2oiNw4X-LeIL4d6rdN7530ENwcYoURRjscqtyVNpXa965eiLa1YpVoS7k_O44vPh5RldPQvC7xrmE0QTsvoJ_zHjbOcIHfJMC-3ViNJWAsgeqzeFCNXCFhJ9VGbSHvlC4xAqt_JYAD7espDOlInmOkRhqq19_hexZXnwS9HPPvWwIcbV-T60YxJrU0HQ8Kzwz-g7dShg","tag":"ZKswfpnvYc8QaPYduW_rUw"}`
	require.Equal(t, expectedEnvelope, envelope)

	keyValue := base64.RawURLEncoding.EncodeToString(key)
	opened, err := HydraCoreOpenSubscriptionJWE(envelope, keyValue)
	require.NoError(t, err)
	require.JSONEq(t, plaintext, opened)
	require.True(t, decodeValidationResult(t, HydraCoreValidateSubscriptionJWE(envelope, keyValue)).Valid)
	require.NotContains(t, HydraCoreInspectSubscriptionJWE(envelope, keyValue), "do-not-expose")

	var tampered hydraSubscriptionJWE
	require.NoError(t, json.Unmarshal([]byte(envelope), &tampered))
	if tampered.Ciphertext[0] == 'A' {
		tampered.Ciphertext = "B" + tampered.Ciphertext[1:]
	} else {
		tampered.Ciphertext = "A" + tampered.Ciphertext[1:]
	}
	tamperedEnvelope, err := json.Marshal(tampered)
	require.NoError(t, err)
	validation := decodeValidationResult(t, HydraCoreValidateSubscriptionJWE(string(tamperedEnvelope), keyValue))
	require.False(t, validation.Valid)
	require.Equal(t, "jwe_authentication_failed", validation.Diagnostics[0].Code)
}

func sealHydraSubscriptionTestJWE(t *testing.T, plaintext []byte, key []byte, iv []byte) string {
	t.Helper()
	protectedJSON := `{"alg":"dir","enc":"A256GCM","typ":"hydra-subscription+jwe","cty":"application/vnd.hydra.subscription+json"}`
	protected := base64.RawURLEncoding.EncodeToString([]byte(protectedJSON))
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)
	sealed := gcm.Seal(nil, iv, plaintext, []byte(protected))
	tagOffset := len(sealed) - gcm.Overhead()
	envelope, err := json.Marshal(hydraSubscriptionJWE{
		Protected:    protected,
		EncryptedKey: "",
		IV:           base64.RawURLEncoding.EncodeToString(iv),
		Ciphertext:   base64.RawURLEncoding.EncodeToString(sealed[:tagOffset]),
		Tag:          base64.RawURLEncoding.EncodeToString(sealed[tagOffset:]),
	})
	require.NoError(t, err)
	return string(envelope)
}
