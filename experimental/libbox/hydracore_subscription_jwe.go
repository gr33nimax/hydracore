package libbox

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
)

const hydraSubscriptionMaxJWEBytes = hydraSubscriptionMaxBytes*4/3 + 4096

type hydraSubscriptionJWE struct {
	Protected    string `json:"protected"`
	EncryptedKey string `json:"encrypted_key"`
	IV           string `json:"iv"`
	Ciphertext   string `json:"ciphertext"`
	Tag          string `json:"tag"`
}

type hydraSubscriptionJWEHeader struct {
	Algorithm   string `json:"alg"`
	Encryption  string `json:"enc"`
	Type        string `json:"typ"`
	ContentType string `json:"cty"`
}

// HydraCoreOpenSubscriptionJWE authenticates, decrypts, and validates a Hydra
// Subscription v2 envelope. keyBase64URL is the raw hydra-key fragment value;
// HydraCore never fetches URLs or persists the key.
func HydraCoreOpenSubscriptionJWE(envelope string, keyBase64URL string) (string, error) {
	plaintext, err := openHydraSubscriptionJWE(envelope, keyBase64URL)
	if err != nil {
		return "", err
	}
	if _, _, err = validateHydraSubscriptionV2(string(plaintext)); err != nil {
		clear(plaintext)
		return "", err
	}
	result := string(plaintext)
	clear(plaintext)
	return result, nil
}

func HydraCoreValidateSubscriptionJWE(envelope string, keyBase64URL string) string {
	plaintext, err := openHydraSubscriptionJWE(envelope, keyBase64URL)
	if err == nil {
		_, _, err = validateHydraSubscriptionV2(string(plaintext))
		clear(plaintext)
	}
	result := hydraCoreValidationResult{
		SchemaVersion: 1,
		Profile:       "subscription_jwe_v2",
		Valid:         err == nil,
		Diagnostics:   []hydraCoreValidationDiagnostic{},
	}
	if err != nil {
		appendHydraValidationError(&result, err)
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return `{"schema_version":1,"profile":"subscription_jwe_v2","valid":false,"diagnostics":[{"severity":"error","code":"internal_error","path":"$","message":"validation result could not be encoded"}]}`
	}
	return string(encoded)
}

func HydraCoreInspectSubscriptionJWE(envelope string, keyBase64URL string) string {
	plaintext, err := openHydraSubscriptionJWE(envelope, keyBase64URL)
	if err != nil {
		result := hydraSubscriptionInspection{
			SchemaVersion: 1,
			Resources:     []hydraSubscriptionResourceInfo{},
			Profiles:      []hydraSubscriptionProfileInfo{},
			Diagnostics:   []hydraCoreValidationDiagnostic{},
		}
		validation := hydraCoreValidationResult{Diagnostics: []hydraCoreValidationDiagnostic{}}
		appendHydraValidationError(&validation, err)
		result.Diagnostics = validation.Diagnostics
		encoded, _ := json.Marshal(result)
		return string(encoded)
	}
	defer clear(plaintext)
	return HydraCoreInspectSubscription(string(plaintext))
}

func openHydraSubscriptionJWE(content string, keyBase64URL string) ([]byte, error) {
	if len(content) == 0 || len(content) > hydraSubscriptionMaxJWEBytes {
		return nil, &hydraCorePolicyError{code: "invalid_jwe_size", path: "$", message: "encrypted subscription size is invalid"}
	}
	if err := rejectDuplicateJSONKeys([]byte(content)); err != nil {
		return nil, remapHydraPolicyError(err, "$")
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(content), &rawFields); err != nil {
		return nil, invalidHydraJWE("invalid_jwe_shape", "encrypted subscription is not strict flattened JWE JSON")
	}
	for _, field := range []string{"protected", "encrypted_key", "iv", "ciphertext", "tag"} {
		if _, exists := rawFields[field]; !exists {
			return nil, invalidHydraJWE("invalid_jwe_shape", "encrypted subscription is missing a required JWE member")
		}
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var envelope hydraSubscriptionJWE
	if err := decoder.Decode(&envelope); err != nil || envelope.EncryptedKey != "" {
		return nil, invalidHydraJWE("invalid_jwe_shape", "encrypted subscription is not strict flattened JWE JSON")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, invalidHydraJWE("invalid_jwe_shape", "encrypted subscription must contain exactly one JSON document")
	}

	protectedJSON, err := base64.RawURLEncoding.DecodeString(envelope.Protected)
	if err != nil || len(protectedJSON) == 0 || len(protectedJSON) > 1024 {
		return nil, invalidHydraJWE("invalid_jwe_header", "JWE protected header is invalid")
	}
	defer clear(protectedJSON)
	if err = rejectDuplicateJSONKeys(protectedJSON); err != nil {
		return nil, invalidHydraJWE("invalid_jwe_header", "JWE protected header is invalid")
	}
	headerDecoder := json.NewDecoder(strings.NewReader(string(protectedJSON)))
	headerDecoder.DisallowUnknownFields()
	var header hydraSubscriptionJWEHeader
	if err = headerDecoder.Decode(&header); err != nil ||
		header.Algorithm != "dir" ||
		header.Encryption != "A256GCM" ||
		header.Type != "hydra-subscription+jwe" ||
		header.ContentType != "application/vnd.hydra.subscription+json" {
		return nil, invalidHydraJWE("unsupported_jwe_policy", "JWE protected header does not match Hydra Subscription v2 policy")
	}
	if headerDecoder.Decode(&trailing) != io.EOF {
		return nil, invalidHydraJWE("invalid_jwe_header", "JWE protected header must contain exactly one JSON document")
	}

	key, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(keyBase64URL))
	if err != nil || len(key) != 32 {
		clear(key)
		return nil, invalidHydraJWE("invalid_jwe_key", "hydra-key must encode exactly 32 bytes")
	}
	defer clear(key)
	iv, err := decodeHydraJWEPart(envelope.IV, 12, "iv")
	if err != nil {
		return nil, err
	}
	defer clear(iv)
	tag, err := decodeHydraJWEPart(envelope.Tag, 16, "tag")
	if err != nil {
		return nil, err
	}
	defer clear(tag)
	ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) == 0 || len(ciphertext) > hydraSubscriptionMaxBytes {
		clear(ciphertext)
		return nil, invalidHydraJWE("invalid_jwe_ciphertext", "JWE ciphertext is invalid")
	}
	defer clear(ciphertext)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, invalidHydraJWE("invalid_jwe_key", "JWE key could not be initialized")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, invalidHydraJWE("invalid_jwe_policy", "JWE cipher could not be initialized")
	}
	sealed := make([]byte, 0, len(ciphertext)+len(tag))
	sealed = append(sealed, ciphertext...)
	sealed = append(sealed, tag...)
	plaintext, err := gcm.Open(nil, iv, sealed, []byte(envelope.Protected))
	clear(sealed)
	if err != nil {
		return nil, invalidHydraJWE("jwe_authentication_failed", "encrypted subscription authentication failed")
	}
	if len(plaintext) == 0 || len(plaintext) > hydraSubscriptionMaxBytes {
		clear(plaintext)
		return nil, invalidHydraJWE("invalid_plaintext_size", "decrypted subscription size is invalid")
	}
	return plaintext, nil
}

func decodeHydraJWEPart(value string, expectedBytes int, name string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != expectedBytes {
		clear(decoded)
		return nil, invalidHydraJWE("invalid_jwe_"+name, "JWE "+name+" is invalid")
	}
	return decoded, nil
}

func invalidHydraJWE(code string, message string) error {
	return &hydraCorePolicyError{code: code, path: "$", message: message}
}
