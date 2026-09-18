package libbox

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
)

// The manifest is a small document by construction, and a verifier that buffers whatever it is
// handed is a verifier somebody can use to exhaust memory.
const hydraUpdateMaxManifestBytes = 64 * 1024

type hydraCoreUpdateKey struct {
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
}

type hydraCoreUpdateKeys struct {
	Keys []hydraCoreUpdateKey `json:"keys"`
}

type hydraCoreUpdateVerificationResult struct {
	SchemaVersion int                             `json:"schema_version"`
	Profile       string                          `json:"profile"`
	Valid         bool                            `json:"valid"`
	KeyID         string                          `json:"key_id,omitempty"`
	Diagnostics   []hydraCoreValidationDiagnostic `json:"diagnostics"`
}

// HydraCoreVerifyUpdateManifest checks a detached Ed25519 signature over the exact bytes of a
// release manifest.
//
// The exact bytes matter: verifying a re-encoded copy of the document is how two different
// documents come to share one signature. The caller therefore hands over the manifest as it
// arrived, base64url encoded, and this function never re-serialises it.
//
// keysJSON is the pinned allowlist of trusted keys — a document cannot name the key that
// vouches for it, or the signature would only prove that somebody signed it.
func HydraCoreVerifyUpdateManifest(manifestBase64 string, signatureBase64 string, keyID string, keysJSON string) string {
	result := hydraCoreUpdateVerificationResult{
		SchemaVersion: 1,
		Profile:       "update_manifest_v1",
		Diagnostics:   []hydraCoreValidationDiagnostic{},
	}
	err := verifyHydraUpdateManifest(manifestBase64, signatureBase64, keyID, keysJSON, &result)
	if err != nil {
		policyErr, loaded := err.(*hydraCorePolicyError)
		if !loaded {
			policyErr = &hydraCorePolicyError{code: "internal_error", path: "$", message: "manifest verification failed"}
		}
		result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
			Severity: "error",
			Code:     policyErr.code,
			Path:     policyErr.path,
			Message:  policyErr.message,
		})
		result.KeyID = ""
		result.Valid = false
	} else {
		result.Valid = true
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return `{"schema_version":1,"profile":"update_manifest_v1","valid":false,"diagnostics":[{"severity":"error","code":"internal_error","path":"$","message":"verification result could not be encoded"}]}`
	}
	return string(encoded)
}

func verifyHydraUpdateManifest(manifestBase64 string, signatureBase64 string, keyID string, keysJSON string, result *hydraCoreUpdateVerificationResult) error {
	manifest, err := decodeHydraUpdatePart(manifestBase64, "$.manifest")
	if err != nil {
		return err
	}
	if len(manifest) == 0 {
		return &hydraCorePolicyError{code: "empty_manifest", path: "$.manifest", message: "manifest is empty"}
	}
	if len(manifest) > hydraUpdateMaxManifestBytes {
		return &hydraCorePolicyError{code: "document_too_large", path: "$.manifest", message: "manifest exceeds the size limit"}
	}
	signature, err := decodeHydraUpdatePart(signatureBase64, "$.signature")
	if err != nil {
		return err
	}
	if len(signature) != ed25519.SignatureSize {
		// A length check before Verify: ed25519.Verify panics on a signature of the wrong size,
		// and a panic in the verifier is a denial of service, not a refusal.
		return &hydraCorePolicyError{code: "invalid_signature", path: "$.signature", message: "signature is not an Ed25519 signature"}
	}
	publicKey, err := hydraUpdatePublicKey(keysJSON, keyID)
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, manifest, signature) {
		return &hydraCorePolicyError{code: "signature_mismatch", path: "$.signature", message: "signature does not match this manifest"}
	}
	result.KeyID = strings.TrimSpace(keyID)
	return nil
}

// hydraUpdatePublicKey resolves the pinned key by identifier, and refuses everything else: an
// unknown identifier is a document asking to be trusted on its own word.
func hydraUpdatePublicKey(keysJSON string, keyID string) (ed25519.PublicKey, error) {
	trimmed := strings.TrimSpace(keyID)
	if trimmed == "" {
		return nil, &hydraCorePolicyError{code: "missing_key_id", path: "$.key_id", message: "no key identifier was given"}
	}
	var keys hydraCoreUpdateKeys
	if err := json.Unmarshal([]byte(keysJSON), &keys); err != nil {
		return nil, &hydraCorePolicyError{code: "invalid_key_list", path: "$.keys", message: "pinned keys could not be read"}
	}
	for _, key := range keys.Keys {
		if strings.TrimSpace(key.KeyID) != trimmed {
			continue
		}
		decoded, err := decodeHydraUpdatePart(key.PublicKey, "$.keys")
		if err != nil {
			return nil, err
		}
		if len(decoded) != ed25519.PublicKeySize {
			return nil, &hydraCorePolicyError{code: "invalid_key", path: "$.keys", message: "pinned key is not an Ed25519 public key"}
		}
		return ed25519.PublicKey(decoded), nil
	}
	return nil, &hydraCorePolicyError{code: "unknown_key_id", path: "$.key_id", message: "no pinned key carries this identifier"}
}

// decodeHydraUpdatePart reads base64url with or without padding. A document that reaches us
// padded by one library and unpadded by another is the same document.
func decodeHydraUpdatePart(value string, path string) ([]byte, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(value), "=")
	if trimmed == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, &hydraCorePolicyError{code: "invalid_encoding", path: path, message: "value is not base64url"}
	}
	return decoded, nil
}
