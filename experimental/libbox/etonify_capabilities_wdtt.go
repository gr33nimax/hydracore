//go:build with_wdtt

package libbox

import "github.com/sagernet/sing-box/protocol/wdtt"

const wdttIncluded = true

// SetHydraWDTTCredential passes one device grant from HydraBox secure storage
// into the process-local HydraCore runtime. It is never serialized in config.
func SetHydraWDTTCredential(credentialRef string, deviceID string, deviceGrant string) error {
	return wdtt.SetRuntimeCredential(credentialRef, deviceID, deviceGrant)
}

// ClearHydraWDTTCredentials clears process-local grants on subscription or
// application lifecycle changes.
func ClearHydraWDTTCredentials() {
	wdtt.ClearRuntimeCredentials()
}

// SetHydraWDTTVKAccountCredentials supplies short-lived TURN data captured by
// HydraBox's VK WebView when anonymous VK authentication requires captcha.
func SetHydraWDTTVKAccountCredentials(credentialRef string, username string, password string, turnURLsJSON string, expiresAtUnix int64) error {
	return wdtt.SetRuntimeAccountCredentials(credentialRef, username, password, turnURLsJSON, expiresAtUnix)
}
