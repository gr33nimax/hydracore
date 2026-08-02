// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	maximumCredentialRefLength = 256
	maximumDeviceIDLength      = 256
	maximumDeviceGrantLength   = 4096
)

type runtimeCredential struct {
	deviceID string
	grant    []byte
}

var (
	runtimeCredentialMu sync.RWMutex
	runtimeCredentials  sync.Map
)

type runtimeAccountCredential struct {
	username  []byte
	password  []byte
	urls      []string
	expiresAt time.Time
}

var runtimeAccountCredentials sync.Map

// SetRuntimeCredential installs device-bound secret material supplied by
// HydraBox secure storage. Subscription documents contain only credential_ref.
func SetRuntimeCredential(credentialRef string, deviceID string, deviceGrant string) error {
	credentialRef = strings.TrimSpace(credentialRef)
	deviceID = strings.TrimSpace(deviceID)
	if !validCredentialReference(credentialRef) {
		return fmt.Errorf("invalid Hydra WDTT credential_ref")
	}
	if !validRuntimeIdentifier(deviceID, maximumDeviceIDLength) {
		return fmt.Errorf("invalid Hydra WDTT device_id")
	}
	if len(deviceGrant) == 0 || len(deviceGrant) > maximumDeviceGrantLength || strings.ContainsAny(deviceGrant, "\r\n\x00") {
		return fmt.Errorf("invalid Hydra WDTT device grant")
	}
	next := &runtimeCredential{deviceID: deviceID, grant: append([]byte(nil), deviceGrant...)}
	runtimeCredentialMu.Lock()
	defer runtimeCredentialMu.Unlock()
	if previous, loaded := runtimeCredentials.Swap(credentialRef, next); loaded {
		zeroRuntimeCredential(previous.(*runtimeCredential))
	}
	return nil
}

// ClearRuntimeCredentials removes all process-local WDTT device grants.
func ClearRuntimeCredentials() {
	runtimeCredentialMu.Lock()
	defer runtimeCredentialMu.Unlock()
	runtimeCredentials.Range(func(key any, value any) bool {
		runtimeCredentials.Delete(key)
		zeroRuntimeCredential(value.(*runtimeCredential))
		return true
	})
	runtimeAccountCredentials.Range(func(key any, value any) bool {
		runtimeAccountCredentials.Delete(key)
		zeroRuntimeAccountCredential(value.(*runtimeAccountCredential))
		return true
	})
}

// SetRuntimeAccountCredentials installs short-lived TURN credentials captured
// by HydraBox's VK WebView fallback. They remain process-local and expire at
// the timestamp supplied by VK/HydraBox.
func SetRuntimeAccountCredentials(credentialRef string, username string, password string, turnURLsJSON string, expiresAtUnix int64) error {
	credentialRef = strings.TrimSpace(credentialRef)
	if !validCredentialReference(credentialRef) || username == "" || password == "" {
		return fmt.Errorf("invalid Hydra WDTT VK account credential")
	}
	if len(username) > 2048 || len(password) > 2048 || strings.ContainsAny(username, "\r\n\x00") || strings.ContainsAny(password, "\r\n\x00") {
		return fmt.Errorf("invalid Hydra WDTT VK account credential")
	}
	var urls []string
	if err := json.Unmarshal([]byte(turnURLsJSON), &urls); err != nil {
		return fmt.Errorf("decode Hydra WDTT TURN URLs: %w", err)
	}
	if len(urls) == 0 || len(urls) > 16 {
		return fmt.Errorf("Hydra WDTT TURN URLs must contain between 1 and 16 entries")
	}
	for index, address := range urls {
		address = strings.TrimSpace(address)
		host, port, err := net.SplitHostPort(address)
		portNumber, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || host == "" || portNumber < 1 || portNumber > 65535 {
			return fmt.Errorf("Hydra WDTT TURN URL %d is invalid", index)
		}
		urls[index] = net.JoinHostPort(host, port)
	}
	expiresAt := time.Unix(expiresAtUnix, 0)
	if !expiresAt.After(time.Now().Add(30 * time.Second)) {
		return fmt.Errorf("Hydra WDTT VK account credential is already expired")
	}
	next := &runtimeAccountCredential{
		username:  append([]byte(nil), username...),
		password:  append([]byte(nil), password...),
		urls:      append([]string(nil), urls...),
		expiresAt: expiresAt,
	}
	runtimeCredentialMu.Lock()
	defer runtimeCredentialMu.Unlock()
	if previous, loaded := runtimeAccountCredentials.Swap(credentialRef, next); loaded {
		zeroRuntimeAccountCredential(previous.(*runtimeAccountCredential))
	}
	return nil
}

func loadRuntimeCredential(credentialRef string) (string, string, bool) {
	runtimeCredentialMu.RLock()
	defer runtimeCredentialMu.RUnlock()
	value, loaded := runtimeCredentials.Load(credentialRef)
	if !loaded {
		return "", "", false
	}
	credential := value.(*runtimeCredential)
	return credential.deviceID, string(credential.grant), true
}

func zeroRuntimeCredential(credential *runtimeCredential) {
	if credential == nil {
		return
	}
	for index := range credential.grant {
		credential.grant[index] = 0
	}
	credential.deviceID = ""
}

func loadRuntimeAccountCredentials(credentialRef string) (*turnCredentials, bool) {
	runtimeCredentialMu.Lock()
	defer runtimeCredentialMu.Unlock()
	value, loaded := runtimeAccountCredentials.Load(credentialRef)
	if !loaded {
		return nil, false
	}
	credential := value.(*runtimeAccountCredential)
	if !time.Now().Before(credential.expiresAt) {
		if runtimeAccountCredentials.CompareAndDelete(credentialRef, credential) {
			zeroRuntimeAccountCredential(credential)
		}
		return nil, false
	}
	return &turnCredentials{
		username: string(credential.username),
		password: string(credential.password),
		urls:     append([]string(nil), credential.urls...),
	}, true
}

func zeroRuntimeAccountCredential(credential *runtimeAccountCredential) {
	if credential == nil {
		return
	}
	for index := range credential.username {
		credential.username[index] = 0
	}
	for index := range credential.password {
		credential.password[index] = 0
	}
	credential.urls = nil
}

func validCredentialReference(value string) bool {
	return validRuntimeIdentifier(value, maximumCredentialRefLength)
}

func validRuntimeIdentifier(value string, maximumLength int) bool {
	if value == "" || len(value) > maximumLength {
		return false
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) || strings.ContainsRune("|/\\?#@", character) {
			return false
		}
	}
	return true
}
