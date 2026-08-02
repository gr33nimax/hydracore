// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"errors"
	"strings"
	"testing"
)

func TestAnnotateCredentialChallenge(t *testing.T) {
	err := annotateCredentialChallenge(
		errVKAccountCredentialsRequired,
		"wdtt:user-1:device-1",
	)
	if !errors.Is(err, errVKAccountCredentialsRequired) {
		t.Fatalf("challenge no longer preserves its stable error: %v", err)
	}
	if !strings.Contains(err.Error(), `credential_ref "wdtt:user-1:device-1"`) {
		t.Fatalf("challenge does not identify its credential_ref: %v", err)
	}
}
