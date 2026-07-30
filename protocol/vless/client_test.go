package vless

import (
	"context"
	"testing"

	"github.com/sagernet/sing/common/logger"
)

const testUserID = "9f1c6db7-2c40-4c62-9db5-65ce35f50f2f"

func TestProtocolClientEncryptionIsOptIn(t *testing.T) {
	for _, config := range []string{"", "none"} {
		client, err := newProtocolClient(context.Background(), testUserID, "", config, logger.NOP())
		if err != nil {
			t.Fatal(err)
		}
		if client.encryption != nil {
			t.Fatalf("encryption initialized for %q", config)
		}
	}
}

func TestProtocolClientRejectsInvalidEncryption(t *testing.T) {
	if _, err := newProtocolClient(context.Background(), testUserID, "", "invalid", logger.NOP()); err == nil {
		t.Fatal("expected invalid encryption error")
	}
}
