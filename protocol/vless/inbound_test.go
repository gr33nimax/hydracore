package vless

import (
	"testing"

	"github.com/sagernet/sing-box/protocol/vless/encryption"
)

func TestInboundVisionSpliceDisabledByEncryption(t *testing.T) {
	inbound := &Inbound{}
	if !inbound.visionCanSplice() {
		t.Fatal("expected Vision splice without an additional framing layer")
	}

	inbound.decryption = &encryption.ServerInstance{}
	if inbound.visionCanSplice() {
		t.Fatal("authenticated VLESS encryption must not be bypassed by Vision splice")
	}
}
