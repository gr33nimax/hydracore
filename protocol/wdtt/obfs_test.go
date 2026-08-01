package wdtt

import (
	"bytes"
	"testing"
)

func TestWrapPacketRoundTrip(t *testing.T) {
	key, err := deriveWrapKey("test-password")
	if err != nil {
		t.Fatal(err)
	}
	aead, err := newWrapAEAD(key)
	if err != nil {
		t.Fatal(err)
	}
	config, err := newObfsConfig("audio")
	if err != nil {
		t.Fatal(err)
	}
	state, err := newObfsState()
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("wireguard packet")
	wire, err := wrapPacket(aead, plain, config, state)
	if err != nil {
		t.Fatal(err)
	}
	decoded := make([]byte, 2048)
	n, err := unwrapPacket(aead, wire, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded[:n], plain) {
		t.Fatalf("round trip mismatch: %q", decoded[:n])
	}

	wire[12] ^= 1
	if _, err = unwrapPacket(aead, wire, decoded); err == nil {
		t.Fatal("expected authentication failure after tampering")
	}
}
