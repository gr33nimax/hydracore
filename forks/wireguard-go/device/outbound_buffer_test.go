package device

import (
	"bytes"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestEnsureOutboundBufferRepairsLegacyS4Layout(t *testing.T) {
	device := newBareTestDevice()
	const s4 = uint32(278)
	plaintext := []byte("S4 must not turn a legacy 256-byte buffer into a process crash")

	legacyBuffer := make([]byte, 256)
	legacyPacket := legacyBuffer[MessageEncapsulatingTransportSize+MessageTransportHeaderSize:]
	copy(legacyPacket, plaintext)
	elem := &QueueOutboundElement{
		buffer:  legacyBuffer,
		packet:  legacyPacket[:len(plaintext)],
		padding: s4,
	}

	if !device.ensureOutboundBuffer(elem, 0) {
		t.Fatal("ensureOutboundBuffer rejected a valid S4 packet")
	}

	required := MessageEncapsulatingTransportSize + int(s4) + MessageTransportHeaderSize + len(plaintext) + chacha20poly1305.Overhead
	if len(elem.buffer) < required || cap(elem.buffer) < required {
		t.Fatalf("buffer length/capacity = %d/%d, want at least %d", len(elem.buffer), cap(elem.buffer), required)
	}
	if !bytes.Equal(elem.packet, plaintext) {
		t.Fatalf("plaintext changed during buffer repair: got %q, want %q", elem.packet, plaintext)
	}

	offset := MessageEncapsulatingTransportSize + int(s4) + MessageTransportHeaderSize
	if !bytes.Equal(elem.buffer[offset:offset+len(plaintext)], plaintext) {
		t.Fatal("plaintext was not moved to the S4-aware encryption offset")
	}
}

func TestEnsureOutboundBufferDropsImpossibleLayout(t *testing.T) {
	device := newBareTestDevice()
	elem := &QueueOutboundElement{
		buffer:  make([]byte, 1),
		packet:  make([]byte, MaxMessageSize),
		padding: 1,
	}

	if device.ensureOutboundBuffer(elem, 0) {
		t.Fatal("ensureOutboundBuffer accepted a layout larger than MaxMessageSize")
	}
}
