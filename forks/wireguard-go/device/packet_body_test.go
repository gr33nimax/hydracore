package device

import "testing"

// An element dropped during encryption keeps its place in the container but loses its packet.
// Slicing it for a transport header used to panic with "slice bounds out of range [8:0]" and take
// the whole process down with it; the sender has to be told there is nothing to send instead.
func TestPacketBodyOfDroppedElement(t *testing.T) {
	if _, present := packetBody(&QueueOutboundElement{packet: nil, padding: 0}); present {
		t.Fatal("a dropped element must not be treated as a packet")
	}

	keepalive := &QueueOutboundElement{
		packet: make([]byte, MessageEncapsulatingTransportSize+MessageKeepaliveSize),
	}
	body, present := packetBody(keepalive)
	if !present {
		t.Fatal("a whole keepalive has to be sent")
	}
	if len(body) != MessageKeepaliveSize {
		t.Fatalf("keepalive body is %d bytes, want %d", len(body), MessageKeepaliveSize)
	}

	// Padding pushes the header further in. A packet that ends exactly where its padding ends
	// carries nothing either, and that is the shape the AmneziaWG padding produces.
	empty := &QueueOutboundElement{
		packet:  make([]byte, MessageEncapsulatingTransportSize+4),
		padding: 4,
	}
	if _, present := packetBody(empty); present {
		t.Fatal("a packet that ends where its padding ends carries nothing")
	}

	// A padded keepalive is still a keepalive: its body starts after the padding.
	padded := &QueueOutboundElement{
		packet:  make([]byte, MessageEncapsulatingTransportSize+4+MessageKeepaliveSize),
		padding: 4,
	}
	body, present = packetBody(padded)
	if !present {
		t.Fatal("a padded keepalive has to be sent")
	}
	if len(body) != MessageKeepaliveSize {
		t.Fatalf("padded keepalive body is %d bytes, want %d", len(body), MessageKeepaliveSize)
	}
}
