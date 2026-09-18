package device

import "testing"

// A handshake message must land inside its own message slice, never across the padded send buffer.
//
// With random trailers the send buffer is longer than the message, and a marshaller handed that longer
// slice returns an error without writing anything — which is exactly how an AmneziaWG 3.1 tunnel ended up
// sending all-zero initiations. The send path now slices to the message size, and this guards the
// contract that makes that necessary.
func TestHandshakeMessageStaysInsideItsTrailerBuffer(t *testing.T) {
	const trailerLen = 64

	packet := make([]byte, MessageInitiationSize+trailerLen)
	msg := MessageInitiation{Type: 7}

	if err := msg.marshal(packet[:MessageInitiationSize]); err != nil {
		t.Fatalf("marshal into an exact message slice failed: %v", err)
	}
	if got := packet[0]; got != 7 {
		t.Fatalf("message type = %d, want 7: the message must be written into its own slice", got)
	}
	for i, b := range packet[4:] {
		if b != 0 {
			t.Fatalf("byte %d outside the type field was written (%d)", i+4, b)
		}
	}

	// The failure mode being guarded: a trailered buffer is not a valid marshal target, so a caller that
	// forgets to slice sends an empty message instead of an error it would notice.
	trailered := make([]byte, MessageInitiationSize+trailerLen)
	if err := msg.marshal(trailered); err == nil {
		t.Fatalf("marshal accepted a trailered buffer; the slicing in the send path is no longer required")
	}
	for i, b := range trailered {
		if b != 0 {
			t.Fatalf("byte %d of the trailered buffer was written (%d)", i, b)
		}
	}
}
