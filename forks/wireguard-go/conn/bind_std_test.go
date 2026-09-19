package conn

import (
	"testing"

	"golang.org/x/net/ipv6"
)

// emptyBatchPanics stands in for the platform batch writer. golang.org/x/net's
// ipv4.payloadHandler.WriteBatch indexes the first message of the batch without checking that
// there is one, so a writer handed an empty batch takes the whole process down with it — which
// is what happened on a phone: a WireGuard container whose every element was dropped during
// encryption left the sender with nothing to send, and the abort that followed killed the core
// and the tunnel with it.
type emptyBatchPanics struct{}

func (emptyBatchPanics) WriteBatch(msgs []ipv6.Message, flags int) (int, error) {
	if len(msgs) == 0 {
		panic("runtime error: index out of range [0] with length 0")
	}
	return len(msgs), nil
}

// An empty batch is a legitimate outcome, not an error: the caller drops elements it cannot
// send, and when that is all of them there is simply nothing to write.
//
// It goes through writeBatches rather than send because that is the part every platform shares
// with the one that aborted: the batch writer is called the same way on a phone as it is here.
func TestSendEmptyBatchIsNotAnError(t *testing.T) {
	if err := writeBatches(emptyBatchPanics{}, nil); err != nil {
		t.Fatalf("an empty batch must be a no-op, got %v", err)
	}
}
