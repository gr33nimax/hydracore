package vkparasite

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthRequestRoundTrip(t *testing.T) {
	t.Parallel()
	request := authRequest{
		SessionID:      [16]byte{1, 2, 3, 4},
		Conv:           0x10203040,
		WorkerID:       2,
		WorkerTotal:    16,
		WorkerEpoch:    9,
		LaneGeneration: 1,
		User:           "alice",
		Password:       "correct horse battery staple",
	}
	frame, err := encodeAuthRequest(request)
	require.NoError(t, err)
	decoded, err := decodeAuthRequest(frame)
	require.NoError(t, err)
	request.ProtocolVersion = authProtocolVersion
	require.Equal(t, request, decoded)
	generation, err := decodeAuthAck(encodeAuthAck(true, 42, AuthRejectUnspecified))
	require.NoError(t, err)
	require.Equal(t, uint64(42), generation)
	_, err = decodeAuthAck(encodeAuthAck(false, 0, AuthRejectCredentials))
	require.Error(t, err)
}

func TestAuthRequestRejectsPreviousWireVersion(t *testing.T) {
	t.Parallel()
	request := authRequest{
		SessionID:      [16]byte{1, 2, 3, 4},
		Conv:           0x10203040,
		WorkerID:       1,
		WorkerTotal:    16,
		WorkerEpoch:    7,
		LaneGeneration: 1,
		User:           "alice",
		Password:       "secret",
	}
	frame, err := encodeAuthRequest(request)
	require.NoError(t, err)
	frame[4] = authProtocolVersion - 1
	_, err = decodeAuthRequest(frame)
	require.ErrorContains(t, err, "unsupported auth frame")

	ack := encodeAuthAck(true, 42, AuthRejectUnspecified)
	ack[4] = authProtocolVersion - 1
	_, err = decodeAuthAck(ack)
	require.ErrorContains(t, err, "invalid server auth response")
}

func TestAuthRequestRejectsInvalidBounds(t *testing.T) {
	t.Parallel()
	_, err := encodeAuthRequest(authRequest{Conv: 1, WorkerTotal: 16, User: "alice", Password: "secret"})
	require.Error(t, err)
	_, err = encodeAuthRequest(authRequest{
		SessionID:   [16]byte{1},
		Conv:        1,
		WorkerID:    16,
		WorkerTotal: 16,
		User:        "alice",
		Password:    "secret",
	})
	require.Error(t, err)
	_, err = encodeAuthRequest(authRequest{
		SessionID:   [16]byte{1},
		Conv:        1,
		WorkerTotal: 16,
		User:        string(bytes.Repeat([]byte{'a'}, maximumUserLength+1)),
		Password:    "secret",
	})
	require.Error(t, err)
}

func TestSupportedWorkerCount(t *testing.T) {
	t.Parallel()
	for _, workers := range []uint16{4, 8, 12, 16, 20} {
		require.True(t, supportedWorkerCount(workers))
	}
	for _, workers := range []uint16{0, 1, 5, 18, 21} {
		require.False(t, supportedWorkerCount(workers))
	}
}

func TestWorkersAreEvenlyDistributedAcrossCalls(t *testing.T) {
	t.Parallel()
	links := []string{"one", "two", "three", "four"}
	for _, workers := range []uint16{4, 8, 12, 16, 20} {
		counts := make(map[string]int, CallCount)
		for workerID := uint16(0); workerID < workers; workerID++ {
			counts[joinLinkForWorker(links, workerID)]++
		}
		for _, link := range links {
			require.Equal(t, int(workers)/CallCount, counts[link])
		}
	}
}

func TestRTPWrapperRoundTripAndWrongKey(t *testing.T) {
	t.Parallel()
	key, err := deriveWrapKey("shared-obfuscation-key")
	require.NoError(t, err)
	sender, err := newRTPCodec(key)
	require.NoError(t, err)
	receiver, err := newRTPCodec(key)
	require.NoError(t, err)
	payload := []byte("dtls application record")
	wire, _, err := sender.wrap(payload)
	require.NoError(t, err)
	require.Equal(t, byte(2), wire[0]>>6)
	pt := wire[1] & 0x7f
	require.GreaterOrEqual(t, pt, byte(96))
	require.LessOrEqual(t, pt, byte(127))
	plain, err := receiver.unwrap(nil, wire)
	require.NoError(t, err)
	require.Equal(t, payload, plain)

	wrongKey, err := deriveWrapKey("different-key")
	require.NoError(t, err)
	wrongReceiver, err := newRTPCodec(wrongKey)
	require.NoError(t, err)
	_, err = wrongReceiver.unwrap(nil, wire)
	require.Error(t, err)
}

// The reason a worker was turned away survives the wire, and the frame it travels in stays the
// fourteen bytes protocol version 9 already used — so a client or server that predates the
// reason keeps working, it just reads nothing there.
func TestAuthAckCarriesRejectReason(t *testing.T) {
	t.Parallel()
	for reason, expected := range map[byte]string{
		AuthRejectCredentials: "user or password rejected",
		AuthRejectWorkerCount: "worker count refused by the server",
		AuthRejectMalformed:   "malformed authentication request",
		AuthRejectSession:     "session identity mismatch",
		AuthRejectUnspecified: "reason not given",
	} {
		frame := encodeAuthAck(false, 0, reason)
		if len(frame) != 14 {
			t.Fatalf("reason %d: frame is %d bytes, protocol 9 says 14", reason, len(frame))
		}
		_, err := decodeAuthAck(frame)
		if err == nil || !strings.Contains(err.Error(), expected) {
			t.Fatalf("reason %d: got %v, want it to name %q", reason, err, expected)
		}
	}
	// An acceptance still carries the generation in those same bytes.
	generation, err := decodeAuthAck(encodeAuthAck(true, 0x0102030405060708, AuthRejectCredentials))
	if err != nil || generation != 0x0102030405060708 {
		t.Fatalf("accepted ack: got %d, %v", generation, err)
	}
}
