package multiuser

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthRequestRoundTrip(t *testing.T) {
	t.Parallel()
	request := authRequest{
		SessionID:   [16]byte{1, 2, 3, 4},
		Conv:        0x10203040,
		WorkerID:    2,
		WorkerTotal: 4,
		WorkerEpoch: 9,
		User:        "alice",
		Password:    "correct horse battery staple",
	}
	frame, err := encodeAuthRequest(request)
	require.NoError(t, err)
	decoded, err := decodeAuthRequest(frame)
	require.NoError(t, err)
	request.ProtocolVersion = authProtocolVersion
	require.Equal(t, request, decoded)
	generation, err := decodeAuthAck(encodeAuthAck(true, 42))
	require.NoError(t, err)
	require.Equal(t, uint64(42), generation)
	_, err = decodeAuthAck(encodeAuthAck(false, 0))
	require.Error(t, err)
}

func TestAuthRequestAcceptsLegacyV1ForServerTransition(t *testing.T) {
	t.Parallel()
	request := authRequest{
		SessionID:   [16]byte{1, 2, 3, 4},
		Conv:        0x10203040,
		WorkerID:    1,
		WorkerTotal: 4,
		User:        "alice",
		Password:    "secret",
	}
	frame, err := encodeAuthRequestVersion(request, authProtocolVersionV1)
	require.NoError(t, err)
	decoded, err := decodeAuthRequest(frame)
	require.NoError(t, err)
	require.Equal(t, byte(authProtocolVersionV1), decoded.ProtocolVersion)
	require.Zero(t, decoded.WorkerEpoch)
	require.Equal(t, request.SessionID, decoded.SessionID)
	require.Equal(t, request.Conv, decoded.Conv)
	require.Equal(t, request.User, decoded.User)
	require.Equal(t, byte(authProtocolVersionV1), encodeAuthAckVersion(true, 42, authProtocolVersionV1)[4])
}

func TestAuthRequestRejectsInvalidBounds(t *testing.T) {
	t.Parallel()
	_, err := encodeAuthRequest(authRequest{Conv: 1, WorkerTotal: 1, User: "alice", Password: "secret"})
	require.Error(t, err)
	_, err = encodeAuthRequest(authRequest{
		SessionID:   [16]byte{1},
		Conv:        1,
		WorkerID:    1,
		WorkerTotal: 1,
		User:        "alice",
		Password:    "secret",
	})
	require.Error(t, err)
	_, err = encodeAuthRequest(authRequest{
		SessionID:   [16]byte{1},
		Conv:        1,
		WorkerTotal: 1,
		User:        string(bytes.Repeat([]byte{'a'}, maximumUserLength+1)),
		Password:    "secret",
	})
	require.Error(t, err)
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
	wire, err := sender.wrap(payload)
	require.NoError(t, err)
	require.Equal(t, byte(2), wire[0]>>6)
	plain, err := receiver.unwrap(wire)
	require.NoError(t, err)
	require.Equal(t, payload, plain)

	wrongKey, err := deriveWrapKey("different-key")
	require.NoError(t, err)
	wrongReceiver, err := newRTPCodec(wrongKey)
	require.NoError(t, err)
	_, err = wrongReceiver.unwrap(wire)
	require.Error(t, err)
}
