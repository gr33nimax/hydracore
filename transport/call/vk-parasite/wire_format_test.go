package vkparasite

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
)

// ai-generated: RTP wire format invariant verification ensuring strict compliance with media masking specification
func TestRTPWireFormatStructure(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := []byte("dtls-inner-packet-payload-for-wire-verification")
	wire, raw, err := codec.wrap(payload)
	require.NoError(t, err)
	defer codec.putBuffer(raw)

	// 1. RTP Header Version must be 2 (first 2 bits: 10)
	version := wire[0] >> 6
	require.Equal(t, byte(2), version, "RTP version must be 2")

	// 2. Padding bit must be set (0x20)
	hasPadding := wire[0]&0x20 != 0
	require.True(t, hasPadding, "RTP padding bit must be set")

	// 3. Dynamic payload type in range [96, 127]
	pt := wire[1] & 0x7f
	require.GreaterOrEqual(t, pt, byte(96), "RTP payload type must be dynamic >= 96")
	require.LessOrEqual(t, pt, byte(127), "RTP payload type must be dynamic <= 127")

	// 4. Sequence number (16-bit uint)
	seq := binary.BigEndian.Uint16(wire[2:4])
	require.Equal(t, codec.initialSeq, seq, "Initial sequence number must match codec seed")

	// 5. Timestamp (32-bit uint)
	ts := binary.BigEndian.Uint32(wire[4:8])
	require.GreaterOrEqual(t, ts, codec.initialTS, "Timestamp must be advanced from initial TS")

	// 6. SSRC (32-bit uint)
	ssrc := binary.BigEndian.Uint32(wire[8:12])
	require.Equal(t, codec.ssrc, ssrc, "SSRC must match codec seed")

	// 7. Padding length is the last byte
	paddingLen := int(wire[len(wire)-1])
	require.GreaterOrEqual(t, paddingLen, 1)
	require.LessOrEqual(t, paddingLen, defaultRTPPadding)

	// 8. Total wire length = 12 (RTP) + payload + 16 (Poly1305 AEAD tag) + padding
	expectedLen := rtpHeaderLength + len(payload) + chacha20poly1305.Overhead + paddingLen
	require.Equal(t, expectedLen, len(wire), "Total packet size must match RTP + payload + tag + padding")

	// 9. Receiver unwrap must recover exact plaintext
	receiver, err := newRTPCodec(key)
	require.NoError(t, err)

	plain, err := receiver.unwrap(wire)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, payload), "Plaintext must match original payload")
}

func TestRTPWireFormatSequenceMonotonicity(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	receiver, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := []byte("monotonicity-test")
	const packetCount = 100
	var lastSeq uint16

	for i := 0; i < packetCount; i++ {
		wire, raw, err := codec.wrap(payload)
		require.NoError(t, err)

		seq := binary.BigEndian.Uint16(wire[2:4])
		if i > 0 {
			require.Equal(t, lastSeq+1, seq, "Sequence numbers must increment by 1")
		}
		lastSeq = seq

		plain, err := receiver.unwrap(wire)
		require.NoError(t, err)
		require.Equal(t, payload, plain)

		codec.putBuffer(raw)
	}
}

func TestRTPWireFormatTimestampProgression(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := []byte("timestamp-test")
	wire1, raw1, err := codec.wrap(payload)
	require.NoError(t, err)
	codec.putBuffer(raw1)

	time.Sleep(10 * time.Millisecond)

	wire2, raw2, err := codec.wrap(payload)
	require.NoError(t, err)
	codec.putBuffer(raw2)

	ts1 := binary.BigEndian.Uint32(wire1[4:8])
	ts2 := binary.BigEndian.Uint32(wire2[4:8])
	require.GreaterOrEqual(t, ts2, ts1, "Timestamp must progress forward with time")
}
