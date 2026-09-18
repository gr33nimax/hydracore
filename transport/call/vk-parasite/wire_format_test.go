package vkparasite

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
)

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

	// 3. Extension bit must be set (0x10) for RFC 8285
	hasExtension := wire[0]&0x10 != 0
	require.True(t, hasExtension, "RTP extension bit must be set")

	// 4. Dynamic payload type in range [96, 127]
	pt := wire[1] & 0x7f
	require.GreaterOrEqual(t, pt, byte(96), "RTP payload type must be dynamic >= 96")
	require.LessOrEqual(t, pt, byte(127), "RTP payload type must be dynamic <= 127")

	// 5. Sequence number (16-bit uint)
	seq := binary.BigEndian.Uint16(wire[2:4])
	require.Equal(t, codec.initialSeq, seq, "Initial sequence number must match codec seed")

	// 6. Timestamp (32-bit uint)
	ts := binary.BigEndian.Uint32(wire[4:8])
	require.GreaterOrEqual(t, ts, codec.initialTS, "Timestamp must be advanced from initial TS")

	// 7. SSRC (32-bit uint)
	ssrc := binary.BigEndian.Uint32(wire[8:12])
	require.Equal(t, codec.ssrc, ssrc, "SSRC must match codec seed")

	// 8. RFC 8285 extension header
	require.Equal(t, byte(0xBE), wire[12])
	require.Equal(t, byte(0xDE), wire[13])
	extWords := binary.BigEndian.Uint16(wire[14:16])
	require.Equal(t, uint16(1), extWords)

	// 9. Padding length is the last byte
	paddingLen := int(wire[len(wire)-1])
	require.GreaterOrEqual(t, paddingLen, 1)
	require.LessOrEqual(t, paddingLen, defaultRTPPadding)

	// 10. Total wire length = 20 (RTP+ext) + payload + 16 (Poly1305 AEAD tag) + padding
	expectedLen := rtpTotalHeaderLength + len(payload) + chacha20poly1305.Overhead + paddingLen
	require.Equal(t, expectedLen, len(wire), "Total packet size must match RTP extended header + payload + tag + padding")

	// 11. Receiver unwrap must recover exact plaintext
	receiver, err := newRTPCodec(key)
	require.NoError(t, err)

	plain, err := receiver.unwrap(nil, wire)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, payload), "Plaintext must match original payload")
}

// TestRTPWireFormatGoldenFixture фиксирует раскладку заголовка на проводе.
//
// Кодек строится через newRTPCodec, а не литералом: литерал уже один раз
// оставил prng нулевым и уронил тест паникой в wrap. Пиннинг делаем поверх
// готового кодека, подменяя только детерминированные поля.
//
// Timestamp и шифртекст выведены из настенных часов (nonce строится из
// ssrc+sequence+timestamp), поэтому побайтово они не проверяются — их
// корректность подтверждает roundtrip ниже. Всё остальное зафиксировано
// точно: расхождение означает несовместимость с уже развёрнутыми
// серверами профиля, гейтящегося call_vk_parasite_quic.
func TestRTPWireFormatGoldenFixture(t *testing.T) {
	t.Parallel()
	key := [wrapKeyLength]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	}

	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	codec.ssrc = 0x12345678
	codec.initialSeq = 0x0042
	codec.initialTS = 0xdeadbeef
	codec.payloadType = 111

	payload := []byte("golden-fixture-payload-1234567890")
	wire, raw, err := codec.wrap(payload)
	require.NoError(t, err)
	defer codec.putBuffer(raw)

	// Первый байт: V=2, P=1, X=1 -> 0x80|0x20|0x10.
	require.Equal(t, byte(0xb0), wire[0])
	require.Equal(t, byte(111), wire[1])
	require.Equal(t, uint16(0x0042), binary.BigEndian.Uint16(wire[2:4]))
	require.Equal(t, uint32(0x12345678), binary.BigEndian.Uint32(wire[8:12]))

	// RFC 8285 One-Byte Header: profile 0xBEDE, длина 1 слово,
	// ssrc-audio-level (id=1, len=0) с нулевым хвостом до границы слова.
	require.Equal(t,
		[]byte{0xbe, 0xde, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00},
		wire[rtpHeaderLength:rtpTotalHeaderLength],
		"расширение RFC 8285 обязано лежать байт-в-байт по этому смещению")

	// Длина: заголовок с расширением, шифртекст с тегом и паддинг,
	// последний байт которого равен его собственной длине.
	paddingLength := int(wire[len(wire)-1])
	require.GreaterOrEqual(t, paddingLength, 1)
	require.LessOrEqual(t, paddingLength, defaultRTPPadding)
	require.Equal(t,
		rtpTotalHeaderLength+len(payload)+chacha20poly1305.Overhead+paddingLength,
		len(wire),
		"общая длина обязана складываться из заголовка, шифртекста и паддинга")

	receiver, err := newRTPCodec(key)
	require.NoError(t, err)
	recovered, err := receiver.unwrap(nil, wire)
	require.NoError(t, err)
	require.Equal(t, payload, recovered)
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

		plain, err := receiver.unwrap(nil, wire)
		require.NoError(t, err)
		require.Equal(t, payload, plain)

		codec.putBuffer(raw)
	}
}
