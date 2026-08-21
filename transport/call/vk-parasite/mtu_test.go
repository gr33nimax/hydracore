package vkparasite

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
)

func TestMTUBudget(t *testing.T) {
	t.Parallel()
	require.GreaterOrEqual(t, quicPacketSize, quicMinimumPacketSize,
		"QUIC требует Initial-пакет не меньше 1200 байт")
	require.LessOrEqual(t, quicPacketSize+overheadDTLSRecord, dtlsMTU,
		"QUIC-пакет плюс DTLS-запись обязаны влезать в dtlsMTU")
	require.LessOrEqual(t,
		dtlsMTU+overheadRTPHeader+overheadRTPAEAD+overheadRTPPadding,
		maxCodecWireBuffer,
		"обёрнутый пакет обязан влезать в буфер пула из obfs.go")
	require.Equal(t, rtpTotalHeaderLength, overheadRTPHeader,
		"бюджет обязан считать заголовок вместе с RFC 8285 extension")
}

// TestWrappedPacketFitsPathMTU измеряет фактический вывод rtpCodec.wrap, а не
// пересчитывает те же константы, что и mtu.go. Арифметический тест не заметил
// бы роста заголовка на 8 байт при добавлении RFC 8285 extension, потому что
// обе стороны сравнения брались из одной устаревшей константы.
func TestWrappedPacketFitsPathMTU(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	// Наибольшая датаграмма, которую может отдать DTLS с MTU = dtlsMTU.
	payload := make([]byte, dtlsMTU)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	// Длина паддинга случайна в диапазоне 1..defaultRTPPadding, поэтому
	// прогоняем достаточно раз, чтобы поймать худший случай.
	for i := 0; i < 256; i++ {
		wire, raw, wrapErr := codec.wrap(payload)
		require.NoError(t, wrapErr)
		onWire := len(wire) + overheadTURNChannel + overheadIPUDP
		require.LessOrEqual(t, onWire, conservativePathMTU,
			"обёрнутый пакет обязан влезать в консервативный path MTU")
		require.LessOrEqual(t, len(wire), maxCodecWireBuffer,
			"обёрнутый пакет обязан влезать в буфер пула")
		codec.putBuffer(raw)
	}
}

// TestWrappedQUICPacketFitsPathMTU проверяет тот же инвариант для пакета
// размера quicPacketSize, обёрнутого вместе с накладными DTLS.
func TestWrappedQUICPacketFitsPathMTU(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := make([]byte, quicPacketSize+overheadDTLSRecord)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	for i := 0; i < 64; i++ {
		wire, raw, wrapErr := codec.wrap(payload)
		require.NoError(t, wrapErr)
		require.LessOrEqual(t,
			len(wire)+overheadTURNChannel+overheadIPUDP,
			conservativePathMTU)
		codec.putBuffer(raw)
	}
	require.Equal(t, overheadRTPAEAD, chacha20poly1305.Overhead,
		"overheadRTPAEAD в mtu.go должен совпадать с тегом AEAD")
}
