package vkparasite

import (
	"crypto/rand"
	"net/netip"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
)

func TestMTUBudget(t *testing.T) {
	t.Parallel()
	require.GreaterOrEqual(t, quicPacketSize, quicMinimumPacketSize,
		"QUIC требует Initial-пакет не меньше 1200 байт")
	require.LessOrEqual(t, quicPacketSize+pathOverheadTotal, conservativePathMTU,
		"QUIC-пакет вместе со всеми внешними накладными обязан влезать в path MTU")
	require.LessOrEqual(t,
		quicPacketSize+overheadRTPHeader+overheadRTPAEAD+overheadRTPPadding,
		maxCodecWireBuffer,
		"обёрнутый пакет обязан влезать в буфер пула из obfs.go")
	require.Equal(t, rtpTotalHeaderLength, overheadRTPHeader,
		"бюджет обязан считать заголовок вместе с RFC 8285 extension")
	require.Equal(t, defaultRTPPadding, overheadRTPPadding,
		"бюджет обязан считать худший случай паддинга")
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

	// Наибольший внешний QUIC-пакет, который может уйти в обёртку.
	payload := make([]byte, quicPacketSize)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	// Длина паддинга случайна в диапазоне 1..defaultRTPPadding, поэтому
	// прогоняем достаточно раз, чтобы поймать худший случай.
	worst := 0
	for i := 0; i < 4096; i++ {
		wire, raw, wrapErr := codec.wrap(payload)
		require.NoError(t, wrapErr)
		onWire := len(wire) + overheadTURNChannel + overheadIPUDP
		require.LessOrEqual(t, onWire, conservativePathMTU,
			"обёрнутый пакет обязан влезать в консервативный path MTU")
		require.LessOrEqual(t, len(wire), maxCodecWireBuffer,
			"обёрнутый пакет обязан влезать в буфер пула")
		if onWire > worst {
			worst = onWire
		}
		codec.putBuffer(raw)
	}
	require.Equal(t, quicPacketSize+pathOverheadTotal, worst,
		"pathOverheadTotal обязан совпадать с измеренным худшим случаем")
	require.Equal(t, overheadRTPAEAD, chacha20poly1305.Overhead,
		"overheadRTPAEAD в mtu.go должен совпадать с тегом AEAD")
}

// TestDatagramFrameFitsQUICPacket закрепляет второй бюджет пути: payload
// DATAGRAM-фрейма плюс заголовок фрейма и накладные внешнего QUIC-пакета
// обязаны влезать в quicPacketSize.
func TestDatagramFrameFitsQUICPacket(t *testing.T) {
	t.Parallel()
	require.LessOrEqual(t,
		maxDatagramFramePayload+overheadDatagramFrame+overheadQUICHeader+overheadQUICAEAD,
		quicPacketSize,
		"DATAGRAM-фрейм обязан физически влезать во внешний QUIC-пакет")
	// Порог quic-go считает худший легальный connection ID (20), а не тот,
	// который выдан. Уйти выше него нельзя: SendDatagram отдаст
	// DatagramTooLargeError.
	require.LessOrEqual(t, maxDatagramFramePayload, quicSendDatagramLimit,
		"payload обязан проходить порог SendDatagram в quic-go")
	require.Positive(t, maxDatagramFramePayload)
	require.Equal(t, quicConnectionIDLength, newQUICTransport(nil).ConnectionIDLength,
		"физический бюджет опирается на фиксированную длину connection ID")
}

// TestTypicalInnerDatagramTravelsUnfragmented — то, ради чего бюджет считается.
//
// Внутренние приложения гонят QUIC пакетами по 1200–1250 байт. Пока бюджет был
// ниже, каждый такой пакет делился на два ненадёжных фрагмента: удвоенный
// packet rate через обёртку и TURN, и потеря любого из двух убивала всю
// датаграмму.
func TestTypicalInnerDatagramTravelsUnfragmented(t *testing.T) {
	t.Parallel()
	for _, destination := range []struct {
		name string
		addr M.Socksaddr
	}{
		{"ipv4", M.SocksaddrFrom(netip.MustParseAddr("93.184.216.34"), 443)},
		{"ipv6", M.SocksaddrFrom(netip.MustParseAddr("2606:2800:220:1:248:1893:25c8:1946"), 443)},
	} {
		t.Run(destination.name, func(t *testing.T) {
			assoc := newDatagramAssociation(1, newDatagramRouter(), nil, destination.addr)
			require.GreaterOrEqual(t, assoc.fragmentBudget(), quicPacketSize-70,
				"бюджет обязан вмещать внутренний QUIC-пакет целиком")
			require.GreaterOrEqual(t, assoc.fragmentBudget(), 1250,
				"внутренняя датаграмма 1250 байт обязана уезжать одним фрагментом")
		})
	}
}

// TestFragmentBudgetDoesNotShrinkOverTime закрепляет резерв под uvarint'ы:
// бюджет обязан быть одинаковым на первой и на миллионной датаграмме.
func TestFragmentBudgetDoesNotShrinkOverTime(t *testing.T) {
	t.Parallel()
	assoc := newDatagramAssociation(
		1<<20,
		newDatagramRouter(),
		nil,
		M.SocksaddrFrom(netip.MustParseAddr("93.184.216.34"), 443),
	)
	first := assoc.fragmentBudget()
	assoc.nextPacket.Store(1 << 30)
	require.Equal(t, first, assoc.fragmentBudget())

	prefix, err := assoc.framePrefix()
	require.NoError(t, err)
	require.LessOrEqual(t, len(prefix), assoc.framePrefixReserve(),
		"фактический заголовок обязан влезать в резерв, из которого считается бюджет")
}
