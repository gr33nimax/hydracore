package vkparasite

import (
	"net/netip"
	"runtime"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
)

// Внешняя DTLS-запись поверх одного QUIC-пакета: 13 (record header) +
// 8 (explicit nonce) + 16 (tag) для DTLS 1.2 с AES-128-GCM. Держим здесь
// измеренное значение, чтобы тест ловил расхождение с бюджетом из mtu.go.
const measuredDTLSRecordOverhead = 13 + 8 + 16

func benchCodec(tb testing.TB) *rtpCodec {
	tb.Helper()
	key, err := deriveWrapKey("bench-outer-secret")
	if err != nil {
		tb.Fatal(err)
	}
	codec, err := newRTPCodec(key)
	if err != nil {
		tb.Fatal(err)
	}
	return codec
}

func benchWire(tb testing.TB) (*rtpCodec, []byte) {
	tb.Helper()
	codec := benchCodec(tb)
	wire, raw, err := codec.wrap(make([]byte, quicPacketSize+measuredDTLSRecordOverhead))
	if err != nil {
		tb.Fatal(err)
	}
	frozen := append([]byte(nil), wire...)
	codec.putBuffer(raw)
	return codec, frozen
}

// BenchmarkRTPWrap измеряет TX-обёртку одного внешнего пакета.
func BenchmarkRTPWrap(b *testing.B) {
	codec := benchCodec(b)
	payload := make([]byte, quicPacketSize+measuredDTLSRecordOverhead)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		wire, raw, err := codec.wrap(payload)
		if err != nil {
			b.Fatal(err)
		}
		_ = wire
		codec.putBuffer(raw)
	}
}

// BenchmarkRTPUnwrap сравнивает рабочий путь приёма — расшифровка в буфер
// вызывающего — с защитным, где буфера нет и Open аллоцирует сам. Разница и
// есть причина, по которой unwrap принимает dst.
func BenchmarkRTPUnwrap(b *testing.B) {
	for _, variant := range []struct {
		name  string
		reuse bool
	}{{"into_caller_buffer", true}, {"allocating_fallback", false}} {
		b.Run(variant.name, func(b *testing.B) {
			codec, wire := benchWire(b)
			dst := make([]byte, maxCodecWireBuffer)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			for b.Loop() {
				target := dst
				if !variant.reuse {
					target = nil
				}
				if _, err := codec.unwrap(target, wire); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(after.NumGC-before.NumGC), "gc-cycles")
		})
	}
}

// BenchmarkDatagramFramePrefix измеряет заголовок, который строится на каждую
// исходящую UDP-датаграмму.
func BenchmarkDatagramFramePrefix(b *testing.B) {
	for _, dest := range benchDestinations() {
		b.Run(dest.name, func(b *testing.B) {
			assoc := newDatagramAssociation(1, newDatagramRouter(), nil, dest.addr)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := assoc.framePrefix(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchDestinations() []struct {
	name string
	addr M.Socksaddr
} {
	return []struct {
		name string
		addr M.Socksaddr
	}{
		{"ipv4", M.SocksaddrFrom(netip.MustParseAddr("93.184.216.34"), 443)},
		{"ipv6", M.SocksaddrFrom(netip.MustParseAddr("2606:2800:220:1:248:1893:25c8:1946"), 443)},
	}
}
