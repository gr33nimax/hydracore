package vkparasite

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/sagernet/quic-go"
)

const quicALPN = "hcvk/1"

// Период keep-alive на путь.
//
// Их четыре и они независимы, поэтому каждое сокращение периода умножается на
// четыре пробуждения радио. При MaxIdleTimeout 60 с период 25 с оставляет два
// шанса дожить до простоя вместо четырёх, которые давали 15 с, и снимает
// 40 % пустых пакетов с простаивающего туннеля.
const quicKeepAlivePeriod = 25 * time.Second

func quicConfig() *quic.Config {
	return &quic.Config{
		InitialPacketSize:       quicPacketSize,
		DisablePathMTUDiscovery: true,
		MaxIdleTimeout:          60 * time.Second,
		KeepAlivePeriod:         quicKeepAlivePeriod,
		EnableDatagrams:         true,
		MaxIncomingStreams:      1024,
		MaxIncomingUniStreams:   0,
	}
}

// newQUICTransport готовит транспорт с фиксированной длиной connection ID.
//
// Длина названа, а не унаследована: бюджет DATAGRAM-фрейма считается от места,
// которое остаётся в пакете после заголовка, а destination connection ID в
// исходящем пакете выбирает удалённая сторона. Оба конца ставят одну и ту же
// длину, поэтому размер пакета в обе стороны становится проверяемой величиной
// вместо худшего легального 20.
func newQUICTransport(packetConn net.PacketConn) *quic.Transport {
	return &quic.Transport{
		Conn:               packetConn,
		ConnectionIDLength: quicConnectionIDLength,
	}
}

// dialQUIC поднимает QUIC прямо на обёрнутом пакетном соединении.
//
// Ownership: quic-go не закрывает переданный PacketConn, поэтому транспорт и
// нижние слои закрываются явно, когда сработает quicConn.Context().Done().
func dialQUIC(ctx context.Context, packetConn net.PacketConn, remote net.Addr, closer io.Closer) (*quic.Conn, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Authenticated by the outer key and the inner user attach.
		NextProtos:         []string{quicALPN},
		MinVersion:         tls.VersionTLS13,
	}
	transport := newQUICTransport(packetConn)
	quicConn, err := transport.Dial(ctx, remote, tlsConfig, quicConfig())
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	go func() {
		<-quicConn.Context().Done()
		_ = transport.Close()
		if closer != nil {
			_ = closer.Close()
		}
	}()
	return quicConn, nil
}

// listenQUIC поднимает QUIC-листенер на обёрнутом пакетном соединении одного
// пира. Возвращённый closer закрывает и листенер, и транспорт.
func listenQUIC(packetConn net.PacketConn, cert tls.Certificate) (*quic.Listener, io.Closer, error) {
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{quicALPN},
		MinVersion:   tls.VersionTLS13,
	}
	transport := newQUICTransport(packetConn)
	listener, err := transport.Listen(tlsConfig, quicConfig())
	if err != nil {
		_ = transport.Close()
		return nil, nil, err
	}
	return listener, closerFunc(func() error {
		_ = listener.Close()
		return transport.Close()
	}), nil
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }
