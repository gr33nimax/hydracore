package vkparasite

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/sagernet/quic-go"
)

// ai-generated: QUIC connection layer over DTLS datagram stream
const quicALPN = "hcvk/1"

type datagramPacketConn struct {
	conn net.Conn
}

func newDatagramPacketConn(conn net.Conn) *datagramPacketConn {
	return &datagramPacketConn{conn: conn}
}

func (c *datagramPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = c.conn.Read(p)
	if err != nil {
		return 0, nil, err
	}
	return n, c.conn.RemoteAddr(), nil
}

func (c *datagramPacketConn) WriteTo(p []byte, _ net.Addr) (n int, err error) {
	return c.conn.Write(p)
}

func (c *datagramPacketConn) Close() error {
	return c.conn.Close()
}

func (c *datagramPacketConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *datagramPacketConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *datagramPacketConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *datagramPacketConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func quicConfig() *quic.Config {
	return &quic.Config{
		InitialPacketSize:       quicPacketSize,
		DisablePathMTUDiscovery: true,
		MaxIdleTimeout:          60 * time.Second,
		KeepAlivePeriod:         15 * time.Second,
		EnableDatagrams:         true,
		MaxIncomingStreams:      1024,
		MaxIncomingUniStreams:   0,
	}
}

// dialQUIC establishes a QUIC connection over an existing DTLS connection.
// Ownership: quic-go does not close the underlying PacketConn when the connection ends,
// so lower layers are explicitly closed when quicConn.Context().Done() fires.
func dialQUIC(ctx context.Context, dtlsConn net.Conn, closer io.Closer) (*quic.Conn, error) {
	packetConn := newDatagramPacketConn(dtlsConn)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Authenticated by the outer key and the inner user attach.
		NextProtos:         []string{quicALPN},
		MinVersion:         tls.VersionTLS13,
	}
	quicConn, err := quic.Dial(ctx, packetConn, dtlsConn.RemoteAddr(), tlsConfig, quicConfig())
	if err != nil {
		_ = packetConn.Close()
		return nil, err
	}
	go func() {
		<-quicConn.Context().Done()
		_ = packetConn.Close()
		if closer != nil {
			_ = closer.Close()
		}
	}()
	return quicConn, nil
}

// listenQUIC creates a QUIC listener over a server-side DTLS connection.
func listenQUIC(dtlsConn net.Conn, cert tls.Certificate) (*quic.Listener, error) {
	packetConn := newDatagramPacketConn(dtlsConn)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{quicALPN},
		MinVersion:   tls.VersionTLS13,
	}
	return quic.Listen(packetConn, tlsConfig, quicConfig())
}
