package vkparasite

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	"github.com/stretchr/testify/require"
)

// ai-generated: test datagram packetConn pair for DTLS/QUIC in-memory tests
type testPacketConnPair struct {
	client net.PacketConn
	server net.PacketConn
}

func newTestPacketConnPair() (*testPacketConnPair, error) {
	clientUDP, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	serverUDP, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		_ = clientUDP.Close()
		return nil, err
	}
	return &testPacketConnPair{
		client: clientUDP,
		server: serverUDP,
	}, nil
}

// ai-generated: unit test for QUIC over DTLS over obfsPacketConn
func TestQUICOverDTLSEcho(t *testing.T) {
	var key [wrapKeyLength]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	pair, err := newTestPacketConnPair()
	require.NoError(t, err)
	defer func() {
		_ = pair.client.Close()
		_ = pair.server.Close()
	}()

	clientCodec, err := newRTPCodec(key)
	require.NoError(t, err)
	serverCodec, err := newRTPCodec(key)
	require.NoError(t, err)

	clientObfs := newObfsPacketConn(pair.client, pair.server.LocalAddr(), clientCodec, nil)
	serverObfs := newObfsPacketConn(pair.server, pair.client.LocalAddr(), serverCodec, nil)
	defer func() {
		_ = clientObfs.Close()
		_ = serverObfs.Close()
	}()

	cert, err := selfsign.GenerateSelfSigned()
	require.NoError(t, err)

	serverDTLSConfig := &dtls.Config{
		Certificates:         []dtls.Certificate{cert},
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		FlightInterval:       100 * time.Millisecond,
		MTU:                  dtlsMTU,
	}

	clientDTLSConfig := &dtls.Config{
		InsecureSkipVerify:   true,
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		FlightInterval:       100 * time.Millisecond,
		MTU:                  dtlsMTU,
	}

	serverDTLSListener, err := dtls.Listen("udp", pair.server.LocalAddr().(*net.UDPAddr), serverDTLSConfig)
	if err != nil {
		// Fall back to connecting DTLS over custom packetConn
		serverDTLSListener = nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var serverDTLS net.Conn
	var clientDTLS net.Conn

	serverErrCh := make(chan error, 1)
	clientErrCh := make(chan error, 1)

	go func() {
		sConn, sErr := dtls.Server(serverObfs, pair.client.LocalAddr(), serverDTLSConfig)
		if sErr != nil {
			serverErrCh <- sErr
			return
		}
		if hsErr := sConn.HandshakeContext(ctx); hsErr != nil {
			_ = sConn.Close()
			serverErrCh <- hsErr
			return
		}
		serverDTLS = sConn
		serverErrCh <- nil
	}()

	go func() {
		cConn, cErr := dtls.Client(clientObfs, pair.server.LocalAddr(), clientDTLSConfig)
		if cErr != nil {
			clientErrCh <- cErr
			return
		}
		if hsErr := cConn.HandshakeContext(ctx); hsErr != nil {
			_ = cConn.Close()
			clientErrCh <- hsErr
			return
		}
		clientDTLS = cConn
		clientErrCh <- nil
	}()

	require.NoError(t, <-serverErrCh)
	require.NoError(t, <-clientErrCh)

	defer func() {
		if serverDTLS != nil {
			_ = serverDTLS.Close()
		}
		if clientDTLS != nil {
			_ = clientDTLS.Close()
		}
	}()

	quicListener, err := listenQUIC(serverDTLS, cert)
	require.NoError(t, err)
	defer func() { _ = quicListener.Close() }()

	quicClientConn, err := dialQUIC(ctx, clientDTLS, nil)
	require.NoError(t, err)
	defer func() { _ = quicClientConn.CloseWithError(0, "") }()

	quicServerConn, err := quicListener.Accept(ctx)
	require.NoError(t, err)
	defer func() { _ = quicServerConn.CloseWithError(0, "") }()

	// Transfer 1 MB of test data over QUIC stream
	const dataSize = 1024 * 1024
	testData := make([]byte, dataSize)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	serverDone := make(chan error, 1)
	go func() {
		stream, aErr := quicServerConn.AcceptStream(ctx)
		if aErr != nil {
			serverDone <- aErr
			return
		}
		defer stream.Close()

		received := make([]byte, dataSize)
		_, rErr := io.ReadFull(stream, received)
		if rErr != nil {
			serverDone <- rErr
			return
		}
		if !bytes.Equal(received, testData) {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		_, wErr := stream.Write(received)
		serverDone <- wErr
	}()

	clientStream, err := quicClientConn.OpenStreamSync(ctx)
	require.NoError(t, err)
	defer clientStream.Close()

	_, err = clientStream.Write(testData)
	require.NoError(t, err)

	echoReceived := make([]byte, dataSize)
	_, err = io.ReadFull(clientStream, echoReceived)
	require.NoError(t, err)
	require.True(t, bytes.Equal(echoReceived, testData))

	require.NoError(t, <-serverDone)
}
