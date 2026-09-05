package vkparasite

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

// TestQUICOverObfsEcho гоняет мегабайт по QUIC, поднятому прямо на
// RTP-обёртке. Слоя DTLS между ними больше нет.
func TestQUICOverObfsEcho(t *testing.T) {
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

	clientObfs := newObfsPacketConn(pair.client, pair.server.LocalAddr(), clientCodec)
	serverObfs := newObfsPacketConn(pair.server, pair.client.LocalAddr(), serverCodec)

	cert, err := newSelfSignedCertificate()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	quicListener, listenerCloser, err := listenQUIC(serverObfs, cert)
	require.NoError(t, err)
	defer func() { _ = listenerCloser.Close() }()

	quicClientConn, err := dialQUIC(ctx, clientObfs, pair.server.LocalAddr(), clientObfs)
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
