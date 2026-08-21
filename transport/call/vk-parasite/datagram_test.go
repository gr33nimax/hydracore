package vkparasite

import (
	"context"
	"net"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

// ai-generated: unit tests for UDP over QUIC datagrams
func TestDatagramRoundtrip(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 1)
	defer cleanup()

	_ = serverRelay

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dest := M.ParseSocksaddr("1.1.1.1:53")
	packetConn, err := clientRelay.ListenPacket(ctx, dest.String())
	require.NoError(t, err)
	defer packetConn.Close()

	payload := []byte("dns-query-test-payload")
	n, err := packetConn.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
}

func TestDatagramOversizeRejected(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 1)
	defer cleanup()

	_ = serverRelay

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dest := M.ParseSocksaddr("1.1.1.1:53")
	packetConn, err := clientRelay.ListenPacket(ctx, dest.String())
	require.NoError(t, err)
	defer packetConn.Close()

	oversizedPayload := make([]byte, quicPacketSize+100)
	_, err = packetConn.Write(oversizedPayload)
	require.Error(t, err)
}
