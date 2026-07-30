package vless

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/protocol/vless/encryption"
	vmess "github.com/sagernet/sing-vmess"
	vlessProtocol "github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

// protocolClient keeps the upstream sing-vmess implementation as the default
// path and only inserts the optional record layer when encryption is present.
// Vision receives that authenticated record layer when encryption is enabled.
type protocolClient struct {
	base       *vlessProtocol.Client
	encryption *encryption.Client
}

func newProtocolClient(ctx context.Context, userID, flow, encryptionConfig string, logger logger.Logger) (*protocolClient, error) {
	base, err := vlessProtocol.NewClient(userID, flow, logger)
	if err != nil {
		return nil, err
	}
	client := &protocolClient{
		base: base,
	}
	switch encryptionConfig {
	case "", "none":
		return client, nil
	default:
		client.encryption, err = encryption.NewClient(ctx, encryptionConfig)
		if err != nil {
			return nil, E.Cause(err, "initialize VLESS encryption")
		}
		logger.Info("VLESS post-quantum encryption enabled")
		return client, nil
	}
}

func (c *protocolClient) encrypt(ctx context.Context, conn net.Conn) (net.Conn, error) {
	if c.encryption == nil {
		return conn, nil
	}
	encryptedConn, err := c.encryption.HandshakeContext(ctx, conn)
	if err != nil {
		common.Close(conn)
		return nil, E.Cause(err, "VLESS encryption handshake")
	}
	return encryptedConn, nil
}

func (c *protocolClient) DialEarlyConn(ctx context.Context, conn net.Conn, destination M.Socksaddr) (net.Conn, error) {
	if c.encryption == nil {
		return c.base.DialEarlyConn(conn, destination)
	}
	encryptedConn, err := c.encrypt(ctx, conn)
	if err != nil {
		return nil, err
	}
	// Vision must inspect the authenticated encryption layer here. Its
	// Upstream still preserves the original TLS/Reality/V2Ray transport, while
	// using the pre-encryption connection as the Vision base fails for raw TCP
	// and opaque transports such as XHTTP, WebSocket and gRPC. Splicing remains
	// disabled because the authenticated record layer may not be bypassed.
	return c.base.DialEarlyConnWithOptions(encryptedConn, encryptedConn, destination, false)
}

func (c *protocolClient) DialEarlyPacketConn(ctx context.Context, conn net.Conn, destination M.Socksaddr) (*vlessProtocol.PacketConn, error) {
	if c.encryption == nil {
		return c.base.DialEarlyPacketConn(conn, destination)
	}
	encryptedConn, err := c.encrypt(ctx, conn)
	if err != nil {
		return nil, err
	}
	packetConn, err := c.base.DialEarlyPacketConn(encryptedConn, destination)
	if err != nil {
		common.Close(conn)
		return nil, err
	}
	return packetConn, nil
}

func (c *protocolClient) DialEarlyXUDPPacketConn(ctx context.Context, conn net.Conn, destination M.Socksaddr) (vmess.PacketConn, error) {
	if c.encryption == nil {
		return c.base.DialEarlyXUDPPacketConn(conn, destination)
	}
	encryptedConn, err := c.encrypt(ctx, conn)
	if err != nil {
		return nil, err
	}
	return c.base.DialEarlyXUDPPacketConn(encryptedConn, destination)
}
