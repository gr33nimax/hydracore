package adapter

import (
	"context"
	"net"

	N "github.com/sagernet/sing/common/network"
)

type V2RayServerTransport interface {
	Network() []string
	Serve(listener net.Listener) error
	ServePacket(listener net.PacketConn) error
	Close() error
}

type V2RayServerTransportHandler interface {
	N.TCPConnectionHandlerEx
}

type V2RayClientTransport interface {
	DialContext(ctx context.Context) (net.Conn, error)
	Close() error
}

// V2RayClientTransportResetter is implemented by transports whose connection
// pools can be discarded without making the transport object terminal.
type V2RayClientTransportResetter interface {
	Reset()
}

// ResetV2RayClientTransport drops connections pinned to an old interface.
// Legacy transports retain their existing Close-based reset behavior.
func ResetV2RayClientTransport(transport V2RayClientTransport) {
	if transport == nil {
		return
	}
	if resetter, isResettable := transport.(V2RayClientTransportResetter); isResettable {
		resetter.Reset()
		return
	}
	_ = transport.Close()
}
