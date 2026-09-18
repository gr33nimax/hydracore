package xhttp

import (
	"errors"
	"io"
	"net"

	"github.com/sagernet/sing/common/baderror"
	E "github.com/sagernet/sing/common/exceptions"
	"golang.org/x/net/http2"
)

// wrapH2Error keeps a fault of the HTTP/2 stream that carries this transport from
// leaving the transport as a fault of HTTP/2.
//
// A splitConn is a net.Conn, and a net.Conn promises bytes and ordinary errors.
// What sits underneath is an HTTP/2 response body, so a reset stream hands up an
// http2.StreamError naming a stream id of the connection to the XHTTP server.
// Passed through, that error reaches whoever dialled through this outbound — and
// when the caller is itself an HTTP/2 client, x/net/http2 reads the StreamError as
// a fault of one of *its own* streams: transport.go:1885 looks the id up, finds
// nothing on its connection, and reads the next frame. The error is sticky and
// costs no I/O, so the read loop turns into a spin at a full CPU core, for the
// lifetime of the process. Nothing can stop it: the loop never touches the socket
// again, so closing the connection does not reach it, and Transport.Close only
// drops the reference.
//
// A DoH resolver with the outbound as its detour is enough to arrive there, which
// is how a device burnt a core with no traffic on it and went on burning it across
// a change of outbound.
//
// The sibling transports carried over this same normalisation from upstream
// (v2rayhttp, v2raygrpclite, naive inbound); XHTTP was the one that did not.
func wrapH2Error(err error) error {
	if err == nil {
		return nil
	}
	err = baderror.WrapH2(err)
	var streamError http2.StreamError
	if errors.As(err, &streamError) {
		// A stream that ended is a conn that ended. Say so in terms no HTTP/2
		// layer above can take for one of its own streams.
		if streamError.Code == http2.ErrCodeNo {
			return io.EOF
		}
		return E.Cause(net.ErrClosed, streamError.Error())
	}
	return err
}
