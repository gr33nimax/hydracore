package wtsignal

import (
	"context"
	"errors"
	"net"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type recordingDialer struct {
	network     string
	destination M.Socksaddr
	err         error
}

func (d *recordingDialer) DialContext(_ context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.network = network
	d.destination = destination
	return nil, d.err
}

func TestDialUsesProvidedUDPDialer(t *testing.T) {
	dialErr := errors.New("dial stopped for test")
	dialer := &recordingDialer{err: dialErr}

	_, err := Dial(
		"https://calls.example.test/session?token=redacted",
		"calls.example.test",
		"192.0.2.10",
		dialer,
	)
	if !errors.Is(err, dialErr) {
		t.Fatalf("expected wrapped dial error, got %v", err)
	}
	if dialer.network != N.NetworkUDP {
		t.Fatalf("expected UDP dial, got %q", dialer.network)
	}
	if got := dialer.destination.String(); got != "192.0.2.10:443" {
		t.Fatalf("unexpected destination %q", got)
	}
}
