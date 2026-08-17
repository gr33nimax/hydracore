package vkparasite

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func TestParseTURNUDPURL(t *testing.T) {
	t.Parallel()
	address, err := parseTURNUDPURL("turn:relay.example.invalid:3478?transport=udp")
	require.NoError(t, err)
	require.Equal(t, "relay.example.invalid", address.Fqdn)
	require.Equal(t, uint16(3478), address.Port)
	address, err = parseTURNUDPURL("turn://192.0.2.10")
	require.NoError(t, err)
	require.Equal(t, uint16(3478), address.Port)
	_, err = parseTURNUDPURL("turns:relay.example.invalid:5349?transport=tcp")
	require.Error(t, err)
	tcpEndpoint, err := parseTURNURL("turn:relay.example.invalid:3478?transport=tcp")
	require.NoError(t, err)
	require.Equal(t, "tcp", tcpEndpoint.network)
	require.False(t, tcpEndpoint.secure)
	tlsEndpoint, err := parseTURNURL("turns:relay.example.invalid:5349?transport=tcp")
	require.NoError(t, err)
	require.Equal(t, "tcp", tlsEndpoint.network)
	require.True(t, tlsEndpoint.secure)
}

func TestRebindInterruptionsAreNotTransportFailures(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	metrics := telemetry.NewAccumulator()
	recordTURNFailure(ctx, metrics, time.Now(), 2, "all_endpoints")
	require.Zero(t, metrics.Value(telemetry.TURNAllocateFailureTotal))
	events := metrics.DrainEvents(1)
	require.Len(t, events, 1)
	require.Equal(t, "turn_allocate_interrupted", events[0].Event)
	require.Equal(t, "rebind", events[0].Reason)

	client := &Client{metrics: metrics}
	client.recordInnerAuthFailure(client.metrics, ctx, time.Now(), 2, "read")
	require.Zero(t, metrics.Value(telemetry.InnerAuthFailureTotal))
	events = metrics.DrainEvents(1)
	require.Len(t, events, 1)
	require.Equal(t, "inner_auth_interrupted", events[0].Event)
	require.Equal(t, "rebind", events[0].Reason)
}

func TestSetupDeadlineRemainsFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	metrics := telemetry.NewAccumulator()
	recordTURNFailure(ctx, metrics, time.Now(), 1, "all_endpoints")
	require.Equal(t, float64(1), metrics.Value(telemetry.TURNAllocateFailureTotal))
	events := metrics.DrainEvents(1)
	require.Len(t, events, 1)
	require.Equal(t, "timeout", events[0].Reason)
}

type mockDNSRouter struct {
	adapter.DNSRouter
	lookupFunc func(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error)
}

func (m *mockDNSRouter) Lookup(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error) {
	if m.lookupFunc != nil {
		return m.lookupFunc(ctx, domain, options)
	}
	return nil, nil
}

type testPacketConnTracker struct {
	net.PacketConn
	closed bool
	mu     sync.Mutex
}

func (c *testPacketConnTracker) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.PacketConn != nil {
		return c.PacketConn.Close()
	}
	return nil
}

func (c *testPacketConnTracker) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type testCustomDialer struct {
	N.Dialer
	listenPacketFunc func(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error)
	dialContextFunc  func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

func (d *testCustomDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if d.listenPacketFunc != nil {
		return d.listenPacketFunc(ctx, destination)
	}
	return nil, errors.New("listen packet not configured")
}

func (d *testCustomDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if d.dialContextFunc != nil {
		return d.dialContextFunc(ctx, network, destination)
	}
	return nil, errors.New("dial context not configured")
}

func TestResolveUDPAddressesDeterministicSorting(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ip1 := netip.MustParseAddr("198.51.100.2")
	ip2 := netip.MustParseAddr("192.0.2.1")
	ip3 := netip.MustParseAddr("203.0.113.5")
	ip6 := netip.MustParseAddr("2001:db8::1")

	// Order in DNS return is scrambled
	router := &mockDNSRouter{
		lookupFunc: func(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error) {
			return []netip.Addr{ip1, ip3, ip6, ip2}, nil
		},
	}

	dest := M.ParseSocksaddr("relay.example.invalid:3478")
	addrs, err := resolveUDPAddresses(ctx, nil, router, dest, true)
	require.NoError(t, err)
	require.Len(t, addrs, 3)
	// Deterministically sorted via netip.Addr.Compare
	require.Equal(t, []netip.Addr{ip2, ip1, ip3}, addrs)

	// IP literal is returned as single-element slice
	literalDest := M.ParseSocksaddr("198.51.100.2:3478")
	literalAddrs, err := resolveUDPAddresses(ctx, nil, router, literalDest, true)
	require.NoError(t, err)
	require.Equal(t, []netip.Addr{ip1}, literalAddrs)

	// IPv6 filtered when requireIPv4 is true
	v6Literal := M.ParseSocksaddr("[2001:db8::1]:3478")
	_, err = resolveUDPAddresses(ctx, nil, router, v6Literal, true)
	require.Error(t, err)
}

func TestAllocateTURNEndpointWorkerRotationAndFallback(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ip1 := netip.MustParseAddr("192.0.2.1")
	ip2 := netip.MustParseAddr("198.51.100.2")
	ip3 := netip.MustParseAddr("203.0.113.5")

	router := &mockDNSRouter{
		lookupFunc: func(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error) {
			return []netip.Addr{ip3, ip1, ip2}, nil
		},
	}

	dest := M.ParseSocksaddr("relay.example.invalid:3478")
	endpoint := turnEndpoint{destination: dest, network: "udp"}
	creds := TURNCredentials{Username: "user", Credential: "password"}

	// (a) IP#1 fails on ListenPacket, IP#2 fails on ListenPacket, IP#3 fails on ListenPacket
	var dialedOrder []netip.Addr
	dialer := &testCustomDialer{
		listenPacketFunc: func(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
			dialedOrder = append(dialedOrder, destination.Addr)
			return nil, fmt.Errorf("connect failed for %s", destination.Addr)
		},
	}

	dialedOrder = nil
	_, err := allocateTURNEndpoint(ctx, dialer, router, creds, endpoint, 0)
	require.Error(t, err)
	// Preferred 0 starts at sorted index 0: ip1, then ip2, then ip3
	require.Equal(t, []netip.Addr{ip1, ip2, ip3}, dialedOrder)

	// (b) preferred=1 starts with IP#2
	dialedOrder = nil
	_, err = allocateTURNEndpoint(ctx, dialer, router, creds, endpoint, 1)
	require.Error(t, err)
	require.Equal(t, []netip.Addr{ip2, ip3, ip1}, dialedOrder)

	// (c) single address behaves as before
	singleRouter := &mockDNSRouter{
		lookupFunc: func(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error) {
			return []netip.Addr{ip1}, nil
		},
	}
	dialedOrder = nil
	_, err = allocateTURNEndpoint(ctx, dialer, singleRouter, creds, endpoint, 2)
	require.Error(t, err)
	require.Equal(t, []netip.Addr{ip1}, dialedOrder)

	// (d) all addresses fail -> returns last error and no socket leak
	createdConns := make([]*testPacketConnTracker, 0)
	dialerWithLeakCheck := &testCustomDialer{
		listenPacketFunc: func(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
			// Return a packet conn whose client.Listen/Allocate will fail because nothing is listening
			realUDP, _ := net.ListenPacket("udp", "127.0.0.1:0")
			tracker := &testPacketConnTracker{PacketConn: realUDP}
			createdConns = append(createdConns, tracker)
			return tracker, nil
		},
	}
	// Short deadline so Pion TURN Allocate fails quickly
	allocCtx, allocCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer allocCancel()
	_, err = allocateTURNEndpoint(allocCtx, dialerWithLeakCheck, router, creds, endpoint, 0)
	require.Error(t, err)
	for _, conn := range createdConns {
		require.True(t, conn.isClosed())
	}
}
