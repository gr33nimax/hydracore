package vkparasite

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v4/stdnet"
	"github.com/pion/turn/v4"
	"github.com/sagernet/sing-box/adapter"
	D "github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type TURNCredentials struct {
	URLs       []string
	Username   string
	Credential string
}

type CredentialProvider func(ctx context.Context, joinLink string) (TURNCredentials, error)

type managedTURNConn struct {
	net.PacketConn
	client    *turn.Client
	base      io.Closer
	closeOnce sync.Once
}

type turnEndpoint struct {
	destination M.Socksaddr
	network     string
	secure      bool
	serverName  string
}

func (c *managedTURNConn) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		closeErr = c.PacketConn.Close()
		c.client.Close()
		if err := c.base.Close(); closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

func allocateTURN(
	ctx context.Context,
	dialer N.Dialer,
	dnsRouter adapter.DNSRouter,
	credentials TURNCredentials,
	preferred int,
	metrics *telemetry.Accumulator,
	workerID uint16,
) (net.PacketConn, error) {
	started := time.Now()
	if credentials.Username == "" || credentials.Credential == "" {
		recordTURNFailure(ctx, metrics, started, workerID, "credentials")
		return nil, errors.New("call vk_parasite: VK returned incomplete TURN credentials")
	}
	udpEndpoints := make([]turnEndpoint, 0, len(credentials.URLs))
	tcpEndpoints := make([]turnEndpoint, 0, len(credentials.URLs))
	for _, rawURL := range credentials.URLs {
		endpoint, parseErr := parseTURNURL(rawURL)
		if parseErr == nil {
			if endpoint.network == "udp" {
				udpEndpoints = append(udpEndpoints, endpoint)
			} else {
				tcpEndpoints = append(tcpEndpoints, endpoint)
			}
		}
	}
	destinations := append(rotateTURNEndpoints(udpEndpoints, preferred), rotateTURNEndpoints(tcpEndpoints, preferred)...)
	if len(destinations) == 0 {
		metrics.Set(telemetry.TURNEndpointCount, 0)
		metrics.Set(telemetry.TURNSelectedEndpointOrdinal, 0)
		recordTURNFailure(ctx, metrics, started, workerID, "no_endpoint")
		return nil, errors.New("call vk_parasite: no usable TURN URL")
	}
	var lastErr error
	metrics.Set(telemetry.TURNEndpointCount, float64(len(destinations)))
	metrics.Set(telemetry.TURNSelectedEndpointOrdinal, 0)
	for offset := 0; offset < len(destinations); offset++ {
		metrics.Add(telemetry.TURNEndpointsTriedTotal, 1)
		endpoint := destinations[offset]
		connection, err := allocateTURNEndpoint(ctx, dialer, dnsRouter, credentials, endpoint)
		if err == nil {
			metrics.Set(telemetry.TURNAllocateLatencyMS, telemetry.LatencyMS(started))
			metrics.Set(telemetry.TURNSelectedEndpointOrdinal, float64(offset+1))
			metrics.Add(telemetry.TURNAllocateSuccessTotal, 1)
			return connection, nil
		}
		lastErr = err
	}
	recordTURNFailure(ctx, metrics, started, workerID, "all_endpoints")
	return nil, fmt.Errorf("call vk_parasite: all VK TURN endpoints failed: %w", lastErr)
}

func rotateTURNEndpoints(endpoints []turnEndpoint, preferred int) []turnEndpoint {
	if len(endpoints) < 2 {
		return append([]turnEndpoint(nil), endpoints...)
	}
	start := preferred % len(endpoints)
	rotated := make([]turnEndpoint, 0, len(endpoints))
	rotated = append(rotated, endpoints[start:]...)
	rotated = append(rotated, endpoints[:start]...)
	return rotated
}

func recordTURNFailure(ctx context.Context, metrics *telemetry.Accumulator, started time.Time, workerID uint16, reason string) {
	metrics.Set(telemetry.TURNAllocateLatencyMS, telemetry.LatencyMS(started))
	if errors.Is(ctx.Err(), context.Canceled) {
		metrics.RecordEvent("turn_allocate_interrupted", "turn", "rebind", &workerID)
		return
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = "timeout"
	}
	metrics.Add(telemetry.TURNAllocateFailureTotal, 1)
	metrics.RecordEvent("turn_allocate_failed", "turn", reason, &workerID)
}

func allocateTURNEndpoint(
	ctx context.Context,
	dialer N.Dialer,
	dnsRouter adapter.DNSRouter,
	credentials TURNCredentials,
	endpoint turnEndpoint,
) (net.PacketConn, error) {
	turnAddress, err := resolveUDPAddress(ctx, dialer, dnsRouter, endpoint.destination, true)
	if err != nil {
		return nil, err
	}
	resolvedDestination := M.SocksaddrFromNet(turnAddress)
	var packetConn net.PacketConn
	var base io.Closer
	if endpoint.network == "udp" {
		udpConn, listenErr := dialer.ListenPacket(ctx, resolvedDestination)
		if listenErr != nil {
			return nil, listenErr
		}
		packetConn = udpConn
		base = udpConn
	} else {
		tcpConn, dialErr := dialer.DialContext(ctx, "tcp", resolvedDestination)
		if dialErr != nil {
			return nil, dialErr
		}
		stream := tcpConn
		if endpoint.secure {
			tlsConn := tls.Client(tcpConn, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: endpoint.serverName})
			if handshakeErr := tlsConn.HandshakeContext(ctx); handshakeErr != nil {
				_ = tcpConn.Close()
				return nil, handshakeErr
			}
			stream = tlsConn
		}
		packetConn = turn.NewSTUNConn(stream)
		base = stream
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = packetConn.SetDeadline(deadline)
	}
	loggerFactory := logging.NewDefaultLoggerFactory()
	loggerFactory.DefaultLogLevel = logging.LogLevelDisabled
	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: turnAddress.String(),
		TURNServerAddr: turnAddress.String(),
		Username:       credentials.Username,
		Password:       credentials.Credential,
		Conn:           packetConn,
		// A zero-value stdnet.Net resolves the already-pinned IP without the
		// Android interface enumeration that can fail under VPN permissions.
		Net:           new(stdnet.Net),
		LoggerFactory: loggerFactory,
	})
	if err != nil {
		_ = base.Close()
		return nil, err
	}
	if err = client.Listen(); err != nil {
		client.Close()
		_ = base.Close()
		return nil, err
	}
	allocation, err := client.Allocate()
	if err != nil {
		client.Close()
		_ = base.Close()
		return nil, err
	}
	_ = packetConn.SetDeadline(time.Time{})
	return &managedTURNConn{PacketConn: allocation, client: client, base: base}, nil
}

func parseTURNURL(rawURL string) (turnEndpoint, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || (!strings.EqualFold(parsed.Scheme, "turn") && !strings.EqualFold(parsed.Scheme, "turns")) {
		return turnEndpoint{}, errors.New("not a TURN URL")
	}
	secure := strings.EqualFold(parsed.Scheme, "turns")
	network := strings.ToLower(parsed.Query().Get("transport"))
	if network == "" {
		if secure {
			network = "tcp"
		} else {
			network = "udp"
		}
	}
	if network != "udp" && network != "tcp" {
		return turnEndpoint{}, errors.New("unsupported TURN transport")
	}
	if secure && network != "tcp" {
		return turnEndpoint{}, errors.New("TURN TLS requires TCP")
	}
	if parsed.User != nil {
		return turnEndpoint{}, errors.New("TURN URL contains unexpected userinfo")
	}
	authority := parsed.Host
	if authority == "" {
		authority = strings.TrimPrefix(parsed.Opaque, "//")
	}
	destination := M.ParseSocksaddr(authority)
	if !destination.IsValid() {
		return turnEndpoint{}, errors.New("TURN URL has no host")
	}
	if destination.Port == 0 {
		if secure {
			destination.Port = 5349
		} else {
			destination.Port = 3478
		}
	}
	serverName := destination.Fqdn
	if serverName == "" && destination.IsIP() {
		serverName = destination.Addr.String()
	}
	return turnEndpoint{destination: destination, network: network, secure: secure, serverName: serverName}, nil
}

func parseTURNUDPURL(rawURL string) (M.Socksaddr, error) {
	endpoint, err := parseTURNURL(rawURL)
	if err != nil {
		return M.Socksaddr{}, err
	}
	if endpoint.network != "udp" || endpoint.secure {
		return M.Socksaddr{}, errors.New("TURN transport is not UDP")
	}
	return endpoint.destination, nil
}

func resolveUDPAddress(
	ctx context.Context,
	dialer N.Dialer,
	dnsRouter adapter.DNSRouter,
	destination M.Socksaddr,
	requireIPv4 bool,
) (*net.UDPAddr, error) {
	if destination.Port == 0 || !destination.IsValid() {
		return nil, errors.New("call vk_parasite: invalid UDP destination")
	}
	if destination.IsIP() {
		if requireIPv4 && !destination.Addr.Unmap().Is4() {
			return nil, errors.New("call vk_parasite: TURN requires an IPv4 address")
		}
		return destination.Unwrap().UDPAddr(), nil
	}
	if dnsRouter == nil {
		return nil, errors.New("call vk_parasite: DNS router unavailable")
	}
	queryOptions := adapter.DNSQueryOptions{}
	if resolveDialer, ok := dialer.(D.ResolveDialer); ok {
		queryOptions = resolveDialer.QueryOptions()
	}
	addresses, err := dnsRouter.Lookup(ctx, destination.Fqdn, queryOptions)
	if err != nil {
		return nil, err
	}
	for _, address := range addresses {
		address = address.Unmap()
		if requireIPv4 && !address.Is4() {
			continue
		}
		return M.SocksaddrFrom(address, destination.Port).UDPAddr(), nil
	}
	return nil, errors.New("call vk_parasite: no usable address returned by DNS")
}
