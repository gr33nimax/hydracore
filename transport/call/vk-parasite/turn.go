package vkparasite

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"sort"
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
	client      *turn.Client
	base        io.Closer
	turnAddress net.Addr
	closeOnce   sync.Once
}

type turnEndpoint struct {
	rawURL      string
	destination M.Socksaddr
	network     string
	secure      bool
	serverName  string
}

type turnEndpointScore struct {
	mu          sync.RWMutex
	failures    int
	lastSuccess time.Time
}

var turnEndpointQualityRegistry sync.Map

func getTURNEndpointPenalty(rawURL string) int {
	if rawURL == "" {
		return 0
	}
	val, ok := turnEndpointQualityRegistry.Load(rawURL)
	if !ok {
		return 0
	}
	stat := val.(*turnEndpointScore)
	stat.mu.RLock()
	defer stat.mu.RUnlock()
	return stat.failures
}

func recordTURNEndpointSuccess(rawURL string) {
	if rawURL == "" {
		return
	}
	val, _ := turnEndpointQualityRegistry.LoadOrStore(rawURL, &turnEndpointScore{})
	stat := val.(*turnEndpointScore)
	stat.mu.Lock()
	stat.failures = max(0, stat.failures-1)
	stat.lastSuccess = time.Now()
	stat.mu.Unlock()
}

func recordTURNEndpointFailure(rawURL string) {
	if rawURL == "" {
		return
	}
	val, _ := turnEndpointQualityRegistry.LoadOrStore(rawURL, &turnEndpointScore{})
	stat := val.(*turnEndpointScore)
	stat.mu.Lock()
	stat.failures++
	stat.mu.Unlock()
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
		connection, err := allocateTURNEndpoint(ctx, dialer, dnsRouter, credentials, endpoint, preferred)
		if err == nil {
			recordTURNEndpointSuccess(endpoint.rawURL)
			metrics.Set(telemetry.TURNAllocateLatencyMS, telemetry.LatencyMS(started))
			metrics.Set(telemetry.TURNSelectedEndpointOrdinal, float64(offset+1))
			metrics.Add(telemetry.TURNAllocateSuccessTotal, 1)
			return connection, nil
		}
		recordTURNEndpointFailure(endpoint.rawURL)
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
	sort.SliceStable(rotated, func(i, j int) bool {
		return getTURNEndpointPenalty(rotated[i].rawURL) < getTURNEndpointPenalty(rotated[j].rawURL)
	})
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
	preferred int,
) (net.PacketConn, error) {
	addrs, err := resolveUDPAddresses(ctx, dialer, dnsRouter, endpoint.destination, true)
	if err != nil {
		return nil, err
	}
	n := len(addrs)
	start := preferred % n
	if start < 0 {
		start += n
	}
	var lastErr error
	for i := 0; i < n; i++ {
		candidateAddr := addrs[(start+i)%n]
		turnAddress := M.SocksaddrFrom(candidateAddr, endpoint.destination.Port).UDPAddr()
		resolvedDestination := M.SocksaddrFrom(candidateAddr, endpoint.destination.Port)
		var packetConn net.PacketConn
		var base io.Closer
		if endpoint.network == "udp" {
			udpConn, listenErr := dialer.ListenPacket(ctx, resolvedDestination)
			if listenErr != nil {
				lastErr = listenErr
				continue
			}
			if setter, ok := udpConn.(packetSocketBufferSetter); ok {
				_ = setter.SetReadBuffer(2 * 1024 * 1024)
				_ = setter.SetWriteBuffer(2 * 1024 * 1024)
			}
			packetConn = udpConn
			base = udpConn
		} else {
			tcpConn, dialErr := dialer.DialContext(ctx, "tcp", resolvedDestination)
			if dialErr != nil {
				lastErr = dialErr
				continue
			}
			stream := tcpConn
			if endpoint.secure {
				tlsConn := tls.Client(tcpConn, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: endpoint.serverName})
				if handshakeErr := tlsConn.HandshakeContext(ctx); handshakeErr != nil {
					_ = tcpConn.Close()
					lastErr = handshakeErr
					continue
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
		client, clientErr := turn.NewClient(&turn.ClientConfig{
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
		if clientErr != nil {
			_ = base.Close()
			lastErr = clientErr
			continue
		}
		if listenErr := client.Listen(); listenErr != nil {
			client.Close()
			_ = base.Close()
			lastErr = listenErr
			continue
		}
		allocation, allocErr := client.Allocate()
		if allocErr != nil {
			client.Close()
			_ = base.Close()
			lastErr = allocErr
			continue
		}
		_ = packetConn.SetDeadline(time.Time{})
		return &managedTURNConn{PacketConn: allocation, client: client, base: base, turnAddress: turnAddress}, nil
	}
	return nil, lastErr
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
	return turnEndpoint{rawURL: rawURL, destination: destination, network: network, secure: secure, serverName: serverName}, nil
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

func resolveUDPAddresses(
	ctx context.Context,
	dialer N.Dialer,
	dnsRouter adapter.DNSRouter,
	destination M.Socksaddr,
	requireIPv4 bool,
) ([]netip.Addr, error) {
	if destination.Port == 0 || !destination.IsValid() {
		return nil, errors.New("call vk_parasite: invalid UDP destination")
	}
	if destination.IsIP() {
		addr := destination.Addr.Unmap()
		if requireIPv4 && !addr.Is4() {
			return nil, errors.New("call vk_parasite: TURN requires an IPv4 address")
		}
		return []netip.Addr{addr}, nil
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
	var filtered []netip.Addr
	for _, address := range addresses {
		address = address.Unmap()
		if requireIPv4 && !address.Is4() {
			continue
		}
		filtered = append(filtered, address)
	}
	if len(filtered) == 0 {
		return nil, errors.New("call vk_parasite: no usable address returned by DNS")
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Compare(filtered[j]) < 0
	})
	return filtered, nil
}

func resolveUDPAddress(
	ctx context.Context,
	dialer N.Dialer,
	dnsRouter adapter.DNSRouter,
	destination M.Socksaddr,
	requireIPv4 bool,
) (*net.UDPAddr, error) {
	addrs, err := resolveUDPAddresses(ctx, dialer, dnsRouter, destination, requireIPv4)
	if err != nil {
		return nil, err
	}
	return M.SocksaddrFrom(addrs[0], destination.Port).UDPAddr(), nil
}
