package multiuser

import (
	"context"
	"errors"
	"fmt"
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
	base      net.PacketConn
	closeOnce sync.Once
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
		return nil, errors.New("call multi_user: VK returned incomplete TURN credentials")
	}
	destinations := make([]M.Socksaddr, 0, len(credentials.URLs))
	for _, rawURL := range credentials.URLs {
		turnDestination, parseErr := parseTURNUDPURL(rawURL)
		if parseErr == nil {
			destinations = append(destinations, turnDestination)
		}
	}
	if len(destinations) == 0 {
		recordTURNFailure(ctx, metrics, started, workerID, "no_endpoint")
		return nil, errors.New("call multi_user: no usable UDP TURN URL")
	}
	var lastErr error
	start := preferred % len(destinations)
	for offset := 0; offset < len(destinations); offset++ {
		metrics.Add(telemetry.TURNEndpointsTriedTotal, 1)
		turnDestination := destinations[(start+offset)%len(destinations)]
		connection, err := allocateTURNEndpoint(ctx, dialer, dnsRouter, credentials, turnDestination)
		if err == nil {
			metrics.Set(telemetry.TURNAllocateLatencyMS, telemetry.LatencyMS(started))
			metrics.Add(telemetry.TURNAllocateSuccessTotal, 1)
			return connection, nil
		}
		lastErr = err
	}
	recordTURNFailure(ctx, metrics, started, workerID, "all_endpoints")
	return nil, fmt.Errorf("call multi_user: all VK TURN endpoints failed: %w", lastErr)
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
	turnDestination M.Socksaddr,
) (net.PacketConn, error) {
	turnAddress, err := resolveUDPAddress(ctx, dialer, dnsRouter, turnDestination, true)
	if err != nil {
		return nil, err
	}
	resolvedDestination := M.SocksaddrFromNet(turnAddress)
	base, err := dialer.ListenPacket(ctx, resolvedDestination)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = base.SetDeadline(deadline)
	}
	loggerFactory := logging.NewDefaultLoggerFactory()
	loggerFactory.DefaultLogLevel = logging.LogLevelDisabled
	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: turnAddress.String(),
		TURNServerAddr: turnAddress.String(),
		Username:       credentials.Username,
		Password:       credentials.Credential,
		Conn:           base,
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
	_ = base.SetDeadline(time.Time{})
	return &managedTURNConn{PacketConn: allocation, client: client, base: base}, nil
}

func parseTURNUDPURL(rawURL string) (M.Socksaddr, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || !strings.EqualFold(parsed.Scheme, "turn") {
		return M.Socksaddr{}, errors.New("not a TURN UDP URL")
	}
	if transport := parsed.Query().Get("transport"); transport != "" && !strings.EqualFold(transport, "udp") {
		return M.Socksaddr{}, errors.New("TURN transport is not UDP")
	}
	if parsed.User != nil {
		return M.Socksaddr{}, errors.New("TURN URL contains unexpected userinfo")
	}
	authority := parsed.Host
	if authority == "" {
		authority = strings.TrimPrefix(parsed.Opaque, "//")
	}
	destination := M.ParseSocksaddr(authority)
	if !destination.IsValid() {
		return M.Socksaddr{}, errors.New("TURN URL has no host")
	}
	if destination.Port == 0 {
		destination.Port = 3478
	}
	return destination, nil
}

func resolveUDPAddress(
	ctx context.Context,
	dialer N.Dialer,
	dnsRouter adapter.DNSRouter,
	destination M.Socksaddr,
	requireIPv4 bool,
) (*net.UDPAddr, error) {
	if destination.Port == 0 || !destination.IsValid() {
		return nil, errors.New("call multi_user: invalid UDP destination")
	}
	if destination.IsIP() {
		if requireIPv4 && !destination.Addr.Unmap().Is4() {
			return nil, errors.New("call multi_user: TURN requires an IPv4 address")
		}
		return destination.Unwrap().UDPAddr(), nil
	}
	if dnsRouter == nil {
		return nil, errors.New("call multi_user: DNS router unavailable")
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
	return nil, errors.New("call multi_user: no usable address returned by DNS")
}
