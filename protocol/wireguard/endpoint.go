package wireguard

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing-box/transport/wireguard"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

var (
	_ adapter.OutboundWithPreferredRoutes = (*Endpoint)(nil)
	_ dialer.PacketDialerWithDestination  = (*Endpoint)(nil)
)

const (
	maxWireGuardWorkers          = 64
	maxWireGuardBuffersPerPool   = 4096
	maxAmneziaJunkPacketCount    = 128
	maxAmneziaPacketPaddingBytes = 65_535
	maxAmneziaHandshakeJunkBytes = 4 * 1024 * 1024
)

func RegisterEndpoint(registry *endpoint.Registry) {
	endpoint.Register[option.WireGuardEndpointOptions](registry, C.TypeWireGuard, NewEndpoint)
}

type Endpoint struct {
	endpoint.Adapter
	ctx            context.Context
	router         adapter.Router
	dnsRouter      adapter.DNSRouter
	logger         logger.ContextLogger
	localAddresses []netip.Prefix
	endpoint       *wireguard.Endpoint
	started        atomic.Bool
}

func NewEndpoint(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.WireGuardEndpointOptions) (adapter.Endpoint, error) {
	if err := validateEndpointResourceLimits(options); err != nil {
		return nil, err
	}
	ep := &Endpoint{
		Adapter:        endpoint.NewAdapterWithDialerOptions(C.TypeWireGuard, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.DialerOptions),
		ctx:            ctx,
		router:         router,
		dnsRouter:      service.FromContext[adapter.DNSRouter](ctx),
		logger:         logger,
		localAddresses: options.Address,
	}
	if options.Detour != "" && options.ListenPort != 0 {
		return nil, E.New("`listen_port` is conflict with `detour`")
	}
	outboundDialer, err := dialer.NewWithOptions(dialer.Options{
		Context: ctx,
		Options: options.DialerOptions,
		RemoteIsDomain: common.Any(options.Peers, func(it option.WireGuardPeer) bool {
			return !M.ParseAddr(it.Address).IsValid()
		}),
		ResolverOnDetour: true,
	})
	if err != nil {
		return nil, err
	}
	var udpTimeout time.Duration
	if options.UDPTimeout != 0 {
		udpTimeout = time.Duration(options.UDPTimeout)
	} else {
		udpTimeout = C.UDPTimeout
	}
	var amnezia *wireguard.AmneziaOptions
	if options.Amnezia != nil {
		amnezia = &wireguard.AmneziaOptions{
			JC:                     options.Amnezia.JC,
			JMin:                   options.Amnezia.JMin,
			JMax:                   options.Amnezia.JMax,
			S1:                     options.Amnezia.S1,
			S2:                     options.Amnezia.S2,
			S3:                     options.Amnezia.S3,
			S4:                     options.Amnezia.S4,
			H1:                     options.Amnezia.H1,
			H2:                     options.Amnezia.H2,
			H3:                     options.Amnezia.H3,
			H4:                     options.Amnezia.H4,
			I1:                     options.Amnezia.I1,
			I2:                     options.Amnezia.I2,
			I3:                     options.Amnezia.I3,
			I4:                     options.Amnezia.I4,
			I5:                     options.Amnezia.I5,
			HeaderProtectionKey:    options.Amnezia.HeaderProtectionKey,
			ContentPaddingAddition: options.Amnezia.ContentPaddingAddition,
			RekeyAfterTime:         options.Amnezia.RekeyAfterTime,
			RekeyTimeout:           options.Amnezia.RekeyTimeout,
			RejectAfterTime:        options.Amnezia.RejectAfterTime,
			KeepaliveTimeout:       options.Amnezia.KeepaliveTimeout,
			MaxHandshakeAttempts:   options.Amnezia.MaxHandshakeAttempts,
		}
	}
	wgEndpoint, err := wireguard.NewEndpoint(wireguard.EndpointOptions{
		Context:     ctx,
		Logger:      logger,
		System:      options.System,
		Handler:     ep,
		UDPTimeout:  udpTimeout,
		ICMPTimeout: C.ICMPTimeout,
		Dialer:      outboundDialer,
		CreateDialer: func(interfaceName string) N.Dialer {
			return common.Must1(dialer.NewDefault(ctx, option.DialerOptions{
				BindInterface: interfaceName,
			}))
		},
		Name:       options.Name,
		MTU:        options.MTU,
		Address:    options.Address,
		PrivateKey: options.PrivateKey,
		ListenPort: options.ListenPort,
		ResolvePeer: func(domain string) (netip.Addr, error) {
			endpointAddresses, lookupErr := ep.dnsRouter.Lookup(ctx, domain, outboundDialer.(dialer.ResolveDialer).QueryOptions())
			if lookupErr != nil {
				return netip.Addr{}, lookupErr
			}
			return endpointAddresses[0], nil
		},
		Peers: common.Map(options.Peers, func(it option.WireGuardPeer) wireguard.PeerOptions {
			return wireguard.PeerOptions{
				Endpoint:                    M.ParseSocksaddrHostPort(it.Address, it.Port),
				PublicKey:                   it.PublicKey,
				PreSharedKey:                it.PreSharedKey,
				AllowedIPs:                  it.AllowedIPs,
				PersistentKeepaliveInterval: it.PersistentKeepaliveInterval,
			}
		}),
		Workers:                    options.Workers,
		PreallocatedBuffersPerPool: options.PreallocatedBuffersPerPool,
		DisablePauses:              options.DisablePauses,
		Amnezia:                    amnezia,
	})
	if err != nil {
		return nil, err
	}
	ep.endpoint = wgEndpoint
	return ep, nil
}

// validateEndpointResourceLimits runs during endpoint construction, so both
// libbox CheckConfig and the real runtime reject dangerous values before the
// WireGuard device starts. The pinned userspace implementation otherwise feeds
// workers directly to sync.WaitGroup.Add and allocates Amnezia junk/padding
// buffers from these publisher-controlled integers.
func validateEndpointResourceLimits(options option.WireGuardEndpointOptions) error {
	if options.Workers < 0 || options.Workers > maxWireGuardWorkers {
		return E.New("wireguard workers must be between 0 and ", maxWireGuardWorkers)
	}
	if options.PreallocatedBuffersPerPool > maxWireGuardBuffersPerPool {
		return E.New("wireguard preallocated_buffers_per_pool must be between 0 and ", maxWireGuardBuffersPerPool)
	}
	amnezia := options.Amnezia
	if amnezia == nil {
		return nil
	}
	if amnezia.JC < 0 || amnezia.JC > maxAmneziaJunkPacketCount {
		return E.New("wireguard amnezia jc must be between 0 and ", maxAmneziaJunkPacketCount)
	}
	if amnezia.JMin < 0 || amnezia.JMin > maxAmneziaPacketPaddingBytes {
		return E.New("wireguard amnezia jmin must be between 0 and ", maxAmneziaPacketPaddingBytes)
	}
	if amnezia.JMax < 0 || amnezia.JMax > maxAmneziaPacketPaddingBytes {
		return E.New("wireguard amnezia jmax must be between 0 and ", maxAmneziaPacketPaddingBytes)
	}
	if amnezia.JMin > amnezia.JMax {
		return E.New("wireguard amnezia jmin must not exceed jmax")
	}
	if int64(amnezia.JC)*int64(amnezia.JMax) > maxAmneziaHandshakeJunkBytes {
		return E.New("wireguard amnezia junk burst exceeds ", maxAmneziaHandshakeJunkBytes, " bytes")
	}
	for _, padding := range []struct {
		name  string
		value int
	}{
		{"s1", amnezia.S1},
		{"s2", amnezia.S2},
		{"s3", amnezia.S3},
		{"s4", amnezia.S4},
	} {
		if padding.value < 0 || padding.value > maxAmneziaPacketPaddingBytes {
			return E.New("wireguard amnezia ", padding.name, " must be between 0 and ", maxAmneziaPacketPaddingBytes)
		}
	}
	return nil
}

func (w *Endpoint) Start(stage adapter.StartStage) error {
	switch stage {
	case adapter.StartStateStart:
		return w.endpoint.Start(false)
	case adapter.StartStatePostStart:
		err := w.endpoint.Start(true)
		if err != nil {
			return err
		}
		w.started.Store(true)
	}
	return nil
}

func (w *Endpoint) Close() error {
	w.started.Store(false)
	return w.endpoint.Close()
}

func (w *Endpoint) PrepareConnection(network string, source M.Socksaddr, destination M.Socksaddr, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	if !w.started.Load() {
		return nil, E.New("WireGuard is not ready yet")
	}
	var ipVersion uint8
	if !destination.IsIPv6() {
		ipVersion = 4
	} else {
		ipVersion = 6
	}
	routeDestination, err := w.router.PreMatch(adapter.InboundContext{
		Inbound:     w.Tag(),
		InboundType: w.Type(),
		IPVersion:   ipVersion,
		Network:     network,
		Source:      source,
		Destination: destination,
	}, routeContext, timeout, false)
	if err != nil {
		switch {
		case rule.IsBypassed(err):
			err = nil
		case rule.IsRejected(err):
			w.logger.Trace("reject ", network, " connection from ", source.AddrString(), " to ", destination.AddrString())
		default:
			if network == N.NetworkICMP {
				w.logger.Warn(E.Cause(err, "link ", network, " connection from ", source.AddrString(), " to ", destination.AddrString()))
			}
		}
	}
	return routeDestination, err
}

func (w *Endpoint) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	var metadata adapter.InboundContext
	metadata.Inbound = w.Tag()
	metadata.InboundType = w.Type()
	metadata.Source = source
	for _, localPrefix := range w.localAddresses {
		if localPrefix.Contains(destination.Addr) {
			metadata.OriginDestination = destination
			if destination.Addr.Is4() {
				destination.Addr = netip.AddrFrom4([4]uint8{127, 0, 0, 1})
			} else {
				destination.Addr = netip.IPv6Loopback()
			}
			break
		}
	}
	metadata.Destination = destination
	w.logger.InfoContext(ctx, "inbound connection from ", source)
	w.logger.InfoContext(ctx, "inbound connection to ", metadata.Destination)
	w.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (w *Endpoint) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	var metadata adapter.InboundContext
	metadata.Inbound = w.Tag()
	metadata.InboundType = w.Type()
	metadata.Source = source
	metadata.Destination = destination
	for _, localPrefix := range w.localAddresses {
		if localPrefix.Contains(destination.Addr) {
			metadata.OriginDestination = destination
			if destination.Addr.Is4() {
				metadata.Destination.Addr = netip.AddrFrom4([4]uint8{127, 0, 0, 1})
			} else {
				metadata.Destination.Addr = netip.IPv6Loopback()
			}
			conn = bufio.NewNATPacketConn(bufio.NewNetPacketConn(conn), metadata.OriginDestination, metadata.Destination)
		}
	}
	w.logger.InfoContext(ctx, "inbound packet connection from ", source)
	w.logger.InfoContext(ctx, "inbound packet connection to ", destination)
	w.router.RoutePacketConnectionEx(ctx, conn, metadata, onClose)
}

func (w *Endpoint) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch network {
	case N.NetworkTCP:
		w.logger.InfoContext(ctx, "outbound connection to ", destination)
	case N.NetworkUDP:
		w.logger.InfoContext(ctx, "outbound packet connection to ", destination)
	}
	if !w.started.Load() {
		return nil, E.New("WireGuard is not ready yet")
	}
	if destination.IsDomain() {
		destinationAddresses, err := w.dnsRouter.Lookup(ctx, destination.Fqdn, adapter.DNSQueryOptions{})
		if err != nil {
			return nil, err
		}
		return N.DialSerial(ctx, w.endpoint, network, destination, destinationAddresses)
	} else if !destination.Addr.IsValid() {
		return nil, E.New("invalid destination: ", destination)
	}
	return w.endpoint.DialContext(ctx, network, destination)
}

func (w *Endpoint) ListenPacketWithDestination(ctx context.Context, destination M.Socksaddr) (net.PacketConn, netip.Addr, error) {
	w.logger.InfoContext(ctx, "outbound packet connection to ", destination)
	if !w.started.Load() {
		return nil, netip.Addr{}, E.New("WireGuard is not ready yet")
	}
	if destination.IsDomain() {
		destinationAddresses, err := w.dnsRouter.Lookup(ctx, destination.Fqdn, adapter.DNSQueryOptions{})
		if err != nil {
			return nil, netip.Addr{}, err
		}
		return N.ListenSerial(ctx, w.endpoint, destination, destinationAddresses)
	}
	packetConn, err := w.endpoint.ListenPacket(ctx, destination)
	if err != nil {
		return nil, netip.Addr{}, err
	}
	if destination.IsIP() {
		return packetConn, destination.Addr, nil
	}
	return packetConn, netip.Addr{}, nil
}

func (w *Endpoint) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	packetConn, destinationAddress, err := w.ListenPacketWithDestination(ctx, destination)
	if err != nil {
		return nil, err
	}
	if destinationAddress.IsValid() && destination != M.SocksaddrFrom(destinationAddress, destination.Port) {
		return bufio.NewNATPacketConn(bufio.NewPacketConn(packetConn), M.SocksaddrFrom(destinationAddress, destination.Port), destination), nil
	}
	return packetConn, nil
}

func (w *Endpoint) PreferredDomain(domain string) bool {
	return false
}

func (w *Endpoint) PreferredAddress(address netip.Addr) bool {
	if !w.started.Load() {
		return false
	}
	return w.endpoint.Lookup(address) != nil
}

func (w *Endpoint) NewDirectRouteConnection(metadata adapter.InboundContext, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	if !w.started.Load() {
		return nil, E.New("WireGuard is not ready yet")
	}
	return w.endpoint.NewDirectRouteConnection(metadata, routeContext, timeout)
}
