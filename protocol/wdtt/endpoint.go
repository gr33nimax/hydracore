// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/wireguard"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	defaultWorkers       = 9
	maximumWorkers       = 36
	maximumHashes        = 4
	maximumHashLength    = 256
	maximumPasswordBytes = 1024
	startupTimeout       = 2 * time.Minute
)

var (
	deviceIDMu       sync.Mutex
	fallbackDeviceID string
)

func RegisterEndpoint(registry *endpoint.Registry) {
	endpoint.Register[option.WDTTEndpointOptions](registry, C.TypeWDTT, NewEndpoint)
}

type Endpoint struct {
	endpoint.Adapter
	ctx       context.Context
	cancel    context.CancelFunc
	router    adapter.Router
	dnsRouter adapter.DNSRouter
	logger    log.ContextLogger
	options   option.WDTTEndpointOptions
	dialer    coreDialer

	started    atomic.Bool
	closed     atomic.Bool
	startOnce  sync.Once
	finishOnce sync.Once
	ready      chan struct{}

	mu        sync.Mutex
	initErr   error
	transport *transport
	inner     adapter.Endpoint
}

func NewEndpoint(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.WDTTEndpointOptions) (adapter.Endpoint, error) {
	if err := normalizeAndValidateOptions(&options); err != nil {
		return nil, err
	}
	endpointContext, cancel := context.WithCancel(ctx)
	outboundDialer, err := dialer.NewWithOptions(dialer.Options{
		Context:          endpointContext,
		Options:          option.DialerOptions{},
		RemoteIsDomain:   true,
		ResolverOnDetour: true,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return &Endpoint{
		Adapter:   endpoint.NewAdapterWithDialerOptions(C.TypeWDTT, tag, []string{N.NetworkTCP, N.NetworkUDP}, option.DialerOptions{}),
		ctx:       endpointContext,
		cancel:    cancel,
		router:    router,
		dnsRouter: service.FromContext[adapter.DNSRouter](ctx),
		logger:    logger,
		options:   options,
		dialer:    outboundDialer,
		ready:     make(chan struct{}),
	}, nil
}

func normalizeAndValidateOptions(options *option.WDTTEndpointOptions) error {
	options.Server = strings.TrimSpace(options.Server)
	if options.Server == "" || len(options.Server) > 253 || strings.ContainsAny(options.Server, "/?#@") || !validServerName(options.Server) {
		return E.New("WDTT server must be a hostname or IP address")
	}
	for _, character := range options.Server {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return E.New("WDTT server must not contain whitespace or control characters")
		}
	}
	if options.ServerPort == 0 {
		return E.New("WDTT server_port must be between 1 and 65535")
	}
	if options.Password == "" || len(options.Password) > maximumPasswordBytes || strings.ContainsAny(options.Password, "|\r\n\x00") {
		return E.New("WDTT password must be non-empty, at most ", maximumPasswordBytes, " bytes, and must not contain protocol delimiters")
	}
	if len(options.VKHashes) == 0 || len(options.VKHashes) > maximumHashes {
		return E.New("WDTT vk_hashes must contain between 1 and ", maximumHashes, " entries")
	}
	seenHashes := make(map[string]struct{}, len(options.VKHashes))
	for index, hash := range options.VKHashes {
		hash = strings.TrimSpace(hash)
		if !validVKHash(hash) {
			return E.New("WDTT vk_hashes[", index, "] is invalid")
		}
		if _, exists := seenHashes[hash]; exists {
			return E.New("WDTT vk_hashes must not contain duplicates")
		}
		seenHashes[hash] = struct{}{}
		options.VKHashes[index] = hash
	}
	if options.Workers == 0 {
		options.Workers = defaultWorkers
	}
	if options.Workers < 1 || options.Workers > maximumWorkers {
		return E.New("WDTT workers must be between 1 and ", maximumWorkers)
	}
	options.Obfs = strings.ToLower(strings.TrimSpace(options.Obfs))
	if options.Obfs == "" {
		options.Obfs = "audio"
	}
	if options.Obfs != "audio" && options.Obfs != "video" {
		return E.New(`WDTT obfs must be "audio" or "video"`)
	}
	options.VKAuth = strings.ToLower(strings.TrimSpace(options.VKAuth))
	if options.VKAuth == "" {
		options.VKAuth = "anonymous"
	}
	if options.VKAuth != "anonymous" {
		return E.New(`WDTT vk_auth currently supports "anonymous" only`)
	}
	options.VKAnonPath = strings.ToLower(strings.TrimSpace(options.VKAnonPath))
	if options.VKAnonPath == "" {
		options.VKAnonPath = "vkcalls"
	}
	if options.VKAnonPath != "vkcalls" {
		return E.New(`WDTT vk_anon_path currently supports "vkcalls" only`)
	}
	return nil
}

func validServerName(server string) bool {
	if _, err := netip.ParseAddr(server); err == nil {
		return true
	}
	if strings.HasSuffix(server, ".") {
		server = strings.TrimSuffix(server, ".")
	}
	if server == "" {
		return false
	}
	for _, label := range strings.Split(server, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func validVKHash(hash string) bool {
	if len(hash) == 0 || len(hash) > maximumHashLength {
		return false
	}
	for _, character := range hash {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("._~:-", character) {
			continue
		}
		return false
	}
	return true
}

func (e *Endpoint) Start(stage adapter.StartStage) error {
	if stage == adapter.StartStatePostStart {
		e.started.Store(true)
	}
	return nil
}

func (e *Endpoint) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if err := e.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	e.mu.Lock()
	inner := e.inner
	e.mu.Unlock()
	return inner.DialContext(ctx, network, destination)
}

func (e *Endpoint) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if err := e.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	e.mu.Lock()
	inner := e.inner
	e.mu.Unlock()
	return inner.ListenPacket(ctx, destination)
}

func (e *Endpoint) ensureInitialized(ctx context.Context) error {
	if !e.started.Load() {
		return E.New("WDTT endpoint is not ready yet")
	}
	if e.closed.Load() {
		return net.ErrClosed
	}
	e.startOnce.Do(func() { go e.initialize() })
	select {
	case <-e.ready:
		e.mu.Lock()
		err := e.initErr
		e.mu.Unlock()
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-e.ctx.Done():
		return context.Cause(e.ctx)
	}
}

func (e *Endpoint) initialize() {
	initializationContext, cancel := context.WithTimeout(e.ctx, startupTimeout)
	defer cancel()
	peer, err := e.resolvePeer(initializationContext)
	if err != nil {
		e.finishInitialization(E.Cause(err, "resolve WDTT server"))
		return
	}
	localConnection, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		e.finishInitialization(E.Cause(err, "create internal WDTT bridge"))
		return
	}
	localPort := uint16(localConnection.LocalAddr().(*net.UDPAddr).Port)
	deviceID := loadOrCreateDeviceID(e.ctx, e.logger)
	wdttTransport, err := newTransport(
		e.ctx,
		e.logger,
		e.dialer,
		peer,
		localConnection,
		localPort,
		deviceID,
		e.options.Password,
		e.options.VKHashes,
		e.options.Workers,
		e.options.Obfs,
	)
	if err != nil {
		_ = localConnection.Close()
		e.finishInitialization(err)
		return
	}
	e.mu.Lock()
	e.transport = wdttTransport
	e.mu.Unlock()
	wdttTransport.start()
	configuration, err := wdttTransport.waitConfiguration(initializationContext)
	if err != nil {
		wdttTransport.close()
		if errors.Is(err, context.DeadlineExceeded) {
			err = E.New("WDTT initialization timed out")
		}
		e.finishInitialization(err)
		return
	}
	dynamicConfig, err := parseWireGuardConfig(configuration)
	configuration = ""
	if err != nil {
		wdttTransport.close()
		e.finishInitialization(err)
		return
	}
	inner, err := wireguard.NewEndpoint(
		e.ctx,
		e.router,
		e.logger,
		e.Tag(),
		option.WireGuardEndpointOptions{
			MTU:        dynamicConfig.mtu,
			Address:    dynamicConfig.addresses,
			PrivateKey: dynamicConfig.privateKey,
			Peers: []option.WireGuardPeer{{
				Address:   "127.0.0.1",
				Port:      localPort,
				PublicKey: dynamicConfig.publicKey,
				AllowedIPs: badoption.Listable[netip.Prefix]{
					netip.MustParsePrefix("0.0.0.0/0"),
					netip.MustParsePrefix("::/0"),
				},
				PersistentKeepaliveInterval: 25,
			}},
		},
	)
	if err != nil {
		wdttTransport.close()
		e.finishInitialization(E.Cause(err, "create WDTT WireGuard endpoint"))
		return
	}
	if err = inner.Start(adapter.StartStateStart); err == nil {
		err = inner.Start(adapter.StartStatePostStart)
	}
	if err != nil {
		_ = inner.Close()
		wdttTransport.close()
		e.finishInitialization(E.Cause(err, "start WDTT WireGuard endpoint"))
		return
	}
	if e.closed.Load() {
		_ = inner.Close()
		wdttTransport.close()
		e.finishInitialization(net.ErrClosed)
		return
	}
	e.mu.Lock()
	e.inner = inner
	e.mu.Unlock()
	e.logger.Info("WDTT endpoint initialized with ", e.options.Workers, " lazy TURN workers")
	e.finishInitialization(nil)
}

func (e *Endpoint) resolvePeer(ctx context.Context) (*net.UDPAddr, error) {
	if address, err := netip.ParseAddr(e.options.Server); err == nil {
		return net.UDPAddrFromAddrPort(netip.AddrPortFrom(address.Unmap(), e.options.ServerPort)), nil
	}
	if e.dnsRouter == nil {
		return nil, E.New("missing DNS router for WDTT server hostname")
	}
	addresses, err := e.dnsRouter.Lookup(ctx, e.options.Server, adapter.DNSQueryOptions{})
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, E.New("WDTT server hostname has no address")
	}
	return net.UDPAddrFromAddrPort(netip.AddrPortFrom(addresses[0].Unmap(), e.options.ServerPort)), nil
}

func (e *Endpoint) finishInitialization(err error) {
	e.finishOnce.Do(func() {
		e.mu.Lock()
		e.initErr = err
		e.mu.Unlock()
		close(e.ready)
	})
}

func (e *Endpoint) Close() error {
	e.closed.Store(true)
	e.started.Store(false)
	e.cancel()
	e.finishInitialization(net.ErrClosed)
	e.mu.Lock()
	inner := e.inner
	wdttTransport := e.transport
	e.mu.Unlock()
	var closeErrors []error
	if inner != nil {
		closeErrors = append(closeErrors, inner.Close())
	}
	if wdttTransport != nil {
		wdttTransport.close()
	}
	return errors.Join(closeErrors...)
}

func loadOrCreateDeviceID(ctx context.Context, logger log.ContextLogger) string {
	deviceIDMu.Lock()
	defer deviceIDMu.Unlock()
	cacheFile := service.FromContext[adapter.CacheFile](ctx)
	deviceIDStore, hasDeviceIDStore := cacheFile.(adapter.WDTTDeviceIDStore)
	if hasDeviceIDStore {
		stored := deviceIDStore.LoadWDTTDeviceID()
		if _, err := uuid.Parse(stored); err == nil {
			return stored
		}
		if fallbackDeviceID == "" {
			fallbackDeviceID = uuid.NewString()
		}
		if err := deviceIDStore.SaveWDTTDeviceID(fallbackDeviceID); err != nil {
			logger.Warn("could not persist WDTT device identity: ", err)
		}
		return fallbackDeviceID
	}
	if fallbackDeviceID == "" {
		fallbackDeviceID = uuid.NewString()
	}
	return fallbackDeviceID
}

var _ adapter.Endpoint = (*Endpoint)(nil)
