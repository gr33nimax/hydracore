//go:build !with_call_client || with_call_server

package call

import (
	"context"
	"net"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/dialer"
	H "github.com/sagernet/sing-box/common/hydracore"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/call"
	"github.com/sagernet/sing-box/transport/call/multiuser"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/bufio/deadline"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.CallInboundOptions](registry, C.TypeCall, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	ctx      context.Context
	router   adapter.ConnectionRouterEx
	logger   logger.ContextLogger
	options  option.CallInboundOptions
	dialer   N.Dialer
	bridge   *call.Bridge
	listener *listener.Listener
	server   *multiuser.Server
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.CallInboundOptions) (adapter.Inbound, error) {
	mode := options.Mode
	if mode == "" {
		mode = "p2p"
	}
	if !H.SupportsCallMode(mode) {
		return nil, E.New("call mode is not included in this HydraCore role: ", mode)
	}
	if options.Platform == "" {
		return nil, E.New("missing platform")
	}
	outboundDialer, err := dialer.New(ctx, options.DialerOptions, true)
	if err != nil {
		return nil, err
	}
	h := &Inbound{
		Adapter: inbound.NewAdapter(C.TypeCall, tag),
		ctx:     ctx,
		router:  router,
		logger:  logger,
		options: options,
		dialer:  outboundDialer,
	}
	if options.Mode == "multi_user" {
		if options.Platform != "vk" {
			return nil, E.New("call multi_user is only supported for vk")
		}
		if options.Listen == nil || options.ListenPort == 0 {
			return nil, E.New("missing listen or listen_port")
		}
		h.listener = listener.New(listener.Options{
			Context: ctx,
			Logger:  logger,
			Listen: option.ListenOptions{
				Listen:        options.Listen,
				ListenPort:    options.ListenPort,
				BindInterface: options.BindInterface,
				RoutingMark:   options.RoutingMark,
				ReuseAddr:     options.ReuseAddr,
				NetNs:         options.NetNs,
				UDPFragment:   options.UDPFragment,
				Detour:        options.Detour,
			},
		})
		users := make([]multiuser.ServerUser, 0, len(options.Users))
		for _, user := range options.Users {
			users = append(users, multiuser.ServerUser{Name: user.Name, Password: user.Password, MaxSessions: user.MaxSessions})
		}
		h.server, err = multiuser.NewServer(ctx, multiuser.ServerOptions{
			ObfsPassword:         options.ObfsPassword,
			Users:                users,
			MaxSessions:          options.MaxSessions,
			MaxWorkersPerSession: options.MaxWorkersPerSession,
			MaxPendingHandshakes: options.MaxPendingHandshakes,
			HandshakeTimeout:     time.Duration(options.HandshakeTimeout),
			SessionIdleTimeout:   time.Duration(options.SessionIdleTimeout),
			SessionHandler:       h.handleMultiUserSession,
		}, logger)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (h *Inbound) Start(stage adapter.StartStage) error {
	if h.server != nil {
		if stage != adapter.StartStateStart {
			return nil
		}
		packetConn, err := h.listener.ListenUDP()
		if err != nil {
			return err
		}
		return h.server.Start(packetConn)
	}
	if stage != adapter.StartStatePostStart {
		return nil
	}
	go h.run()
	return nil
}

func (h *Inbound) Close() error {
	if h.server != nil {
		serverErr := h.server.Close()
		listenerErr := h.listener.Close()
		if serverErr != nil {
			return serverErr
		}
		return listenerErr
	}
	if h.bridge == nil {
		return nil
	}
	return h.bridge.Close()
}

func (h *Inbound) run() {
	dnsRouter := service.FromContext[adapter.DNSRouter](h.ctx)
	bridge, err := call.Connect(h.ctx, call.Config{
		Platform:     h.options.Platform,
		Mode:         h.options.Mode,
		JoinLink:     h.options.JoinLink,
		CookieString: h.options.Cookies.Header(),
		Email:        h.options.Email,
		Password:     h.options.Password,
		ReadBuffer:   h.options.ReadBuffer,
		Role:         call.RoleCreator,
		Dialer:       h.dialer,
		DNSRouter:    dnsRouter,
		Logger:       h.logger,
	})
	if err != nil {
		h.logger.ErrorContext(h.ctx, err)
		return
	}
	h.bridge = bridge
	bridge.SetAcceptHandler(func(conn net.Conn, destination string) {
		h.handleConnection(conn, M.ParseSocksaddr(destination), "")
	})
	bridge.SetUDPAcceptHandler(func(conn net.Conn, destination string) {
		h.handlePacketConnection(bufio.NewUnbindPacketConnWithAddr(conn, M.ParseSocksaddr(destination)), M.ParseSocksaddr(destination), "")
	})
}

func (h *Inbound) handleMultiUserSession(info multiuser.SessionInfo, dataTunnel *multiuser.PooledTunnel) error {
	bridge := calltunnel.NewRelayBridge(dataTunnel, "creator", normalizedReadBuffer(h.options.ReadBuffer), h.dialer, h.logger)
	bridge.SetAcceptHandler(func(conn net.Conn, destination string) {
		h.handleConnection(conn, M.ParseSocksaddr(destination), info.User)
	})
	bridge.SetUDPAcceptHandler(func(conn net.Conn, destination string) {
		parsed := M.ParseSocksaddr(destination)
		h.handlePacketConnection(bufio.NewUnbindPacketConnWithAddr(conn, parsed), parsed, info.User)
	})
	bridge.MarkReady()
	return nil
}

func normalizedReadBuffer(value int) int {
	if value <= 0 {
		return 32768
	}
	return value
}

func (h *Inbound) handleConnection(conn net.Conn, destination M.Socksaddr, user string) {
	ctx := log.ContextWithNewID(h.ctx)
	var metadata adapter.InboundContext
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	metadata.Source = M.Socksaddr{}
	metadata.Destination = destination
	metadata.User = user
	h.logger.InfoContext(ctx, "inbound connection to ", destination)
	h.router.RouteConnectionEx(ctx, deadline.NewConn(conn), metadata, N.OnceClose(func(it error) {
		conn.Close()
	}))
}

func (h *Inbound) handlePacketConnection(conn N.PacketConn, destination M.Socksaddr, user string) {
	ctx := log.ContextWithNewID(h.ctx)
	var metadata adapter.InboundContext
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	metadata.Source = M.Socksaddr{}
	metadata.Destination = destination
	metadata.User = user
	h.logger.InfoContext(ctx, "inbound packet connection to ", destination)
	h.router.RoutePacketConnectionEx(ctx, conn, metadata, N.OnceClose(func(it error) {
		conn.Close()
	}))
}
