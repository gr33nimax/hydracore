package call

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/call"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/bufio/deadline"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.CallOutboundOptions](registry, C.TypeCall, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	ctx          context.Context
	logger       logger.ContextLogger
	options      option.CallOutboundOptions
	dialer       N.Dialer
	bridge       *call.Bridge
	startHandler func()
	await        chan struct{}
	awaitOnce    sync.Once
	started      atomic.Bool
	closed       atomic.Bool
	cancel       context.CancelFunc
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.CallOutboundOptions) (adapter.Outbound, error) {
	if options.Platform == "" {
		return nil, E.New("missing platform")
	}
	if options.Mode == "multi_user" {
		if options.Platform != "vk" {
			return nil, E.New("call multi_user is only supported for vk")
		}
		if !options.ServerOptions.Build().IsValid() || options.ServerPort == 0 {
			return nil, E.New("missing server or server_port")
		}
		if len(options.JoinLinks) == 0 {
			return nil, E.New("missing join_links")
		}
	} else if options.JoinLink == "" {
		return nil, E.New("missing join_link")
	}
	outboundDialer, err := dialer.New(ctx, options.DialerOptions, true)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	ob := &Outbound{
		Adapter: outbound.NewAdapterWithDialerOptions(C.TypeCall, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.DialerOptions),
		ctx:     ctx,
		logger:  logger,
		options: options,
		dialer:  outboundDialer,
		await:   make(chan struct{}),
		cancel:  cancel,
	}
	dnsRouter := service.FromContext[adapter.DNSRouter](ctx)
	ob.startHandler = func() {
		defer ob.finishStart()
		bridge, err := call.Connect(runCtx, call.Config{
			Platform:     options.Platform,
			Mode:         options.Mode,
			JoinLink:     options.JoinLink,
			JoinLinks:    options.JoinLinks,
			Server:       options.ServerOptions.Build(),
			User:         options.User,
			UserPassword: options.Password,
			ObfsPassword: options.ObfsPassword,
			Workers:      options.Workers,
			WorkerConnectTimeout: time.Duration(options.WorkerConnectTimeout),
			CookieString: options.Cookies.Header(),
			ReadBuffer:   options.ReadBuffer,
			Role:         call.RoleJoiner,
			Dialer:       outboundDialer,
			DNSRouter:    dnsRouter,
			Logger:       logger,
		})
		if err != nil {
			if !ob.closed.Load() {
				logger.ErrorContext(runCtx, err)
			}
			return
		}
		if ob.closed.Load() {
			_ = bridge.Close()
			return
		}
		ob.bridge = bridge
	}
	return ob, nil
}

func (o *Outbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStatePostStart {
		return nil
	}
	if o.closed.Load() {
		o.finishStart()
		return nil
	}
	if !o.started.CompareAndSwap(false, true) {
		return nil
	}
	go o.startHandler()
	return nil
}

func (o *Outbound) Close() error {
	if !o.closed.CompareAndSwap(false, true) {
		return nil
	}
	o.cancel()
	if o.started.Load() {
		<-o.await
	} else {
		o.finishStart()
	}
	if o.bridge == nil {
		return nil
	}
	return o.bridge.Close()
}

func (o *Outbound) finishStart() {
	o.awaitOnce.Do(func() { close(o.await) })
}

func (o *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if err := o.awaitBridge(ctx); err != nil {
		return nil, err
	}
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		o.logger.InfoContext(ctx, "outbound connection to ", destination)
		conn, err := o.bridge.DialContext(ctx, destination.String())
		if err != nil {
			return nil, err
		}
		return deadline.NewConn(conn), nil
	default:
		return nil, E.New("call: unsupported network: ", network)
	}
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if err := o.awaitBridge(ctx); err != nil {
		return nil, err
	}
	o.logger.InfoContext(ctx, "outbound packet connection to ", destination)
	conn, err := o.bridge.ListenPacket(ctx, destination.String())
	if err != nil {
		return nil, err
	}
	return bufio.NewUnbindPacketConnWithAddr(conn, destination), nil
}

func (o *Outbound) awaitBridge(ctx context.Context) error {
	select {
	case <-o.await:
	case <-ctx.Done():
		return ctx.Err()
	}
	if o.bridge == nil {
		return E.New("call: tunnel not initialized")
	}
	return nil
}
