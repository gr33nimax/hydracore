package call

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/dion"
	"github.com/sagernet/sing-box/transport/call/multiuser"
	"github.com/sagernet/sing-box/transport/call/telemost"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/vk"
	"github.com/sagernet/sing-box/transport/call/wbstream"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type Role int

const (
	RoleCreator Role = iota
	RoleJoiner
)

type Config struct {
	Platform     string
	Mode         string
	JoinLink     string
	JoinLinks    []string
	Server       M.Socksaddr
	User         string
	UserPassword string
	ObfsPassword string
	Workers      int
	WorkerConnectTimeout time.Duration
	Cookies      string
	CookieString string
	Email        string
	Password     string
	ReadBuffer   int
	Role         Role
	Dialer       N.Dialer
	DNSRouter    adapter.DNSRouter
	Logger       logger.ContextLogger
}

func Connect(ctx context.Context, cfg Config) (*Bridge, error) {
	readBuf := cfg.ReadBuffer
	if readBuf <= 0 {
		readBuf = 32768
	}
	log := cfg.Logger
	if log == nil {
		log = logger.NOP()
	}
	cookieStr := cfg.CookieString
	if cookieStr == "" {
		cookieStr = cfg.Cookies
	}
	if cfg.Mode == "multi_user" {
		if cfg.Platform != "vk" {
			return nil, E.New("call: multi_user mode is only supported for vk")
		}
		if cfg.Role != RoleJoiner {
			return nil, E.New("call: multi_user creator role is hosted by the native inbound")
		}
		provider := vk.NewTURNCredentialProvider(cfg.Dialer, log)
		client, err := multiuser.ConnectClient(ctx, multiuser.ClientOptions{
			Server:               cfg.Server,
			JoinLinks:            append([]string(nil), cfg.JoinLinks...),
			User:                 cfg.User,
			Password:             cfg.UserPassword,
			ObfsPassword:         cfg.ObfsPassword,
			Workers:              cfg.Workers,
			WorkerConnectTimeout: cfg.WorkerConnectTimeout,
			Dialer:               cfg.Dialer,
			DNSRouter:            cfg.DNSRouter,
			Credentials: func(fetchCtx context.Context, joinLink string) (multiuser.TURNCredentials, error) {
				server, fetchErr := provider.Fetch(fetchCtx, joinLink)
				return multiuser.TURNCredentials{
					URLs:       server.URLs,
					Username:   server.Username,
					Credential: server.Credential,
				}, fetchErr
			},
		}, log)
		if err != nil {
			return nil, err
		}
		relay := tunnel.NewRelayBridge(client.Tunnel(), "joiner", readBuf, cfg.Dialer, log)
		relay.MarkReady()
		go func() {
			<-client.Done()
			relay.Close()
		}()
		return &Bridge{relay: relay, closer: client}, nil
	}
	switch cfg.Platform {
	case "telemost":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := telemost.ConnectCreator(ctx, cookieStr, cfg.JoinLink, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[telemost]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := telemost.ConnectJoiner(ctx, cfg.JoinLink, "", readBuf, cfg.Dialer, cfg.DNSRouter, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	case "wbstream":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := wbstream.ConnectCreator(ctx, cookieStr, cfg.JoinLink, cfg.Mode, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[wbstream]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := wbstream.ConnectJoiner(ctx, cfg.JoinLink, "", cfg.Mode, readBuf, cfg.Dialer, cfg.DNSRouter, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	case "vk":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := vk.ConnectCreator(ctx, cookieStr, cfg.JoinLink, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[vk]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := vk.ConnectJoiner(ctx, cfg.JoinLink, "", readBuf, cfg.Dialer, cfg.DNSRouter, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	case "dion":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := dion.ConnectCreator(ctx, cookieStr, cfg.JoinLink, cfg.Email, cfg.Password, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[dion]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := dion.ConnectJoiner(ctx, cfg.JoinLink, "", readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	}
	return nil, E.New("call: unsupported platform ", cfg.Platform)
}

type Bridge struct {
	relay  *tunnel.RelayBridge
	closer interface{ Close() error }
}

func NewBridge(relay *tunnel.RelayBridge) *Bridge {
	return &Bridge{relay: relay}
}

func (b *Bridge) Close() error {
	b.relay.Close()
	if b.closer != nil {
		return b.closer.Close()
	}
	return nil
}

func (b *Bridge) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	return b.relay.DialContext(ctx, destination)
}

func (b *Bridge) ListenPacket(ctx context.Context, destination string) (net.Conn, error) {
	return b.relay.ListenPacket(ctx, destination)
}

func (b *Bridge) SetAcceptHandler(fn func(conn net.Conn, destination string)) {
	b.relay.SetAcceptHandler(fn)
}

func (b *Bridge) SetUDPAcceptHandler(fn func(conn net.Conn, destination string)) {
	b.relay.SetUDPAcceptHandler(fn)
}
