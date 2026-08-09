package call

import (
	"context"
	"sync"
	"time"

	callcommon "github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing-box/transport/call/multiuser"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/vk"
	"github.com/sagernet/sing/common/logger"
)

const multiUserReconnectMaxBackoff = 30 * time.Second

type managedMultiUserClient interface {
	Tunnel() *multiuser.PooledTunnel
	Done() <-chan struct{}
	Close() error
}

type multiUserConnector func(ctx context.Context) (managedMultiUserClient, error)

type multiUserBridgeManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	relay   *tunnel.RelayBridge
	connect multiUserConnector
	logger  logger.ContextLogger

	clientMu  sync.Mutex
	client    managedMultiUserClient
	done      chan struct{}
	closeOnce sync.Once
}

func connectMultiUserBridge(ctx context.Context, cfg Config, readBuffer int, log logger.ContextLogger) (*Bridge, error) {
	provider := vk.NewTURNCredentialProvider(cfg.Dialer, log)
	options := multiuser.ClientOptions{
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
	}
	managerCtx, cancel := context.WithCancel(ctx)
	connector := func(connectCtx context.Context) (managedMultiUserClient, error) {
		return multiuser.ConnectClient(connectCtx, options, log)
	}
	initial, err := connector(managerCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	relay := tunnel.NewRelayBridge(initial.Tunnel(), "joiner", readBuffer, cfg.Dialer, log)
	relay.MarkReady()
	manager := newMultiUserBridgeManager(managerCtx, cancel, relay, connector, initial, log)
	return &Bridge{relay: relay, closer: manager}, nil
}

func newMultiUserBridgeManager(
	ctx context.Context,
	cancel context.CancelFunc,
	relay *tunnel.RelayBridge,
	connector multiUserConnector,
	initial managedMultiUserClient,
	log logger.ContextLogger,
) *multiUserBridgeManager {
	manager := &multiUserBridgeManager{
		ctx:     ctx,
		cancel:  cancel,
		relay:   relay,
		connect: connector,
		logger:  log,
		client:  initial,
		done:    make(chan struct{}),
	}
	go manager.run(initial)
	return manager
}

func (m *multiUserBridgeManager) run(initial managedMultiUserClient) {
	defer close(m.done)
	current := initial
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-current.Done():
		}
		_ = current.Close()
		if m.ctx.Err() != nil {
			return
		}
		next := m.reconnect()
		if next == nil {
			return
		}
		m.clientMu.Lock()
		if m.ctx.Err() != nil {
			m.clientMu.Unlock()
			_ = next.Close()
			return
		}
		m.client = next
		m.clientMu.Unlock()
		m.relay.SwapTunnel(next.Tunnel())
		current = next
	}
}

func (m *multiUserBridgeManager) reconnect() managedMultiUserClient {
	backoff := time.Second
	for {
		if m.ctx.Err() != nil {
			return nil
		}
		client, err := m.connect(m.ctx)
		if err == nil {
			m.logger.Info("call multi_user: native session reconnected")
			return client
		}
		m.logger.Warn("call multi_user: reconnect failed, retrying: ", callcommon.MaskError(err))
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-m.ctx.Done():
			timer.Stop()
			return nil
		}
		if backoff < multiUserReconnectMaxBackoff {
			backoff *= 2
			if backoff > multiUserReconnectMaxBackoff {
				backoff = multiUserReconnectMaxBackoff
			}
		}
	}
}

func (m *multiUserBridgeManager) Close() error {
	var closeErr error
	m.closeOnce.Do(func() {
		m.cancel()
		m.clientMu.Lock()
		current := m.client
		m.clientMu.Unlock()
		if current != nil {
			closeErr = current.Close()
		}
		<-m.done
	})
	return closeErr
}
