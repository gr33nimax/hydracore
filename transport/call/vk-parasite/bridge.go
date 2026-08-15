package vkparasite

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	callcommon "github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/vk"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const parasiteReconnectMaxBackoff = 30 * time.Second
const parasiteReconnectInitialBackoff = time.Second

type managedParasiteClient interface {
	Tunnel() *ParasiteTunnel
	Done() <-chan struct{}
	RebindNetwork()
	Close() error
}

type BridgeOptions struct {
	Server               M.Socksaddr
	JoinLinks            []string
	User                 string
	Password             string
	ObfsPassword         string
	Workers              int
	WorkerConnectTimeout time.Duration
	ReadBuffer           int
	Dialer               N.Dialer
	DNSRouter            adapter.DNSRouter
}

type parasiteConnector func(ctx context.Context) (managedParasiteClient, error)

type parasiteBridgeManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	relay   *tunnel.RelayBridge
	connect parasiteConnector
	logger  logger.ContextLogger

	clientMu  sync.Mutex
	client    managedParasiteClient
	done      chan struct{}
	closeOnce sync.Once
}

func ConnectBridge(ctx context.Context, cfg BridgeOptions, log logger.ContextLogger) (*tunnel.RelayBridge, io.Closer, error) {
	metrics := telemetry.NewAccumulator()
	provider := vk.NewTURNCredentialProvider(cfg.Dialer, log)
	provider.SetTelemetry(metrics)
	options := ClientOptions{
		Server:               cfg.Server,
		JoinLinks:            append([]string(nil), cfg.JoinLinks...),
		User:                 cfg.User,
		Password:             cfg.Password,
		ObfsPassword:         cfg.ObfsPassword,
		Workers:              cfg.Workers,
		WorkerConnectTimeout: cfg.WorkerConnectTimeout,
		Dialer:               cfg.Dialer,
		DNSRouter:            cfg.DNSRouter,
		Credentials: func(fetchCtx context.Context, joinLink string) (TURNCredentials, error) {
			server, fetchErr := provider.Fetch(fetchCtx, joinLink)
			return TURNCredentials{
				URLs:       server.URLs,
				Username:   server.Username,
				Credential: server.Credential,
			}, fetchErr
		},
		InvalidateCredentials: provider.Invalidate,
		Telemetry: metrics,
	}
	validatedOptions, err := validateClientOptions(options)
	if err != nil {
		return nil, nil, err
	}
	options = validatedOptions
	managerCtx, cancel := context.WithCancel(ctx)
	connector := func(connectCtx context.Context) (managedParasiteClient, error) {
		return ConnectClient(connectCtx, options, log)
	}
	initial := connectParasiteClientWithRetry(
		managerCtx,
		connector,
		log,
		parasiteReconnectInitialBackoff,
		parasiteReconnectMaxBackoff,
	)
	if initial == nil {
		cancel()
		return nil, nil, managerCtx.Err()
	}
	relay := tunnel.NewRelayBridge(initial.Tunnel(), "joiner", cfg.ReadBuffer, cfg.Dialer, log)
	relay.MarkReady()
	manager := newParasiteBridgeManager(managerCtx, cancel, relay, connector, initial, log)
	return relay, manager, nil
}

func newParasiteBridgeManager(
	ctx context.Context,
	cancel context.CancelFunc,
	relay *tunnel.RelayBridge,
	connector parasiteConnector,
	initial managedParasiteClient,
	log logger.ContextLogger,
) *parasiteBridgeManager {
	manager := &parasiteBridgeManager{
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

func (m *parasiteBridgeManager) run(initial managedParasiteClient) {
	defer close(m.done)
	current := initial
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-current.Done():
		}
		if m.ctx.Err() != nil {
			return
		}
		m.clientMu.Lock()
		if m.client == current {
			m.client = nil
		}
		m.clientMu.Unlock()
		// Cleanup must never gate recovery. A transport callback or a third-party
		// connection implementation can misbehave during Close; the replacement
		// session still has to start and swap into the relay independently.
		go func(stale managedParasiteClient) { _ = stale.Close() }(current)
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

func (m *parasiteBridgeManager) reconnect() managedParasiteClient {
	return connectParasiteClientWithRetry(
		m.ctx,
		m.connect,
		m.logger,
		parasiteReconnectInitialBackoff,
		parasiteReconnectMaxBackoff,
	)
}

func connectParasiteClientWithRetry(
	ctx context.Context,
	connect parasiteConnector,
	log logger.ContextLogger,
	initialBackoff time.Duration,
	maximumBackoff time.Duration,
) managedParasiteClient {
	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}
		client, err := connect(ctx)
		if err == nil {
			log.Info("call vk_parasite: native session connected")
			return client
		}
		log.Warn("call vk_parasite: session connect failed, retrying: ", callcommon.MaskError(err))
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
		if backoff < maximumBackoff {
			backoff *= 2
			if backoff > maximumBackoff {
				backoff = maximumBackoff
			}
		}
	}
}

func (m *parasiteBridgeManager) RebindNetwork() {
	m.clientMu.Lock()
	current := m.client
	m.clientMu.Unlock()
	if current != nil {
		current.RebindNetwork()
	}
}

func (m *parasiteBridgeManager) Close() error {
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
