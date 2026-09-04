package vkparasite

import (
	"context"
	"io"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing-box/transport/call/vk"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type BridgeOptions struct {
	TransportTag         string
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

func ConnectBridge(ctx context.Context, cfg BridgeOptions, log logger.ContextLogger) (*QUICRelay, io.Closer, error) {
	metrics := telemetry.NewAccumulator()
	provider := vk.NewTURNCredentialProvider(cfg.Dialer, log)
	provider.SetTelemetry(metrics)
	options := ClientOptions{
		TransportTag:         cfg.TransportTag,
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
		Telemetry:             metrics,
	}
	validatedOptions, err := validateClientOptions(options)
	if err != nil {
		return nil, nil, err
	}
	options = validatedOptions
	client, err := ConnectClient(ctx, options, log)
	if err != nil {
		return nil, nil, err
	}
	return client.Relay(), client, nil
}
