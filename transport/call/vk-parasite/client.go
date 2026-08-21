package vkparasite

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/sagernet/quic-go"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type ClientOptions struct {
	Server                M.Socksaddr
	JoinLinks             []string
	User                  string
	Password              string
	ObfsPassword          string
	Workers               int
	WorkerConnectTimeout  time.Duration
	Dialer                N.Dialer
	DNSRouter             adapter.DNSRouter
	Credentials           CredentialProvider
	InvalidateCredentials func(string)
	Telemetry             *telemetry.Accumulator
}

type Client struct {
	ctx          context.Context
	cancel       context.CancelFunc
	options      ClientOptions
	logger       logger.ContextLogger
	server       *net.UDPAddr
	key          [wrapKeyLength]byte
	sessionID    [16]byte
	conv         uint32
	relay        *QUICRelay
	metrics      *telemetry.Accumulator
	generation   atomic.Uint64
	closeOnce    sync.Once
	rebindMu     sync.Mutex
	rebindCancel context.CancelFunc
}

func ConnectClient(parent context.Context, options ClientOptions, log logger.ContextLogger) (*Client, error) {
	if log == nil {
		log = logger.NOP()
	}
	options, err := validateClientOptions(options)
	if err != nil {
		return nil, err
	}
	server, err := resolveUDPAddress(parent, options.Dialer, options.DNSRouter, options.Server, true)
	if err != nil {
		return nil, err
	}
	key, err := deriveWrapKey(options.ObfsPassword)
	if err != nil {
		return nil, err
	}
	var sessionID [16]byte
	if _, err = rand.Read(sessionID[:]); err != nil {
		return nil, err
	}
	if sessionID == ([16]byte{}) {
		sessionID[0] = 1
	}
	conv, err := randomConversationID()
	if err != nil {
		return nil, err
	}
	metrics := options.Telemetry
	if metrics == nil {
		metrics = telemetry.NewAccumulator()
	}
	ctx, cancel := context.WithCancel(parent)
	client := &Client{
		ctx:       ctx,
		cancel:    cancel,
		options:   options,
		logger:    log,
		server:    server,
		key:       key,
		sessionID: sessionID,
		conv:      conv,
		metrics:   metrics,
	}
	client.relay = NewQUICRelay(ctx, QUICRelayOptions{
		PathCount: options.Workers,
		DialPath:  client.DialPath,
		Logger:    log,
	})
	client.relay.Start()
	return client, nil
}

func (c *Client) Relay() *QUICRelay {
	return c.relay
}

func (c *Client) DialPath(ctx context.Context, workerID uint16) (*quic.Conn, io.Closer, error) {
	joinLink := c.options.JoinLinks[int(workerID)%len(c.options.JoinLinks)]
	credentials, err := c.options.Credentials(ctx, joinLink)
	if err != nil {
		return nil, nil, fmt.Errorf("worker %d credentials: %w", workerID, err)
	}
	releaseTURN, err := sharedTransportSupervisor.acquireTURN(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("worker %d TURN gate: %w", workerID, err)
	}
	allocation, err := allocateTURN(ctx, c.options.Dialer, c.options.DNSRouter, credentials, int(workerID), c.metrics, workerID)
	releaseTURN()
	if err != nil {
		return nil, nil, fmt.Errorf("worker %d TURN allocate: %w", workerID, err)
	}
	codec, err := newRTPCodec(c.key)
	if err != nil {
		_ = allocation.Close()
		return nil, nil, fmt.Errorf("worker %d RTP codec: %w", workerID, err)
	}
	packetConn := newObfsPacketConn(allocation, c.server, codec, c.metrics)
	releaseDTLS, err := sharedTransportSupervisor.acquireDTLS(ctx)
	if err != nil {
		_ = packetConn.Close()
		return nil, nil, fmt.Errorf("worker %d DTLS gate: %w", workerID, err)
	}
	dtlsConn, err := dtls.Client(packetConn, c.server, &dtls.Config{
		InsecureSkipVerify:   true, // Authenticated by the outer key and the inner user attach.
		CipherSuites:         []dtls.CipherSuiteID{dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		FlightInterval:       500 * time.Millisecond,
		MTU:                  dtlsMTU,
	})
	if err != nil {
		releaseDTLS()
		c.metrics.Add(telemetry.DTLSHandshakeFailureTotal, 1)
		_ = packetConn.Close()
		return nil, nil, fmt.Errorf("worker %d DTLS initialize: %w", workerID, err)
	}
	handshakeStarted := time.Now()
	if err = dtlsConn.HandshakeContext(ctx); err != nil {
		releaseDTLS()
		c.metrics.Set(telemetry.DTLSHandshakeLatencyMS, telemetry.LatencyMS(handshakeStarted))
		c.metrics.Add(telemetry.DTLSHandshakeFailureTotal, 1)
		_ = dtlsConn.Close()
		return nil, nil, fmt.Errorf("worker %d DTLS handshake: %w", workerID, err)
	}
	releaseDTLS()
	c.metrics.Set(telemetry.DTLSHandshakeLatencyMS, telemetry.LatencyMS(handshakeStarted))
	c.metrics.Add(telemetry.DTLSHandshakeSuccessTotal, 1)

	innerAuthStarted := time.Now()
	request, err := encodeAuthRequest(authRequest{
		SessionID:      c.sessionID,
		Conv:           c.conv,
		WorkerID:       workerID,
		WorkerTotal:    uint16(c.options.Workers),
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           c.options.User,
		Password:       c.options.Password,
	})
	if err != nil {
		_ = dtlsConn.Close()
		return nil, nil, fmt.Errorf("worker %d inner auth encode: %w", workerID, err)
	}
	_ = dtlsConn.SetDeadline(time.Now().Add(c.options.WorkerConnectTimeout))
	if _, err = dtlsConn.Write(request); err != nil {
		_ = dtlsConn.Close()
		return nil, nil, fmt.Errorf("worker %d inner auth write: %w", workerID, err)
	}
	ack := make([]byte, 14)
	n, err := dtlsConn.Read(ack)
	if err != nil {
		_ = dtlsConn.Close()
		return nil, nil, fmt.Errorf("worker %d inner auth read: %w", workerID, err)
	}
	generation, err := decodeAuthAck(ack[:n])
	if err != nil {
		_ = dtlsConn.Close()
		return nil, nil, fmt.Errorf("worker %d inner auth rejected: %w", workerID, err)
	}
	expectedGeneration := c.generation.Load()
	if expectedGeneration == 0 {
		c.generation.CompareAndSwap(0, generation)
		expectedGeneration = c.generation.Load()
	}
	if generation != expectedGeneration {
		_ = dtlsConn.Close()
		return nil, nil, errors.New("call vk_parasite: server session state was reset")
	}
	_ = dtlsConn.SetDeadline(time.Time{})
	c.metrics.Set(telemetry.InnerAuthLatencyMS, telemetry.LatencyMS(innerAuthStarted))
	c.metrics.Add(telemetry.InnerAuthSuccessTotal, 1)

	quicConn, err := dialQUIC(ctx, dtlsConn, dtlsConn)
	if err != nil {
		_ = dtlsConn.Close()
		return nil, nil, fmt.Errorf("worker %d QUIC dial: %w", workerID, err)
	}
	return quicConn, dtlsConn, nil
}

func (c *Client) RebindNetwork() {
	c.rebindMu.Lock()
	defer c.rebindMu.Unlock()
	if c.rebindCancel != nil {
		c.rebindCancel()
	}
}

func (c *Client) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		if c.relay != nil {
			c.relay.Close()
		}
	})
	return nil
}

func randomConversationID() (uint32, error) {
	var convBuf [4]byte
	if _, err := rand.Read(convBuf[:]); err != nil {
		return 0, err
	}
	conv := binary.BigEndian.Uint32(convBuf[:])
	if conv == 0 {
		conv = 1
	}
	return conv, nil
}

func validateClientOptions(options ClientOptions) (ClientOptions, error) {
	if !options.Server.IsValid() || options.Server.Port == 0 {
		return options, errors.New("call vk_parasite: missing server or server_port")
	}
	if len(options.JoinLinks) < 1 || len(options.JoinLinks) > 4 {
		return options, errors.New("call vk_parasite: join_links must contain between 1 and 4 links")
	}
	normalizedLinks := make([]string, 0, len(options.JoinLinks))
	seenLinks := make(map[string]struct{}, len(options.JoinLinks))
	for _, link := range options.JoinLinks {
		link = strings.TrimSpace(link)
		if link == "" || len(link) > 2048 {
			return options, errors.New("call vk_parasite: invalid join_links entry")
		}
		if _, exists := seenLinks[link]; exists {
			return options, errors.New("call vk_parasite: duplicate join_links entry")
		}
		seenLinks[link] = struct{}{}
		normalizedLinks = append(normalizedLinks, link)
	}
	options.JoinLinks = normalizedLinks
	if err := validateAuthStrings(options.User, options.Password); err != nil {
		return options, err
	}
	if len(options.ObfsPassword) == 0 || len(options.ObfsPassword) > maximumPasswordLen {
		return options, errors.New("call vk_parasite: invalid obfs_password length")
	}
	if options.Workers == 0 {
		options.Workers = LaneCount
	}
	if options.Workers != LaneCount {
		return options, errors.New("call vk_parasite: exactly sixteen VK lanes are required")
	}
	if options.WorkerConnectTimeout == 0 {
		options.WorkerConnectTimeout = 30 * time.Second
	}
	if options.WorkerConnectTimeout < time.Second || options.WorkerConnectTimeout > 2*time.Minute {
		return options, errors.New("call vk_parasite: worker_connect_timeout must be between 1s and 2m")
	}
	if options.Credentials == nil {
		return options, errors.New("call vk_parasite: missing credentials provider")
	}
	return options, nil
}

func (c *Client) recordInnerAuthFailure(metrics *telemetry.Accumulator, ctx context.Context, started time.Time, workerID uint16, reason string) {
	metrics.Set(telemetry.InnerAuthLatencyMS, telemetry.LatencyMS(started))
	if errors.Is(ctx.Err(), context.Canceled) {
		metrics.RecordEvent("inner_auth_interrupted", "inner_auth", "rebind", &workerID)
		return
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = "timeout"
	}
	metrics.Add(telemetry.InnerAuthFailureTotal, 1)
	metrics.RecordEvent("inner_auth_failed", "inner_auth", reason, &workerID)
}
