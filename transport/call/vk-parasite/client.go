package vkparasite

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/dtls/v3"
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
	ctx                   context.Context
	cancel                context.CancelFunc
	options               ClientOptions
	logger                logger.ContextLogger
	server                *net.UDPAddr
	key                   [wrapKeyLength]byte
	sessionID             [16]byte
	conv                  uint32
	tunnel                *ParasiteTunnel
	metrics               *telemetry.Accumulator
	telemetryLease        atomic.Int64
	telemetryLeaseExpired atomic.Bool
	telemetrySequence     atomic.Uint64
	telemetryBacklogMu    sync.Mutex
	telemetryEvents       [][]byte
	telemetrySnapshots    map[int][]byte
	processSampler        telemetry.ProcessSampler
	ready                 chan struct{}
	readyOnce             sync.Once
	generation            atomic.Uint64
	errors                chan error
	closeOnce             sync.Once
	workers               []clientWorkerControl
	workerReconnectMu     sync.Mutex
	rebindMu              sync.Mutex
	rebindCancel          context.CancelFunc
}

type clientWorkerControl struct {
	mu     sync.Mutex
	epoch  uint64
	cancel context.CancelFunc
	wake   chan struct{}
}

func newClientWorkerControl() clientWorkerControl {
	return clientWorkerControl{wake: make(chan struct{}, 1)}
}

func (c *clientWorkerControl) begin(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, uint64) {
	c.mu.Lock()
	c.epoch++
	epoch := c.epoch
	ctx, cancel := context.WithTimeout(parent, timeout)
	c.cancel = cancel
	c.mu.Unlock()
	return ctx, cancel, epoch
}

func (c *clientWorkerControl) finish(epoch uint64, cancel context.CancelFunc) {
	c.mu.Lock()
	if c.epoch == epoch {
		c.cancel = nil
	}
	c.mu.Unlock()
	cancel()
}

func (c *clientWorkerControl) interrupt() {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
	c.mu.Unlock()
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
	tunnel, err := newParasiteTunnel(conv, log, metrics)
	if err != nil {
		return nil, err
	}
	tunnel.SetRecoveryCoordinator(true)
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
		tunnel:    tunnel,
		metrics:   metrics,
		telemetrySnapshots: make(map[int][]byte, options.Workers+1),
		ready:     make(chan struct{}),
		errors:    make(chan error, options.Workers),
		workers:   make([]clientWorkerControl, options.Workers),
	}
	tunnel.SetTelemetryControlHandler(client.enableTelemetry)
	go client.telemetryLoop()
	for workerID := 0; workerID < options.Workers; workerID++ {
		client.workers[workerID] = newClientWorkerControl()
		go client.maintainWorker(uint16(workerID), options.JoinLinks[workerID%len(options.JoinLinks)])
	}
	go client.monitorConnectivity()
	timer := time.NewTimer(options.WorkerConnectTimeout)
	defer timer.Stop()
	var lastErr error
	for {
		select {
		case <-client.ready:
			return client, nil
		case err = <-client.errors:
			lastErr = err
		case <-timer.C:
			_ = client.Close()
			if lastErr != nil {
				return nil, fmt.Errorf("call vk_parasite: no VK TURN worker connected: %w", lastErr)
			}
			return nil, errors.New("call vk_parasite: no VK TURN worker connected before timeout")
		case <-parent.Done():
			_ = client.Close()
			return nil, parent.Err()
		}
	}
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
		return options, errors.New("call vk_parasite: exactly four VK lanes are required")
	}
	if options.WorkerConnectTimeout == 0 {
		options.WorkerConnectTimeout = 30 * time.Second
	}
	if options.WorkerConnectTimeout < time.Second || options.WorkerConnectTimeout > 2*time.Minute {
		return options, errors.New("call vk_parasite: worker_connect_timeout must be between 1s and 2m")
	}
	if options.Dialer == nil || options.Credentials == nil {
		return options, errors.New("call vk_parasite: missing network dependencies")
	}
	return options, nil
}

func (c *Client) maintainWorker(workerID uint16, joinLink string) {
	control := &c.workers[int(workerID)]
	metrics := c.tunnel.telemetryWorker(workerID)
	backoff := time.Second
	attempt := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		reconnecting := attempt > 0
		if reconnecting {
			metrics.Add(telemetry.WorkerReconnectTotal, 1)
		}
		attempt++
		if reconnecting {
			c.workerReconnectMu.Lock()
		}
		done, err := c.connectWorker(workerID, joinLink, control)
		if reconnecting {
			c.workerReconnectMu.Unlock()
		}
		if err == nil {
			backoff = time.Second
			c.readyOnce.Do(func() { close(c.ready) })
			select {
			case <-done:
				c.invalidateCredentials(joinLink)
			case <-c.ctx.Done():
				return
			}
		} else {
			select {
			case c.errors <- err:
			default:
			}
			c.logger.Debug(fmt.Sprintf("call vk_parasite: worker %d reconnecting after transport error", workerID))
		}
		metrics.Set(telemetry.WorkerReconnectBackoffMS, float64(backoff/time.Millisecond))
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-control.wake:
			timer.Stop()
			backoff = time.Second
		case <-c.ctx.Done():
			timer.Stop()
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (c *Client) connectWorker(workerID uint16, joinLink string, control *clientWorkerControl) (<-chan struct{}, error) {
	ctx, cancel, workerEpoch := control.begin(c.ctx, c.options.WorkerConnectTimeout)
	defer control.finish(workerEpoch, cancel)
	metrics := c.tunnel.telemetryWorker(workerID)
	ctx = telemetry.ContextWithAccumulator(ctx, metrics)
	credentials, err := c.options.Credentials(ctx, joinLink)
	if err != nil {
		return nil, err
	}
	attached := false
	defer func() {
		if !attached {
			c.invalidateCredentials(joinLink)
		}
	}()
	allocation, err := allocateTURN(ctx, c.options.Dialer, c.options.DNSRouter, credentials, int(workerID), metrics, workerID)
	if err != nil {
		return nil, err
	}
	codec, err := newRTPCodec(c.key)
	if err != nil {
		_ = allocation.Close()
		return nil, err
	}
	packetConn := newObfsPacketConn(allocation, c.server, codec, metrics)
	conn, err := dtls.Client(packetConn, c.server, &dtls.Config{
		InsecureSkipVerify:   true, // Authenticated by the outer key and the inner user attach.
		CipherSuites:         []dtls.CipherSuiteID{dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		MTU:                  1100,
	})
	if err != nil {
		metrics.Add(telemetry.DTLSHandshakeFailureTotal, 1)
		metrics.RecordEvent("dtls_handshake_failed", "dtls", "initialize", &workerID)
		_ = packetConn.Close()
		return nil, err
	}
	handshakeStarted := time.Now()
	if err = conn.HandshakeContext(ctx); err != nil {
		metrics.Set(telemetry.DTLSHandshakeLatencyMS, telemetry.LatencyMS(handshakeStarted))
		reason := telemetryFailureReason(err)
		if reason == "rebind" {
			metrics.RecordEvent("dtls_handshake_interrupted", "dtls", reason, &workerID)
		} else {
			metrics.Add(telemetry.DTLSHandshakeFailureTotal, 1)
			metrics.RecordEvent("dtls_handshake_failed", "dtls", reason, &workerID)
		}
		_ = conn.Close()
		return nil, err
	}
	metrics.Set(telemetry.DTLSHandshakeLatencyMS, telemetry.LatencyMS(handshakeStarted))
	metrics.Add(telemetry.DTLSHandshakeSuccessTotal, 1)
	stopAuthInterrupt := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopAuthInterrupt()
	innerAuthStarted := time.Now()
	laneGeneration := c.tunnel.LaneGeneration(workerID)
	request, err := encodeAuthRequest(authRequest{
		SessionID:   c.sessionID,
		Conv:        c.conv,
		WorkerID:    workerID,
		WorkerTotal: uint16(c.options.Workers),
		WorkerEpoch: workerEpoch,
		LaneGeneration: laneGeneration,
		User:        c.options.User,
		Password:    c.options.Password,
	})
	if err != nil {
		c.recordInnerAuthFailure(metrics, ctx, innerAuthStarted, workerID, "encode")
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(c.options.WorkerConnectTimeout))
	if _, err = conn.Write(request); err != nil {
		c.recordInnerAuthFailure(metrics, ctx, innerAuthStarted, workerID, "write")
		_ = conn.Close()
		return nil, err
	}
	ack := make([]byte, 14)
	n, err := conn.Read(ack)
	if err != nil {
		c.recordInnerAuthFailure(metrics, ctx, innerAuthStarted, workerID, "read")
		_ = conn.Close()
		return nil, err
	}
	generation, err := decodeAuthAck(ack[:n])
	if err != nil {
		c.recordInnerAuthFailure(metrics, ctx, innerAuthStarted, workerID, "rejected")
		_ = conn.Close()
		return nil, err
	}
	expectedGeneration := c.generation.Load()
	if expectedGeneration == 0 {
		c.generation.CompareAndSwap(0, generation)
		expectedGeneration = c.generation.Load()
	}
	if generation != expectedGeneration {
		c.recordInnerAuthFailure(metrics, ctx, innerAuthStarted, workerID, "generation")
		_ = conn.Close()
		_ = c.Close()
		return nil, errors.New("call vk_parasite: server session state was reset")
	}
	_ = conn.SetDeadline(time.Time{})
	done, err := c.tunnel.AddWorkerGenerationEpoch(workerID, laneGeneration, workerEpoch, conn)
	if err != nil {
		c.recordInnerAuthFailure(metrics, ctx, innerAuthStarted, workerID, "attach")
		_ = conn.Close()
		return nil, err
	}
	metrics.Set(telemetry.InnerAuthLatencyMS, telemetry.LatencyMS(innerAuthStarted))
	metrics.Add(telemetry.InnerAuthSuccessTotal, 1)
	attached = true
	return done, nil
}

func (c *Client) invalidateCredentials(joinLink string) {
	if c.options.InvalidateCredentials != nil {
		c.options.InvalidateCredentials(joinLink)
	}
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

func telemetryFailureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "rebind"
	default:
		return "transport"
	}
}

func (c *Client) Tunnel() *ParasiteTunnel { return c.tunnel }

func (c *Client) Done() <-chan struct{} { return c.ctx.Done() }

// RebindNetwork replaces physical TURN/DTLS transports one lane at a time.
// Keeping the other calls alive avoids a self-inflicted zero-path interval and
// the VK auth/TURN/DTLS thundering herd observed when all four were dropped at
// once. A second network notification supersedes the unfinished handover.
func (c *Client) RebindNetwork() {
	select {
	case <-c.ctx.Done():
		return
	default:
	}
	c.metrics.Add(telemetry.NetworkChangeTotal, 1)
	if c.tunnel.ActiveWorkers() > 0 {
		c.metrics.Add(telemetry.NetworkHandoverTotal, 1)
	}
	c.metrics.RecordEvent("network_changed", "network", "default_interface", nil)
	c.rebindMu.Lock()
	if c.rebindCancel != nil {
		c.rebindCancel()
	}
	rebindContext, cancel := context.WithCancel(c.ctx)
	c.rebindCancel = cancel
	c.rebindMu.Unlock()
	go c.rebindWorkers(rebindContext)
	c.logger.Info("call vk_parasite: network changed, staging worker transport rebinds")
}

func (c *Client) rebindWorkers(ctx context.Context) {
	for workerID := range c.workers {
		id := uint16(workerID)
		previousEpoch, _ := c.tunnel.WorkerEpoch(id)
		c.tunnel.recordEvent("network_rebind_lane_started", "network", "staged", &id)
		c.workers[workerID].interrupt()
		c.tunnel.DropWorker(id)
		if !c.waitWorkerReplacement(ctx, id, previousEpoch) {
			if ctx.Err() == nil {
				c.tunnel.recordEvent("network_rebind_lane_failed", "network", "timeout", &id)
				c.logger.Warn(fmt.Sprintf("call vk_parasite: worker %d did not recover during staged network handover", id))
			}
			return
		}
		c.tunnel.recordEvent("network_rebind_lane_completed", "network", "staged", &id)
	}
}

func (c *Client) waitWorkerReplacement(ctx context.Context, workerID uint16, previousEpoch uint64) bool {
	timeout := c.options.WorkerConnectTimeout / 3
	if timeout < 3*time.Second {
		timeout = 3 * time.Second
	}
	if timeout > 8*time.Second {
		timeout = 8 * time.Second
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	timer := time.NewTimer(timeout)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		if c.tunnel.workerReadyAfter(workerID, previousEpoch) {
			return true
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		case <-c.tunnel.Done():
			return false
		}
	}
}

func (c *Client) monitorConnectivity() {
	select {
	case <-c.ready:
	case <-c.ctx.Done():
		return
	case <-c.tunnel.Done():
		_ = c.Close()
		return
	}
	grace := c.options.WorkerConnectTimeout / 3
	if grace < 5*time.Second {
		grace = 5 * time.Second
	}
	if grace > 10*time.Second {
		grace = 10 * time.Second
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var disconnectedSince time.Time
	for {
		select {
		case now := <-ticker.C:
			if c.tunnel.ActiveWorkers() > 0 {
				disconnectedSince = time.Time{}
				continue
			}
			if disconnectedSince.IsZero() {
				disconnectedSince = now
				continue
			}
			if now.Sub(disconnectedSince) >= grace {
				_ = c.Close()
				return
			}
		case <-c.ctx.Done():
			return
		case <-c.tunnel.Done():
			_ = c.Close()
			return
		}
	}
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.rebindMu.Lock()
		if c.rebindCancel != nil {
			c.rebindCancel()
			c.rebindCancel = nil
		}
		c.rebindMu.Unlock()
		c.cancel()
		_ = c.tunnel.Close()
	})
	return nil
}

func randomConversationID() (uint32, error) {
	var buffer [4]byte
	for {
		if _, err := rand.Read(buffer[:]); err != nil {
			return 0, err
		}
		value := binary.BigEndian.Uint32(buffer[:])
		if value != 0 {
			return value, nil
		}
	}
}
