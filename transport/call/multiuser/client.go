package multiuser

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
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type ClientOptions struct {
	Server               M.Socksaddr
	JoinLinks            []string
	User                 string
	Password             string
	ObfsPassword         string
	Workers              int
	WorkerConnectTimeout time.Duration
	Dialer               N.Dialer
	DNSRouter            adapter.DNSRouter
	Credentials          CredentialProvider
}

type Client struct {
	ctx        context.Context
	cancel     context.CancelFunc
	options    ClientOptions
	logger     logger.ContextLogger
	server     *net.UDPAddr
	key        [wrapKeyLength]byte
	sessionID  [16]byte
	conv       uint32
	tunnel     *PooledTunnel
	ready      chan struct{}
	readyOnce  sync.Once
	generation atomic.Uint64
	errors     chan error
	closeOnce  sync.Once
	workers    []clientWorkerControl
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
	tunnel, err := NewPooledTunnel(conv, options.Workers, log)
	if err != nil {
		return nil, err
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
		tunnel:    tunnel,
		ready:     make(chan struct{}),
		errors:    make(chan error, options.Workers),
		workers:   make([]clientWorkerControl, options.Workers),
	}
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
				return nil, fmt.Errorf("call multi_user: no VK TURN worker connected: %w", lastErr)
			}
			return nil, errors.New("call multi_user: no VK TURN worker connected before timeout")
		case <-parent.Done():
			_ = client.Close()
			return nil, parent.Err()
		}
	}
}

func validateClientOptions(options ClientOptions) (ClientOptions, error) {
	if !options.Server.IsValid() || options.Server.Port == 0 {
		return options, errors.New("call multi_user: missing server or server_port")
	}
	if len(options.JoinLinks) < 1 || len(options.JoinLinks) > 4 {
		return options, errors.New("call multi_user: join_links must contain between 1 and 4 links")
	}
	normalizedLinks := make([]string, 0, len(options.JoinLinks))
	seenLinks := make(map[string]struct{}, len(options.JoinLinks))
	for _, link := range options.JoinLinks {
		link = strings.TrimSpace(link)
		if link == "" || len(link) > 2048 {
			return options, errors.New("call multi_user: invalid join_links entry")
		}
		if _, exists := seenLinks[link]; exists {
			return options, errors.New("call multi_user: duplicate join_links entry")
		}
		seenLinks[link] = struct{}{}
		normalizedLinks = append(normalizedLinks, link)
	}
	options.JoinLinks = normalizedLinks
	if err := validateAuthStrings(options.User, options.Password); err != nil {
		return options, err
	}
	if len(options.ObfsPassword) == 0 || len(options.ObfsPassword) > maximumPasswordLen {
		return options, errors.New("call multi_user: invalid obfs_password length")
	}
	if options.Workers == 0 {
		options.Workers = len(options.JoinLinks)
	}
	if options.Workers < 1 || options.Workers > HardMaxWorkers {
		return options, errors.New("call multi_user: workers outside hard bounds")
	}
	if options.Workers > 27*len(options.JoinLinks) {
		return options, errors.New("call multi_user: workers exceed 27 allocations per join link")
	}
	if options.WorkerConnectTimeout == 0 {
		options.WorkerConnectTimeout = 30 * time.Second
	}
	if options.WorkerConnectTimeout < time.Second || options.WorkerConnectTimeout > 2*time.Minute {
		return options, errors.New("call multi_user: worker_connect_timeout must be between 1s and 2m")
	}
	if options.Dialer == nil || options.Credentials == nil {
		return options, errors.New("call multi_user: missing network dependencies")
	}
	return options, nil
}

func (c *Client) maintainWorker(workerID uint16, joinLink string) {
	control := &c.workers[int(workerID)]
	backoff := time.Second
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		done, err := c.connectWorker(workerID, joinLink, control)
		if err == nil {
			backoff = time.Second
			c.readyOnce.Do(func() { close(c.ready) })
			select {
			case <-done:
			case <-c.ctx.Done():
				return
			}
		} else {
			select {
			case c.errors <- err:
			default:
			}
			c.logger.Debug(fmt.Sprintf("call multi_user: worker %d reconnecting after transport error", workerID))
		}
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
	credentials, err := c.options.Credentials(ctx, joinLink)
	if err != nil {
		return nil, err
	}
	allocation, err := allocateTURN(ctx, c.options.Dialer, c.options.DNSRouter, credentials, int(workerID))
	if err != nil {
		return nil, err
	}
	codec, err := newRTPCodec(c.key)
	if err != nil {
		_ = allocation.Close()
		return nil, err
	}
	packetConn := newObfsPacketConn(allocation, c.server, codec)
	conn, err := dtls.Client(packetConn, c.server, &dtls.Config{
		InsecureSkipVerify:   true, // Authenticated by the outer key and the inner user attach.
		CipherSuites:         []dtls.CipherSuiteID{dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		MTU:                  1100,
	})
	if err != nil {
		_ = packetConn.Close()
		return nil, err
	}
	if err = conn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	request, err := encodeAuthRequest(authRequest{
		SessionID:   c.sessionID,
		Conv:        c.conv,
		WorkerID:    workerID,
		WorkerTotal: uint16(c.options.Workers),
		WorkerEpoch: workerEpoch,
		User:        c.options.User,
		Password:    c.options.Password,
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(c.options.WorkerConnectTimeout))
	if _, err = conn.Write(request); err != nil {
		_ = conn.Close()
		return nil, err
	}
	ack := make([]byte, 14)
	n, err := conn.Read(ack)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	generation, err := decodeAuthAck(ack[:n])
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	expectedGeneration := c.generation.Load()
	if expectedGeneration == 0 {
		c.generation.CompareAndSwap(0, generation)
		expectedGeneration = c.generation.Load()
	}
	if generation != expectedGeneration {
		_ = conn.Close()
		_ = c.Close()
		return nil, errors.New("call multi_user: server session state was reset")
	}
	_ = conn.SetDeadline(time.Time{})
	done, err := c.tunnel.AddWorkerEpoch(workerID, workerEpoch, conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return done, nil
}

func (c *Client) Tunnel() *PooledTunnel { return c.tunnel }

func (c *Client) Done() <-chan struct{} { return c.ctx.Done() }

// RebindNetwork tears down only the physical TURN/DTLS worker transports.
// KCP, the logical session and RelayBridge remain alive while every worker
// immediately reconnects through the new default Android network.
func (c *Client) RebindNetwork() {
	select {
	case <-c.ctx.Done():
		return
	default:
	}
	for workerID := range c.workers {
		c.workers[workerID].interrupt()
		c.tunnel.DropWorker(uint16(workerID))
	}
	c.logger.Info("call multi_user: network changed, rebinding worker transports")
}

func (c *Client) monitorConnectivity() {
	select {
	case <-c.ready:
	case <-c.ctx.Done():
		return
	}
	grace := c.options.WorkerConnectTimeout / 2
	if grace < 15*time.Second {
		grace = 15 * time.Second
	}
	if grace > 30*time.Second {
		grace = 30 * time.Second
	}
	ticker := time.NewTicker(5 * time.Second)
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
		}
	}
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
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
