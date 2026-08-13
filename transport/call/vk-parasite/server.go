package vkparasite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
)

const (
	HardMaxWorkers           = LaneCount
	HardMaxSessions          = 4096
	HardMaxUsers             = 4096
	HardMaxPendingHandshakes = 4096
	HardMaxIngressWorkers     = 32
	HardMaxIngressQueuePackets = 65536
	HardMaxPeerReadQueuePackets = 4096

	defaultMaxWorkers           = 4
	defaultMaxPendingHandshakes = 256
	defaultHandshakeTimeout     = 15 * time.Second
	defaultSessionIdleTimeout   = 5 * time.Minute
	defaultUDPReceiveBufferBytes = 4 * 1024 * 1024
	defaultUDPSendBufferBytes    = 4 * 1024 * 1024
	defaultIngressQueuePackets   = 4096
	defaultPeerReadQueuePackets  = 512
	sessionTakeoverGrace        = workerStaleReplacement
)

type ServerUser struct {
	Name        string
	Password    string
	MaxSessions int
}

type SessionInfo struct {
	ID   [16]byte
	User string
}

type SessionHandler func(info SessionInfo, tunnel *ParasiteTunnel) error

type ServerOptions struct {
	ObfsPassword             string
	Users                    []ServerUser
	MaxSessions              int
	MaxWorkersPerSession     int
	MaxPendingHandshakes     int
	HandshakeTimeout         time.Duration
	SessionIdleTimeout       time.Duration
	UDPReceiveBufferBytes    int
	UDPSendBufferBytes       int
	IngressWorkers           int
	IngressQueuePackets      int
	PeerReadQueuePackets     int
	SessionHandler           SessionHandler
	TelemetryStateDirectory string
	TelemetryOutputPath     string
	TelemetryInterval       time.Duration
}

type serverUser struct {
	passwordHash [sha256.Size]byte
	maxSessions  int
}

type serverSession struct {
	id              [16]byte
	user            string
	conv            uint32
	expected        uint16
	tunnel          *ParasiteTunnel
	ready           chan struct{}
	setupErr        error
	generation      uint64
	createdAt       time.Time
	pendingAttaches int
	telemetryMu      sync.Mutex
	telemetryWindow  time.Time
	telemetryRecords int
	telemetryBytes   int
}

type Server struct {
	ctx        context.Context
	cancel     context.CancelFunc
	logger     logger.ContextLogger
	options    ServerOptions
	users      map[string]serverUser
	key        [wrapKeyLength]byte
	dtlsConfig *dtls.Config

	packetConn net.PacketConn
	decoder    *rtpCodec
	peersMu    sync.Mutex
	peers      map[string]*peerPacketConn
	ingressQueues []chan receivedPacket
	ingressDepth atomic.Int64

	sessionsMu   sync.Mutex
	sessions     map[[16]byte]*serverSession
	userSessions map[string]int
	pending      chan struct{}
	closeOnce    sync.Once
	done         chan struct{}
	telemetry    *serverTelemetry
}

func NewServer(parent context.Context, options ServerOptions, log logger.ContextLogger) (*Server, error) {
	if log == nil {
		log = logger.NOP()
	}
	normalized, users, err := validateServerOptions(options)
	if err != nil {
		return nil, err
	}
	key, err := deriveWrapKey(normalized.ObfsPassword)
	if err != nil {
		return nil, err
	}
	certificate, err := selfsign.GenerateSelfSigned()
	if err != nil {
		return nil, fmt.Errorf("call vk_parasite: generate DTLS certificate: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	server := &Server{
		ctx:     ctx,
		cancel:  cancel,
		logger:  log,
		options: normalized,
		users:   users,
		key:     key,
		dtlsConfig: &dtls.Config{
			Certificates:         []tls.Certificate{certificate},
			CipherSuites:         []dtls.CipherSuiteID{dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
			ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
			MTU:                  1100,
		},
		peers:        make(map[string]*peerPacketConn),
		sessions:     make(map[[16]byte]*serverSession),
		userSessions: make(map[string]int),
		pending:      make(chan struct{}, normalized.MaxPendingHandshakes),
		done:         make(chan struct{}),
	}
	server.telemetry = newServerTelemetry(server, normalized, log)
	return server, nil
}

func validateServerOptions(options ServerOptions) (ServerOptions, map[string]serverUser, error) {
	if options.SessionHandler == nil {
		return options, nil, errors.New("call vk_parasite: missing session handler")
	}
	if len(options.ObfsPassword) == 0 || len(options.ObfsPassword) > maximumPasswordLen {
		return options, nil, errors.New("call vk_parasite: invalid obfs_password length")
	}
	if len(options.Users) == 0 || len(options.Users) > HardMaxUsers {
		return options, nil, errors.New("call vk_parasite: users must contain between 1 and 4096 entries")
	}
	if options.MaxSessions == 0 {
		options.MaxSessions = len(options.Users)
	}
	if options.MaxSessions < 1 || options.MaxSessions > HardMaxSessions {
		return options, nil, errors.New("call vk_parasite: max_sessions outside hard bounds")
	}
	if options.MaxWorkersPerSession == 0 {
		options.MaxWorkersPerSession = defaultMaxWorkers
	}
	if options.MaxWorkersPerSession != LaneCount {
		return options, nil, errors.New("call vk_parasite: max_workers_per_session must be four")
	}
	if options.MaxPendingHandshakes == 0 {
		options.MaxPendingHandshakes = defaultMaxPendingHandshakes
	}
	if options.MaxPendingHandshakes < 1 || options.MaxPendingHandshakes > HardMaxPendingHandshakes {
		return options, nil, errors.New("call vk_parasite: max_pending_handshakes outside hard bounds")
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = defaultHandshakeTimeout
	}
	if options.HandshakeTimeout < time.Second || options.HandshakeTimeout > time.Minute {
		return options, nil, errors.New("call vk_parasite: handshake_timeout must be between 1s and 1m")
	}
	if options.SessionIdleTimeout == 0 {
		options.SessionIdleTimeout = defaultSessionIdleTimeout
	}
	if options.SessionIdleTimeout < 30*time.Second || options.SessionIdleTimeout > 24*time.Hour {
		return options, nil, errors.New("call vk_parasite: session_idle_timeout must be between 30s and 24h")
	}
	if options.UDPReceiveBufferBytes == 0 {
		options.UDPReceiveBufferBytes = defaultUDPReceiveBufferBytes
	}
	if options.UDPSendBufferBytes == 0 {
		options.UDPSendBufferBytes = defaultUDPSendBufferBytes
	}
	if options.UDPReceiveBufferBytes < 256*1024 || options.UDPReceiveBufferBytes > 64*1024*1024 ||
		options.UDPSendBufferBytes < 256*1024 || options.UDPSendBufferBytes > 64*1024*1024 {
		return options, nil, errors.New("call vk_parasite: UDP socket buffers must be between 256 KiB and 64 MiB")
	}
	if options.IngressWorkers == 0 {
		options.IngressWorkers = min(4, max(1, runtime.GOMAXPROCS(0)))
	}
	if options.IngressWorkers < 1 || options.IngressWorkers > HardMaxIngressWorkers {
		return options, nil, errors.New("call vk_parasite: ingress_workers outside hard bounds")
	}
	if options.IngressQueuePackets == 0 {
		options.IngressQueuePackets = defaultIngressQueuePackets
	}
	if options.IngressQueuePackets < options.IngressWorkers || options.IngressQueuePackets > HardMaxIngressQueuePackets {
		return options, nil, errors.New("call vk_parasite: ingress_queue_packets outside hard bounds")
	}
	if options.PeerReadQueuePackets == 0 {
		options.PeerReadQueuePackets = defaultPeerReadQueuePackets
	}
	if options.PeerReadQueuePackets < 16 || options.PeerReadQueuePackets > HardMaxPeerReadQueuePackets {
		return options, nil, errors.New("call vk_parasite: peer_read_queue_packets outside hard bounds")
	}
	users := make(map[string]serverUser, len(options.Users))
	for _, user := range options.Users {
		if err := validateAuthStrings(user.Name, user.Password); err != nil {
			return options, nil, err
		}
		if _, exists := users[user.Name]; exists {
			return options, nil, errors.New("call vk_parasite: duplicate user name")
		}
		maxSessions := user.MaxSessions
		if maxSessions == 0 {
			maxSessions = 1
		}
		if maxSessions < 1 || maxSessions > options.MaxSessions {
			return options, nil, errors.New("call vk_parasite: user max_sessions outside global bounds")
		}
		users[user.Name] = serverUser{passwordHash: sha256.Sum256([]byte(user.Password)), maxSessions: maxSessions}
	}
	return options, users, nil
}

func (s *Server) Start(packetConn net.PacketConn) error {
	if packetConn == nil {
		return errors.New("call vk_parasite: missing UDP listener")
	}
	s.peersMu.Lock()
	if s.packetConn != nil {
		s.peersMu.Unlock()
		return errors.New("call vk_parasite: server already started")
	}
	s.packetConn = packetConn
	decoders := make([]*rtpCodec, s.options.IngressWorkers)
	for index := range decoders {
		decoder, err := newRTPCodec(s.key)
		if err != nil {
			s.peersMu.Unlock()
			return err
		}
		decoders[index] = decoder
	}
	s.decoder = decoders[0]
	s.ingressQueues = makeIngressQueues(s.options.IngressWorkers, s.options.IngressQueuePackets)
	s.peersMu.Unlock()
	s.configurePacketSocket(packetConn)
	go s.readLoop(packetConn, decoders)
	go s.reapLoop()
	s.telemetry.start()
	return nil
}

func (s *Server) readLoop(packetConn net.PacketConn, decoders []*rtpCodec) {
	var processors sync.WaitGroup
	for index, queue := range s.ingressQueues {
		processors.Add(1)
		go func(packets <-chan receivedPacket, decoder *rtpCodec) {
			defer processors.Done()
			s.processIngress(packets, decoder)
		}(queue, decoders[index])
	}
	defer func() {
		for _, queue := range s.ingressQueues {
			close(queue)
		}
		processors.Wait()
		close(s.done)
	}()
	buffer := make([]byte, maximumWirePacket)
	for {
		n, remote, err := packetConn.ReadFrom(buffer)
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Warn("call vk_parasite: UDP listener stopped: ", err)
				return
			}
		}
		key := remote.Network() + "|" + remote.String()
		packet := receivedPacket{payload: append([]byte(nil), buffer[:n]...), addr: remote}
		queue := s.ingressQueues[ingressShard(key, len(s.ingressQueues))]
		s.ingressDepth.Add(1)
		select {
		case queue <- packet:
		default:
			s.ingressDepth.Add(-1)
			s.telemetry.metrics.AddHot(telemetry.UDPIngressQueueDropsTotal, 1)
		}
	}
}

func (s *Server) processIngress(packets <-chan receivedPacket, decoder *rtpCodec) {
	for {
		select {
		case packet, open := <-packets:
			if !open {
				return
			}
			s.ingressDepth.Add(-1)
			s.processWirePacket(packet.payload, packet.addr, decoder)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) processWirePacket(wire []byte, remote net.Addr, decoder *rtpCodec) {
	key := remote.Network() + "|" + remote.String()
	s.peersMu.Lock()
	peer := s.peers[key]
	s.peersMu.Unlock()
	metrics := s.telemetry.metrics
	if peer != nil {
		metrics = peer.telemetryMetrics()
	}
	plain, err := decoder.unwrap(wire)
	if err != nil {
		metrics.AddHot(telemetry.OuterAuthFailuresTotal, 1)
		return
	}
	metrics.AddHot(telemetry.OuterPacketsInTotal, 1)
	metrics.AddHot(telemetry.OuterBytesInTotal, uint64(len(wire)))
	metrics.AddHot(telemetry.OuterPayloadBytesInTotal, uint64(len(plain)))
	metrics.AddHot(telemetry.OuterOverheadBytesInTotal, uint64(len(wire)-len(plain)))
	if metrics.CollectionActive() {
		metrics.ObserveOuterPacket(wire, time.Now())
	}
	s.peersMu.Lock()
	peer = s.peers[key]
	if peer == nil {
		select {
		case s.pending <- struct{}{}:
			codec, codecErr := newRTPCodec(s.key)
			if codecErr == nil {
				peer = newPeerPacketConn(s.packetConn, remote, codec, s.telemetry.metrics, s.options.PeerReadQueuePackets)
				s.peers[key] = peer
				go s.handlePeer(key, peer)
			} else {
				<-s.pending
			}
		default:
			s.telemetry.metrics.Add(telemetry.HandshakeRejectedTotal, 1)
			s.telemetry.event("handshake_rejected", "handshake", "pending_limit", "", [16]byte{}, nil)
		}
	}
	s.peersMu.Unlock()
	if peer != nil {
		peer.enqueue(plain, remote)
	}
}

func makeIngressQueues(workers, totalCapacity int) []chan receivedPacket {
	queues := make([]chan receivedPacket, workers)
	baseCapacity := totalCapacity / workers
	remainder := totalCapacity % workers
	for index := range queues {
		capacity := baseCapacity
		if index < remainder {
			capacity++
		}
		queues[index] = make(chan receivedPacket, capacity)
	}
	return queues
}

func ingressShard(key string, shards int) int {
	hash := uint32(2166136261)
	for index := 0; index < len(key); index++ {
		hash ^= uint32(key[index])
		hash *= 16777619
	}
	return int(hash % uint32(shards))
}

func (s *Server) handlePeer(key string, peer *peerPacketConn) {
	pendingReleased := false
	releasePending := func() {
		if !pendingReleased {
			<-s.pending
			pendingReleased = true
		}
	}
	defer func() {
		releasePending()
		s.peersMu.Lock()
		if s.peers[key] == peer {
			delete(s.peers, key)
		}
		s.peersMu.Unlock()
		_ = peer.Close()
	}()

	handshakeStarted := time.Now()
	handshakeMeasured := false
	defer func() {
		if !handshakeMeasured {
			s.telemetry.metrics.Set(telemetry.HandshakeLatencyMS, telemetry.LatencyMS(handshakeStarted))
		}
	}()
	conn, err := dtls.Server(peer, peer.remote, s.dtlsConfig)
	if err != nil {
		s.telemetry.metrics.Add(telemetry.DTLSHandshakeFailureTotal, 1)
		s.telemetry.event("dtls_handshake_failed", "dtls", "initialize", "", [16]byte{}, nil)
		return
	}
	defer conn.Close()
	dtlsStarted := time.Now()
	handshakeCtx, cancel := context.WithTimeout(s.ctx, s.options.HandshakeTimeout)
	err = conn.HandshakeContext(handshakeCtx)
	cancel()
	s.telemetry.metrics.Set(telemetry.DTLSHandshakeLatencyMS, telemetry.LatencyMS(dtlsStarted))
	if err != nil {
		if s.ctx.Err() != nil {
			return
		}
		s.telemetry.metrics.Add(telemetry.DTLSHandshakeFailureTotal, 1)
		reason := telemetryFailureReason(err)
		if reason == "timeout" {
			s.telemetry.metrics.Add(telemetry.HandshakeTimeoutTotal, 1)
		}
		s.telemetry.event("dtls_handshake_failed", "dtls", reason, "", [16]byte{}, nil)
		return
	}
	s.telemetry.metrics.Add(telemetry.DTLSHandshakeSuccessTotal, 1)
	_ = conn.SetReadDeadline(time.Now().Add(s.options.HandshakeTimeout))
	authBuffer := make([]byte, maximumAuthFrameLen)
	n, err := conn.Read(authBuffer)
	if err != nil {
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			s.telemetry.metrics.Add(telemetry.HandshakeTimeoutTotal, 1)
			s.telemetry.event("handshake_timeout", "inner_auth", "timeout", "", [16]byte{}, nil)
		}
		return
	}
	request, err := decodeAuthRequest(authBuffer[:n])
	if err != nil || int(request.WorkerTotal) > s.options.MaxWorkersPerSession || !s.authorize(request.User, request.Password) {
		s.telemetry.metrics.Add(telemetry.AuthFailureTotal, 1)
		s.telemetry.event("auth_failed", "inner_auth", "rejected", "", [16]byte{}, nil)
		_, _ = conn.Write(encodeAuthAck(false, 0))
		return
	}
	s.telemetry.metrics.Add(telemetry.AuthSuccessTotal, 1)
	session, created, err := s.getOrCreateSession(request)
	if err != nil {
		s.telemetry.metrics.Add(telemetry.HandshakeRejectedTotal, 1)
		s.telemetry.metrics.Add(telemetry.WorkerAttachFailureTotal, 1)
		s.telemetry.event("worker_attach_failed", "worker", "session_rejected", request.User, request.SessionID, &request.WorkerID)
		_, _ = conn.Write(encodeAuthAck(false, 0))
		return
	}
	workerMetrics := session.tunnel.telemetryWorker(request.WorkerID)
	peer.setTelemetryMetrics(workerMetrics)
	done, err := session.tunnel.AttachWorkerEpoch(request.WorkerID, request.WorkerEpoch, conn, func() error {
		_, writeErr := conn.Write(encodeAuthAck(true, session.generation))
		if writeErr == nil {
			writeErr = conn.SetDeadline(time.Time{})
		}
		return writeErr
	})
	s.releaseSessionAttach(session)
	if err != nil {
		workerMetrics.Add(telemetry.WorkerAttachFailureTotal, 1)
		s.telemetry.event("worker_attach_failed", "worker", "attach", request.User, request.SessionID, &request.WorkerID)
		if created {
			s.deleteSessionIfUnattached(request.SessionID, session)
		}
		_, _ = conn.Write(encodeAuthAck(false, 0))
		return
	}
	workerMetrics.Add(telemetry.WorkerAttachSuccessTotal, 1)
	s.telemetry.event("worker_attached", "worker", "success", request.User, request.SessionID, &request.WorkerID)
	s.telemetry.metrics.Set(telemetry.HandshakeLatencyMS, telemetry.LatencyMS(handshakeStarted))
	handshakeMeasured = true
	releasePending()
	select {
	case <-done:
	case <-s.ctx.Done():
	}
}

func (s *Server) authorize(name, password string) bool {
	record, exists := s.users[name]
	expected := record.passwordHash
	actual := sha256.Sum256([]byte(password))
	matched := subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
	return exists && matched
}

func (s *Server) getOrCreateSession(request authRequest) (*serverSession, bool, error) {
	s.sessionsMu.Lock()
	if session := s.sessions[request.SessionID]; session != nil {
		if session.user != request.User || session.conv != request.Conv || session.expected != request.WorkerTotal {
			s.sessionsMu.Unlock()
			return nil, false, errors.New("call vk_parasite: session identity mismatch")
		}
		ready := session.ready
		s.sessionsMu.Unlock()
		select {
		case <-ready:
			s.sessionsMu.Lock()
			if s.sessions[request.SessionID] != session {
				s.sessionsMu.Unlock()
				return nil, false, errors.New("call vk_parasite: session was replaced")
			}
			if session.setupErr != nil {
				sessionsErr := session.setupErr
				s.sessionsMu.Unlock()
				return nil, false, sessionsErr
			}
			session.pendingAttaches++
			s.sessionsMu.Unlock()
			return session, false, nil
		case <-s.ctx.Done():
			return nil, false, s.ctx.Err()
		}
	}
	record := s.users[request.User]
	var evicted []*serverSession
	if len(s.sessions) >= s.options.MaxSessions || s.userSessions[request.User] >= record.maxSessions {
		evicted = s.evictDisconnectedUserSessionsLocked(request.User, time.Now())
	}
	if len(s.sessions) >= s.options.MaxSessions {
		s.sessionsMu.Unlock()
		closeServerSessions(evicted)
		return nil, false, errors.New("call vk_parasite: global session limit reached")
	}
	if s.userSessions[request.User] >= record.maxSessions {
		s.sessionsMu.Unlock()
		closeServerSessions(evicted)
		return nil, false, errors.New("call vk_parasite: user session limit reached")
	}
	tunnel, err := NewParasiteTunnel(request.Conv, s.logger)
	if err != nil {
		s.sessionsMu.Unlock()
		closeServerSessions(evicted)
		return nil, false, err
	}
	tunnel.SetTelemetryCounterParent(s.telemetry.metrics)
	tunnel.SetTelemetryCollectionActive(s.telemetry.metrics.CollectionActive())
	generation, err := randomSessionGeneration()
	if err != nil {
		s.sessionsMu.Unlock()
		_ = tunnel.Close()
		closeServerSessions(evicted)
		return nil, false, err
	}
	session := &serverSession{
		id:         request.SessionID,
		user:       request.User,
		conv:       request.Conv,
		expected:   request.WorkerTotal,
		tunnel:     tunnel,
		ready:      make(chan struct{}),
		generation: generation,
		createdAt:  time.Now(),
	}
	tunnel.SetTelemetryClientRecordHandler(func(payload []byte) {
		s.telemetry.clientRecord(session, payload)
	})
	s.sessions[request.SessionID] = session
	s.userSessions[request.User]++
	s.telemetry.metrics.Add(telemetry.SessionCreatedTotal, 1)
	s.sessionsMu.Unlock()
	closeServerSessions(evicted)

	err = s.options.SessionHandler(SessionInfo{ID: request.SessionID, User: request.User}, session.tunnel)
	s.sessionsMu.Lock()
	session.setupErr = err
	close(session.ready)
	if err != nil && s.sessions[request.SessionID] == session {
		delete(s.sessions, request.SessionID)
		s.telemetry.metrics.Add(telemetry.SessionClosedTotal, 1)
		if s.userSessions[request.User] <= 1 {
			delete(s.userSessions, request.User)
		} else {
			s.userSessions[request.User]--
		}
	} else if s.sessions[request.SessionID] == session {
		session.pendingAttaches++
	}
	s.sessionsMu.Unlock()
	if err != nil {
		_ = tunnel.Close()
		return nil, false, err
	}
	return session, true, nil
}

func (s *Server) evictDisconnectedUserSessionsLocked(user string, now time.Time) []*serverSession {
	evicted := make([]*serverSession, 0, 1)
	for id, session := range s.sessions {
		if session.user != user || session.pendingAttaches != 0 || now.Sub(session.createdAt) < sessionTakeoverGrace {
			continue
		}
		select {
		case <-session.ready:
		default:
			continue
		}
		if session.tunnel.ActiveWorkers() != 0 {
			continue
		}
		delete(s.sessions, id)
		s.telemetry.metrics.Add(telemetry.SessionClosedTotal, 1)
		evicted = append(evicted, session)
	}
	if len(evicted) > 0 {
		remaining := s.userSessions[user] - len(evicted)
		if remaining <= 0 {
			delete(s.userSessions, user)
		} else {
			s.userSessions[user] = remaining
		}
	}
	return evicted
}

func (s *Server) releaseSessionAttach(session *serverSession) {
	s.sessionsMu.Lock()
	if session.pendingAttaches > 0 {
		session.pendingAttaches--
	}
	s.sessionsMu.Unlock()
}

func closeServerSessions(sessions []*serverSession) {
	for _, session := range sessions {
		_ = session.tunnel.Close()
	}
}

func randomSessionGeneration() (uint64, error) {
	var buffer [8]byte
	for {
		if _, err := rand.Read(buffer[:]); err != nil {
			return 0, err
		}
		generation := binary.BigEndian.Uint64(buffer[:])
		if generation != 0 {
			return generation, nil
		}
	}
}

func (s *Server) deleteIdleSession(id [16]byte, expected *serverSession, idleCutoff time.Time) {
	s.sessionsMu.Lock()
	session := s.sessions[id]
	if session == nil || session != expected || session.pendingAttaches != 0 || session.tunnel.ActiveWorkers() != 0 || session.tunnel.LastActivity().After(idleCutoff) {
		s.sessionsMu.Unlock()
		return
	}
	delete(s.sessions, id)
	s.telemetry.metrics.Add(telemetry.SessionClosedTotal, 1)
	if s.userSessions[session.user] <= 1 {
		delete(s.userSessions, session.user)
	} else {
		s.userSessions[session.user]--
	}
	s.sessionsMu.Unlock()
	_ = session.tunnel.Close()
}

func (s *Server) deleteSessionIfUnattached(id [16]byte, expected *serverSession) {
	s.sessionsMu.Lock()
	session := s.sessions[id]
	if session == nil || session != expected || session.pendingAttaches != 0 || session.tunnel.ActiveWorkers() != 0 {
		s.sessionsMu.Unlock()
		return
	}
	delete(s.sessions, id)
	s.telemetry.metrics.Add(telemetry.SessionClosedTotal, 1)
	if s.userSessions[session.user] <= 1 {
		delete(s.userSessions, session.user)
	} else {
		s.userSessions[session.user]--
	}
	s.sessionsMu.Unlock()
	_ = session.tunnel.Close()
}

func (s *Server) reapLoop() {
	interval := s.options.SessionIdleTimeout / 2
	if interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			idleCutoff := now.Add(-s.options.SessionIdleTimeout)
			s.sessionsMu.Lock()
			stale := make([]*serverSession, 0)
			for _, session := range s.sessions {
				if session.pendingAttaches == 0 && session.tunnel.ActiveWorkers() == 0 && !session.tunnel.LastActivity().After(idleCutoff) {
					stale = append(stale, session)
				}
			}
			s.sessionsMu.Unlock()
			for _, session := range stale {
				s.deleteIdleSession(session.id, session, idleCutoff)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.peersMu.Lock()
		packetConn := s.packetConn
		s.peersMu.Unlock()
		if packetConn != nil {
			_ = packetConn.Close()
		}
		s.peersMu.Lock()
		peers := make([]*peerPacketConn, 0, len(s.peers))
		for _, peer := range s.peers {
			peers = append(peers, peer)
		}
		s.peers = make(map[string]*peerPacketConn)
		s.peersMu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
		s.sessionsMu.Lock()
		sessions := make([]*serverSession, 0, len(s.sessions))
		for _, session := range s.sessions {
			sessions = append(sessions, session)
		}
		s.sessions = make(map[[16]byte]*serverSession)
		s.userSessions = make(map[string]int)
		s.sessionsMu.Unlock()
		s.telemetry.metrics.Add(telemetry.SessionClosedTotal, uint64(len(sessions)))
		for _, session := range sessions {
			_ = session.tunnel.Close()
		}
		_ = s.telemetry.close()
	})
	return nil
}
