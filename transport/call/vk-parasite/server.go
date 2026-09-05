package vkparasite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"github.com/sagernet/sing/common/logger"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HardMaxWorkers              = MaximumWorkerCount
	HardMaxSessions             = 4096
	HardMaxUsers                = 4096
	HardMaxPendingHandshakes    = 4096
	HardMaxIngressWorkers       = 32
	HardMaxIngressQueuePackets  = 65536
	HardMaxPeerReadQueuePackets = 4096

	defaultMaxWorkers            = DefaultWorkerCount
	defaultMaxPendingHandshakes  = 256
	defaultHandshakeTimeout      = 15 * time.Second
	defaultSessionIdleTimeout    = 5 * time.Minute
	defaultUDPReceiveBufferBytes = 4 * 1024 * 1024
	defaultUDPSendBufferBytes    = 4 * 1024 * 1024
	defaultIngressQueuePackets   = 4096
	defaultPeerReadQueuePackets  = HardMaxPeerReadQueuePackets
	minimumPeerReadQueuePackets  = HardMaxPeerReadQueuePackets
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

type SessionHandler func(info SessionInfo, relay *QUICRelay) error

type ServerOptions struct {
	ObfsPassword          string
	Users                 []ServerUser
	MaxSessions           int
	MaxWorkersPerSession  int
	MaxPendingHandshakes  int
	HandshakeTimeout      time.Duration
	SessionIdleTimeout    time.Duration
	UDPReceiveBufferBytes int
	UDPSendBufferBytes    int
	IngressWorkers        int
	IngressQueuePackets   int
	PeerReadQueuePackets  int
	SessionHandler        SessionHandler
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
	relay           *QUICRelay
	ready           chan struct{}
	setupErr        error
	generation      uint64
	createdAt       time.Time
	pendingAttaches int
}

type Server struct {
	ctx        context.Context
	cancel     context.CancelFunc
	logger     logger.ContextLogger
	options    ServerOptions
	users      map[string]serverUser
	key        [wrapKeyLength]byte
	serverCert tls.Certificate

	packetConn    net.PacketConn
	peersMu       sync.Mutex
	peers         map[string]*peerPacketConn
	attachPeers   map[[32]byte]*peerPacketConn
	ingressQueues []chan receivedPacket
	ingressDepth  atomic.Int64

	sessionsMu   sync.Mutex
	sessions     map[[16]byte]*serverSession
	userSessions map[string]int
	pending      chan struct{}
	closeOnce    sync.Once
	done         chan struct{}
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
	certificate, err := newSelfSignedCertificate()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	server := &Server{
		ctx:          ctx,
		cancel:       cancel,
		logger:       log,
		options:      normalized,
		users:        users,
		key:          key,
		serverCert:   certificate,
		peers:        make(map[string]*peerPacketConn),
		attachPeers:  make(map[[32]byte]*peerPacketConn),
		sessions:     make(map[[16]byte]*serverSession),
		userSessions: make(map[string]int),
		pending:      make(chan struct{}, normalized.MaxPendingHandshakes),
		done:         make(chan struct{}),
	}
	return server, nil
}

func supportedWorkerCount(total uint16) bool {
	return total >= DefaultWorkerCount && total <= MaximumWorkerCount && total%CallCount == 0
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
	if options.MaxWorkersPerSession < DefaultWorkerCount || options.MaxWorkersPerSession > MaximumWorkerCount || options.MaxWorkersPerSession%CallCount != 0 {
		return options, nil, errors.New("call vk_parasite: max_workers_per_session must be 4, 8, 12, 16, or 20")
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
	// A speed-test burst can stop the DTLS reader briefly while one lane is being
	// recycled. A smaller per-peer queue dropped authenticated media packets and
	// converted that pause into RTO retransmission pressure. Keep the queue
	// bounded, but use the validated hard maximum for all wire-v9 sessions.
	if options.PeerReadQueuePackets < minimumPeerReadQueuePackets {
		options.PeerReadQueuePackets = minimumPeerReadQueuePackets
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
	s.ingressQueues = makeIngressQueues(s.options.IngressWorkers, s.options.IngressQueuePackets)
	s.peersMu.Unlock()
	s.configurePacketSocket(packetConn)
	go s.readLoop(packetConn, decoders)
	go s.reapLoop()
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
		payload, owner := takePacketCopy(buffer[:n])
		packet := receivedPacket{payload: payload, addr: remote, owner: owner}
		queue := s.ingressQueues[ingressShard(key, len(s.ingressQueues))]
		s.ingressDepth.Add(1)
		select {
		case queue <- packet:
		default:
			s.ingressDepth.Add(-1)
			releasePacketCopy(owner)
		}
	}
}

func (s *Server) processIngress(packets <-chan receivedPacket, decoder *rtpCodec) {
	plainBuffer := make([]byte, 0, maximumWirePacket)
	for {
		select {
		case packet, open := <-packets:
			if !open {
				return
			}
			s.ingressDepth.Add(-1)
			s.processWirePacket(packet.payload, packet.addr, decoder, plainBuffer)
			releasePacketCopy(packet.owner)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) processWirePacket(wire []byte, remote net.Addr, decoder *rtpCodec, plainBuffer []byte) {
	key := remote.Network() + "|" + remote.String()
	s.peersMu.Lock()
	peer := s.peers[key]
	s.peersMu.Unlock()
	plain, err := decoder.unwrap(plainBuffer, wire)
	if err != nil {
		return
	}
	var replacedPeer *peerPacketConn
	var duplicateAttachPeer *peerPacketConn
	rejectNewPeer := false
	s.peersMu.Lock()
	peer = s.peers[key]
	attach, isAttach := authAttachIdentity(plain)
	if peer != nil && isAttach && (peer.isEstablished() || peer.rememberAttach(attach)) {
		select {
		case s.pending <- struct{}{}:
			codec, codecErr := newRTPCodec(s.key)
			if codecErr == nil {
				replacedPeer = peer
				peer = newPeerPacketConn(s.packetConn, remote, codec, s.options.PeerReadQueuePackets)
				peer.rememberAttach(attach)
				s.peers[key] = peer
				duplicateAttachPeer = s.registerAttachLocked(key, attach, peer)
				go s.handlePeer(key, peer)
			} else {
				<-s.pending
			}
		default:
			peer = nil
			rejectNewPeer = true
		}
	}
	if peer == nil && !rejectNewPeer {
		select {
		case s.pending <- struct{}{}:
			codec, codecErr := newRTPCodec(s.key)
			if codecErr == nil {
				peer = newPeerPacketConn(s.packetConn, remote, codec, s.options.PeerReadQueuePackets)
				if isAttach {
					peer.rememberAttach(attach)
					duplicateAttachPeer = s.registerAttachLocked(key, attach, peer)
				}
				s.peers[key] = peer
				go s.handlePeer(key, peer)
			} else {
				<-s.pending
			}
		default:
		}
	}
	s.peersMu.Unlock()
	if replacedPeer != nil {
		_ = replacedPeer.Close()
	}
	if duplicateAttachPeer != nil && duplicateAttachPeer != replacedPeer {
		_ = duplicateAttachPeer.Close()
	}
	if peer == nil {
		return
	}
	// Повтор того же auth-фрейма пиру, который ещё не поднял QUIC, означает
	// потерянный ack: отвечаем тем же ответом. Отдать повтор в очередь нельзя —
	// quic-go выбросит его как мусор, и клиент будет ждать до таймаута.
	if isAttach && !peer.isEstablished() && peer.replayAuthAck() {
		return
	}
	peer.enqueue(plain, remote)
}

// registerAttachLocked keeps one pending attach per worker identity even when a
// TURN allocation changes the observable UDP endpoint. Without this, stale
// copies occupied slots until timeout and could reject the replacement workers
// needed by path recovery.
func (s *Server) registerAttachLocked(key string, identity [32]byte, peer *peerPacketConn) *peerPacketConn {
	previous := s.attachPeers[identity]
	if previous != nil && previous != peer {
		for previousKey, candidate := range s.peers {
			if candidate == previous && previousKey != key {
				delete(s.peers, previousKey)
			}
		}
	}
	s.attachPeers[identity] = peer
	return previous
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
		if identity := peer.attach.Load(); identity != nil && s.attachPeers[*identity] == peer {
			delete(s.attachPeers, *identity)
		}
		s.peersMu.Unlock()
		_ = peer.Close()
	}()

	// Auth-фрейм — первый пакет пира, он уже в очереди. Отвечаем на нём же
	// пакетном соединении: DTLS, который раньше несла эта фаза, снят.
	_ = peer.SetReadDeadline(time.Now().Add(s.options.HandshakeTimeout))
	authBuffer := make([]byte, maximumAuthFrameLen)
	n, _, err := peer.ReadFrom(authBuffer)
	if err != nil {
		return
	}
	request, err := decodeAuthRequest(authBuffer[:n])
	if reason, refused := s.refuseAuth(request, err); refused {
		refusal := encodeAuthAck(false, 0, reason)
		peer.storeAuthAck(refusal)
		_, _ = peer.WriteTo(refusal, peer.remote)
		return
	}
	session, created, err := s.getOrCreateSession(request)
	if err != nil {
		refusal := encodeAuthAck(false, 0, AuthRejectSession)
		peer.storeAuthAck(refusal)
		_, _ = peer.WriteTo(refusal, peer.remote)
		return
	}
	ack := encodeAuthAck(true, session.generation, AuthRejectUnspecified)
	peer.storeAuthAck(ack)
	if _, writeErr := peer.WriteTo(ack, peer.remote); writeErr != nil {
		s.releaseSessionAttach(session)
		if created {
			s.deleteSessionIfUnattached(request.SessionID, session)
		}
		return
	}
	_ = peer.SetDeadline(time.Time{})
	quicListener, listenerCloser, err := listenQUIC(peer, s.serverCert)
	if err != nil {
		s.releaseSessionAttach(session)
		if created {
			s.deleteSessionIfUnattached(request.SessionID, session)
		}
		return
	}
	defer listenerCloser.Close()
	quicConn, err := quicListener.Accept(s.ctx)
	s.releaseSessionAttach(session)
	if err != nil {
		if created {
			s.deleteSessionIfUnattached(request.SessionID, session)
		}
		return
	}
	session.relay.AttachServerConn(quicConn, listenerCloser)
	peer.markEstablished()
	releasePending()
	select {
	case <-quicConn.Context().Done():
	case <-s.ctx.Done():
	}
}

// refuseAuth decides whether a worker is turned away, and says which of the reasons it was.
//
// The three used to be one condition and one accept bit on the wire, so a wrong password and a
// worker count this server will not host were indistinguishable to the client — and to whoever
// was reading its log.
func (s *Server) refuseAuth(request authRequest, decodeErr error) (byte, bool) {
	switch {
	case decodeErr != nil:
		return AuthRejectMalformed, true
	case !supportedWorkerCount(request.WorkerTotal):
		return AuthRejectWorkerCount, true
	case int(request.WorkerTotal) > s.options.MaxWorkersPerSession:
		return AuthRejectWorkerCount, true
	case !s.authorize(request.User, request.Password):
		return AuthRejectCredentials, true
	default:
		return AuthRejectUnspecified, false
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
		evicted = s.evictSupersededUserSessionsLocked(request.User, record.maxSessions)
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
	relay := NewQUICRelay(s.ctx, QUICRelayOptions{
		PathCount: int(request.WorkerTotal),
		Logger:    s.logger,
	})
	generation, err := randomSessionGeneration()
	if err != nil {
		s.sessionsMu.Unlock()
		relay.Close()
		closeServerSessions(evicted)
		return nil, false, err
	}
	session := &serverSession{
		id:         request.SessionID,
		user:       request.User,
		conv:       request.Conv,
		expected:   request.WorkerTotal,
		relay:      relay,
		ready:      make(chan struct{}),
		generation: generation,
		createdAt:  time.Now(),
	}
	s.sessions[request.SessionID] = session
	s.userSessions[request.User]++
	s.sessionsMu.Unlock()
	closeServerSessions(evicted)

	err = s.options.SessionHandler(SessionInfo{ID: request.SessionID, User: request.User}, session.relay)
	s.sessionsMu.Lock()
	session.setupErr = err
	close(session.ready)
	if err != nil && s.sessions[request.SessionID] == session {
		delete(s.sessions, request.SessionID)
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
		relay.Close()
		return nil, false, err
	}
	return session, true, nil
}

func (s *Server) evictSupersededUserSessionsLocked(user string, maxSessions int) []*serverSession {
	needed := s.userSessions[user] - maxSessions + 1
	if needed < 1 {
		needed = 1
	}
	evicted := make([]*serverSession, 0, needed)
	for id, session := range s.sessions {
		if len(evicted) >= needed || session.user != user || session.pendingAttaches != 0 {
			continue
		}
		select {
		case <-session.ready:
		default:
			continue
		}
		delete(s.sessions, id)
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
		// Admission of a fully authenticated replacement session must not wait
		// for callbacks owned by the superseded relay. The old session has
		// already been removed from both accounting maps, so cleanup is isolated.
		go func(stale *serverSession) {
			if stale.relay != nil {
				stale.relay.Close()
			}
		}(session)
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
	if session == nil || session != expected || session.pendingAttaches != 0 || (session.relay != nil && session.relay.ActivePaths() != 0) {
		s.sessionsMu.Unlock()
		return
	}
	delete(s.sessions, id)
	if s.userSessions[session.user] <= 1 {
		delete(s.userSessions, session.user)
	} else {
		s.userSessions[session.user]--
	}
	s.sessionsMu.Unlock()
	if session.relay != nil {
		session.relay.Close()
	}
}

func (s *Server) deleteSessionIfUnattached(id [16]byte, expected *serverSession) {
	s.sessionsMu.Lock()
	session := s.sessions[id]
	if session == nil || session != expected || session.pendingAttaches != 0 || (session.relay != nil && session.relay.ActivePaths() != 0) {
		s.sessionsMu.Unlock()
		return
	}
	delete(s.sessions, id)
	if s.userSessions[session.user] <= 1 {
		delete(s.userSessions, session.user)
	} else {
		s.userSessions[session.user]--
	}
	s.sessionsMu.Unlock()
	if session.relay != nil {
		session.relay.Close()
	}
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
				if session.pendingAttaches == 0 && (session.relay == nil || session.relay.ActivePaths() == 0) && session.createdAt.Before(idleCutoff) {
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
		for _, session := range sessions {
			if session.relay != nil {
				session.relay.Close()
			}
		}
	})
	return nil
}
