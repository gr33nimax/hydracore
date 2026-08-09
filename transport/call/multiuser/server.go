package multiuser

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
	"sync"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	"github.com/sagernet/sing/common/logger"
)

const (
	HardMaxWorkers           = 108
	HardMaxSessions          = 4096
	HardMaxUsers             = 4096
	HardMaxPendingHandshakes = 4096

	defaultMaxWorkers           = 4
	defaultMaxPendingHandshakes = 256
	defaultHandshakeTimeout     = 15 * time.Second
	defaultSessionIdleTimeout   = 5 * time.Minute
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

type SessionHandler func(info SessionInfo, tunnel *PooledTunnel) error

type ServerOptions struct {
	ObfsPassword         string
	Users                []ServerUser
	MaxSessions          int
	MaxWorkersPerSession int
	MaxPendingHandshakes int
	HandshakeTimeout     time.Duration
	SessionIdleTimeout   time.Duration
	SessionHandler       SessionHandler
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
	tunnel          *PooledTunnel
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
	dtlsConfig *dtls.Config

	packetConn net.PacketConn
	decoder    *rtpCodec
	peersMu    sync.Mutex
	peers      map[string]*peerPacketConn

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
	certificate, err := selfsign.GenerateSelfSigned()
	if err != nil {
		return nil, fmt.Errorf("call multi_user: generate DTLS certificate: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	return &Server{
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
	}, nil
}

func validateServerOptions(options ServerOptions) (ServerOptions, map[string]serverUser, error) {
	if options.SessionHandler == nil {
		return options, nil, errors.New("call multi_user: missing session handler")
	}
	if len(options.ObfsPassword) == 0 || len(options.ObfsPassword) > maximumPasswordLen {
		return options, nil, errors.New("call multi_user: invalid obfs_password length")
	}
	if len(options.Users) == 0 || len(options.Users) > HardMaxUsers {
		return options, nil, errors.New("call multi_user: users must contain between 1 and 4096 entries")
	}
	if options.MaxSessions == 0 {
		options.MaxSessions = len(options.Users)
	}
	if options.MaxSessions < 1 || options.MaxSessions > HardMaxSessions {
		return options, nil, errors.New("call multi_user: max_sessions outside hard bounds")
	}
	if options.MaxWorkersPerSession == 0 {
		options.MaxWorkersPerSession = defaultMaxWorkers
	}
	if options.MaxWorkersPerSession < 1 || options.MaxWorkersPerSession > HardMaxWorkers {
		return options, nil, errors.New("call multi_user: max_workers_per_session outside hard bounds")
	}
	if options.MaxPendingHandshakes == 0 {
		options.MaxPendingHandshakes = defaultMaxPendingHandshakes
	}
	if options.MaxPendingHandshakes < 1 || options.MaxPendingHandshakes > HardMaxPendingHandshakes {
		return options, nil, errors.New("call multi_user: max_pending_handshakes outside hard bounds")
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = defaultHandshakeTimeout
	}
	if options.HandshakeTimeout < time.Second || options.HandshakeTimeout > time.Minute {
		return options, nil, errors.New("call multi_user: handshake_timeout must be between 1s and 1m")
	}
	if options.SessionIdleTimeout == 0 {
		options.SessionIdleTimeout = defaultSessionIdleTimeout
	}
	if options.SessionIdleTimeout < 30*time.Second || options.SessionIdleTimeout > 24*time.Hour {
		return options, nil, errors.New("call multi_user: session_idle_timeout must be between 30s and 24h")
	}
	users := make(map[string]serverUser, len(options.Users))
	for _, user := range options.Users {
		if err := validateAuthStrings(user.Name, user.Password); err != nil {
			return options, nil, err
		}
		if _, exists := users[user.Name]; exists {
			return options, nil, errors.New("call multi_user: duplicate user name")
		}
		maxSessions := user.MaxSessions
		if maxSessions == 0 {
			maxSessions = 1
		}
		if maxSessions < 1 || maxSessions > options.MaxSessions {
			return options, nil, errors.New("call multi_user: user max_sessions outside global bounds")
		}
		users[user.Name] = serverUser{passwordHash: sha256.Sum256([]byte(user.Password)), maxSessions: maxSessions}
	}
	return options, users, nil
}

func (s *Server) Start(packetConn net.PacketConn) error {
	if packetConn == nil {
		return errors.New("call multi_user: missing UDP listener")
	}
	s.peersMu.Lock()
	if s.packetConn != nil {
		s.peersMu.Unlock()
		return errors.New("call multi_user: server already started")
	}
	s.packetConn = packetConn
	decoder, err := newRTPCodec(s.key)
	if err != nil {
		s.peersMu.Unlock()
		return err
	}
	s.decoder = decoder
	s.peersMu.Unlock()
	go s.readLoop(packetConn, decoder)
	go s.reapLoop()
	return nil
}

func (s *Server) readLoop(packetConn net.PacketConn, decoder *rtpCodec) {
	defer close(s.done)
	buffer := make([]byte, maximumWirePacket)
	for {
		n, remote, err := packetConn.ReadFrom(buffer)
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Warn("call multi_user: UDP listener stopped: ", err)
				return
			}
		}
		plain, err := decoder.unwrap(buffer[:n])
		if err != nil {
			continue
		}
		key := remote.Network() + "|" + remote.String()
		s.peersMu.Lock()
		peer := s.peers[key]
		if peer == nil {
			select {
			case s.pending <- struct{}{}:
				codec, codecErr := newRTPCodec(s.key)
				if codecErr == nil {
					peer = newPeerPacketConn(packetConn, remote, codec)
					s.peers[key] = peer
					go s.handlePeer(key, peer)
				} else {
					<-s.pending
				}
			default:
			}
		}
		s.peersMu.Unlock()
		if peer != nil {
			peer.enqueue(plain, remote)
		}
	}
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

	conn, err := dtls.Server(peer, peer.remote, s.dtlsConfig)
	if err != nil {
		return
	}
	defer conn.Close()
	handshakeCtx, cancel := context.WithTimeout(s.ctx, s.options.HandshakeTimeout)
	err = conn.HandshakeContext(handshakeCtx)
	cancel()
	if err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(s.options.HandshakeTimeout))
	authBuffer := make([]byte, maximumAuthFrameLen)
	n, err := conn.Read(authBuffer)
	if err != nil {
		return
	}
	request, err := decodeAuthRequest(authBuffer[:n])
	if err != nil || int(request.WorkerTotal) > s.options.MaxWorkersPerSession || !s.authorize(request.User, request.Password) {
		_, _ = conn.Write(encodeAuthAck(false, 0))
		return
	}
	session, created, err := s.getOrCreateSession(request)
	if err != nil {
		_, _ = conn.Write(encodeAuthAck(false, 0))
		return
	}
	done, err := session.tunnel.AttachWorker(request.WorkerID, conn, func() error {
		_, writeErr := conn.Write(encodeAuthAck(true, session.generation))
		if writeErr == nil {
			writeErr = conn.SetDeadline(time.Time{})
		}
		return writeErr
	})
	s.releaseSessionAttach(session)
	if err != nil {
		if created {
			s.deleteSessionIfUnattached(request.SessionID, session)
		}
		_, _ = conn.Write(encodeAuthAck(false, 0))
		return
	}
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
			return nil, false, errors.New("call multi_user: session identity mismatch")
		}
		ready := session.ready
		s.sessionsMu.Unlock()
		select {
		case <-ready:
			s.sessionsMu.Lock()
			if s.sessions[request.SessionID] != session {
				s.sessionsMu.Unlock()
				return nil, false, errors.New("call multi_user: session was replaced")
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
		return nil, false, errors.New("call multi_user: global session limit reached")
	}
	if s.userSessions[request.User] >= record.maxSessions {
		s.sessionsMu.Unlock()
		closeServerSessions(evicted)
		return nil, false, errors.New("call multi_user: user session limit reached")
	}
	tunnel, err := NewPooledTunnel(request.Conv, s.options.MaxWorkersPerSession, s.logger)
	if err != nil {
		s.sessionsMu.Unlock()
		closeServerSessions(evicted)
		return nil, false, err
	}
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
	s.sessions[request.SessionID] = session
	s.userSessions[request.User]++
	s.sessionsMu.Unlock()
	closeServerSessions(evicted)

	err = s.options.SessionHandler(SessionInfo{ID: request.SessionID, User: request.User}, session.tunnel)
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

func (s *Server) deleteSession(id [16]byte, expected *serverSession) {
	s.sessionsMu.Lock()
	session := s.sessions[id]
	if session == nil || session != expected || session.pendingAttaches != 0 {
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
			s.sessionsMu.Lock()
			stale := make([]*serverSession, 0)
			for _, session := range s.sessions {
				if session.pendingAttaches == 0 && session.tunnel.ActiveWorkers() == 0 && now.Sub(session.tunnel.LastActivity()) >= s.options.SessionIdleTimeout {
					stale = append(stale, session)
				}
			}
			s.sessionsMu.Unlock()
			for _, session := range stale {
				s.deleteSession(session.id, session)
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
			_ = session.tunnel.Close()
		}
	})
	return nil
}
