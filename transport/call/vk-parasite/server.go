package vkparasite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/sing/common/logger"
)

const (
	HardMaxWorkers           = MaximumWorkerCount
	HardMaxSessions          = 4096
	HardMaxUsers             = 4096
	HardMaxPendingHandshakes = 4096

	defaultMaxWorkers           = DefaultWorkerCount
	defaultMaxPendingHandshakes = 256
	defaultHandshakeTimeout     = 15 * time.Second

	// Сколько сервер ждёт, пока отвергнутый клиент прочитает причину отказа.
	authRefusalLinger            = 2 * time.Second
	defaultSessionIdleTimeout    = 5 * time.Minute
	defaultUDPReceiveBufferBytes = 4 * 1024 * 1024
	defaultUDPSendBufferBytes    = 4 * 1024 * 1024
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

	startMu      sync.Mutex
	packetConn   net.PacketConn
	obfsConn     *serverObfsConn
	quicListener *quic.Listener
	quicCloser   io.Closer

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
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.packetConn != nil {
		return errors.New("call vk_parasite: server already started")
	}
	s.configurePacketSocket(packetConn)
	obfsConn, err := newServerObfsConn(packetConn, s.key)
	if err != nil {
		return err
	}
	listener, closer, err := listenQUIC(obfsConn, s.serverCert)
	if err != nil {
		return err
	}
	s.packetConn = packetConn
	s.obfsConn = obfsConn
	s.quicListener = listener
	s.quicCloser = closer
	go s.acceptLoop(listener)
	go s.reapLoop()
	return nil
}

// acceptLoop принимает QUIC-соединения с общего листенера.
//
// Разбор по соединениям делает quic-go по connection ID в заголовке пакета.
// Прежний сервер делал это сам: карта пиров по UDP-адресу, отдельная копия
// пакета в шардированную очередь, вторая копия в очередь пира и по одному
// QUIC-листенеру на пир. Ни одна из этих копий больше не нужна.
func (s *Server) acceptLoop(listener *quic.Listener) {
	defer close(s.done)
	for {
		conn, err := listener.Accept(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.logger.Warn("call vk_parasite: QUIC listener stopped: ", err)
			}
			return
		}
		go s.handleQUICConn(conn)
	}
}

// handleQUICConn аутентифицирует worker'а на первом потоке соединения.
//
// Auth уехал в поток по одной причине: поток надёжен. Прежняя схема слала
// auth-фрейм отдельной датаграммой, и её потеря стоила целого дозвона, поэтому
// клиент повторял фрейм, а сервер держал последний ack, чтобы ответить на
// повтор. Поток делает это сам.
func (s *Server) handleQUICConn(conn *quic.Conn) {
	pendingReleased := false
	releasePending := func() {
		if !pendingReleased {
			<-s.pending
			pendingReleased = true
		}
	}
	select {
	case s.pending <- struct{}{}:
	default:
		_ = conn.CloseWithError(0, "handshake capacity")
		return
	}
	defer func() {
		releasePending()
	}()

	handshakeCtx, cancel := context.WithTimeout(s.ctx, s.options.HandshakeTimeout)
	defer cancel()
	stream, err := conn.AcceptStream(handshakeCtx)
	if err != nil {
		_ = conn.CloseWithError(0, "")
		return
	}
	request, err := readAuthRequest(stream)
	if reason, refused := s.refuseAuth(request, err); refused {
		s.refuseConnection(conn, stream, encodeAuthAck(false, 0, reason), releasePending)
		return
	}
	session, created, err := s.getOrCreateSession(request)
	if err != nil {
		s.refuseConnection(conn, stream, encodeAuthAck(false, 0, AuthRejectSession), releasePending)
		return
	}
	_, writeErr := stream.Write(encodeAuthAck(true, session.generation, AuthRejectUnspecified))
	_ = stream.Close()
	if writeErr != nil {
		s.releaseSessionAttach(session)
		if created {
			s.deleteSessionIfUnattached(request.SessionID, session)
		}
		_ = conn.CloseWithError(0, "")
		return
	}
	session.relay.AttachServerConn(conn, nil)
	s.releaseSessionAttach(session)
	releasePending()
	select {
	case <-conn.Context().Done():
	case <-s.ctx.Done():
	}
	if created {
		s.deleteSessionIfUnattached(request.SessionID, session)
	}
}

// refuseConnection отдаёт причину отказа и только потом закрывает соединение.
//
// Закрыть сразу после записи нельзя: QUIC не обязан доставить данные потока,
// если следом ушёл CONNECTION_CLOSE, и клиент вместо названной причины увидел бы
// «соединение закрыто» — ровно ту неразличимость, ради устранения которой у
// отказа есть отдельные причины. Слот handshake отпускается до ожидания, иначе
// отказы копили бы его на время linger.
func (s *Server) refuseConnection(conn *quic.Conn, stream *quic.Stream, ack []byte, releasePending func()) {
	if stream != nil {
		_, _ = stream.Write(ack)
		_ = stream.Close()
	}
	releasePending()
	timer := time.NewTimer(authRefusalLinger)
	defer timer.Stop()
	select {
	case <-conn.Context().Done():
	case <-timer.C:
	case <-s.ctx.Done():
	}
	_ = conn.CloseWithError(0, "")
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
			// Обёртки отправки живут по адресу пира, а не по сессии, поэтому
			// их чистит тот же цикл: иначе карта растёт на каждого пира за всё
			// время работы сервера.
			s.startMu.Lock()
			obfsConn := s.obfsConn
			s.startMu.Unlock()
			if obfsConn != nil {
				obfsConn.reapCodecs(s.options.SessionIdleTimeout)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.startMu.Lock()
		packetConn := s.packetConn
		quicCloser := s.quicCloser
		s.startMu.Unlock()
		if quicCloser != nil {
			_ = quicCloser.Close()
		}
		if packetConn != nil {
			_ = packetConn.Close()
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
