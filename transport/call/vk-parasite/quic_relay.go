package vkparasite

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

// ai-generated: QUICRelay transport implementation fulfilling call.RelayTransport seam
const (
	defaultQUICPathCount = 16
	pathDialTimeout      = 30 * time.Second
)

type quicPathConn struct {
	id     uint16
	conn   *quic.Conn
	closer io.Closer
	cancel context.CancelFunc
}

type QUICRelayOptions struct {
	PathCount int
	DialPath  func(ctx context.Context, workerID uint16) (*quic.Conn, io.Closer, error)
	Logger    logger.ContextLogger
}

type QUICRelay struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      logger.ContextLogger
	dialPath    func(ctx context.Context, workerID uint16) (*quic.Conn, io.Closer, error)
	pathCount   int
	nextPath    atomic.Uint64
	onAccept    atomic.Pointer[func(net.Conn, string)]
	onUDPAccept atomic.Pointer[func(net.Conn, string)]
	pathsMu     sync.RWMutex
	paths       []*quicPathConn
	closed      atomic.Bool
	ready       chan struct{}
	readyOnce   sync.Once
	closeOnce   sync.Once
	dgramRouter *datagramRouter
}

func NewQUICRelay(parent context.Context, options QUICRelayOptions) *QUICRelay {
	pathCount := options.PathCount
	if pathCount <= 0 {
		pathCount = defaultQUICPathCount
	}
	ctx, cancel := context.WithCancel(parent)
	relay := &QUICRelay{
		ctx:         ctx,
		cancel:      cancel,
		logger:      options.Logger,
		dialPath:    options.DialPath,
		pathCount:   pathCount,
		paths:       make([]*quicPathConn, 0, pathCount),
		ready:       make(chan struct{}),
		dgramRouter: newDatagramRouter(),
	}
	return relay
}

// Start launches parallel path establishment.
func (r *QUICRelay) Start() {
	if r.dialPath == nil {
		r.readyOnce.Do(func() { close(r.ready) })
		return
	}
	var startupGroup sync.WaitGroup
	for index := 0; index < r.pathCount; index++ {
		workerID := uint16(index)
		startupGroup.Add(1)
		go func(id uint16) {
			defer startupGroup.Done()
			r.initPath(id)
		}(workerID)
	}
	go func() {
		startupGroup.Wait()
		r.readyOnce.Do(func() { close(r.ready) })
	}()
}

func (r *QUICRelay) initPath(workerID uint16) {
	if r.closed.Load() {
		return
	}
	pathCtx, pathCancel := context.WithCancel(r.ctx)
	conn, closer, err := r.dialPath(pathCtx, workerID)
	if err != nil {
		pathCancel()
		if r.logger != nil {
			r.logger.Warn("call vk_parasite: path connect failed for worker ", workerID, ": ", err)
		}
		go r.reconnectPath(workerID)
		return
	}
	path := &quicPathConn{
		id:     workerID,
		conn:   conn,
		closer: closer,
		cancel: pathCancel,
	}
	r.addPath(path)
}

func (r *QUICRelay) addPath(path *quicPathConn) {
	r.pathsMu.Lock()
	r.paths = append(r.paths, path)
	r.pathsMu.Unlock()

	go r.watchPath(path)
	go r.acceptStreamsLoop(path)
	go r.acceptDatagramsLoop(path)
}

func (r *QUICRelay) removePath(path *quicPathConn) {
	r.pathsMu.Lock()
	defer r.pathsMu.Unlock()
	for i, candidate := range r.paths {
		if candidate == path {
			r.paths = append(r.paths[:i], r.paths[i+1:]...)
			break
		}
	}
}

func (r *QUICRelay) watchPath(path *quicPathConn) {
	<-path.conn.Context().Done()
	r.removePath(path)
	path.cancel()
	if path.closer != nil {
		_ = path.closer.Close()
	}
	if !r.closed.Load() && r.dialPath != nil {
		go r.reconnectPath(path.id)
	}
}

func (r *QUICRelay) reconnectPath(workerID uint16) {
	backoff := 500 * time.Millisecond
	for {
		if r.closed.Load() || r.ctx.Err() != nil {
			return
		}
		pathCtx, pathCancel := context.WithCancel(r.ctx)
		conn, closer, err := r.dialPath(pathCtx, workerID)
		if err == nil {
			path := &quicPathConn{
				id:     workerID,
				conn:   conn,
				closer: closer,
				cancel: pathCancel,
			}
			r.addPath(path)
			return
		}
		pathCancel()
		select {
		case <-time.After(backoff):
		case <-r.ctx.Done():
			return
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (r *QUICRelay) pickPath(ctx context.Context) (*quicPathConn, error) {
	r.pathsMu.RLock()
	activeCount := len(r.paths)
	if activeCount > 0 {
		index := int(r.nextPath.Add(1)-1) % activeCount
		path := r.paths[index]
		r.pathsMu.RUnlock()
		return path, nil
	}
	r.pathsMu.RUnlock()

	select {
	case <-r.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	r.pathsMu.RLock()
	defer r.pathsMu.RUnlock()
	activeCount = len(r.paths)
	if activeCount == 0 {
		return nil, errors.New("call vk_parasite: no active QUIC paths")
	}
	index := int(r.nextPath.Add(1)-1) % activeCount
	return r.paths[index], nil
}

func (r *QUICRelay) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	path, err := r.pickPath(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := path.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, qtls.WrapError(err)
	}
	dest := M.ParseSocksaddr(destination)
	if err := writeStreamHeader(stream, streamKindTCP, dest); err != nil {
		stream.CancelWrite(0)
		stream.CancelRead(0)
		_ = stream.Close()
		return nil, err
	}
	return &quicStreamConn{Conn: path.conn, Stream: stream}, nil
}

func (r *QUICRelay) acceptStreamsLoop(path *quicPathConn) {
	for {
		stream, err := path.conn.AcceptStream(r.ctx)
		if err != nil {
			return
		}
		go r.handleInboundStream(path.conn, stream)
	}
}

func (r *QUICRelay) handleInboundStream(conn *quic.Conn, stream *quic.Stream) {
	kind, dest, err := readStreamHeader(stream)
	if err != nil {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		_ = stream.Close()
		return
	}
	streamConn := &quicStreamConn{Conn: conn, Stream: stream}
	if kind == streamKindTCP {
		if handler := r.onAccept.Load(); handler != nil && *handler != nil {
			(*handler)(streamConn, dest.String())
		} else {
			_ = streamConn.Close()
		}
	} else {
		_ = streamConn.Close()
	}
}

func (r *QUICRelay) acceptDatagramsLoop(path *quicPathConn) {
	for {
		dgram, err := path.conn.ReceiveDatagram(r.ctx)
		if err != nil {
			return
		}
		r.dgramRouter.routeReceivedDatagram(dgram, path.conn)
	}
}

func (r *QUICRelay) SetAcceptHandler(fn func(conn net.Conn, destination string)) {
	r.onAccept.Store(&fn)
}

func (r *QUICRelay) SetUDPAcceptHandler(fn func(conn net.Conn, destination string)) {
	r.onUDPAccept.Store(&fn)
}

func (r *QUICRelay) ListenPacket(ctx context.Context, destination string) (net.Conn, error) {
	path, err := r.pickPath(ctx)
	if err != nil {
		return nil, err
	}
	assoc := r.dgramRouter.newAssociation(path.conn, M.ParseSocksaddr(destination))
	return assoc, nil
}

func (r *QUICRelay) Close() {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.cancel()
		r.pathsMu.Lock()
		for _, path := range r.paths {
			path.cancel()
			if path.conn != nil {
				_ = path.conn.CloseWithError(0, "")
			}
			if path.closer != nil {
				_ = path.closer.Close()
			}
		}
		r.paths = nil
		r.pathsMu.Unlock()
		r.dgramRouter.close()
	})
}

// ActivePaths returns the current number of active QUIC paths in the pool.
func (r *QUICRelay) ActivePaths() int {
	r.pathsMu.RLock()
	defer r.pathsMu.RUnlock()
	return len(r.paths)
}

// AttachServerConn adds an accepted server-side QUIC connection to the relay pool.
func (r *QUICRelay) AttachServerConn(conn *quic.Conn, closer io.Closer) {
	pathCtx, pathCancel := context.WithCancel(r.ctx)
	path := &quicPathConn{
		conn:   conn,
		closer: closer,
		cancel: pathCancel,
	}
	r.addPath(path)
}

type quicStreamConn struct {
	Conn *quic.Conn
	*quic.Stream
}

func (s *quicStreamConn) Read(p []byte) (n int, err error) {
	return s.Stream.Read(p)
}

func (s *quicStreamConn) Write(p []byte) (n int, err error) {
	return s.Stream.Write(p)
}

func (s *quicStreamConn) LocalAddr() net.Addr {
	return s.Conn.LocalAddr()
}

func (s *quicStreamConn) RemoteAddr() net.Addr {
	return s.Conn.RemoteAddr()
}

func (s *quicStreamConn) Upstream() any {
	return s.Stream
}

func (s *quicStreamConn) Close() error {
	s.Stream.CancelRead(0)
	s.Stream.Close()
	s.Stream.SetWriteDeadline(time.Now())
	return nil
}
