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

const (
	defaultQUICPathCount = DefaultWorkerCount
	pathDialTimeout      = 30 * time.Second

	// Путь перестаёт получать новые потоки, когда его сглаженный RTT и
	// превышает лучший более чем в pathRTTTolerance раз, и отстаёт от него
	// хотя бы на pathRTTSlack. Второе условие нужно, чтобы на быстрой сети
	// 6 мс против 2 мс не выбрасывали три пути из четырёх.
	pathRTTTolerance = 2
	pathRTTSlack     = 30 * time.Millisecond

	// Доля потерь, за которой путь тоже перестаёт получать новые потоки, и
	// минимальное число отправленных пакетов, до которого доля не считается.
	pathLossLimit      = 0.2
	pathLossMinPackets = 50
)

var ErrNoActiveQUICPaths = errors.New("call vk_parasite: no active QUIC paths")

type quicPathConn struct {
	id      uint16
	conn    *quic.Conn
	closer  io.Closer
	cancel  context.CancelFunc
	streams atomic.Uint64
}

// PathQuality — снимок одного пути. Числа приходят из счётчиков самого quic-go
// (они атомарные, поэтому читать их можно из любой горутины), кроме streams,
// который relay считает сам.
type PathQuality struct {
	WorkerID        uint16
	SmoothedRTT     time.Duration
	MinRTT          time.Duration
	MeanDeviation   time.Duration
	PacketsSent     uint64
	PacketsLost     uint64
	BytesSent       uint64
	BytesReceived   uint64
	StreamsAssigned uint64
}

// LossRatio возвращает долю потерянных пакетов, пока выборка достаточна.
func (q PathQuality) LossRatio() float64 {
	if q.PacketsSent < pathLossMinPackets {
		return 0
	}
	return float64(q.PacketsLost) / float64(q.PacketsSent)
}

func (p *quicPathConn) quality() PathQuality {
	quality := PathQuality{WorkerID: p.id, StreamsAssigned: p.streams.Load()}
	if p.conn == nil {
		return quality
	}
	stats := p.conn.ConnectionStats()
	quality.SmoothedRTT = stats.SmoothedRTT
	quality.MinRTT = stats.MinRTT
	quality.MeanDeviation = stats.MeanDeviation
	quality.PacketsSent = stats.PacketsSent
	quality.PacketsLost = stats.PacketsLost
	quality.BytesSent = stats.BytesSent
	quality.BytesReceived = stats.BytesReceived
	return quality
}

type QUICRelayOptions struct {
	PathCount int
	DialPath  func(ctx context.Context, workerID uint16) (*quic.Conn, io.Closer, error)
	Logger    logger.ContextLogger
}

type QUICRelay struct {
	ctx                      context.Context
	cancel                   context.CancelFunc
	logger                   logger.ContextLogger
	dialPath                 func(ctx context.Context, workerID uint16) (*quic.Conn, io.Closer, error)
	pathCount                int
	nextPath                 atomic.Uint64
	onAccept                 atomic.Pointer[func(net.Conn, string)]
	onPathsChanged           atomic.Pointer[func()]
	pathsMu                  sync.RWMutex
	paths                    []*quicPathConn
	generationCtx            context.Context
	generationCancel         context.CancelFunc
	appliedNetworkGeneration atomic.Uint64
	closed                   atomic.Bool
	closeOnce                sync.Once
	dgramRouter              *datagramRouter
}

func NewQUICRelay(parent context.Context, options QUICRelayOptions) *QUICRelay {
	pathCount := options.PathCount
	if pathCount <= 0 {
		pathCount = defaultQUICPathCount
	}
	ctx, cancel := context.WithCancel(parent)
	generationCtx, generationCancel := context.WithCancel(ctx)
	relay := &QUICRelay{
		ctx:              ctx,
		cancel:           cancel,
		logger:           options.Logger,
		dialPath:         options.DialPath,
		pathCount:        pathCount,
		paths:            make([]*quicPathConn, 0, pathCount),
		generationCtx:    generationCtx,
		generationCancel: generationCancel,
		dgramRouter:      newDatagramRouter(),
	}
	return relay
}

// Start launches parallel path establishment.
func (r *QUICRelay) Start() {
	if r.dialPath == nil {
		return
	}
	for index := 0; index < r.pathCount; index++ {
		workerID := uint16(index)
		go func(id uint16) {
			r.initPath(id)
		}(workerID)
	}
}

func (r *QUICRelay) initPath(workerID uint16) {
	if r.closed.Load() {
		return
	}
	generationCtx := r.currentGenerationContext()
	pathCtx, pathCancel := context.WithCancel(generationCtx)
	conn, closer, err := r.dialPath(pathCtx, workerID)
	if err != nil {
		pathCancel()
		if r.logger != nil {
			r.logger.Warn("call vk_parasite: path connect failed for worker ", workerID, ": ", err)
		}
		var outcome *dialOutcome
		if errors.As(err, &outcome) && outcome.failure != nil && outcome.failure.Terminal {
			return
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
	r.notifyPathsChanged()

	go r.watchPath(path)
	go r.acceptStreamsLoop(path)
	go r.acceptDatagramsLoop(path)
}

func (r *QUICRelay) removePath(path *quicPathConn) {
	r.pathsMu.Lock()
	for i, candidate := range r.paths {
		if candidate == path {
			r.paths = append(r.paths[:i], r.paths[i+1:]...)
			break
		}
	}
	r.pathsMu.Unlock()
	r.notifyPathsChanged()
}

// SetPathsChangedHandler регистрирует обработчик, который вызывается после
// каждого изменения набора активных путей. Он заменяет опрос: состояние
// транспорта меняется только здесь, поэтому опрашивать его раз в секунду было
// нечем оправдать.
func (r *QUICRelay) SetPathsChangedHandler(handler func()) {
	r.onPathsChanged.Store(&handler)
}

// notifyPathsChanged вызывается только вне pathsMu: обработчик читает набор
// путей, а RWMutex в Go не рекурсивен.
func (r *QUICRelay) notifyPathsChanged() {
	if handler := r.onPathsChanged.Load(); handler != nil && *handler != nil {
		(*handler)()
	}
}

func (r *QUICRelay) watchPath(path *quicPathConn) {
	<-path.conn.Context().Done()
	r.removePath(path)
	r.dgramRouter.closeConn(path.conn)
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
		generationCtx := r.currentGenerationContext()
		pathCtx, pathCancel := context.WithCancel(generationCtx)
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
		var outcome *dialOutcome
		if errors.As(err, &outcome) && outcome.failure != nil && outcome.failure.Terminal {
			return
		}
		select {
		case <-time.After(backoff):
		case <-generationCtx.Done():
			return
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// pickPath раздаёт потоки по кругу, пропуская пути, которые заметно хуже
// лучшего активного.
//
// Раньше обход был чисто круговым, и живой, но плохой путь — высокий RTT или
// потери — получал ровно ту же долю новых потоков, что и здоровые. Порядок
// обхода сохранён: счётчик по-прежнему двигается на каждый вызов, так что
// среди годных путей раздача остаётся равномерной.
func (r *QUICRelay) pickPath(_ context.Context) (*quicPathConn, error) {
	r.pathsMu.RLock()
	defer r.pathsMu.RUnlock()
	activeCount := len(r.paths)
	if activeCount == 0 {
		return nil, ErrNoActiveQUICPaths
	}
	start := int(r.nextPath.Add(1) - 1)
	if activeCount == 1 {
		return r.paths[0], nil
	}
	best := time.Duration(0)
	for _, path := range r.paths {
		if rtt := path.quality().SmoothedRTT; rtt > 0 && (best == 0 || rtt < best) {
			best = rtt
		}
	}
	for offset := range activeCount {
		path := r.paths[(start+offset)%activeCount]
		if pathIsUsable(path.quality(), best) {
			return path, nil
		}
	}
	// Все пути хуже порога: раздаём по кругу, как раньше, — лучше плохой путь,
	// чем отказ в соединении.
	return r.paths[start%activeCount], nil
}

func pathIsUsable(quality PathQuality, best time.Duration) bool {
	if quality.LossRatio() > pathLossLimit {
		return false
	}
	rtt := quality.SmoothedRTT
	if rtt <= 0 || best <= 0 {
		return true
	}
	return rtt <= best*pathRTTTolerance || rtt <= best+pathRTTSlack
}

// PathStats снимает качество всех активных путей. Единственный способ увидеть
// на устройстве, какой из путей плохой: транспорт больше ничего наружу не
// публикует.
func (r *QUICRelay) PathStats() []PathQuality {
	r.pathsMu.RLock()
	defer r.pathsMu.RUnlock()
	stats := make([]PathQuality, 0, len(r.paths))
	for _, path := range r.paths {
		stats = append(stats, path.quality())
	}
	return stats
}

func (r *QUICRelay) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	path, err := r.pickPath(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := path.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	path.streams.Add(1)
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

// SetUDPAcceptHandler включает приём UDP-ассоциаций, открытых удалённой
// стороной. Обработчик держит датаграммный роутер: ассоциация создаётся по
// первому же фрейму с неизвестным идентификатором.
func (r *QUICRelay) SetUDPAcceptHandler(fn func(conn net.Conn, destination string)) {
	r.dgramRouter.setAcceptHandler(fn)
}

func (r *QUICRelay) ListenPacket(ctx context.Context, destination string) (net.Conn, error) {
	path, err := r.pickPath(ctx)
	if err != nil {
		return nil, err
	}
	path.streams.Add(1)
	assoc := r.dgramRouter.newAssociation(path.conn, M.ParseSocksaddr(destination))
	return assoc, nil
}

func (r *QUICRelay) Close() {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.cancel()
		r.pathsMu.Lock()
		r.generationCancel()
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

// RebindNetwork drops paths tied to the previous network. Their watchers
// establish replacements using the relay's existing reconnect loop.
func (r *QUICRelay) RebindNetwork(generation ...uint64) {
	gen := uint64(0)
	if len(generation) > 0 {
		gen = generation[0]
	}
	if gen != 0 {
		for {
			applied := r.appliedNetworkGeneration.Load()
			if gen <= applied {
				return
			}
			if r.appliedNetworkGeneration.CompareAndSwap(applied, gen) {
				break
			}
		}
	}
	r.pathsMu.RLock()
	paths := append([]*quicPathConn(nil), r.paths...)
	r.pathsMu.RUnlock()
	r.pathsMu.Lock()
	r.generationCancel()
	r.generationCtx, r.generationCancel = context.WithCancel(r.ctx)
	r.pathsMu.Unlock()
	for _, path := range paths {
		if path.conn != nil {
			_ = path.conn.CloseWithError(0, "network changed")
		}
	}
}

func (r *QUICRelay) currentGenerationContext() context.Context {
	r.pathsMu.RLock()
	ctx := r.generationCtx
	r.pathsMu.RUnlock()
	return ctx
}

// ActivePaths returns the current number of active QUIC paths in the pool.
func (r *QUICRelay) ActivePaths() int {
	r.pathsMu.RLock()
	defer r.pathsMu.RUnlock()
	return len(r.paths)
}

// SmoothedRTT is the mean RTT reported by active QUIC paths. A zero value means
// that no path has collected an RTT sample yet.
func (r *QUICRelay) SmoothedRTT() time.Duration {
	r.pathsMu.RLock()
	defer r.pathsMu.RUnlock()
	var total time.Duration
	count := 0
	for _, path := range r.paths {
		if path.conn == nil {
			continue
		}
		rtt := path.conn.ConnectionStats().SmoothedRTT
		if rtt > 0 {
			total += rtt
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}

// AttachServerConn adds an accepted server-side QUIC connection to the relay pool.
func (r *QUICRelay) AttachServerConn(conn *quic.Conn, closer io.Closer) {
	_, pathCancel := context.WithCancel(r.ctx)
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
