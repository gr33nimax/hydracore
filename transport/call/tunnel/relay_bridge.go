package tunnel

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const (
	relayFlowWindowBytes = 256 * 1024
	relayFlowChunkBytes  = 16 * 1024
)

type udpClient struct {
	pendingMu sync.RWMutex
	pending   chan []byte
	closed    atomic.Bool
	addr      string
	rb        *RelayBridge
}

func (c *udpClient) deliver(payload []byte) bool {
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()
	if c.closed.Load() {
		return false
	}
	select {
	case c.pending <- payload:
		if c.rb != nil {
			c.rb.relayQueueDelta(len(payload))
		}
		return true
	default:
		return false
	}
}

func (c *udpClient) closePending() (bool, int) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if c.closed.Load() {
		return false, 0
	}
	c.closed.Store(true)
	close(c.pending)
	discarded := 0
	for payload := range c.pending {
		discarded += len(payload)
	}
	if c.rb != nil {
		c.rb.relayQueueDelta(-discarded)
	}
	return true, discarded
}

type RelayBridge struct {
	tunnelMu   sync.RWMutex
	tunnel     DataTunnel
	conns      sync.Map
	udpClients sync.Map
	nextID     atomic.Uint32
	logger     logger.ContextLogger
	mode       string
	readBuf    int
	ready      chan struct{}
	once       sync.Once
	closed     atomic.Bool
	dialer     N.Dialer
	flowControl bool

	acceptHandlerMu sync.Mutex
	acceptHandler   func(conn net.Conn, destination string)

	udpAcceptHandlerMu sync.Mutex
	udpAcceptHandler   func(conn net.Conn, destination string)

	onPeerConfigMu sync.Mutex
	onPeerConfig   func(fps, batch, trackCount int)
}

func NewRelayBridge(tunnel DataTunnel, mode string, readBuf int, dialer N.Dialer, logger logger.ContextLogger) *RelayBridge {
	flowController, _ := tunnel.(FlowControlledDataTunnel)
	rb := &RelayBridge{
		tunnel:  tunnel,
		logger:  logger,
		mode:    mode,
		readBuf: readBuf,
		dialer:  dialer,
		ready:   make(chan struct{}),
		flowControl: flowController != nil && flowController.FlowControlEnabled(),
	}
	tunnel.SetOnData(rb.handleTunnelData)
	tunnel.SetOnClose(rb.handleTunnelClose)
	return rb
}

func (rb *RelayBridge) SetAcceptHandler(fn func(conn net.Conn, destination string)) {
	rb.acceptHandlerMu.Lock()
	rb.acceptHandler = fn
	rb.acceptHandlerMu.Unlock()
}

func (rb *RelayBridge) SetUDPAcceptHandler(fn func(conn net.Conn, destination string)) {
	rb.udpAcceptHandlerMu.Lock()
	rb.udpAcceptHandler = fn
	rb.udpAcceptHandlerMu.Unlock()
}

func (rb *RelayBridge) SetOnPeerConfig(fn func(fps, batch, trackCount int)) {
	rb.onPeerConfigMu.Lock()
	rb.onPeerConfig = fn
	rb.onPeerConfigMu.Unlock()
}

func (rb *RelayBridge) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	if rb.closed.Load() {
		return nil, fmt.Errorf("relay: bridge already closed")
	}
	if M.ParseSocksaddr(destination).IsIPv6() {
		return nil, fmt.Errorf("relay: network unreachable (ipv6): %s", common.MaskAddr(destination))
	}
	select {
	case <-rb.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	id := rb.nextID.Add(1)
	tc := newTunnelConn(id, rb)
	rb.conns.Store(id, tc)
	rb.updateRelayActive()
	rb.logger.Debug(fmt.Sprintf("relay: DIAL %d -> %s", id, common.MaskAddr(destination)))
	rb.send(id, MsgConnect, []byte(destination))
	select {
	case err := <-tc.rdy:
		if err != nil {
			rb.conns.Delete(id)
			rb.updateRelayActive()
			return nil, err
		}
		if tc.closed.Load() {
			rb.conns.Delete(id)
			rb.updateRelayActive()
			return nil, io.ErrClosedPipe
		}
		return tc, nil
	case <-ctx.Done():
		rb.conns.Delete(id)
		rb.updateRelayActive()
		rb.send(id, MsgClose, nil)
		return nil, ctx.Err()
	}
}

func (rb *RelayBridge) ListenPacket(ctx context.Context, destination string) (net.Conn, error) {
	if rb.closed.Load() {
		return nil, fmt.Errorf("relay: bridge already closed")
	}
	if M.ParseSocksaddr(destination).IsIPv6() {
		return nil, fmt.Errorf("relay: network unreachable (ipv6): %s", common.MaskAddr(destination))
	}
	select {
	case <-rb.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	id := rb.nextID.Add(1)
	uc := &udpClient{pending: make(chan []byte, 64), addr: destination, rb: rb}
	rb.udpClients.Store(id, uc)
	rb.updateRelayActive()
	return &tunnelPacketConn{id: id, rb: rb, uc: uc, destStr: destination}, nil
}

func (rb *RelayBridge) Reset() {
	rb.closeAll(true)
}

func (rb *RelayBridge) Close() {
	if !rb.closed.CompareAndSwap(false, true) {
		return
	}
	rb.closeAll(true)
}

func (rb *RelayBridge) MarkReady() {
	rb.once.Do(func() { close(rb.ready) })
}

func (rb *RelayBridge) currentTunnel() DataTunnel {
	rb.tunnelMu.RLock()
	defer rb.tunnelMu.RUnlock()
	return rb.tunnel
}

func (rb *RelayBridge) relayTelemetry() RelayTelemetry {
	telemetry, _ := rb.currentTunnel().(RelayTelemetry)
	return telemetry
}

func (rb *RelayBridge) addRelayBytes(size int) {
	if size <= 0 {
		return
	}
	if telemetry := rb.relayTelemetry(); telemetry != nil {
		telemetry.RelayAddBytes(uint64(size))
	}
}

func (rb *RelayBridge) relayQueueDelta(size int) {
	if size == 0 {
		return
	}
	if telemetry := rb.relayTelemetry(); telemetry != nil {
		telemetry.RelayQueueDelta(size)
	}
}

func (rb *RelayBridge) relayQueueDrop() {
	if telemetry := rb.relayTelemetry(); telemetry != nil {
		telemetry.RelayQueueDrop()
	}
}

func (rb *RelayBridge) resetRelayQueue() {
	if telemetry := rb.relayTelemetry(); telemetry != nil {
		telemetry.RelayResetQueue()
	}
}

func (rb *RelayBridge) relayConnectFailure() {
	if telemetry := rb.relayTelemetry(); telemetry != nil {
		telemetry.RelayConnectFailure()
	}
}

func (rb *RelayBridge) updateRelayActive() {
	tcp := 0
	udp := 0
	rb.conns.Range(func(_, _ any) bool {
		tcp++
		return true
	})
	rb.udpClients.Range(func(_, _ any) bool {
		udp++
		return true
	})
	if telemetry := rb.relayTelemetry(); telemetry != nil {
		telemetry.RelaySetActive(tcp, udp)
	}
}

func relayPayloadBytes(messageType byte, payload []byte) int {
	switch messageType {
	case MsgData, MsgUDPReply:
		return len(payload)
	case MsgUDP:
		if len(payload) == 0 {
			return 0
		}
		addressLength := int(payload[0])
		if addressLength == 0 || 1+addressLength > len(payload) {
			return 0
		}
		return len(payload) - 1 - addressLength
	default:
		return 0
	}
}

func (rb *RelayBridge) SwapTunnel(newTunnel DataTunnel) {
	rb.tunnelMu.Lock()
	rb.tunnel = newTunnel
	rb.tunnelMu.Unlock()
	newTunnel.SetOnData(rb.handleTunnelData)
	newTunnel.SetOnClose(rb.handleTunnelClose)
	rb.closeAll(false)
}

func (rb *RelayBridge) IsClosed() bool {
	return rb.closed.Load()
}

func (rb *RelayBridge) handleTunnelClose() {
	// The data tunnel is already closing. Tear down local relay state without
	// sending MsgClose back through that same tunnel: the close callback may
	// have been reached from SendData while a per-flow lock is still owned.
	rb.closeAll(false)
}

func (rb *RelayBridge) closeAll(notifyPeer bool) {
	var ids []uint32
	rb.conns.Range(func(key, value any) bool {
		if id, ok := key.(uint32); ok {
			ids = append(ids, id)
		}
		switch connection := value.(type) {
		case *tunnelConn:
			if notifyPeer {
				_ = connection.Close()
			} else {
				connection.closeLocal()
			}
		case net.Conn:
			_ = connection.Close()
		}
		rb.conns.Delete(key)
		return true
	})
	udpCount := 0
	rb.udpClients.Range(func(key, value any) bool {
		udpCount++
		switch connection := value.(type) {
		case *udpClient:
			connection.closePending()
		case *creatorUDPConn:
			if notifyPeer {
				_ = connection.Close()
			} else {
				connection.closeLocal()
			}
		case net.Conn:
			_ = connection.Close()
		}
		rb.udpClients.Delete(key)
		return true
	})
	rb.updateRelayActive()
	rb.resetRelayQueue()
	rb.logger.Debug(fmt.Sprintf("relay: closeAll mode=%s tcp=%d udp=%d ids=%v nextID=%d", rb.mode, len(ids), udpCount, ids, rb.nextID.Load()))
}

func (rb *RelayBridge) send(connID uint32, msgType byte, payload []byte) {
	rb.addRelayBytes(relayPayloadBytes(msgType, payload))
	frame := EncodeFrame(connID, msgType, payload)
	rb.currentTunnel().SendData(frame)
}

func (rb *RelayBridge) handleTunnelData(data []byte) {
	DecodeFrames(data, func(connID uint32, msgType byte, payload []byte) {
		rb.addRelayBytes(relayPayloadBytes(msgType, payload))
		if msgType == MsgFlowCredit {
			rb.handleFlowCredit(connID, payload)
			return
		}
		if connID == ControlConnID && msgType == MsgConfig {
			fps, batch, trackCount, ok := DecodeVP8Config(payload)
			if !ok {
				return
			}
			if rb.mode == "creator" {
				rb.logger.Debug(fmt.Sprintf("relay: peer requested vp8 pacing fps=%d batch=%d trackCount=%d", fps, batch, trackCount))
				rb.currentTunnel().Reconfigure(fps, batch)
				rb.send(ControlConnID, MsgConfigAck, nil)
				rb.onPeerConfigMu.Lock()
				cb := rb.onPeerConfig
				rb.onPeerConfigMu.Unlock()
				if cb != nil {
					cb(fps, batch, trackCount)
				}
			}
			return
		}
		if connID == ControlConnID && msgType == MsgConfigAck {
			return
		}
		switch rb.mode {
		case "joiner":
			rb.handleJoinerMessage(connID, msgType, payload)
		case "creator":
			rb.handleCreatorMessage(connID, msgType, payload)
		}
	})
}

func (rb *RelayBridge) handleFlowCredit(connID uint32, payload []byte) {
	if !rb.flowControl || connID == ControlConnID || len(payload) != 4 {
		return
	}
	credit := int(binary.BigEndian.Uint32(payload))
	if credit <= 0 || credit > relayFlowWindowBytes {
		return
	}
	if value, ok := rb.conns.Load(connID); ok {
		if connection, ok := value.(*tunnelConn); ok {
			connection.addSendCredit(credit)
		}
	}
}

func (rb *RelayBridge) handleJoinerMessage(connID uint32, msgType byte, payload []byte) {
	if msgType == MsgUDPReply {
		uval, ok := rb.udpClients.Load(connID)
		if !ok {
			return
		}
		uc := uval.(*udpClient)
		cp := make([]byte, len(payload))
		copy(cp, payload)
		if uc.deliver(cp) {
		} else {
			rb.relayQueueDrop()
		}
		return
	}
	val, ok := rb.conns.Load(connID)
	if !ok {
		if msgType != MsgClose {
			rb.logger.Debug(fmt.Sprintf("relay[joiner]: drop msgType=%d for unknown conn %d (payload=%dB)", msgType, connID, len(payload)))
		}
		return
	}
	tc := val.(*tunnelConn)
	switch msgType {
	case MsgConnectOK:
		select {
		case tc.rdy <- nil:
		default:
		}
	case MsgConnectErr:
		select {
		case tc.rdy <- fmt.Errorf("%s", payload):
		default:
		}
	case MsgData:
		tc.deliver(payload)
	case MsgClose:
		tc.remoteClosed()
		rb.conns.Delete(connID)
		rb.updateRelayActive()
	}
}

func (rb *RelayBridge) handleCreatorMessage(connID uint32, msgType byte, payload []byte) {
	switch msgType {
	case MsgConnect:
		rb.acceptHandlerMu.Lock()
		handler := rb.acceptHandler
		rb.acceptHandlerMu.Unlock()
		if handler != nil {
			destination := string(payload)
			tc := newTunnelConn(connID, rb)
			rb.conns.Store(connID, tc)
			rb.updateRelayActive()
			rb.send(connID, MsgConnectOK, nil)
			go handler(tc, destination)
			return
		}
		go rb.connectTCP(connID, string(payload))
	case MsgUDP:
		payloadCopy := make([]byte, len(payload))
		copy(payloadCopy, payload)
		go rb.handleUDP(connID, payloadCopy)
	case MsgData:
		val, ok := rb.conns.Load(connID)
		if !ok {
			rb.logger.Debug(fmt.Sprintf("relay[creator]: drop MsgData for unknown conn %d (payload=%dB)", connID, len(payload)))
			rb.send(connID, MsgClose, nil)
			return
		}
		switch c := val.(type) {
		case *tunnelConn:
			c.deliver(payload)
		case net.Conn:
			if _, err := c.Write(payload); err != nil {
				rb.logger.Debug(fmt.Sprintf("relay[creator]: write to target %d failed: %s", connID, common.MaskError(err)))
			}
		}
	case MsgClose:
		found := false
		if val, ok := rb.conns.LoadAndDelete(connID); ok {
			found = true
			switch c := val.(type) {
			case *tunnelConn:
				c.remoteClosed()
			case net.Conn:
				c.Close()
			}
		}
		if uval, ok := rb.udpClients.LoadAndDelete(connID); ok {
			found = true
			switch uc := uval.(type) {
			case *creatorUDPConn:
				uc.remoteClosed()
			case net.Conn:
				uc.Close()
			}
		}
		if !found {
			rb.logger.Debug(fmt.Sprintf("relay[creator]: drop MsgClose for unknown conn %d", connID))
		}
		rb.updateRelayActive()
	}
}

func (rb *RelayBridge) handleUDP(connID uint32, payload []byte) {
	if len(payload) < 2 {
		return
	}
	addrLen := int(payload[0])
	if addrLen == 0 || len(payload) < 1+addrLen {
		return
	}
	if bytes.IndexByte(payload[1:1+addrLen], 0) != -1 {
		return
	}
	addr := string(payload[1 : 1+addrLen])
	data := payload[1+addrLen:]
	rb.udpAcceptHandlerMu.Lock()
	handler := rb.udpAcceptHandler
	rb.udpAcceptHandlerMu.Unlock()
	if handler != nil {
		var cuc *creatorUDPConn
		if val, ok := rb.udpClients.Load(connID); ok {
			existing, ok := val.(*creatorUDPConn)
			if !ok {
				return
			}
			cuc = existing
		} else {
			created := newCreatorUDPConn(connID, rb, addr)
			if actual, loaded := rb.udpClients.LoadOrStore(connID, created); loaded {
				existing, ok := actual.(*creatorUDPConn)
				if !ok {
					return
				}
				cuc = existing
			} else {
				cuc = created
				rb.updateRelayActive()
				go handler(cuc, addr)
			}
		}
		cuc.deliver(data)
		return
	}
	var egress net.Conn
	if val, ok := rb.udpClients.Load(connID); ok {
		existing, ok := val.(net.Conn)
		if !ok {
			return
		}
		egress = existing
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		created, err := rb.dialer.DialContext(ctx, N.NetworkUDP, M.ParseSocksaddr(addr))
		cancel()
		if err != nil {
			rb.logger.Warn(fmt.Sprintf("relay[creator]: UDP %d open %s failed: %v", connID, common.MaskAddr(addr), err))
			rb.relayConnectFailure()
			return
		}
		if actual, loaded := rb.udpClients.LoadOrStore(connID, created); loaded {
			created.Close()
			existing, ok := actual.(net.Conn)
			if !ok {
				return
			}
			egress = existing
		} else {
			egress = created
			rb.updateRelayActive()
			go func(conn net.Conn, id uint32, target string) {
				defer conn.Close()
				defer func() {
					rb.udpClients.Delete(id)
					rb.updateRelayActive()
				}()
				defer rb.send(id, MsgClose, nil)
				buf := make([]byte, common.UDPBufSize)
				for {
					conn.SetReadDeadline(time.Now().Add(60 * time.Second))
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					rb.send(id, MsgUDPReply, buf[:n])
				}
			}(egress, connID, addr)
		}
	}
	egress.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := egress.Write(data); err != nil {
		rb.logger.Debug(fmt.Sprintf("relay[creator]: UDP %d write %s failed: %v", connID, common.MaskAddr(addr), err))
	}
}

func (rb *RelayBridge) connectTCP(connID uint32, addr string) {
	rb.logger.Debug(fmt.Sprintf("relay: CONNECT %d -> %s", connID, common.MaskAddr(addr)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := rb.dialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr(addr))
	cancel()
	if err != nil {
		rb.logger.Warn(fmt.Sprintf("relay: CONNECT %d failed: %s", connID, common.MaskError(err)))
		rb.send(connID, MsgConnectErr, []byte(common.MaskError(err)))
		rb.relayConnectFailure()
		return
	}
	rb.conns.Store(connID, conn)
	rb.updateRelayActive()
	rb.send(connID, MsgConnectOK, nil)
	rb.logger.Debug(fmt.Sprintf("relay: CONNECTED %d -> %s", connID, common.MaskAddr(addr)))
	buf := make([]byte, rb.readBuf)
	var totalRead int64
	var reads int
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			rb.send(connID, MsgData, buf[:n])
			totalRead += int64(n)
			reads++
		}
		if err != nil {
			if err != io.EOF {
				rb.logger.Warn(fmt.Sprintf("relay: conn %d read error: %s (read %d times, %dB)", connID, common.MaskError(err), reads, totalRead))
			}
			break
		}
	}
	rb.send(connID, MsgClose, nil)
	rb.conns.Delete(connID)
	rb.updateRelayActive()
}

type tunnelAddr struct{}

func (tunnelAddr) Network() string { return "call" }
func (tunnelAddr) String() string  { return "call" }

type tunnelConn struct {
	id          uint32
	rb          *RelayBridge
	rdy         chan error
	readBuf     bytes.Buffer
	readMu      sync.Mutex
	readCond    chan struct{}
	closed      atomic.Bool
	closeCh     chan struct{}
	writeMu     sync.Mutex
	creditMu    sync.Mutex
	sendCredit  int
	creditCh    chan struct{}
	flowControl bool
}

func newTunnelConn(id uint32, rb *RelayBridge) *tunnelConn {
	flowControl := rb != nil && rb.flowControl
	credit := 0
	if flowControl {
		credit = relayFlowWindowBytes
	}
	return &tunnelConn{
		id:          id,
		rb:          rb,
		rdy:         make(chan error, 1),
		readCond:    make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
		creditCh:    make(chan struct{}, 1),
		sendCredit:  credit,
		flowControl: flowControl,
	}
}

func (tc *tunnelConn) Read(b []byte) (int, error) {
	for {
		tc.readMu.Lock()
		if tc.readBuf.Len() > 0 {
			n, _ := tc.readBuf.Read(b)
			tc.rb.relayQueueDelta(-n)
			tc.readMu.Unlock()
			tc.returnReadCredit(n)
			return n, nil
		}
		tc.readMu.Unlock()
		select {
		case <-tc.closeCh:
			tc.readMu.Lock()
			if tc.readBuf.Len() > 0 {
				n, _ := tc.readBuf.Read(b)
				tc.rb.relayQueueDelta(-n)
				tc.readMu.Unlock()
				tc.returnReadCredit(n)
				return n, nil
			}
			tc.readMu.Unlock()
			return 0, io.EOF
		case <-tc.readCond:
		}
	}
}

func (tc *tunnelConn) Write(b []byte) (int, error) {
	if tc.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if !tc.flowControl {
		tc.rb.send(tc.id, MsgData, b)
		return len(b), nil
	}
	tc.writeMu.Lock()
	defer tc.writeMu.Unlock()
	written := 0
	for written < len(b) {
		chunkSize := min(relayFlowChunkBytes, len(b)-written)
		if err := tc.takeSendCredit(chunkSize); err != nil {
			return written, err
		}
		tc.rb.send(tc.id, MsgData, b[written:written+chunkSize])
		written += chunkSize
	}
	return written, nil
}

func (tc *tunnelConn) takeSendCredit(size int) error {
	for {
		tc.creditMu.Lock()
		if tc.sendCredit >= size {
			tc.sendCredit -= size
			tc.creditMu.Unlock()
			return nil
		}
		tc.creditMu.Unlock()
		select {
		case <-tc.creditCh:
		case <-tc.closeCh:
			return io.ErrClosedPipe
		}
	}
}

func (tc *tunnelConn) addSendCredit(credit int) {
	if !tc.flowControl || credit <= 0 || tc.closed.Load() {
		return
	}
	tc.creditMu.Lock()
	tc.sendCredit = min(relayFlowWindowBytes, tc.sendCredit+credit)
	tc.creditMu.Unlock()
	select {
	case tc.creditCh <- struct{}{}:
	default:
	}
}

func (tc *tunnelConn) returnReadCredit(credit int) {
	if !tc.flowControl || credit <= 0 || tc.closed.Load() {
		return
	}
	var payload [4]byte
	binary.BigEndian.PutUint32(payload[:], uint32(credit))
	tc.rb.send(tc.id, MsgFlowCredit, payload[:])
}

func (tc *tunnelConn) Close() error {
	return tc.close(true)
}

func (tc *tunnelConn) closeLocal() {
	_ = tc.close(false)
}

func (tc *tunnelConn) close(notifyPeer bool) error {
	if tc.closed.CompareAndSwap(false, true) {
		select {
		case tc.rdy <- io.ErrClosedPipe:
		default:
		}
		close(tc.closeCh)
		if tc.rb != nil && notifyPeer {
			tc.rb.send(tc.id, MsgClose, nil)
			tc.rb.conns.Delete(tc.id)
			tc.rb.updateRelayActive()
		}
	}
	tc.discardBuffered()
	return nil
}

func (tc *tunnelConn) LocalAddr() net.Addr                { return tunnelAddr{} }
func (tc *tunnelConn) RemoteAddr() net.Addr               { return tunnelAddr{} }
func (tc *tunnelConn) SetDeadline(t time.Time) error      { return nil }
func (tc *tunnelConn) SetReadDeadline(t time.Time) error  { return nil }
func (tc *tunnelConn) SetWriteDeadline(t time.Time) error { return nil }

func (tc *tunnelConn) deliver(payload []byte) {
	tc.readMu.Lock()
	if tc.closed.Load() {
		tc.readMu.Unlock()
		return
	}
	if tc.flowControl && tc.readBuf.Len()+len(payload) > relayFlowWindowBytes {
		tc.readMu.Unlock()
		tc.rb.relayQueueDrop()
		_ = tc.Close()
		return
	}
	tc.readBuf.Write(payload)
	tc.rb.relayQueueDelta(len(payload))
	tc.readMu.Unlock()
	select {
	case tc.readCond <- struct{}{}:
	default:
	}
}

func (tc *tunnelConn) discardBuffered() {
	tc.readMu.Lock()
	discarded := tc.readBuf.Len()
	tc.readBuf.Reset()
	tc.rb.relayQueueDelta(-discarded)
	tc.readMu.Unlock()
}

func (tc *tunnelConn) remoteClosed() {
	if tc.closed.CompareAndSwap(false, true) {
		select {
		case tc.rdy <- io.ErrClosedPipe:
		default:
		}
		close(tc.closeCh)
	}
}

type tunnelPacketConn struct {
	id      uint32
	rb      *RelayBridge
	uc      *udpClient
	destStr string
}

func (pc *tunnelPacketConn) Read(b []byte) (int, error) {
	data, ok := <-pc.uc.pending
	if !ok {
		return 0, io.EOF
	}
	n := copy(b, data)
	pc.rb.relayQueueDelta(-len(data))
	return n, nil
}

func (pc *tunnelPacketConn) Write(b []byte) (int, error) {
	if pc.uc.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	payload := make([]byte, 1+len(pc.destStr)+len(b))
	payload[0] = byte(len(pc.destStr))
	copy(payload[1:], pc.destStr)
	copy(payload[1+len(pc.destStr):], b)
	pc.rb.send(pc.id, MsgUDP, payload)
	return len(b), nil
}

func (pc *tunnelPacketConn) Close() error {
	if closed, _ := pc.uc.closePending(); closed {
		pc.rb.udpClients.Delete(pc.id)
		pc.rb.updateRelayActive()
		pc.rb.send(pc.id, MsgClose, nil)
	}
	return nil
}

func (pc *tunnelPacketConn) LocalAddr() net.Addr                { return tunnelAddr{} }
func (pc *tunnelPacketConn) RemoteAddr() net.Addr               { return tunnelAddr{} }
func (pc *tunnelPacketConn) SetDeadline(t time.Time) error      { return nil }
func (pc *tunnelPacketConn) SetReadDeadline(t time.Time) error  { return nil }
func (pc *tunnelPacketConn) SetWriteDeadline(t time.Time) error { return nil }

type creatorUDPConn struct {
	id       uint32
	rb       *RelayBridge
	addr     string
	readBuf  bytes.Buffer
	readMu   sync.Mutex
	readCond chan struct{}
	closed   atomic.Bool
	closeCh  chan struct{}
}

func newCreatorUDPConn(id uint32, rb *RelayBridge, addr string) *creatorUDPConn {
	return &creatorUDPConn{
		id:       id,
		rb:       rb,
		addr:     addr,
		readCond: make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
	}
}

func (uc *creatorUDPConn) Read(b []byte) (int, error) {
	for {
		uc.readMu.Lock()
		if uc.readBuf.Len() > 0 {
			n, _ := uc.readBuf.Read(b)
			uc.rb.relayQueueDelta(-n)
			uc.readMu.Unlock()
			return n, nil
		}
		uc.readMu.Unlock()
		select {
		case <-uc.closeCh:
			uc.readMu.Lock()
			if uc.readBuf.Len() > 0 {
				n, _ := uc.readBuf.Read(b)
				uc.rb.relayQueueDelta(-n)
				uc.readMu.Unlock()
				return n, nil
			}
			uc.readMu.Unlock()
			return 0, io.EOF
		case <-uc.readCond:
		}
	}
}

func (uc *creatorUDPConn) Write(b []byte) (int, error) {
	if uc.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	uc.rb.send(uc.id, MsgUDPReply, b)
	return len(b), nil
}

func (uc *creatorUDPConn) Close() error {
	return uc.close(true)
}

func (uc *creatorUDPConn) closeLocal() {
	_ = uc.close(false)
}

func (uc *creatorUDPConn) close(notifyPeer bool) error {
	if uc.closed.CompareAndSwap(false, true) {
		close(uc.closeCh)
		if uc.rb != nil && notifyPeer {
			uc.rb.send(uc.id, MsgClose, nil)
			uc.rb.udpClients.Delete(uc.id)
			uc.rb.updateRelayActive()
		}
	}
	uc.discardBuffered()
	return nil
}

func (uc *creatorUDPConn) LocalAddr() net.Addr                { return tunnelAddr{} }
func (uc *creatorUDPConn) RemoteAddr() net.Addr               { return tunnelAddr{} }
func (uc *creatorUDPConn) SetDeadline(t time.Time) error      { return nil }
func (uc *creatorUDPConn) SetReadDeadline(t time.Time) error  { return nil }
func (uc *creatorUDPConn) SetWriteDeadline(t time.Time) error { return nil }

func (uc *creatorUDPConn) deliver(payload []byte) {
	uc.readMu.Lock()
	if uc.closed.Load() {
		uc.readMu.Unlock()
		return
	}
	uc.readBuf.Write(payload)
	uc.rb.relayQueueDelta(len(payload))
	uc.readMu.Unlock()
	select {
	case uc.readCond <- struct{}{}:
	default:
	}
}

func (uc *creatorUDPConn) discardBuffered() {
	uc.readMu.Lock()
	discarded := uc.readBuf.Len()
	uc.readBuf.Reset()
	uc.rb.relayQueueDelta(-discarded)
	uc.readMu.Unlock()
}

func (uc *creatorUDPConn) remoteClosed() {
	if uc.closed.CompareAndSwap(false, true) {
		close(uc.closeCh)
	}
}
