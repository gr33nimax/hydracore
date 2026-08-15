package vkparasite

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

type testDatagramConn struct {
	incoming chan []byte
	peer     *testDatagramConn
	closed   chan struct{}
	once     sync.Once
	writes   atomic.Int32
}

func newTestDatagramPair() (*testDatagramConn, *testDatagramConn) {
	left := &testDatagramConn{incoming: make(chan []byte, 256), closed: make(chan struct{})}
	right := &testDatagramConn{incoming: make(chan []byte, 256), closed: make(chan struct{})}
	left.peer = right
	right.peer = left
	return left, right
}

func (c *testDatagramConn) Read(buffer []byte) (int, error) {
	select {
	case payload := <-c.incoming:
		return copy(buffer, payload), nil
	case <-c.closed:
		return 0, io.EOF
	}
}

func (c *testDatagramConn) Write(payload []byte) (int, error) {
	copyPayload := append([]byte(nil), payload...)
	select {
	case c.peer.incoming <- copyPayload:
		c.writes.Add(1)
		return len(payload), nil
	case <-c.peer.closed:
		return 0, io.ErrClosedPipe
	}
}

func (c *testDatagramConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *testDatagramConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *testDatagramConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *testDatagramConn) SetDeadline(time.Time) error      { return nil }
func (c *testDatagramConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testDatagramConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func connectTestLanes(t *testing.T, left, right *ParasiteTunnel) {
	t.Helper()
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		leftConn, rightConn := newTestDatagramPair()
		_, err := left.AddWorker(laneID, leftConn)
		require.NoError(t, err)
		_, err = right.AddWorker(laneID, rightConn)
		require.NoError(t, err)
	}
}

func TestLaneConversationsAreStableUniqueAndNonzero(t *testing.T) {
	t.Parallel()
	seen := make(map[uint32]struct{}, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		conversation := laneConversation(0x11223344, laneID)
		require.NotZero(t, conversation)
		require.Equal(t, conversation, laneConversation(0x11223344, laneID))
		_, duplicate := seen[conversation]
		require.False(t, duplicate)
		seen[conversation] = struct{}{}
	}
}

func TestLaneFrameRejectsWireV5(t *testing.T) {
	t.Parallel()
	encoded := encodeLaneFrame(1, 0, calltunnel.EncodeFrame(1, calltunnel.MsgData, []byte("payload")))
	encoded[6] = 5
	_, _, _, ok := decodeLaneFrame(encoded)
	require.False(t, ok)
}

func TestParasiteTunnelUsesFourIndependentLanes(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x11223344, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x11223344, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	received := make(chan uint32, 8)
	server.SetOnData(func(frame []byte) {
		_, _, ok := relayFrameIdentity(frame)
		require.True(t, ok)
		received <- binary.BigEndian.Uint32(frame[4:8])
	})
	for connID := uint32(1); connID <= 8; connID++ {
		client.SendData(calltunnel.EncodeFrame(connID, calltunnel.MsgData, []byte{byte(connID)}))
	}
	for count := 0; count < 8; count++ {
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for lane payload")
		}
	}
	used := make(map[uint16]struct{}, LaneCount)
	client.sendMu.Lock()
	for _, flow := range client.sendFlows {
		for laneID := uint16(0); laneID < LaneCount; laneID++ {
			if flow.laneMask&(1<<laneID) != 0 {
				used[laneID] = struct{}{}
			}
		}
	}
	client.sendMu.Unlock()
	require.Len(t, used, LaneCount)
}

func TestParasiteTunnelHasNoLegacy64KiBAdmissionBurstCap(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x22334455, logger.NOP())
	require.NoError(t, err)
	workerConn, workerPeer := newTestDatagramPair()
	_, err = tunnel.AddWorker(0, workerConn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tunnel.Close()
		_ = workerPeer.Close()
	})

	// The removed admission controller started with a fixed 64 KiB token
	// bucket. Keep this burst below KCP and worker-queue bounds but above that
	// legacy cap, so a fixed application-rate limiter cannot return unnoticed.
	payload := make([]byte, 800)
	for frame := 0; frame < 84; frame++ {
		require.NotNil(t, tunnel.trySendEncoded(payload, 1, nil), "frame %d hit a fixed admission cap", frame)
	}
}

func TestParasiteTunnelPinsAFlowAndPreservesOrder(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x22334455, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x22334455, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	received := make(chan byte, 64)
	server.SetOnData(func(frame []byte) { received <- frame[9] })
	for sequence := byte(0); sequence < 64; sequence++ {
		client.SendData(calltunnel.EncodeFrame(99, calltunnel.MsgData, []byte{sequence}))
	}
	for expected := byte(0); expected < 64; expected++ {
		select {
		case actual := <-received:
			require.Equal(t, expected, actual)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for ordered flow payload")
		}
	}
	client.sendMu.Lock()
	require.Len(t, client.sendFlows, 1)
	require.Equal(t, uint64(64), client.sendFlows[99].nextSequence)
	require.NotZero(t, client.sendFlows[99].laneMask)
	require.Zero(t, client.sendFlows[99].laneMask&(client.sendFlows[99].laneMask-1), "one ordered flow must stay on one KCP lane")
	require.True(t, client.sendFlows[99].laneAssigned)
	client.sendMu.Unlock()
}

func TestParasiteTunnelStripesUDPWithoutCrossLaneHeadOfLineBlocking(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x22334456, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x22334456, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	received := make(chan byte, 64)
	server.SetOnData(func(frame []byte) { received <- frame[9] })
	for sequence := byte(0); sequence < 64; sequence++ {
		client.SendData(calltunnel.EncodeFrame(100, calltunnel.MsgUDPReply, []byte{sequence}))
	}
	seen := make(map[byte]struct{}, 64)
	for len(seen) < 64 {
		select {
		case sequence := <-received:
			seen[sequence] = struct{}{}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for striped UDP payload")
		}
	}
	client.sendMu.Lock()
	require.Equal(t, uint8((1<<LaneCount)-1), client.sendFlows[100].laneMask)
	require.False(t, client.sendFlows[100].laneAssigned)
	client.sendMu.Unlock()
}

func TestRemoteCloseReleasesLocalLaneFlowAccounting(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x23456789, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	state := &sendFlowState{laneMask: 0b00000101}
	tunnel.sendMu.Lock()
	tunnel.sendFlows[77] = state
	tunnel.sendMu.Unlock()
	tunnel.deliverMu.Lock()
	tunnel.receiveFlows[77] = &receiveFlowState{pending: make(map[uint64][]byte)}
	tunnel.lanes[0].flowCount.Store(1)
	tunnel.lanes[2].flowCount.Store(1)
	tunnel.deliverFrameLocked(77, calltunnel.EncodeFrame(77, calltunnel.MsgClose, nil))
	tunnel.deliverMu.Unlock()

	tunnel.sendMu.Lock()
	require.NotContains(t, tunnel.sendFlows, uint32(77))
	tunnel.sendMu.Unlock()
	tunnel.deliverMu.Lock()
	require.NotContains(t, tunnel.receiveFlows, uint32(77))
	tunnel.deliverMu.Unlock()
	require.Zero(t, tunnel.lanes[0].flowCount.Load())
	require.Zero(t, tunnel.lanes[2].flowCount.Load())
}

func TestParasiteTunnelHigherEpochImmediatelyReplacesLane(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x33445566, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	first, firstPeer := newTestDatagramPair()
	second, secondPeer := newTestDatagramPair()
	firstDone, err := tunnel.AddWorkerEpoch(0, 7, first)
	require.NoError(t, err)
	secondDone, err := tunnel.AddWorkerEpoch(0, 8, second)
	require.NoError(t, err)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("older lane transport was not replaced")
	}
	select {
	case <-secondDone:
		t.Fatal("newer lane transport was closed")
	default:
	}
	_, err = tunnel.AddWorkerEpoch(0, 7, firstPeer)
	require.ErrorContains(t, err, "stale lane epoch")
	_ = firstPeer.Close()
	_ = secondPeer.Close()
}

func TestParasiteTunnelCarriesTelemetryControl(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x44556677, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x44556677, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	connectTestLanes(t, client, server)

	leaseReceived := make(chan time.Duration, 1)
	client.SetTelemetryControlHandler(func(lease time.Duration) { leaseReceived <- lease })
	require.True(t, server.RequestClientTelemetry(6*time.Second))
	select {
	case lease := <-leaseReceived:
		require.Equal(t, 6*time.Second, lease)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for telemetry lease")
	}

	recordReceived := make(chan []byte, 1)
	server.SetTelemetryClientRecordHandler(func(record []byte) { recordReceived <- record })
	record := telemetry.Snapshot("client", "", "", telemetry.NewAccumulator().Snapshot(telemetry.ClientRequired))
	payload, err := telemetry.Marshal(record)
	require.NoError(t, err)
	require.True(t, client.SendClientTelemetry(payload))
	select {
	case received := <-recordReceived:
		_, err = telemetry.DecodeClientRecord(received)
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for telemetry record")
	}
}

func TestParasiteTunnelWaitingSendReselectsLaneAndKeepsTelemetryLive(t *testing.T) {
	t.Parallel()
	client, err := NewParasiteTunnel(0x55667788, logger.NOP())
	require.NoError(t, err)
	server, err := NewParasiteTunnel(0x55667788, logger.NOP())
	require.NoError(t, err)
	client.sendStallTimeout = 2 * time.Second
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	received := make(chan struct{}, 1)
	server.SetOnData(func([]byte) { received <- struct{}{} })
	sendDone := make(chan struct{})
	go func() {
		client.SendData(calltunnel.EncodeFrame(77, calltunnel.MsgData, []byte("payload")))
		close(sendDone)
	}()
	time.Sleep(20 * time.Millisecond)

	telemetryDone := make(chan struct{})
	go func() {
		_ = client.TelemetryValues()
		close(telemetryDone)
	}()
	select {
	case <-telemetryDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("telemetry blocked behind a waiting data send")
	}

	clientConn, serverConn := newTestDatagramPair()
	_, err = client.AddWorker(3, clientConn)
	require.NoError(t, err)
	_, err = server.AddWorker(3, serverConn)
	require.NoError(t, err)
	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("waiting send did not reselect the newly available lane")
	}
	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("reselected lane did not deliver the frame")
	}
}

func TestParasiteTunnelSendStallClosesLogicalSession(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x66778899, logger.NOP())
	require.NoError(t, err)
	tunnel.sendStallTimeout = 40 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })
	events := make(chan telemetry.Event, 1)
	tunnel.SetTelemetryEventHandler(func(event telemetry.Event) { events <- event })

	go tunnel.SendData(calltunnel.EncodeFrame(88, calltunnel.MsgData, []byte("payload")))
	select {
	case event := <-events:
		require.Equal(t, "lane_send_stalled", event.Event)
		require.Equal(t, "pending_timeout", event.Reason)
	case <-time.After(time.Second):
		t.Fatal("stalled logical session did not emit a telemetry event")
	}
	select {
	case <-tunnel.Done():
	case <-time.After(time.Second):
		t.Fatal("stalled logical session did not close")
	}
}

func TestRelayBridgeSendStallClosesWithoutReentrantDeadlock(t *testing.T) {
	t.Parallel()
	dataTunnel, err := NewParasiteTunnel(0x6677889b, logger.NOP())
	require.NoError(t, err)
	dataTunnel.sendStallTimeout = 40 * time.Millisecond
	relay := calltunnel.NewRelayBridge(dataTunnel, "joiner", 32768, nil, logger.NOP())
	relay.MarkReady()
	t.Cleanup(func() {
		relay.Close()
		_ = dataTunnel.Close()
	})

	result := make(chan error, 1)
	go func() {
		_, dialErr := relay.DialContext(context.Background(), "1.1.1.1:80")
		result <- dialErr
	}()
	select {
	case dialErr := <-result:
		require.ErrorIs(t, dialErr, io.ErrClosedPipe)
	case <-time.After(time.Second):
		t.Fatal("relay close callback re-entered the stalled send path")
	}
	select {
	case <-dataTunnel.Done():
	case <-time.After(time.Second):
		t.Fatal("stalled data tunnel did not finish closing")
	}
}

func TestParasiteTunnelTelemetryTrySendDoesNotWaitForControlFlow(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x6677889a, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	tunnel.controlSendMu.Lock()
	defer tunnel.controlSendMu.Unlock()
	result := make(chan bool, 1)
	go func() {
		result <- tunnel.trySendControlData(calltunnel.EncodeFrame(
			calltunnel.ControlConnID,
			calltunnel.MsgData,
			[]byte("telemetry"),
		))
	}()
	select {
	case sent := <-result:
		require.False(t, sent)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("telemetry try-send waited for the control-flow mutex")
	}
}

func TestParasiteTunnelReorderGapExpires(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x778899aa, logger.NOP())
	require.NoError(t, err)
	tunnel.reorderGapTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })

	frame := calltunnel.EncodeFrame(99, calltunnel.MsgData, []byte("second"))
	tunnel.deliver(1, encodeLaneFrame(99, 1, frame))
	require.True(t, tunnel.expireReorderGaps(time.Now().Add(time.Second)))
	select {
	case <-tunnel.Done():
	default:
		t.Fatal("expired sequence gap did not close the logical session")
	}
}

func TestParasiteTunnelUDPReorderGapDoesNotKillLogicalSession(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x778899ab, logger.NOP())
	require.NoError(t, err)
	tunnel.reorderGapTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })

	frame := calltunnel.EncodeFrame(100, calltunnel.MsgUDPReply, []byte("second"))
	tunnel.deliver(1, encodeLaneFrame(100, 1, frame))
	require.False(t, tunnel.expireReorderGaps(time.Now().Add(time.Second)))
	select {
	case <-tunnel.Done():
		t.Fatal("an unordered UDP gap closed the entire logical session")
	default:
	}
	tunnel.deliverMu.Lock()
	require.NotContains(t, tunnel.receiveFlows, uint32(100))
	tunnel.deliverMu.Unlock()
}

func TestClientNetworkRebindReplacesOneWorkerAtATime(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x778899ac, logger.NOP())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		ctx:     ctx,
		cancel:  cancel,
		tunnel:  tunnel,
		metrics: telemetry.NewAccumulator(),
		logger:  logger.NOP(),
		options: ClientOptions{WorkerConnectTimeout: 9 * time.Second},
		workers: make([]clientWorkerControl, LaneCount),
	}
	oldDone := make([]<-chan struct{}, LaneCount)
	peers := make([]net.Conn, 0, 2*LaneCount)
	for workerID := 0; workerID < LaneCount; workerID++ {
		client.workers[workerID] = newClientWorkerControl()
		connection, peer := newTestDatagramPair()
		peers = append(peers, peer)
		oldDone[workerID], err = tunnel.AddWorkerEpoch(uint16(workerID), 1, connection)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	client.RebindNetwork()
	for workerID := 0; workerID < LaneCount; workerID++ {
		select {
		case <-oldDone[workerID]:
		case <-time.After(time.Second):
			t.Fatalf("worker %d was not selected for staged replacement", workerID)
		}
		require.Equal(t, LaneCount-1, tunnel.ActiveWorkers(), "another lane was dropped before worker %d recovered", workerID)
		connection, peer := newTestDatagramPair()
		peers = append(peers, peer)
		_, err = tunnel.AddWorkerEpoch(uint16(workerID), 2, connection)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return tunnel.ActiveWorkers() == LaneCount }, time.Second, 10*time.Millisecond)
}

func TestClientClosesImmediatelyWithLogicalTunnel(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x8899aabb, logger.NOP())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		ctx:     ctx,
		cancel:  cancel,
		tunnel:  tunnel,
		ready:   make(chan struct{}),
		options: ClientOptions{WorkerConnectTimeout: 30 * time.Second},
	}
	close(client.ready)
	go client.monitorConnectivity()
	t.Cleanup(func() { _ = client.Close() })

	require.NoError(t, tunnel.Close())
	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatal("client did not propagate logical tunnel closure")
	}
}

func TestServerRequiresExactlyFourLanes(t *testing.T) {
	t.Parallel()
	base := ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}
	normalized, _, err := validateServerOptions(base)
	require.NoError(t, err)
	require.Equal(t, LaneCount, normalized.MaxWorkersPerSession)
	base.MaxWorkersPerSession = 8
	_, _, err = validateServerOptions(base)
	require.ErrorContains(t, err, "must be four")
	base.MaxWorkersPerSession = LaneCount - 1
	_, _, err = validateServerOptions(base)
	require.ErrorContains(t, err, "must be four")

	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	request := authRequest{SessionID: [16]byte{1}, Conv: 1, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	session, created, err := server.getOrCreateSession(request)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(session)
}

func TestServerAllowsAuthenticatedSessionTakeover(t *testing.T) {
	t.Parallel()
	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	oldRequest := authRequest{SessionID: [16]byte{1}, Conv: 1, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	oldSession, created, err := server.getOrCreateSession(oldRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(oldSession)
	workerConn, workerPeer := newTestDatagramPair()
	_, err = oldSession.tunnel.AddWorker(0, workerConn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = workerPeer.Close() })
	require.Equal(t, 1, oldSession.tunnel.ActiveWorkers())

	newRequest := authRequest{SessionID: [16]byte{2}, Conv: 2, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	newSession, created, err := server.getOrCreateSession(newRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(newSession)
	select {
	case <-oldSession.tunnel.Done():
	case <-time.After(time.Second):
		t.Fatal("old active session was not evicted during authenticated takeover")
	}
}

func TestServerReplacesProgressingSessionForSameAuthenticatedUser(t *testing.T) {
	t.Parallel()
	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		MaxSessions:  2,
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	oldRequest := authRequest{SessionID: [16]byte{3}, Conv: 3, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	oldSession, created, err := server.getOrCreateSession(oldRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(oldSession)
	oldSession.tunnel.markProgress()
	workerConn, workerPeer := newTestDatagramPair()
	_, err = oldSession.tunnel.AddWorker(0, workerConn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = workerPeer.Close() })

	newRequest := authRequest{SessionID: [16]byte{4}, Conv: 4, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	newSession, created, err := server.getOrCreateSession(newRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(newSession)
	select {
	case <-oldSession.tunnel.Done():
	case <-time.After(time.Second):
		t.Fatal("progressing old session was not evicted during authenticated takeover")
	}
}

func TestServerTakeoverDoesNotWaitForSupersededSessionCleanup(t *testing.T) {
	t.Parallel()
	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users:        []ServerUser{{Name: "alice", Password: "secret"}},
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}, logger.NOP())
	require.NoError(t, err)
	closeGate := make(chan struct{})
	var closeGateOnce sync.Once
	releaseCloseGate := func() { closeGateOnce.Do(func() { close(closeGate) }) }
	t.Cleanup(func() {
		releaseCloseGate()
		_ = server.Close()
	})

	oldRequest := authRequest{SessionID: [16]byte{5}, Conv: 5, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
	oldSession, created, err := server.getOrCreateSession(oldRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(oldSession)
	oldSession.tunnel.SetOnClose(func() { <-closeGate })

	type takeoverResult struct {
		session *serverSession
		created bool
		err     error
	}
	result := make(chan takeoverResult, 1)
	go func() {
		newRequest := authRequest{SessionID: [16]byte{6}, Conv: 6, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, User: "alice", Password: "secret"}
		session, wasCreated, createErr := server.getOrCreateSession(newRequest)
		result <- takeoverResult{session: session, created: wasCreated, err: createErr}
	}()

	select {
	case takeover := <-result:
		require.NoError(t, takeover.err)
		require.True(t, takeover.created)
		server.releaseSessionAttach(takeover.session)
	case <-time.After(time.Second):
		t.Fatal("new authenticated session waited for superseded session cleanup")
	}
	releaseCloseGate()
}
