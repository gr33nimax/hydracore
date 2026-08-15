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

type blockingWriteConn struct {
	net.Conn
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func newBlockingWriteConn(conn net.Conn) *blockingWriteConn {
	return &blockingWriteConn{
		Conn:    conn,
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (c *blockingWriteConn) Write([]byte) (int, error) {
	c.startOnce.Do(func() { close(c.started) })
	<-c.closed
	return 0, io.ErrClosedPipe
}

func (c *blockingWriteConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
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

func TestParasiteTunnelAdmissionWindowHasNonCollapsingFloor(t *testing.T) {
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

	// Admission is ACK-clocked, expressed in segments rather than a fixed byte
	// rate, and must retain enough initial flight to avoid Reno's one-segment
	// collapse while leaving room for control traffic. Exercise the admission
	// policy while holding the lane lock: trySendEncoded is deliberately
	// non-blocking and may lose a TryLock race with the 10 ms update loop under
	// the race detector, which is unrelated to the window floor being tested.
	payload := make([]byte, 800)
	lane := tunnel.lanes[0]
	func() {
		lane.mu.Lock()
		defer lane.mu.Unlock()
		admissionLimit := lane.admissionLimitLocked(false)
		require.Equal(t, laneKCPInitialAdmission-laneKCPControlReserve, admissionLimit)
		for frame := 0; frame < admissionLimit; frame++ {
			require.LessOrEqual(t, lane.kcp.WaitSnd()+1, admissionLimit, "frame %d hit the initial admission floor", frame)
			require.GreaterOrEqual(t, lane.kcp.Send(payload), 0)
		}
		require.Equal(t, admissionLimit, lane.kcp.WaitSnd())
		require.Equal(t, laneKCPInitialAdmission, lane.admissionWindow)
	}()
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

func TestParasiteTunnelDoesNotSpillPinnedFlowAcrossLossDomains(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x22334457, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	for laneID := uint16(0); laneID < 2; laneID++ {
		connection, _ := newTestDatagramPair()
		_, err = tunnel.reserveWorker(laneID, 1, connection)
		require.NoError(t, err)
	}
	preferred := uint16(0)
	tunnel.lanes[preferred].mu.Lock()
	limit := tunnel.lanes[preferred].admissionLimitLocked(false)
	for index := 0; index < limit; index++ {
		require.GreaterOrEqual(t, tunnel.lanes[preferred].kcp.Send([]byte{byte(index)}), 0)
	}
	tunnel.lanes[preferred].mu.Unlock()

	selected := tunnel.trySendEncoded([]byte("spill"), 1, &preferred, false)
	require.Nil(t, selected, "ordered flow must wait for or recover its assigned lane")
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

func TestParasiteTunnelSendStallAbortsOnlyAffectedFlow(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x66778899, logger.NOP())
	require.NoError(t, err)
	tunnel.sendStallTimeout = 40 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })
	events := make(chan telemetry.Event, 1)
	tunnel.SetTelemetryEventHandler(func(event telemetry.Event) {
		if event.Event == "lane_send_stalled" {
			events <- event
		}
	})
	closedFlow := make(chan uint32, 1)
	tunnel.SetOnData(func(frame []byte) {
		connID, msgType, ok := relayFrameIdentity(frame)
		if ok && msgType == calltunnel.MsgClose {
			closedFlow <- connID
		}
	})

	go tunnel.SendData(calltunnel.EncodeFrame(88, calltunnel.MsgData, []byte("payload")))
	select {
	case event := <-events:
		require.Equal(t, "lane_send_stalled", event.Event)
		require.Equal(t, "pending_timeout", event.Reason)
	case <-time.After(time.Second):
		t.Fatal("stalled logical session did not emit a telemetry event")
	}
	select {
	case connID := <-closedFlow:
		require.Equal(t, uint32(88), connID)
	case <-time.After(time.Second):
		t.Fatal("stalled flow was not aborted")
	}
	select {
	case <-tunnel.Done():
		t.Fatal("one stalled lane closed the logical session")
	default:
	}
}

func TestParasiteTunnelSendStallRecyclesOnlyBlockedLane(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x6677889c, logger.NOP())
	require.NoError(t, err)
	tunnel.sendStallTimeout = 40 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })

	blockedConn, blockedPeer := newTestDatagramPair()
	worker, err := tunnel.reserveWorker(0, 1, blockedConn)
	require.NoError(t, err)
	for len(worker.sendQueue) < cap(worker.sendQueue) {
		worker.sendQueue <- queuedSegment{payload: []byte("blocked"), enqueuedAt: time.Now()}
	}
	tunnel.lanes[0].mu.Lock()
	for len(tunnel.lanes[0].outputPending) < laneKCPOutputBacklog {
		tunnel.lanes[0].outputPending = append(tunnel.lanes[0].outputPending, queuedSegment{
			payload:    []byte("staged"),
			enqueuedAt: time.Now(),
		})
	}
	tunnel.lanes[0].mu.Unlock()

	events := make(chan telemetry.Event, 4)
	tunnel.SetTelemetryEventHandler(func(event telemetry.Event) { events <- event })
	sendDone := make(chan bool, 1)
	preferred := uint16(0)
	go func() {
		_, sent := tunnel.sendEncoded([]byte("payload"), true, &preferred, false)
		sendDone <- sent
	}()

	for {
		select {
		case event := <-events:
			if event.Event == "lane_send_recovery" {
				require.NotNil(t, event.WorkerID)
				require.Equal(t, uint16(0), *event.WorkerID)
				goto recovered
			}
		case <-time.After(time.Second):
			t.Fatal("blocked lane was not recycled")
		}
	}

recovered:
	select {
	case <-tunnel.Done():
		t.Fatal("single-lane recovery closed the logical session")
	default:
	}
	replacement, replacementPeer := newTestDatagramPair()
	_, err = tunnel.reserveWorker(0, 2, replacement)
	require.NoError(t, err)
	select {
	case sent := <-sendDone:
		require.True(t, sent)
	case <-time.After(time.Second):
		t.Fatal("send did not resume on the replacement lane")
	}
	_ = blockedPeer.Close()
	_ = replacementPeer.Close()
}

func TestParasiteTunnelCoalescesConcurrentLaneRecovery(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x6677889d, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	peers := make([]net.Conn, 0, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		workerConn, peerConn := newTestDatagramPair()
		_, err = tunnel.reserveWorker(laneID, 1, workerConn)
		require.NoError(t, err)
		peers = append(peers, peerConn)
	}
	t.Cleanup(func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	first := uint16(0)
	workerID, result := tunnel.recoverStalledLane(&first)
	require.Equal(t, laneRecoveryStarted, result)
	require.Equal(t, uint16(0), workerID)
	require.Equal(t, LaneCount-1, tunnel.ActiveWorkers())

	second := uint16(1)
	workerID, result = tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryInProgress, result)
	require.Equal(t, uint16(0), workerID)
	require.Equal(t, LaneCount-1, tunnel.ActiveWorkers(), "coalesced recovery must not recycle another lane")

	replacement, replacementPeer := newTestDatagramPair()
	peers = append(peers, replacementPeer)
	_, err = tunnel.reserveWorker(0, 2, replacement)
	require.NoError(t, err)
	// Cooldown behavior has its own regression test below. End it explicitly so
	// this test can keep checking that a completed recovery permits a different
	// lane to become the next recovery target.
	tunnel.recoveryMu.Lock()
	tunnel.recoveryReadyAt = time.Time{}
	tunnel.recoveryMu.Unlock()
	workerID, result = tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryStarted, result)
	require.Equal(t, uint16(1), workerID)
}

func TestKCPRetransmissionReasonEstimate(t *testing.T) {
	t.Parallel()
	require.False(t, isEstimatedRTO(40*time.Millisecond, 100*time.Millisecond))
	require.True(t, isEstimatedRTO(75*time.Millisecond, 100*time.Millisecond))
}

func TestRelayBridgeSendStallAbortsFlowWithoutReentrantDeadlock(t *testing.T) {
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
		t.Fatal("one stalled relay flow closed the data tunnel")
	default:
	}
}

func TestParasiteTunnelRecycleSignalDetachesPeerLane(t *testing.T) {
	t.Parallel()
	left, err := NewParasiteTunnel(0x6677889e, logger.NOP())
	require.NoError(t, err)
	right, err := NewParasiteTunnel(0x6677889e, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	leftConn, rightConn := newTestDatagramPair()
	_, err = left.AddWorkerEpoch(0, 1, leftConn)
	require.NoError(t, err)
	_, err = right.AddWorkerEpoch(0, 1, rightConn)
	require.NoError(t, err)
	workerID := uint16(0)
	recoveredID, result := left.recoverStalledLane(&workerID)
	require.Equal(t, laneRecoveryStarted, result)
	require.Equal(t, workerID, recoveredID)
	require.Eventually(t, func() bool {
		return left.ActiveWorkers() == 0 && right.ActiveWorkers() == 0
	}, time.Second, 10*time.Millisecond)
	select {
	case <-left.Done():
		t.Fatal("lane recycle closed the left logical session")
	case <-right.Done():
		t.Fatal("lane recycle closed the right logical session")
	default:
	}
}

func TestParasiteTunnelRecycleUsesHealthyPeerLaneWhenTargetWriterIsBlocked(t *testing.T) {
	t.Parallel()
	left, err := NewParasiteTunnel(0x667788a1, logger.NOP())
	require.NoError(t, err)
	right, err := NewParasiteTunnel(0x667788a1, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	leftTarget, rightTarget := newTestDatagramPair()
	blockedTarget := newBlockingWriteConn(leftTarget)
	_, err = left.AddWorkerEpoch(0, 1, blockedTarget)
	require.NoError(t, err)
	_, err = right.AddWorkerEpoch(0, 1, rightTarget)
	require.NoError(t, err)
	for laneID := uint16(1); laneID < LaneCount; laneID++ {
		leftConn, rightConn := newTestDatagramPair()
		_, err = left.AddWorkerEpoch(laneID, 1, leftConn)
		require.NoError(t, err)
		_, err = right.AddWorkerEpoch(laneID, 1, rightConn)
		require.NoError(t, err)
	}

	left.lanes[0].workerMu.RLock()
	worker := left.lanes[0].worker
	left.lanes[0].workerMu.RUnlock()
	worker.sendQueue <- queuedSegment{payload: []byte("block target writer"), enqueuedAt: time.Now()}
	select {
	case <-blockedTarget.started:
	case <-time.After(time.Second):
		t.Fatal("target writer did not block")
	}

	workerID := uint16(0)
	recoveredID, result := left.recoverStalledLane(&workerID)
	require.Equal(t, laneRecoveryStarted, result)
	require.Equal(t, workerID, recoveredID)
	require.Eventually(t, func() bool {
		return left.ActiveWorkers() == LaneCount-1 && right.ActiveWorkers() == LaneCount-1
	}, time.Second, 10*time.Millisecond)
	for laneID := uint16(1); laneID < LaneCount; laneID++ {
		_, leftActive := left.WorkerEpoch(laneID)
		_, rightActive := right.WorkerEpoch(laneID)
		require.True(t, leftActive, "left healthy lane %d was recycled", laneID)
		require.True(t, rightActive, "right healthy lane %d was recycled", laneID)
	}

	leftReplacement, rightReplacement := newTestDatagramPair()
	_, err = left.AddWorkerEpoch(0, 2, leftReplacement)
	require.NoError(t, err)
	_, err = right.AddWorkerEpoch(0, 2, rightReplacement)
	require.NoError(t, err)
	require.Equal(t, LaneCount, left.ActiveWorkers())
	require.Equal(t, LaneCount, right.ActiveWorkers())
}

func TestParasiteTunnelRecycleControlCannotDropNewerWorkerEpoch(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a2, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	connection, peer := newTestDatagramPair()
	done, err := tunnel.AddWorkerEpoch(0, 2, connection)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	tunnel.recyclePeerWorker(0, 1)
	require.Equal(t, 1, tunnel.ActiveWorkers())
	select {
	case <-done:
		t.Fatal("stale recycle control removed a newer worker epoch")
	default:
	}
}

func TestParasiteTunnelRecoveryCooldownProtectsFreshWorker(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a3, logger.NOP())
	require.NoError(t, err)
	tunnel.recoveryCooldown = 40 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })
	peers := make([]net.Conn, 0, 3)
	for laneID := uint16(0); laneID < 2; laneID++ {
		connection, peer := newTestDatagramPair()
		peers = append(peers, peer)
		_, err = tunnel.AddWorkerEpoch(laneID, 1, connection)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	first := uint16(0)
	_, result := tunnel.recoverStalledLane(&first)
	require.Equal(t, laneRecoveryStarted, result)
	replacement, peer := newTestDatagramPair()
	peers = append(peers, peer)
	_, err = tunnel.AddWorkerEpoch(first, 2, replacement)
	require.NoError(t, err)

	second := uint16(1)
	_, result = tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryInProgress, result)
	require.Equal(t, 2, tunnel.ActiveWorkers(), "fresh recovery immediately recycled another lane")
	time.Sleep(60 * time.Millisecond)
	workerID, result := tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryStarted, result)
	require.Equal(t, second, workerID)
}

func TestLaneAckStallTimeoutTracksMeasuredRTO(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a4, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	lane := tunnel.lanes[0]
	lane.mu.Lock()
	require.Equal(t, laneAckStallTimeout, lane.ackStallTimeoutLocked())
	lane.kcpSRTTMS = 600
	lane.kcpRTTVARMS = 200
	require.Equal(t, 11200*time.Millisecond, lane.ackStallTimeoutLocked())
	lane.kcpSRTTMS = 2000
	lane.kcpRTTVARMS = 1000
	require.Equal(t, laneAckStallMaximum, lane.ackStallTimeoutLocked())
	lane.mu.Unlock()
}

func TestParasiteTunnelUnavailableLaneAbortsOnlyItsFlows(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x6677889f, logger.NOP())
	require.NoError(t, err)
	tunnel.laneRecoveryGrace = 40 * time.Millisecond
	t.Cleanup(func() { _ = tunnel.Close() })

	connection, peer := newTestDatagramPair()
	_, err = tunnel.AddWorkerEpoch(0, 1, connection)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	state := tunnel.sendFlow(101)
	state.mu.Lock()
	state.initialized = true
	state.laneAssigned = true
	state.laneID = 0
	state.laneOwner.Store(1)
	state.laneMask = 1
	tunnel.lanes[0].flowCount.Add(1)
	state.mu.Unlock()
	closedFlow := make(chan uint32, 1)
	tunnel.SetOnData(func(frame []byte) {
		connID, msgType, ok := relayFrameIdentity(frame)
		if ok && msgType == calltunnel.MsgClose {
			closedFlow <- connID
		}
	})

	tunnel.DropWorker(0)
	select {
	case connID := <-closedFlow:
		require.Equal(t, uint32(101), connID)
	case <-time.After(time.Second):
		t.Fatal("flow pinned to an unavailable lane was not aborted")
	}
	select {
	case <-tunnel.Done():
		t.Fatal("unavailable lane closed the logical session")
	default:
	}
}

func TestParasiteTunnelMissingPreferredLaneDoesNotRecycleHealthyLane(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a0, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	connection, peer := newTestDatagramPair()
	_, err = tunnel.AddWorkerEpoch(1, 1, connection)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	missing := uint16(0)
	workerID, result := tunnel.recoverStalledLane(&missing)
	require.Equal(t, laneRecoveryUnavailable, result)
	require.Equal(t, missing, workerID)
	require.Equal(t, 1, tunnel.ActiveWorkers())
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
	tunnel.SetTelemetryCollectionActive(true)
	t.Cleanup(func() { _ = tunnel.Close() })

	closedFlow := make(chan uint32, 1)
	tunnel.SetOnData(func(frame []byte) {
		connID, msgType, ok := relayFrameIdentity(frame)
		if ok && msgType == calltunnel.MsgClose {
			closedFlow <- connID
		}
	})
	frame := calltunnel.EncodeFrame(99, calltunnel.MsgData, []byte("second"))
	tunnel.deliver(1, encodeLaneFrame(99, 1, frame))
	require.True(t, tunnel.expireReorderGaps(time.Now().Add(time.Second)))
	select {
	case connID := <-closedFlow:
		require.Equal(t, uint32(99), connID)
	case <-time.After(time.Second):
		t.Fatal("expired sequence gap did not close the affected flow")
	}
	select {
	case <-tunnel.Done():
		t.Fatal("one expired flow closed the logical session")
	default:
	}
	require.Equal(t, float64(1), tunnel.metrics.Value(telemetry.FlowReorderAbortTotal))
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
	require.Equal(t, minimumPeerReadQueuePackets, normalized.PeerReadQueuePackets)
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
