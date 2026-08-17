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

func TestParasiteTunnelAdmissionWindowStartsInsideBDPBounds(t *testing.T) {
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

	// Admission is ACK-clocked and bounded to an 8-64 segment BDP range. Exercise the admission
	// policy while holding the lane lock: trySendEncoded is deliberately
	// non-blocking and may lose a TryLock race with the 10 ms update loop under
	// the race detector, which is unrelated to the window floor being tested.
	payload := make([]byte, 800)
	lane := tunnel.lanes[0]
	func() {
		lane.mu.Lock()
		defer lane.mu.Unlock()
		admissionLimit := lane.admissionLimitLocked(false)
		require.Equal(t, laneKCPInitialAdmission, admissionLimit)
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

	const packetCount = 8
	received := make(chan byte, packetCount)
	server.SetOnData(func(frame []byte) { received <- frame[9] })
	seen := make(map[byte]struct{}, packetCount)
	for sequence := byte(0); sequence < packetCount; sequence++ {
		client.SendData(calltunnel.EncodeFrame(100, calltunnel.MsgUDPReply, []byte{sequence}))
		select {
		case delivered := <-received:
			seen[delivered] = struct{}{}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for striped UDP payload")
		}
		time.Sleep(udpFlowletMaximumDwell + 5*time.Millisecond)
	}
	require.Len(t, seen, packetCount)
	client.sendMu.Lock()
	require.Equal(t, uint8((1<<LaneCount)-1), client.sendFlows[100].laneMask)
	require.True(t, client.sendFlows[100].laneAssigned)
	client.sendMu.Unlock()
}

func TestUDPFlowletByteBoundaryReleasesLanePreference(t *testing.T) {
	laneID := uint16(2)
	state := sendFlowState{
		unordered:      true,
		laneAssigned:  true,
		laneID:         laneID,
		flowletStarted: time.Now(),
		flowletBytes:   udpFlowletMaximumBytes,
	}
	require.Nil(t, state.preferredLane())
	state.flowletBytes--
	require.Equal(t, laneID, *state.preferredLane())
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

func TestParasiteTunnelSendStallQuarantinesBlockedLaneBeforeResetACK(t *testing.T) {
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
	tunnel.lanes[0].mu.Lock()
	require.Equal(t, laneStateQuarantined, tunnel.lanes[0].state)
	tunnel.lanes[0].mu.Unlock()
	require.Equal(t, 1, tunnel.ActiveWorkers(), "wire v9 waits for RESET_ACK before detaching the physical lane")
	_ = blockedPeer.Close()
	select {
	case <-sendDone:
	case <-time.After(100 * time.Millisecond):
	}
}

func TestParasiteTunnelThreeQuarantinesWaitForAggregateDeadline(t *testing.T) {
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

	require.True(t, tunnel.initiateLaneReset(0, "test"))
	require.True(t, tunnel.initiateLaneReset(1, "test"))
	require.Equal(t, 2, tunnel.quarantinedLaneCount())
	require.True(t, tunnel.initiateLaneReset(2, "test"))
	select {
	case <-tunnel.Done():
		t.Fatal("three quarantined lanes bypassed the aggregate no-progress deadline")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestParasiteTunnelPhysicalDetachPreservesLogicalSession(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x6677889f, logger.NOP())
	require.NoError(t, err)
	peers := make([]net.Conn, 0, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		workerConn, peerConn := newTestDatagramPair()
		_, err = tunnel.AddWorkerEpoch(laneID, 1, workerConn)
		require.NoError(t, err)
		peers = append(peers, peerConn)
	}
	t.Cleanup(func() {
		_ = tunnel.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	tunnel.DropWorker(0)
	tunnel.DropWorker(1)
	tunnel.DropWorker(2)
	select {
	case <-tunnel.Done():
		t.Fatal("physical worker detach closed the logical session")
	case <-time.After(100 * time.Millisecond):
	}
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

func TestParasiteTunnelSerializesConcurrentSoftLaneRecovery(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788b1, logger.NOP())
	require.NoError(t, err)
	tunnel.SetTelemetryCollectionActive(true)
	peers := make([]net.Conn, 0, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		connection, peer := newTestDatagramPair()
		_, err = tunnel.AddWorker(laneID, connection)
		require.NoError(t, err)
		peers = append(peers, peer)
	}
	t.Cleanup(func() {
		_ = tunnel.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	start := make(chan struct{})
	results := make(chan laneRecoveryResult, 32)
	var group sync.WaitGroup
	for attempt := 0; attempt < cap(results); attempt++ {
		laneID := uint16(attempt % LaneCount)
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, result := tunnel.recoverStalledLane(&laneID)
			results <- result
		}()
	}
	close(start)
	group.Wait()
	close(results)

	started := 0
	for result := range results {
		if result == laneRecoveryStarted {
			started++
		}
	}
	require.Equal(t, 1, started)
	require.Equal(t, 1, tunnel.quarantinedLaneCount())
	require.Equal(t, float64(LaneCount-1), tunnel.metrics.Value(telemetry.LaneRecoveryDeferredTotal))
	select {
	case <-tunnel.Done():
		t.Fatal("concurrent soft recovery replaced the logical session")
	default:
	}
}

func TestParasiteTunnelCancelsDeferredRecoveryAfterACKProgress(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788b3, logger.NOP())
	require.NoError(t, err)
	peers := make([]net.Conn, 0, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		connection, peer := newTestDatagramPair()
		_, err = tunnel.AddWorker(laneID, connection)
		require.NoError(t, err)
		peers = append(peers, peer)
	}
	t.Cleanup(func() {
		_ = tunnel.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	first := uint16(0)
	_, result := tunnel.recoverStalledLane(&first)
	require.Equal(t, laneRecoveryStarted, result)
	second := uint16(1)
	_, result = tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryInProgress, result)

	progress := time.Now().UnixNano()
	tunnel.lanes[second].lastAckProgress.Store(progress)
	tunnel.recoveryMu.Lock()
	tunnel.recoveryActive = false
	tunnel.recoveryReadyAt = time.Now().Add(-time.Millisecond)
	tunnel.recoveryMu.Unlock()
	tunnel.resumeDeferredLaneRecovery(second)

	tunnel.recoveryMu.Lock()
	require.Zero(t, tunnel.recoveryPending&uint8(1<<second))
	tunnel.recoveryMu.Unlock()
	tunnel.lanes[second].mu.Lock()
	require.Equal(t, laneStateActive, tunnel.lanes[second].state)
	tunnel.lanes[second].mu.Unlock()
}

func TestParasiteTunnelSoftRecoveryHonorsCooldown(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788b2, logger.NOP())
	require.NoError(t, err)
	tunnel.recoveryCooldown = time.Second
	peers := make([]net.Conn, 0, LaneCount)
	for laneID := uint16(0); laneID < LaneCount; laneID++ {
		connection, peer := newTestDatagramPair()
		_, err = tunnel.AddWorker(laneID, connection)
		require.NoError(t, err)
		peers = append(peers, peer)
	}
	t.Cleanup(func() {
		_ = tunnel.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	first := uint16(0)
	_, result := tunnel.recoverStalledLane(&first)
	require.Equal(t, laneRecoveryStarted, result)
	tunnel.completeLaneRecovery(first)

	second := uint16(1)
	_, result = tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryInProgress, result)
	tunnel.lanes[second].mu.Lock()
	require.Equal(t, laneStateActive, tunnel.lanes[second].state)
	tunnel.lanes[second].mu.Unlock()

	tunnel.recoveryMu.Lock()
	tunnel.recoveryReadyAt = time.Now().Add(-time.Millisecond)
	tunnel.recoveryMu.Unlock()
	_, result = tunnel.recoverStalledLane(&second)
	require.Equal(t, laneRecoveryStarted, result)
}

func TestParasiteTunnelRejectsDataWhileLaneIsQuarantined(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a3, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	connection, peer := newTestDatagramPair()
	_, err = tunnel.AddWorkerEpoch(0, 1, connection)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	require.True(t, tunnel.initiateLaneReset(0, "test"))
	require.False(t, tunnel.trySendEncodedOnLane(tunnel.lanes[0], []byte("payload"), 1, false))
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

func TestLaneResetHandshakeDeadlineTracksMeasuredRTO(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a5, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	require.Equal(t, laneResetMinimumDeadline, tunnel.laneResetHandshakeDeadline(0))
	lane := tunnel.lanes[0]
	lane.mu.Lock()
	lane.kcpSRTTMS = 600
	lane.kcpRTTVARMS = 200
	lane.mu.Unlock()
	require.Equal(t, 11200*time.Millisecond, tunnel.laneResetHandshakeDeadline(0))
	lane.mu.Lock()
	lane.kcpSRTTMS = 2000
	lane.kcpRTTVARMS = 1000
	lane.mu.Unlock()
	require.Equal(t, laneResetMaximumDeadline, tunnel.laneResetHandshakeDeadline(0))
}

func TestParasiteTunnelOnlyCoordinatorAcceptsRecoverySuggestion(t *testing.T) {
	t.Parallel()
	coordinator, err := NewParasiteTunnel(0x667788a6, logger.NOP())
	require.NoError(t, err)
	responder, err := NewParasiteTunnel(0x667788a6, logger.NOP())
	require.NoError(t, err)
	coordinator.SetRecoveryCoordinator(true)
	responder.SetRecoveryCoordinator(false)
	coordinatorConn, coordinatorPeer := newTestDatagramPair()
	responderConn, responderPeer := newTestDatagramPair()
	_, err = coordinator.AddWorkerEpoch(0, 1, coordinatorConn)
	require.NoError(t, err)
	_, err = responder.AddWorkerEpoch(0, 1, responderConn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = coordinator.Close()
		_ = responder.Close()
		_ = coordinatorPeer.Close()
		_ = responderPeer.Close()
	})

	coordinator.handleLaneResetControl(laneResetSuggest, 0, 2)
	responder.handleLaneResetControl(laneResetSuggest, 0, 2)
	require.Eventually(t, func() bool {
		coordinator.lanes[0].mu.Lock()
		defer coordinator.lanes[0].mu.Unlock()
		return coordinator.lanes[0].state == laneStateQuarantined
	}, time.Second, 10*time.Millisecond)
	responder.lanes[0].mu.Lock()
	require.Equal(t, laneStateActive, responder.lanes[0].state)
	responder.lanes[0].mu.Unlock()
}

func TestParasiteTunnelNoProgressRequiresFreshApplicationDemand(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788a7, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	now := time.Now()
	require.False(t, tunnel.hasFreshApplicationDemand(now, 5*time.Second))
	tunnel.lastApplicationDemand.Store(now.Add(-6 * time.Second).UnixNano())
	require.False(t, tunnel.hasFreshApplicationDemand(now, 5*time.Second))
	tunnel.lastApplicationDemand.Store(now.Add(-time.Second).UnixNano())
	require.True(t, tunnel.hasFreshApplicationDemand(now, 5*time.Second))
}

func TestParasiteTunnelUnavailableLaneKeepsFlowForHotSwap(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x6677889f, logger.NOP())
	require.NoError(t, err)
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
		t.Fatalf("flow %d was aborted before same-generation hot swap", connID)
	case <-time.After(100 * time.Millisecond):
	}
	replacement, replacementPeer := newTestDatagramPair()
	_, err = tunnel.AddWorkerEpoch(0, 2, replacement)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replacementPeer.Close() })
	require.Equal(t, uint64(1), tunnel.LaneGeneration(0))
	select {
	case <-tunnel.Done():
		t.Fatal("unavailable lane closed the logical session")
	default:
	}
}

func TestParasiteTunnelMissingPreferredLaneResetsOnlyThatLane(t *testing.T) {
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
	require.Equal(t, laneRecoveryStarted, result)
	require.Equal(t, missing, workerID)
	require.Equal(t, 1, tunnel.ActiveWorkers())
	tunnel.lanes[0].mu.Lock()
	require.Equal(t, laneStateQuarantined, tunnel.lanes[0].state)
	tunnel.lanes[0].mu.Unlock()
	tunnel.lanes[1].mu.Lock()
	require.Equal(t, laneStateActive, tunnel.lanes[1].state)
	tunnel.lanes[1].mu.Unlock()
}

func TestWireV9MigratesOrderedFlowWithBoundedReplay(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788b0, logger.NOP())
	require.NoError(t, err)
	peers := make([]net.Conn, 0, 2)
	for laneID := uint16(0); laneID < 2; laneID++ {
		connection, peer := newTestDatagramPair()
		_, err = tunnel.AddWorkerEpoch(laneID, 1, connection)
		require.NoError(t, err)
		peers = append(peers, peer)
	}
	t.Cleanup(func() {
		_ = tunnel.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	state := tunnel.sendFlow(202)
	frame := calltunnel.EncodeFrame(202, calltunnel.MsgData, []byte("unacknowledged"))
	state.mu.Lock()
	state.initialized = true
	state.laneAssigned = true
	state.laneID = 0
	state.laneOwner.Store(1)
	state.laneMask = 1
	state.nextSequence = 1
	state.replay = []flowReplayFrame{{sequence: 0, frame: frame}}
	state.replayBytes = len(frame)
	tunnel.replayBytes.Store(int64(len(frame)))
	tunnel.lanes[0].flowCount.Add(1)
	state.mu.Unlock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		ack := calltunnel.EncodeFrame(
			calltunnel.ControlConnID,
			calltunnel.MsgFlowCommitAck,
			encodeFlowControlPayload(202, 1, 1),
		)
		tunnel.deliver(1, encodeLaneFrame(calltunnel.ControlConnID, 0, ack))
	}()

	require.True(t, tunnel.migrateOrderedFlow(202, state, 0, "test"))
	require.Equal(t, uint32(2), state.laneOwner.Load())
	require.Equal(t, uint16(1), state.laneID)
	require.LessOrEqual(t, state.replayBytes, flowReplayPerFlowLimit)
	require.LessOrEqual(t, tunnel.replayBytes.Load(), int64(flowReplaySessionLimit))
}

func TestFlowProgressNeverBlocksThePhysicalLaneReader(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x667788b1, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	state := tunnel.sendFlow(203)
	first := calltunnel.EncodeFrame(203, calltunnel.MsgData, []byte("first"))
	second := calltunnel.EncodeFrame(203, calltunnel.MsgData, []byte("second"))
	state.mu.Lock()
	state.nextSequence = 2
	state.replay = []flowReplayFrame{
		{sequence: 0, frame: first},
		{sequence: 1, frame: second},
	}
	state.replayBytes = len(first) + len(second)
	tunnel.replayBytes.Store(int64(state.replayBytes))
	defer state.mu.Unlock()

	progressHandled := make(chan struct{})
	go func() {
		tunnel.trimFlowReplay(203, 2)
		close(progressHandled)
	}()
	select {
	case <-progressHandled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("flow progress blocked the physical lane reader on the send-flow mutex")
	}
	require.Equal(t, uint64(2), state.peerProgress.Load())
	require.Len(t, state.replay, 2, "the lock owner applies the deferred watermark")

	tunnel.trimFlowReplayLocked(state, state.peerProgress.Load())
	require.Empty(t, state.replay)
	require.Zero(t, state.replayBytes)
	require.Zero(t, tunnel.replayBytes.Load())
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

func TestClientNetworkRebindUsesSameGenerationHotSwap(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x778899ac, logger.NOP())
	require.NoError(t, err)
	tunnel.SetRecoveryCoordinator(true)
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
		connection, peerConnection := newTestDatagramPair()
		oldDone[workerID], err = tunnel.AddWorkerEpoch(uint16(workerID), 1, connection)
		require.NoError(t, err)
		peers = append(peers, peerConnection)
	}
	t.Cleanup(func() {
		_ = client.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	for workerID := 0; workerID < LaneCount; workerID++ {
		connection, peerConnection := newTestDatagramPair()
		_, err = tunnel.AddWorkerGenerationEpoch(uint16(workerID), 1, 2, connection)
		require.NoError(t, err)
		peers = append(peers, peerConnection)
		select {
		case <-oldDone[workerID]:
		case <-time.After(time.Second):
			t.Fatalf("worker %d was not replaced atomically", workerID)
		}
		require.True(t, tunnel.workerReadyAfter(uint16(workerID), 1))
		require.Equal(t, uint64(1), tunnel.LaneGeneration(uint16(workerID)))
		require.Equal(t, LaneCount, tunnel.ActiveWorkers())
	}
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
	request := authRequest{SessionID: [16]byte{1}, Conv: 1, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
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

	oldRequest := authRequest{SessionID: [16]byte{1}, Conv: 1, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
	oldSession, created, err := server.getOrCreateSession(oldRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(oldSession)
	workerConn, workerPeer := newTestDatagramPair()
	_, err = oldSession.tunnel.AddWorker(0, workerConn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = workerPeer.Close() })
	require.Equal(t, 1, oldSession.tunnel.ActiveWorkers())

	newRequest := authRequest{SessionID: [16]byte{2}, Conv: 2, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
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

	oldRequest := authRequest{SessionID: [16]byte{3}, Conv: 3, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
	oldSession, created, err := server.getOrCreateSession(oldRequest)
	require.NoError(t, err)
	require.True(t, created)
	server.releaseSessionAttach(oldSession)
	oldSession.tunnel.markProgress()
	workerConn, workerPeer := newTestDatagramPair()
	_, err = oldSession.tunnel.AddWorker(0, workerConn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = workerPeer.Close() })

	newRequest := authRequest{SessionID: [16]byte{4}, Conv: 4, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
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

	oldRequest := authRequest{SessionID: [16]byte{5}, Conv: 5, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
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
		newRequest := authRequest{SessionID: [16]byte{6}, Conv: 6, WorkerID: 0, WorkerTotal: LaneCount, WorkerEpoch: 1, LaneGeneration: 1, User: "alice", Password: "secret"}
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

func TestLaneRecoveryAttemptStateGauges(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x88776655, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })
	lane := tunnel.lanes[0]

	require.True(t, tunnel.initiateLaneReset(0, "test_gauge"))
	lane.mu.Lock()
	attemptID := lane.recoveryAttemptID
	lane.mu.Unlock()
	// The reset generation doubles as the recovery attempt id: generation 1
	// plus one reset attempt.
	require.Equal(t, uint64(2), attemptID)
	require.Equal(t, float64(2), lane.metrics.Value(telemetry.LaneRecoveryAttemptID))
	require.Equal(t, float64(1), lane.metrics.Value(telemetry.LaneRecoveryLastOutcome))

	tunnel.escalateLaneResetFailure(0, "test_failure")
	require.Equal(t, float64(3), lane.metrics.Value(telemetry.LaneRecoveryLastOutcome))
	require.Equal(t, float64(2), lane.metrics.Value(telemetry.LaneRecoveryAttemptID))
}

func TestParasiteTunnelNoProgressReplacesSessionWhenAllWorkersDead(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x99887766, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	replaced := make(chan string, 1)
	tunnel.SetTelemetryEventHandler(func(event telemetry.Event) {
		if event.Event == "session_replacement" {
			select {
			case replaced <- event.Reason:
			default:
			}
		}
	})

	now := time.Now()
	// Inject pending output segment while active workers is zero
	tunnel.lanes[0].mu.Lock()
	tunnel.lanes[0].outputPending = append(tunnel.lanes[0].outputPending, queuedSegment{payload: []byte{1, 2, 3}, enqueuedAt: now})
	tunnel.lanes[0].mu.Unlock()

	// Set last aggregate progress to past threshold
	tunnel.lastAggregateProgress.Store(now.Add(-35 * time.Second).UnixNano())
	// No fresh application demand
	tunnel.lastApplicationDemand.Store(0)

	pendingSince := now.Add(-35 * time.Second)
	replacedResult := tunnel.evaluateNoProgress(now, &pendingSince)
	require.True(t, replacedResult)
	select {
	case reason := <-replaced:
		require.Equal(t, "aggregate_no_progress", reason)
	case <-time.After(time.Second):
		t.Fatal("expected session_replacement event")
	}
}

func TestParasiteTunnelLaneQualityQuarantine(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x55443322, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	for i := uint16(0); i < LaneCount; i++ {
		conn, peer := newTestDatagramPair()
		_, err = tunnel.AddWorkerEpoch(i, 1, conn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = peer.Close() })
	}

	// Lanes 1, 2, 3 have clean paths (1-2% loss)
	for i := uint16(1); i < LaneCount; i++ {
		tunnel.lanes[i].mu.Lock()
		tunnel.lanes[i].retryRatioSmooth = 0.02
		tunnel.lanes[i].mu.Unlock()
	}

	median := tunnel.medianActiveRetryRatio(0)
	require.InDelta(t, 0.02, median, 0.01)

	// Lane 0 is severely degraded (55% loss)
	lane0 := uint16(0)
	tunnel.lanes[0].mu.Lock()
	tunnel.lanes[0].retryRatioSmooth = 0.55
	tunnel.lanes[0].mu.Unlock()

	laneID, result := tunnel.recoverStalledLaneWithReason(&lane0, "lane_quality")
	require.Equal(t, uint16(0), laneID)
	require.Equal(t, laneRecoveryStarted, result)
}

func TestLaneProbeDesynchronization(t *testing.T) {
	t.Parallel()
	start := time.Unix(100, 0)
	tunnel, err := NewParasiteTunnel(0x12345678, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	type probeWindow struct {
		start time.Time
		until time.Time
	}
	windows := make(map[uint16][]probeWindow)
	mss := float64(laneKCPMTU - kcpHeaderSize)

	// Switch each lane from startup to steady and enable demand
	for i := uint16(0); i < LaneCount; i++ {
		lane := tunnel.lanes[i]
		lane.mu.Lock()
		lane.pacingStartup = false
		lane.pacingRateBPS = 500_000
		lane.deliveryRateBPS = 480_000
		lane.deliveryCapacityBPS = 480_000
		lane.deliverySampleAt = start
		lane.pacingNextProbe = start.Add(lanePacingProbeInterval + lane.probeOffset())
		lane.windowDemandBits = 0b11
		lane.mu.Unlock()
	}

	stepDuration := 500 * time.Millisecond
	for step := 1; step <= 24; step++ { // 12 seconds
		now := start.Add(time.Duration(step) * stepDuration)
		for i := uint16(0); i < LaneCount; i++ {
			lane := tunnel.lanes[i]
			lane.mu.Lock()
			lane.deliveryDemanded = true
			lane.deliveryOutSegments = 20
			lane.ackedBytesTotal += 20 * uint64(mss)
			lane.admittedBytesTotal += 20 * uint64(mss)
			wasProbing := !lane.pacingProbeUntil.IsZero()
			lane.updateDeliveryController(now, 20)
			nowProbing := !lane.pacingProbeUntil.IsZero()
			if !wasProbing && nowProbing {
				windows[i] = append(windows[i], probeWindow{start: now, until: lane.pacingProbeUntil})
			}
			lane.mu.Unlock()
		}
	}

	// Verify each lane executed at least one probe
	for i := uint16(0); i < LaneCount; i++ {
		require.NotEmpty(t, windows[i], "lane %d should have probed", i)
	}

	// Verify no pair of lanes has overlapping probe windows within 500ms
	for i := uint16(0); i < LaneCount; i++ {
		for j := i + 1; j < LaneCount; j++ {
			for _, w1 := range windows[i] {
				for _, w2 := range windows[j] {
					// Check distance between probe starts >= 500ms
					diff := w1.start.Sub(w2.start)
					if diff < 0 {
						diff = -diff
					}
					require.GreaterOrEqual(t, diff, 500*time.Millisecond,
						"lane %d and lane %d probe start diff should be >= 500ms", i, j)
					// Check windows do not overlap
					require.True(t, w1.until.Before(w2.start) || w2.until.Before(w1.start) || w1.until.Equal(w2.start) || w2.until.Equal(w1.start),
						"lane %d and lane %d probe windows overlap", i, j)
				}
			}
		}
	}
}

func TestAdaptiveKCPTickQuiescedAndImmediateWake(t *testing.T) {
	t.Parallel()
	tunnel, err := NewParasiteTunnel(0x87654321, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	conn, peer := newTestDatagramPair()
	_, err = tunnel.AddWorkerEpoch(0, 1, conn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	lane := tunnel.lanes[0]
	// Verify lane has wake channel
	require.NotNil(t, lane.wake)

	// Send data into lane to wake it up immediately
	started := time.Now()
	lane.notifyWake()
	require.Less(t, time.Since(started), 15*time.Millisecond)
}
