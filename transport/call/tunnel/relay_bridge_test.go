package tunnel

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

type discardDataTunnel struct {
	onData  func([]byte)
	onClose func()
}

type telemetryDataTunnel struct {
	discardDataTunnel
	queue atomic.Int64
}

func (*telemetryDataTunnel) RelaySetActive(int, int) {}
func (*telemetryDataTunnel) RelayAddBytes(uint64)    {}
func (t *telemetryDataTunnel) RelayQueueDelta(bytes int) {
	t.queue.Add(int64(bytes))
}
func (t *telemetryDataTunnel) RelayResetQueue() { t.queue.Store(0) }
func (*telemetryDataTunnel) RelayQueueDrop()    {}
func (*telemetryDataTunnel) RelayConnectFailure() {}

func (*discardDataTunnel) SendData([]byte)             {}
func (t *discardDataTunnel) SetOnData(fn func([]byte)) { t.onData = fn }
func (t *discardDataTunnel) SetOnClose(fn func())      { t.onClose = fn }
func (*discardDataTunnel) Reconfigure(int, int)        {}

type readyThenCloseDataTunnel struct {
	relay *RelayBridge
	fired atomic.Bool
}

func (t *readyThenCloseDataTunnel) SendData(frame []byte) {
	if !t.fired.CompareAndSwap(false, true) || len(frame) < 8 {
		return
	}
	connectionID := binary.BigEndian.Uint32(frame[4:8])
	t.relay.handleTunnelData(EncodeFrame(connectionID, MsgConnectOK, nil))
	t.relay.closeAll()
}

func (*readyThenCloseDataTunnel) SetOnData(func([]byte)) {}
func (*readyThenCloseDataTunnel) SetOnClose(func())      {}
func (*readyThenCloseDataTunnel) Reconfigure(int, int)   {}

func TestRelayBridgeCloseUnblocksPendingTunnelConnection(t *testing.T) {
	t.Parallel()
	relay := NewRelayBridge(&discardDataTunnel{}, "joiner", 32768, nil, logger.NOP())
	connection := newTunnelConn(1, relay)
	relay.conns.Store(uint32(1), connection)

	relay.closeAll()
	select {
	case err := <-connection.rdy:
		require.ErrorIs(t, err, io.ErrClosedPipe)
	default:
		t.Fatal("pending tunnel connection was not notified when the bridge closed it")
	}
}

func TestTunnelConnectionRemoteCloseUnblocksPendingDial(t *testing.T) {
	t.Parallel()
	connection := newTunnelConn(1, nil)
	connection.remoteClosed()
	select {
	case err := <-connection.rdy:
		require.ErrorIs(t, err, io.ErrClosedPipe)
	default:
		t.Fatal("pending tunnel connection was not notified by remote close")
	}
}

func TestRelayBridgeRejectsQueuedReadyConnectionClosedBeforeDialReturns(t *testing.T) {
	t.Parallel()
	dataTunnel := &readyThenCloseDataTunnel{}
	relay := NewRelayBridge(dataTunnel, "joiner", 32768, nil, logger.NOP())
	dataTunnel.relay = relay
	relay.MarkReady()

	connection, err := relay.DialContext(context.Background(), "1.1.1.1:80")
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Nil(t, connection)
}

func TestUDPClientCloseAndDeliverAreConcurrentSafe(t *testing.T) {
	t.Parallel()
	for attempt := 0; attempt < 100; attempt++ {
		client := &udpClient{pending: make(chan []byte, 64)}
		start := make(chan struct{})
		var workers sync.WaitGroup
		for index := 0; index < 4; index++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for packet := 0; packet < 32; packet++ {
					client.deliver([]byte{byte(packet)})
				}
			}()
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				client.closePending()
			}()
		}
		close(start)
		workers.Wait()
		require.True(t, client.closed.Load())
		closed, discarded := client.closePending()
		require.False(t, closed)
		require.Zero(t, discarded)
		require.False(t, client.deliver([]byte("closed")))
		for range client.pending {
		}
	}
}

func TestRelayQueueTelemetryReturnsToZeroWhenBufferedConnectionsClose(t *testing.T) {
	t.Parallel()
	dataTunnel := &telemetryDataTunnel{}
	relay := NewRelayBridge(dataTunnel, "joiner", 32768, nil, logger.NOP())

	tcp := newTunnelConn(1, relay)
	relay.conns.Store(uint32(1), tcp)
	tcp.deliver([]byte("buffered-tcp"))
	require.Equal(t, int64(len("buffered-tcp")), dataTunnel.queue.Load())
	require.NoError(t, tcp.Close())
	require.Zero(t, dataTunnel.queue.Load())

	relay.MarkReady()
	packetConn, err := relay.ListenPacket(context.Background(), "1.1.1.1:53")
	require.NoError(t, err)
	udp := packetConn.(*tunnelPacketConn)
	require.True(t, udp.uc.deliver([]byte("buffered-udp")))
	require.Equal(t, int64(len("buffered-udp")), dataTunnel.queue.Load())
	require.NoError(t, udp.Close())
	require.Zero(t, dataTunnel.queue.Load())

	creator := newCreatorUDPConn(3, relay, "1.1.1.1:53")
	creator.deliver([]byte("creator-udp"))
	require.Equal(t, int64(len("creator-udp")), dataTunnel.queue.Load())
	require.NoError(t, creator.Close())
	require.Zero(t, dataTunnel.queue.Load())
}

func TestRelayPayloadBytesExcludeFramingAndDestination(t *testing.T) {
	t.Parallel()
	require.Equal(t, 5, relayPayloadBytes(MsgData, []byte("hello")))
	require.Equal(t, 5, relayPayloadBytes(MsgUDPReply, []byte("hello")))
	udpPayload := append([]byte{byte(len("1.1.1.1:53"))}, []byte("1.1.1.1:53")...)
	udpPayload = append(udpPayload, []byte("hello")...)
	require.Equal(t, 5, relayPayloadBytes(MsgUDP, udpPayload))
	require.Zero(t, relayPayloadBytes(MsgConnect, []byte("1.1.1.1:80")))
	require.Zero(t, relayPayloadBytes(MsgUDP, []byte{8, 'x'}))
}
