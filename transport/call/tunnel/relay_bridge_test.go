package tunnel

import (
	"context"
	"encoding/binary"
	"io"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

type discardDataTunnel struct {
	onData  func([]byte)
	onClose func()
}

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
