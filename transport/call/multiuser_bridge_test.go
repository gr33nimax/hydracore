package call

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/multiuser"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

type fakeManagedMultiUserClient struct {
	tunnel    *multiuser.PooledTunnel
	done      chan struct{}
	closeGate <-chan struct{}
	closeOnce sync.Once
	rebinds   atomic.Int32
}

func newFakeManagedMultiUserClient(t *testing.T, conv uint32) *fakeManagedMultiUserClient {
	t.Helper()
	dataTunnel, err := multiuser.NewPooledTunnel(conv, 1, logger.NOP())
	require.NoError(t, err)
	return &fakeManagedMultiUserClient{tunnel: dataTunnel, done: make(chan struct{})}
}

func (c *fakeManagedMultiUserClient) Tunnel() *multiuser.PooledTunnel { return c.tunnel }
func (c *fakeManagedMultiUserClient) Done() <-chan struct{}           { return c.done }
func (c *fakeManagedMultiUserClient) RebindNetwork()                  { c.rebinds.Add(1) }

func (c *fakeManagedMultiUserClient) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.closeGate != nil {
			<-c.closeGate
		}
		_ = c.tunnel.Close()
	})
	return nil
}

func TestMultiUserBridgeManagerSwapsClientAfterTerminalFailure(t *testing.T) {
	t.Parallel()
	initial := newFakeManagedMultiUserClient(t, 0x44556677)
	replacement := newFakeManagedMultiUserClient(t, 0x55667788)
	relay := tunnel.NewRelayBridge(initial.Tunnel(), "joiner", 32768, nil, logger.NOP())
	relay.MarkReady()
	ctx, cancel := context.WithCancel(context.Background())
	manager := newMultiUserBridgeManager(ctx, cancel, relay, func(context.Context) (managedMultiUserClient, error) {
		return replacement, nil
	}, initial, logger.NOP())
	t.Cleanup(func() {
		_ = manager.Close()
		relay.Close()
	})

	require.NoError(t, initial.Close())
	require.Eventually(t, func() bool {
		manager.clientMu.Lock()
		defer manager.clientMu.Unlock()
		return manager.client == replacement
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, manager.Close())
	select {
	case <-replacement.Done():
	default:
		t.Fatal("replacement client was not closed with the manager")
	}
}

func TestMultiUserBridgeManagerRebindsCurrentClient(t *testing.T) {
	t.Parallel()
	initial := newFakeManagedMultiUserClient(t, 0x44556678)
	relay := tunnel.NewRelayBridge(initial.Tunnel(), "joiner", 32768, nil, logger.NOP())
	relay.MarkReady()
	ctx, cancel := context.WithCancel(context.Background())
	manager := newMultiUserBridgeManager(ctx, cancel, relay, func(context.Context) (managedMultiUserClient, error) {
		t.Fatal("network rebind must not recreate the logical client")
		return nil, context.Canceled
	}, initial, logger.NOP())
	t.Cleanup(func() {
		_ = manager.Close()
		relay.Close()
	})

	manager.RebindNetwork()
	require.Equal(t, int32(1), initial.rebinds.Load())
}

func TestMultiUserBridgeManagerCloseCancelsReconnect(t *testing.T) {
	t.Parallel()
	initial := newFakeManagedMultiUserClient(t, 0x66778899)
	relay := tunnel.NewRelayBridge(initial.Tunnel(), "joiner", 32768, nil, logger.NOP())
	relay.MarkReady()
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	manager := newMultiUserBridgeManager(ctx, cancel, relay, func(connectCtx context.Context) (managedMultiUserClient, error) {
		close(started)
		<-connectCtx.Done()
		return nil, connectCtx.Err()
	}, initial, logger.NOP())
	t.Cleanup(relay.Close)

	require.NoError(t, initial.Close())
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not start reconnecting")
	}
	require.NoError(t, manager.Close())
	select {
	case <-manager.done:
	default:
		t.Fatal("manager reconnect loop did not stop")
	}
}

func TestMultiUserBridgeManagerWaitsForOldClientCleanupBeforeReconnect(t *testing.T) {
	t.Parallel()
	initial := newFakeManagedMultiUserClient(t, 0x778899aa)
	closeGate := make(chan struct{})
	var closeGateOnce sync.Once
	releaseCloseGate := func() { closeGateOnce.Do(func() { close(closeGate) }) }
	initial.closeGate = closeGate
	replacement := newFakeManagedMultiUserClient(t, 0x8899aabb)
	relay := tunnel.NewRelayBridge(initial.Tunnel(), "joiner", 32768, nil, logger.NOP())
	relay.MarkReady()
	connectCalled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	manager := newMultiUserBridgeManager(ctx, cancel, relay, func(context.Context) (managedMultiUserClient, error) {
		close(connectCalled)
		return replacement, nil
	}, initial, logger.NOP())
	t.Cleanup(func() {
		releaseCloseGate()
		_ = manager.Close()
		relay.Close()
	})

	closeReturned := make(chan struct{})
	go func() {
		_ = initial.Close()
		close(closeReturned)
	}()
	select {
	case <-initial.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("initial client did not enter terminal cleanup")
	}
	select {
	case <-connectCalled:
		t.Fatal("manager reconnected before old client cleanup completed")
	case <-time.After(50 * time.Millisecond):
	}
	releaseCloseGate()
	select {
	case <-closeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("initial client cleanup did not finish")
	}
	select {
	case <-connectCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not reconnect after old client cleanup")
	}
}
