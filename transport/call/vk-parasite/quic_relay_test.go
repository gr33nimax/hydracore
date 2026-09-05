package vkparasite

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	"github.com/sagernet/quic-go"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func setupTestRelayPair(t *testing.T, pathCount int) (clientRelay *QUICRelay, serverRelay *QUICRelay, closer func()) {
	var key [wrapKeyLength]byte
	_, _ = rand.Read(key[:])

	cert, err := selfsign.GenerateSelfSigned()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	serverRelay = NewQUICRelay(ctx, QUICRelayOptions{
		PathCount: pathCount,
	})

	dialPath := func(dialCtx context.Context, workerID uint16) (*quic.Conn, io.Closer, error) {
		pair, pErr := newTestPacketConnPair()
		if pErr != nil {
			return nil, nil, pErr
		}
		cCodec, _ := newRTPCodec(key)
		sCodec, _ := newRTPCodec(key)

		cObfs := newObfsPacketConn(pair.client, pair.server.LocalAddr(), cCodec)
		sObfs := newObfsPacketConn(pair.server, pair.client.LocalAddr(), sCodec)

		sDTLSConfig := &dtls.Config{
			Certificates:         []tls.Certificate{cert},
			ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
			FlightInterval:       100 * time.Millisecond,
			MTU:                  dtlsMTU,
		}
		cDTLSConfig := &dtls.Config{
			InsecureSkipVerify:   true,
			ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
			FlightInterval:       100 * time.Millisecond,
			MTU:                  dtlsMTU,
		}

		sConnCh := make(chan net.Conn, 1)
		go func() {
			sConn, sErr := dtls.Server(sObfs, pair.client.LocalAddr(), sDTLSConfig)
			if sErr == nil {
				_ = sConn.HandshakeContext(dialCtx)
			}
			sConnCh <- sConn
		}()

		cConn, cErr := dtls.Client(cObfs, pair.server.LocalAddr(), cDTLSConfig)
		if cErr != nil {
			_ = cObfs.Close()
			_ = sObfs.Close()
			return nil, nil, cErr
		}
		if hsErr := cConn.HandshakeContext(dialCtx); hsErr != nil {
			_ = cConn.Close()
			_ = cObfs.Close()
			_ = sObfs.Close()
			return nil, nil, hsErr
		}

		sConn := <-sConnCh
		sQL, qlErr := listenQUIC(sConn, cert)
		if qlErr != nil {
			_ = cConn.Close()
			_ = sConn.Close()
			return nil, nil, qlErr
		}

		sQConnCh := make(chan *quic.Conn, 1)
		go func() {
			sQC, _ := sQL.Accept(dialCtx)
			sQConnCh <- sQC
		}()

		cQConn, qErr := dialQUIC(dialCtx, cConn, nil)
		if qErr != nil {
			_ = cConn.Close()
			_ = sConn.Close()
			return nil, nil, qErr
		}

		sQConn := <-sQConnCh
		if sQConn != nil {
			serverRelay.AttachServerConn(sQConn, sConn)
		}

		return cQConn, cConn, nil
	}

	clientRelay = NewQUICRelay(ctx, QUICRelayOptions{
		PathCount: pathCount,
		DialPath:  dialPath,
	})
	clientRelay.Start()
	require.Eventually(t, func() bool { return clientRelay.ActivePaths() > 0 }, 10*time.Second, 10*time.Millisecond)

	closer = func() {
		clientRelay.Close()
		serverRelay.Close()
		cancel()
	}

	return clientRelay, serverRelay, closer
}

func TestDialContextCarriesDestination(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 2)
	defer cleanup()

	targetDest := "example.com:443"
	receivedDestCh := make(chan string, 1)

	serverRelay.SetAcceptHandler(func(conn net.Conn, destination string) {
		defer conn.Close()
		receivedDestCh <- destination
		_, _ = conn.Write([]byte("pong"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := clientRelay.DialContext(ctx, targetDest)
	require.NoError(t, err)
	defer conn.Close()

	var received string
	select {
	case received = <-receivedDestCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server accept")
	}

	require.Equal(t, targetDest, received)

	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "pong", string(buf))
}

func TestDialContextWithoutActivePathsFailsImmediately(t *testing.T) {
	pathStarted := make(chan struct{})
	relay := NewQUICRelay(t.Context(), QUICRelayOptions{
		PathCount: 1,
		DialPath: func(ctx context.Context, _ uint16) (*quic.Conn, io.Closer, error) {
			close(pathStarted)
			<-ctx.Done()
			return nil, nil, ctx.Err()
		},
	})
	defer relay.Close()
	relay.Start()
	<-pathStarted

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := relay.DialContext(ctx, "example.com:53")

	require.ErrorIs(t, err, ErrNoActiveQUICPaths)
	require.Less(t, time.Since(started), 100*time.Millisecond)
}

func TestConcurrentDials(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 4)
	defer cleanup()

	serverRelay.SetAcceptHandler(func(conn net.Conn, destination string) {
		defer conn.Close()
		buf := make([]byte, 8)
		if _, err := io.ReadFull(conn, buf); err == nil {
			_, _ = conn.Write(buf)
		}
	})

	const concurrentCount = 100
	var wg sync.WaitGroup
	wg.Add(concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			dest := M.ParseSocksaddr("target.local:8080").String()
			conn, err := clientRelay.DialContext(ctx, dest)
			if err != nil {
				t.Errorf("dial %d failed: %v", idx, err)
				return
			}
			defer conn.Close()

			msg := []byte("pingping")
			if _, err := conn.Write(msg); err != nil {
				t.Errorf("write %d failed: %v", idx, err)
				return
			}
			recv := make([]byte, len(msg))
			if _, err := io.ReadFull(conn, recv); err != nil {
				t.Errorf("read %d failed: %v", idx, err)
				return
			}
		}(i)
	}

	wg.Wait()
}

func TestRebindNetworkReplacesPaths(t *testing.T) {
	clientRelay, _, cleanup := setupTestRelayPair(t, 1)
	defer cleanup()

	require.Eventually(t, func() bool { return clientRelay.ActivePaths() == 1 }, 10*time.Second, 10*time.Millisecond)
	clientRelay.pathsMu.RLock()
	oldPath := clientRelay.paths[0]
	clientRelay.pathsMu.RUnlock()

	clientRelay.RebindNetwork()
	require.Eventually(t, func() bool {
		clientRelay.pathsMu.RLock()
		defer clientRelay.pathsMu.RUnlock()
		return len(clientRelay.paths) == 1 && clientRelay.paths[0] != oldPath
	}, 10*time.Second, 10*time.Millisecond)
}

func TestRebindNetworkGeneration(t *testing.T) {
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	relay := NewQUICRelay(t.Context(), QUICRelayOptions{
		PathCount: 1,
		DialPath: func(ctx context.Context, _ uint16) (*quic.Conn, io.Closer, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			cancelled <- struct{}{}
			return nil, nil, ctx.Err()
		},
	})
	defer relay.Close()
	relay.Start()
	<-started

	relay.RebindNetwork(1)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("generation rebind did not cancel an in-flight path")
	}

	relay.pathsMu.RLock()
	generationCtx := relay.generationCtx
	relay.pathsMu.RUnlock()
	relay.RebindNetwork(1)
	relay.pathsMu.RLock()
	require.Same(t, generationCtx, relay.generationCtx)
	relay.pathsMu.RUnlock()

	relay.RebindNetwork(0)
	relay.pathsMu.RLock()
	require.NotSame(t, generationCtx, relay.generationCtx)
	relay.pathsMu.RUnlock()
}
