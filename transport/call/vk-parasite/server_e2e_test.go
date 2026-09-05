package vkparasite

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/stretchr/testify/require"
)

// dialWorker поднимает то, что делает клиент: TURN-путь заменён прямым UDP,
// остальное настоящее — RTP-обёртка, QUIC поверх неё и auth на первом потоке.
func dialWorker(
	t *testing.T,
	ctx context.Context,
	serverAddr net.Addr,
	key [wrapKeyLength]byte,
	request authRequest,
) (*quic.Conn, uint64, error) {
	t.Helper()
	clientUDP, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	packetConn := newObfsPacketConn(clientUDP, serverAddr, codec)

	quicConn, err := dialQUIC(ctx, packetConn, serverAddr, packetConn)
	if err != nil {
		_ = packetConn.Close()
		return nil, 0, err
	}
	frame, err := encodeAuthRequest(request)
	require.NoError(t, err)
	generation, err := exchangeAuth(ctx, quicConn, frame, 10*time.Second)
	if err != nil {
		_ = quicConn.CloseWithError(0, "")
		return nil, 0, err
	}
	return quicConn, generation, nil
}

func startTestServer(t *testing.T, attached *atomic.Int32) (*Server, net.Addr, [wrapKeyLength]byte) {
	t.Helper()
	const obfsPassword = "outer-secret"
	key, err := deriveWrapKey(obfsPassword)
	require.NoError(t, err)

	serverUDP, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword:         obfsPassword,
		Users:                []ServerUser{{Name: "tester", Password: "per-user-secret"}},
		MaxWorkersPerSession: DefaultWorkerCount,
		SessionHandler: func(SessionInfo, *QUICRelay) error {
			attached.Add(1)
			return nil
		},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, server.Start(serverUDP))
	t.Cleanup(func() { _ = server.Close() })
	return server, serverUDP.LocalAddr(), key
}

func validAuthRequest(worker uint16) authRequest {
	return authRequest{
		SessionID:      [16]byte{1, 2, 3},
		Conv:           0x0a0b0c0d,
		WorkerID:       worker,
		WorkerTotal:    DefaultWorkerCount,
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           "tester",
		Password:       "per-user-secret",
	}
}

// TestServerAttachesWorkersOverSharedListener — главный тест новой формы
// сервера: четыре worker'а приходят на один UDP-порт, quic-go разбирает их по
// connection ID, и каждый доходит до сессии.
func TestServerAttachesWorkersOverSharedListener(t *testing.T) {
	var attached atomic.Int32
	server, serverAddr, key := startTestServer(t, &attached)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var generation uint64
	conns := make([]*quic.Conn, 0, DefaultWorkerCount)
	for worker := range uint16(DefaultWorkerCount) {
		conn, workerGeneration, err := dialWorker(t, ctx, serverAddr, key, validAuthRequest(worker))
		require.NoError(t, err, "worker %d", worker)
		require.NotZero(t, workerGeneration)
		if generation == 0 {
			generation = workerGeneration
		}
		require.Equal(t, generation, workerGeneration, "все worker'ы одной сессии обязаны видеть одну generation")
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.CloseWithError(0, "")
		}
	}()

	require.EqualValues(t, 1, attached.Load(), "сессия обязана подняться ровно один раз")

	require.Eventually(t, func() bool {
		server.sessionsMu.Lock()
		defer server.sessionsMu.Unlock()
		for _, session := range server.sessions {
			if session.relay != nil && session.relay.ActivePaths() == DefaultWorkerCount {
				return true
			}
		}
		return false
	}, 15*time.Second, 20*time.Millisecond, "все четыре пути обязаны прикрепиться к relay")

	// Одна обёртка отправки на адрес: четыре worker'а — четыре адреса.
	require.Equal(t, DefaultWorkerCount, server.obfsConn.codecCount())
}

// Неверный пароль обязан получить отказ, а не таймаут.
func TestServerRefusesWrongPassword(t *testing.T) {
	var attached atomic.Int32
	_, serverAddr, key := startTestServer(t, &attached)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request := validAuthRequest(0)
	request.Password = "wrong-secret"
	conn, _, err := dialWorker(t, ctx, serverAddr, key, request)
	if conn != nil {
		_ = conn.CloseWithError(0, "")
	}
	require.ErrorIs(t, err, errAuthRejected)
	require.Zero(t, attached.Load())
}

// Пакет, зашифрованный другим внешним ключом, не должен доходить до QUIC вовсе.
func TestServerIgnoresForeignOuterKey(t *testing.T) {
	var attached atomic.Int32
	_, serverAddr, _ := startTestServer(t, &attached)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	foreignKey, err := deriveWrapKey("not-the-outer-secret")
	require.NoError(t, err)
	conn, _, err := dialWorker(t, ctx, serverAddr, foreignKey, validAuthRequest(0))
	if conn != nil {
		_ = conn.CloseWithError(0, "")
	}
	require.Error(t, err, "чужой ключ обёртки обязан привести к отказу дозвона")
	require.Zero(t, attached.Load())
}
