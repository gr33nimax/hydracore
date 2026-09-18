package vkparasite

import (
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type receivedDatagram struct {
	destination string
	payload     []byte
}

// echoDatagrams поднимает на серверном relay обработчик, который отдаёт
// пришедшую датаграмму обратно и сообщает о ней в канал.
func echoDatagrams(relay *QUICRelay) <-chan receivedDatagram {
	inbound := make(chan receivedDatagram, 8)
	relay.SetUDPAcceptHandler(func(conn net.Conn, destination string) {
		buffer := make([]byte, maxDatagramPayload)
		for {
			n, err := conn.Read(buffer)
			if err != nil {
				return
			}
			inbound <- receivedDatagram{
				destination: destination,
				payload:     append([]byte(nil), buffer[:n]...),
			}
			if _, err = conn.Write(buffer[:n]); err != nil {
				return
			}
		}
	})
	return inbound
}

func TestDatagramRoundtrip(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 1)
	defer cleanup()

	inbound := echoDatagrams(serverRelay)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dest := M.ParseSocksaddr("1.1.1.1:53")
	packetConn, err := clientRelay.ListenPacket(ctx, dest.String())
	require.NoError(t, err)
	defer packetConn.Close()

	payload := []byte("dns-query-test-payload")
	n, err := packetConn.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	select {
	case received := <-inbound:
		require.Equal(t, dest.String(), received.destination)
		require.Equal(t, payload, received.payload)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the server to accept the association")
	}

	buffer := make([]byte, maxDatagramPayload)
	read, err := packetConn.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, payload, buffer[:read])
}

// TestDatagramFragmentedRoundtrip гоняет датаграмму, которая заведомо не влезает
// в один DATAGRAM-фрейм: внутренний QUIC приложений шлёт именно такие.
func TestDatagramFragmentedRoundtrip(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 1)
	defer cleanup()

	inbound := echoDatagrams(serverRelay)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dest := M.ParseSocksaddr("1.1.1.1:443")
	packetConn, err := clientRelay.ListenPacket(ctx, dest.String())
	require.NoError(t, err)
	defer packetConn.Close()

	payload := make([]byte, 4*maxDatagramFramePayload)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	n, err := packetConn.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	select {
	case received := <-inbound:
		require.Equal(t, payload, received.payload,
			"фрагменты обязаны собираться в исходную датаграмму")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the reassembled datagram")
	}

	buffer := make([]byte, maxDatagramPayload)
	read, err := packetConn.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, payload, buffer[:read])
}

// TestDatagramFragmentBudget фиксирует, что заголовок фрейма не съедает
// бюджет: иначе мелкая датаграмма поехала бы фрагментами.
func TestDatagramFragmentBudget(t *testing.T) {
	t.Parallel()
	assoc := newDatagramAssociation(1, newDatagramRouter(), nil, M.ParseSocksaddr("1.1.1.1:53"))
	prefix, err := assoc.framePrefix()
	require.NoError(t, err)
	budget := maxDatagramFramePayload - len(prefix) - datagramFragmentHeader
	require.GreaterOrEqual(t, budget, 1024,
		"на IPv4-назначении заголовок обязан оставлять больше килобайта")
}

func TestDatagramTooLargeRejected(t *testing.T) {
	clientRelay, serverRelay, cleanup := setupTestRelayPair(t, 1)
	defer cleanup()

	_ = serverRelay

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	packetConn, err := clientRelay.ListenPacket(ctx, "1.1.1.1:53")
	require.NoError(t, err)
	defer packetConn.Close()

	_, err = packetConn.Write(make([]byte, maxDatagramPayload+1))
	require.ErrorIs(t, err, errDatagramTooLarge)
}

func TestDatagramFrameHeaderRoundtrip(t *testing.T) {
	t.Parallel()
	assoc := newDatagramAssociation(7, newDatagramRouter(), nil, M.ParseSocksaddr("1.1.1.1:53"))
	prefix, err := assoc.framePrefix()
	require.NoError(t, err)

	packet := append([]byte(nil), prefix...)
	packet = append(packet, 3, 1)
	packet = append(packet, []byte("fragment")...)

	frame, err := parseDatagramFrame(packet)
	require.NoError(t, err)
	require.Equal(t, uint64(7), frame.associationID)
	require.Equal(t, "1.1.1.1:53", frame.destination.String())
	require.Equal(t, uint64(1), frame.packetID)
	require.Equal(t, uint8(3), frame.total)
	require.Equal(t, uint8(1), frame.index)
	require.Equal(t, []byte("fragment"), frame.fragment)
}

func TestDatagramFrameHeaderRejectsMalformed(t *testing.T) {
	t.Parallel()
	assoc := newDatagramAssociation(7, newDatagramRouter(), nil, M.ParseSocksaddr("1.1.1.1:53"))
	prefix, err := assoc.framePrefix()
	require.NoError(t, err)

	zeroTotal := append(append([]byte(nil), prefix...), 0, 0)
	_, err = parseDatagramFrame(zeroTotal)
	require.ErrorIs(t, err, errDatagramMalformed)

	indexOutOfRange := append(append([]byte(nil), prefix...), 2, 2)
	_, err = parseDatagramFrame(indexOutOfRange)
	require.ErrorIs(t, err, errDatagramMalformed)
}

// TestDatagramReassemblyDropsDuplicateFragment проверяет, что повтор фрагмента
// не завершает сборку раньше времени: датаграммы QUIC могут дублироваться.
func TestDatagramReassemblyDropsDuplicateFragment(t *testing.T) {
	t.Parallel()
	assoc := newDatagramAssociation(1, newDatagramRouter(), nil, M.ParseSocksaddr("1.1.1.1:53"))

	first := datagramFrame{packetID: 1, total: 2, index: 0, fragment: []byte("aaa")}
	assoc.deliverFragment(first)
	assoc.deliverFragment(first)
	require.Empty(t, assoc.recvQueue, "неполная датаграмма не должна доставляться")

	assoc.deliverFragment(datagramFrame{packetID: 1, total: 2, index: 1, fragment: []byte("bbb")})
	select {
	case payload := <-assoc.recvQueue:
		require.Equal(t, []byte("aaabbb"), payload)
	default:
		t.Fatal("собранная датаграмма не доставлена")
	}
}
