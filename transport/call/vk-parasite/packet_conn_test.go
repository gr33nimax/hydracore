package vkparasite

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Auth-фрейм читается из потока: его длина записана в самом фрейме, поэтому
// кадрирование поверх потока не нужно.
func TestReadAuthRequestFromStream(t *testing.T) {
	t.Parallel()
	request := authRequest{
		SessionID:      [16]byte{7},
		Conv:           0x11223344,
		WorkerID:       2,
		WorkerTotal:    4,
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           "tester",
		Password:       "secret",
	}
	frame, err := encodeAuthRequest(request)
	require.NoError(t, err)

	// Хвост после фрейма читаться не должен: сервер берёт ровно свою длину.
	stream := bytes.NewReader(append(append([]byte(nil), frame...), []byte("trailing")...))
	decoded, err := readAuthRequest(stream)
	require.NoError(t, err)
	require.Equal(t, request.SessionID, decoded.SessionID)
	require.Equal(t, request.Conv, decoded.Conv)
	require.Equal(t, request.WorkerID, decoded.WorkerID)
	require.Equal(t, request.User, decoded.User)
	require.Equal(t, request.Password, decoded.Password)
	require.Equal(t, len("trailing"), stream.Len(), "хвост обязан остаться непрочитанным")

	_, err = readAuthRequest(bytes.NewReader(frame[:20]))
	require.Error(t, err, "обрезанный заголовок обязан отказать")

	truncated := append([]byte(nil), frame...)
	_, err = readAuthRequest(bytes.NewReader(truncated[:len(truncated)-3]))
	require.Error(t, err, "обрезанный хвост обязан отказать")
}

// Обёртка на отправке своя на каждый удалённый адрес: RTP-поток описывает одного
// отправителя, и делить последовательность между пирами нельзя.
func TestServerObfsConnKeepsOneCodecPerRemote(t *testing.T) {
	t.Parallel()
	conn, err := newServerObfsConn(&recordingPacketConn{}, [wrapKeyLength]byte{5})
	require.NoError(t, err)

	first, err := conn.codecFor(testAddr("10.0.0.1:1"))
	require.NoError(t, err)
	again, err := conn.codecFor(testAddr("10.0.0.1:1"))
	require.NoError(t, err)
	other, err := conn.codecFor(testAddr("10.0.0.2:2"))
	require.NoError(t, err)

	require.Same(t, first, again, "один адрес обязан переиспользовать обёртку")
	require.NotSame(t, first, other)
	require.Equal(t, 2, conn.codecCount())

	// Сдвигаем отметки в прошлое, а не спим: разрешение часов на некоторых
	// платформах грубее, чем расстояние между двумя вызовами time.Now.
	conn.codecsMu.Lock()
	for _, entry := range conn.codecs {
		entry.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	}
	conn.codecsMu.Unlock()
	conn.reapCodecs(time.Minute)
	require.Zero(t, conn.codecCount(), "молчащие адреса обязаны выбрасываться")
}

type recordingPacketConn struct {
	inertPacketConn
	writes int
	last   []byte
}

func (c *recordingPacketConn) WriteTo(payload []byte, _ net.Addr) (int, error) {
	c.writes++
	c.last = append([]byte(nil), payload...)
	return len(payload), nil
}

type testAddr string

func (a testAddr) Network() string { return "udp" }
func (a testAddr) String() string  { return string(a) }

type inertPacketConn struct{}

func (inertPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, errors.New("not implemented")
}

func (inertPacketConn) WriteTo(payload []byte, _ net.Addr) (int, error) {
	return len(payload), nil
}

func (inertPacketConn) Close() error                     { return nil }
func (inertPacketConn) LocalAddr() net.Addr              { return testAddr("local") }
func (inertPacketConn) SetDeadline(time.Time) error      { return nil }
func (inertPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (inertPacketConn) SetWriteDeadline(time.Time) error { return nil }
