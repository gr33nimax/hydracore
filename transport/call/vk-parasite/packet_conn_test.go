package vkparasite

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuthFrameDetection(t *testing.T) {
	t.Parallel()
	frame, err := encodeAuthRequest(authRequest{
		SessionID:      [16]byte{7},
		Conv:           0x11223344,
		WorkerID:       2,
		WorkerTotal:    4,
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           "tester",
		Password:       "secret",
	})
	require.NoError(t, err)

	require.True(t, isAuthFrame(frame))
	identity, ok := authAttachIdentity(frame)
	require.True(t, ok)
	require.Equal(t, frame[5:21], identity[0:16], "идентичность обязана нести session id")
	require.Equal(t, frame[21:25], identity[16:20], "идентичность обязана нести conv")
	require.Equal(t, frame[25:27], identity[20:22], "идентичность обязана нести worker id")

	// Тот же worker с другой сессией — другая попытка.
	other, err := encodeAuthRequest(authRequest{
		SessionID:      [16]byte{8},
		Conv:           0x11223344,
		WorkerID:       2,
		WorkerTotal:    4,
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           "tester",
		Password:       "secret",
	})
	require.NoError(t, err)
	otherIdentity, ok := authAttachIdentity(other)
	require.True(t, ok)
	require.NotEqual(t, identity, otherIdentity)

	// Ни QUIC-пакет, ни обрезок auth-фреймом не считаются.
	require.False(t, isAuthFrame(frame[:47]))
	wrongVersion := append([]byte(nil), frame...)
	wrongVersion[4] = authProtocolVersion - 1
	require.False(t, isAuthFrame(wrongVersion))
	wrongMagic := append([]byte(nil), frame...)
	wrongMagic[0] = 'X'
	require.False(t, isAuthFrame(wrongMagic))
}

func TestPeerPacketConnDistinguishesRetransmittedAndNewAttach(t *testing.T) {
	t.Parallel()
	peer := &peerPacketConn{}
	first := [32]byte{1}
	second := [32]byte{2}

	require.False(t, peer.rememberAttach(first))
	require.False(t, peer.rememberAttach(first))
	require.True(t, peer.rememberAttach(second))
}

// Повтор auth-фрейма должен получить тот же ack: DTLS, который раньше
// переспрашивал потерянное сам, снят.
func TestPeerPacketConnReplaysStoredAuthAck(t *testing.T) {
	t.Parallel()
	codec, err := newRTPCodec([wrapKeyLength]byte{9})
	require.NoError(t, err)
	sink := &recordingPacketConn{}
	peer := newPeerPacketConn(sink, testAddr("peer"), codec, 4)

	require.False(t, peer.replayAuthAck(), "без запомненного ответа переотправлять нечего")

	ack := encodeAuthAck(true, 42, AuthRejectUnspecified)
	peer.storeAuthAck(ack)
	require.True(t, peer.replayAuthAck())
	require.Equal(t, 1, sink.writes)

	plain, err := codec.unwrap(nil, sink.last)
	require.NoError(t, err)
	require.Equal(t, ack, plain, "переотправка обязана нести тот же ack")
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

func TestPeerPacketConnAppliesDeadlineChangedAfterReadStarted(t *testing.T) {
	t.Parallel()
	key, err := deriveWrapKey("outer-secret")
	require.NoError(t, err)
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	peer := newPeerPacketConn(inertPacketConn{}, testAddr("remote"), codec, 16)
	t.Cleanup(func() { _ = peer.Close() })
	require.False(t, peer.isEstablished())
	peer.markEstablished()
	require.True(t, peer.isEstablished())

	result := make(chan error, 1)
	go func() {
		_, _, readErr := peer.ReadFrom(make([]byte, 1500))
		result <- readErr
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	select {
	case readErr := <-result:
		require.Error(t, readErr)
		timeout, ok := readErr.(net.Error)
		require.True(t, ok)
		require.True(t, timeout.Timeout())
	case <-time.After(time.Second):
		t.Fatal("read did not observe a newly installed deadline")
	}
}

func TestPeerPacketConnIgnoresSupersededDeadlineTimer(t *testing.T) {
	t.Parallel()
	key, err := deriveWrapKey("outer-secret")
	require.NoError(t, err)
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	peer := newPeerPacketConn(inertPacketConn{}, testAddr("remote"), codec, 16)
	t.Cleanup(func() { _ = peer.Close() })

	type readResult struct {
		payload string
		err     error
	}
	result := make(chan readResult, 1)
	go func() {
		buffer := make([]byte, 64)
		n, _, readErr := peer.ReadFrom(buffer)
		result <- readResult{payload: string(buffer[:n]), err: readErr}
	}()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(30*time.Millisecond)))
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	time.Sleep(50 * time.Millisecond)
	require.True(t, peer.enqueue([]byte("still-open"), testAddr("remote")))
	select {
	case read := <-result:
		require.NoError(t, read.err)
		require.Equal(t, "still-open", read.payload)
	case <-time.After(time.Second):
		t.Fatal("read did not survive a superseded deadline")
	}
}

// Буфер из пула обязан вернуться в пул после чтения, а пакет крупнее пула —
// доехать целым и в пул не попасть.
func TestPacketCopyOwnership(t *testing.T) {
	t.Parallel()
	small := make([]byte, 1200)
	for index := range small {
		small[index] = byte(index)
	}
	payload, owner := takePacketCopy(small)
	require.NotNil(t, owner, "пакет по path MTU обязан приходить из пула")
	require.Equal(t, small, payload)
	releasePacketCopy(owner)

	large := make([]byte, maxCodecWireBuffer+1)
	payload, owner = takePacketCopy(large)
	require.Nil(t, owner, "пакет больше пула не должен в него возвращаться")
	require.Equal(t, large, payload)
	releasePacketCopy(owner)
}

// Полная очередь не должна терять буфер: иначе пул течёт при каждом дропе.
func TestPeerPacketConnEnqueueReleasesOnFullQueue(t *testing.T) {
	t.Parallel()
	codec, err := newRTPCodec([wrapKeyLength]byte{3})
	require.NoError(t, err)
	peer := newPeerPacketConn(inertPacketConn{}, testAddr("peer"), codec, 1)

	require.True(t, peer.enqueue([]byte("first"), testAddr("peer")))
	require.False(t, peer.enqueue([]byte("second"), testAddr("peer")))

	buffer := make([]byte, 16)
	n, _, err := peer.ReadFrom(buffer)
	require.NoError(t, err)
	require.Equal(t, "first", string(buffer[:n]))
}
