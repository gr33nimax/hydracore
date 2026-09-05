package vkparasite

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDTLSClientHelloDetection(t *testing.T) {
	t.Parallel()
	packet := make([]byte, 59)
	packet[0] = 22
	packet[1] = 0xfe
	packet[2] = 0xfd
	binary.BigEndian.PutUint16(packet[11:13], uint16(len(packet)-13))
	packet[13] = 1
	for index := 27; index < 59; index++ {
		packet[index] = byte(index)
	}

	require.True(t, isDTLSClientHello(packet))
	identity, ok := dtlsClientHelloIdentity(packet)
	require.True(t, ok)
	require.Equal(t, packet[27:59], identity[:])
	packet[13] = 2
	require.False(t, isDTLSClientHello(packet))
	packet[13] = 1
	packet[3] = 1
	require.False(t, isDTLSClientHello(packet))
	require.False(t, isDTLSClientHello(packet[:13]))
}

func TestPeerPacketConnDistinguishesRetransmittedAndNewClientHello(t *testing.T) {
	t.Parallel()
	peer := &peerPacketConn{}
	first := [32]byte{1}
	second := [32]byte{2}

	require.False(t, peer.rememberClientHello(first))
	require.False(t, peer.rememberClientHello(first))
	require.True(t, peer.rememberClientHello(second))
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
