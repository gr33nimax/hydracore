package telemetry

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOuterSequenceWindowSeparatesLossReorderingAndDuplicates(t *testing.T) {
	t.Parallel()
	metrics := NewAccumulator()
	metrics.SetCollectionActive(true)
	started := time.Now()
	metrics.ObserveOuterPacket(testRTPPacket(10), started)
	metrics.ObserveOuterPacket(testRTPPacket(12), started.Add(20*time.Millisecond))
	require.InDelta(t, 1.0/3.0, metrics.Value(NetworkLossRatio), 0.0001)
	metrics.ObserveOuterPacket(testRTPPacket(11), started.Add(25*time.Millisecond))
	require.Zero(t, metrics.Value(NetworkLossRatio))
	require.Equal(t, float64(1), metrics.Value(OuterReorderedPacketsTotal))
	metrics.ObserveOuterPacket(testRTPPacket(11), started.Add(30*time.Millisecond))
	require.Equal(t, float64(1), metrics.Value(OuterDuplicatePacketsTotal))
	require.Equal(t, float64(1), metrics.Value(OuterReorderedPacketsTotal))
}

func testRTPPacket(sequence uint16) []byte {
	packet := make([]byte, 12)
	packet[0] = 0x80
	packet[1] = 111
	binary.BigEndian.PutUint16(packet[2:4], sequence)
	binary.BigEndian.PutUint32(packet[8:12], 0x11223344)
	return packet
}
