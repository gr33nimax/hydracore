package vkparasite

import (
	"testing"
	"time"

	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func createBenchmarkTunnelPair(b *testing.B) (*ParasiteTunnel, *ParasiteTunnel) {
	b.Helper()
	left, err := NewParasiteTunnel(0x11223344, logger.NOP())
	require.NoError(b, err)
	right, err := NewParasiteTunnel(0x11223344, logger.NOP())
	require.NoError(b, err)
	connectTestLanes(b, left, right)
	b.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	return left, right
}

func BenchmarkKCPUpdate(b *testing.B) {
	left, _ := createBenchmarkTunnelPair(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lane := left.lanes[i%len(left.lanes)]
		lane.mu.Lock()
		if len(lane.outputPending) < laneKCPOutputBacklog {
			lane.kcp.Update()
		}
		lane.mu.Unlock()
	}
}

func BenchmarkParasiteTunnelThroughput(b *testing.B) {
	left, right := createBenchmarkTunnelPair(b)
	received := make(chan []byte, 1024)
	right.SetOnData(func(data []byte) {
		select {
		case received <- data:
		default:
		}
	})

	payload := make([]byte, 1024)
	frame := calltunnel.EncodeFrame(1, calltunnel.MsgData, payload)
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		left.SendData(frame)
		select {
		case <-received:
		case <-time.After(100 * time.Millisecond):
		}
	}
}
