package vkparasite

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/transport/call/telemetry"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func TestWorkerJoinLinkDistribution(t *testing.T) {
	t.Parallel()

	t.Run("FourDistinctLinks", func(t *testing.T) {
		links := []string{
			"https://vk.com/call/join/call-0",
			"https://vk.com/call/join/call-1",
			"https://vk.com/call/join/call-2",
			"https://vk.com/call/join/call-3",
		}
		for workerID := uint16(0); workerID < LaneCount; workerID++ {
			expectedCall := int(workerID) % CallCount
			require.Equal(t, links[expectedCall], workerJoinLink(links, workerID))
		}
	})

	t.Run("TwoLinksFallback", func(t *testing.T) {
		links := []string{
			"https://vk.com/call/join/call-0",
			"https://vk.com/call/join/call-1",
		}
		for workerID := uint16(0); workerID < LaneCount; workerID++ {
			expectedCall := (int(workerID) % CallCount) % len(links)
			require.Equal(t, links[expectedCall], workerJoinLink(links, workerID))
		}
	})

	t.Run("ThreeLinksFallback", func(t *testing.T) {
		links := []string{
			"https://vk.com/call/join/call-0",
			"https://vk.com/call/join/call-1",
			"https://vk.com/call/join/call-2",
		}
		for workerID := uint16(0); workerID < LaneCount; workerID++ {
			expectedCall := (int(workerID) % CallCount) % len(links)
			require.Equal(t, links[expectedCall], workerJoinLink(links, workerID))
		}
	})

	t.Run("SingleLinkFallback", func(t *testing.T) {
		links := []string{"https://vk.com/call/join/call-0"}
		for workerID := uint16(0); workerID < LaneCount; workerID++ {
			require.Equal(t, links[0], workerJoinLink(links, workerID))
		}
	})
}

func TestServerDualTopologyPolicy(t *testing.T) {
	t.Parallel()

	require.True(t, supportedSessionLaneCount(LegacyLaneCount))
	require.True(t, supportedSessionLaneCount(LaneCount))
	require.False(t, supportedSessionLaneCount(0))
	require.False(t, supportedSessionLaneCount(2))
	require.False(t, supportedSessionLaneCount(8))
	require.False(t, supportedSessionLaneCount(32))

	server, err := NewServer(context.Background(), ServerOptions{
		ObfsPassword: "outer-secret",
		Users: []ServerUser{
			{Name: "alice", Password: "secret"},
			{Name: "bob", Password: "secret"},
		},
		MaxSessions: 2,
		SessionHandler: func(SessionInfo, *ParasiteTunnel) error { return nil },
	}, logger.NOP())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	// Legacy 4-lane session
	req4 := authRequest{
		SessionID:      [16]byte{1},
		Conv:           100,
		WorkerID:       0,
		WorkerTotal:    LegacyLaneCount,
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           "bob",
		Password:       "secret",
	}
	session4, created4, err := server.getOrCreateSession(req4)
	require.NoError(t, err)
	require.True(t, created4)
	require.Equal(t, LegacyLaneCount, int(session4.tunnel.laneCount()))
	server.releaseSessionAttach(session4)

	// Modern 16-lane session (4x4)
	req16 := authRequest{
		SessionID:      [16]byte{2},
		Conv:           200,
		WorkerID:       0,
		WorkerTotal:    LaneCount,
		WorkerEpoch:    1,
		LaneGeneration: 1,
		User:           "alice",
		Password:       "secret",
	}
	session16, created16, err := server.getOrCreateSession(req16)
	require.NoError(t, err)
	require.True(t, created16)
	require.Equal(t, LaneCount, int(session16.tunnel.laneCount()))
	server.releaseSessionAttach(session16)
	server.sessionsMu.Lock()
	require.Len(t, server.sessions, 2)
	server.sessionsMu.Unlock()

	mismatch := req4
	mismatch.WorkerTotal = LaneCount
	_, _, err = server.getOrCreateSession(mismatch)
	require.ErrorContains(t, err, "session identity mismatch")
}

func TestAuthRequestStructuralValidation(t *testing.T) {
	t.Parallel()

	var validReq authRequest
	for _, workerTotal := range []uint16{LegacyLaneCount, LaneCount} {
		validReq = authRequest{
			SessionID:      [16]byte{1},
			Conv:           1,
			WorkerID:       workerTotal - 1,
			WorkerTotal:    workerTotal,
			WorkerEpoch:    1,
			LaneGeneration: 1,
			User:           "alice",
			Password:       "secret",
		}
		encoded, err := encodeAuthRequest(validReq)
		require.NoError(t, err)
		decoded, err := decodeAuthRequest(encoded)
		require.NoError(t, err)
		require.Equal(t, validReq, decoded)
	}

	// WorkerTotal > MaximumLaneCount (32)
	invalidReq := validReq
	invalidReq.WorkerTotal = MaximumLaneCount + 1
	_, err := encodeAuthRequest(invalidReq)
	require.Error(t, err)

	// WorkerID >= WorkerTotal
	invalidReq = validReq
	invalidReq.WorkerID = invalidReq.WorkerTotal
	_, err = encodeAuthRequest(invalidReq)
	require.Error(t, err)

	// WorkerTotal == 0
	invalidReq = validReq
	invalidReq.WorkerTotal = 0
	_, err = encodeAuthRequest(invalidReq)
	require.Error(t, err)

	// Conv == 0
	invalidReq = validReq
	invalidReq.Conv = 0
	_, err = encodeAuthRequest(invalidReq)
	require.Error(t, err)
}

func TestQuarantineQuorumThresholds(t *testing.T) {
	t.Parallel()

	for _, laneCount := range []int{LegacyLaneCount, LaneCount} {
		tunnel, err := newParasiteTunnel(uint32(laneCount), uint16(laneCount), logger.NOP(), nil)
		require.NoError(t, err)
		quorum := (3*laneCount + 3) / 4
		for laneID := 0; laneID < quorum-1; laneID++ {
			tunnel.lanes[laneID].state = laneStateQuarantined
		}
		select {
		case <-tunnel.Done():
			t.Fatalf("%d-lane session closed before quorum", laneCount)
		default:
		}
		tunnel.handleLaneResetControl(laneResetPrepare, uint16(quorum-1), 2)
		select {
		case <-tunnel.Done():
		case <-time.After(time.Second):
			t.Fatalf("%d-lane session stayed open at quorum %d", laneCount, quorum)
		}
	}
}

func TestWorkerTelemetrySnapshotGauges(t *testing.T) {
	t.Parallel()

	tunnel, err := newParasiteTunnel(3, LaneCount, logger.NOP(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tunnel.Close() })

	lane15 := tunnel.lanes[15]
	lane15.mu.Lock()
	lane15.kcpSRTTMS = 75.5
	lane15.kcpRTTVARMS = 12.0
	require.GreaterOrEqual(t, lane15.kcp.Send([]byte("pending")), 0)
	lane15.kcpSent[1] = kcpSentSegment{}
	lane15.outputPending = append(lane15.outputPending, queuedSegment{})
	lane15.admissionWindow = 128
	lane15.pacingRateBPS = 8_000_000
	lane15.deliveryRateBPS = 7_500_000
	lane15.generation = 42
	lane15.state = laneStateActive
	lane15.mu.Unlock()
	lane15.flowCount.Store(5)

	snapshots := tunnel.telemetryWorkerSnapshots(telemetry.TunnelMetrics)
	require.Len(t, snapshots, int(LaneCount))

	snap15 := snapshots[15]
	require.Equal(t, uint16(15), snap15.id)
	require.Equal(t, 75.5, snap15.metrics["kcp_rtt_ms"])
	require.Equal(t, 123.5, snap15.metrics["kcp_rto_ms"])
	require.Equal(t, float64(12), snap15.metrics["kcp_rttvar_ms"])
	require.Equal(t, float64(1), snap15.metrics["kcp_wait_snd"])
	require.Equal(t, float64(1), snap15.metrics["kcp_inflight_segments"])
	require.Equal(t, float64(laneKCPSendWindow), snap15.metrics["kcp_send_window_segments"])
	require.Equal(t, float64(laneKCPReceiveWindow), snap15.metrics["kcp_receive_window_segments"])
	require.Equal(t, float64(laneKCPMaximumAdmission), snap15.metrics["kcp_max_pending_segments"])
	require.Equal(t, float64(1), snap15.metrics["kcp_output_queue_depth"])
	require.Equal(t, float64(rtpPayloadTypeVideo), snap15.metrics["outer_rtp_payload_type"])
	require.Equal(t, float64(128), snap15.metrics["lane_admission_window_segments"])
	require.Equal(t, float64(8_000_000), snap15.metrics["lane_admission_rate_bps"])
	require.Equal(t, float64(7_500_000), snap15.metrics["lane_delivered_rate_bps"])
	require.Equal(t, float64(42), snap15.metrics["lane_generation"])
	require.Equal(t, float64(laneStateActive), snap15.metrics["lane_state"])
	require.Equal(t, float64(5), snap15.metrics["lane_flow_count"])
	require.Equal(t, float64(1), snap15.metrics["lane_count"])
}
