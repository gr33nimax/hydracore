package vkparasite

import (
	"context"
	"encoding/binary"

	calltunnel "github.com/sagernet/sing-box/transport/call/tunnel"
)

const flowControlPayloadSize = 14

type flowMigrationAckKey struct {
	connID   uint32
	sequence uint64
	laneID   uint16
}

func encodeFlowControlPayload(connID uint32, sequence uint64, laneID uint16) []byte {
	payload := make([]byte, flowControlPayloadSize)
	binary.BigEndian.PutUint32(payload[0:4], connID)
	binary.BigEndian.PutUint64(payload[4:12], sequence)
	binary.BigEndian.PutUint16(payload[12:14], laneID)
	return payload
}

func decodeFlowControlPayload(payload []byte) (uint32, uint64, uint16, bool) {
	if len(payload) != flowControlPayloadSize {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint32(payload[0:4]), binary.BigEndian.Uint64(payload[4:12]), binary.BigEndian.Uint16(payload[12:14]), true
}

func (t *ParasiteTunnel) sendFlowControl(msgType byte, connID uint32, sequence uint64, laneID uint16) bool {
	frame := calltunnel.EncodeFrame(calltunnel.ControlConnID, msgType, encodeFlowControlPayload(connID, sequence, laneID))
	return t.trySendControlData(frame)
}

func (t *ParasiteTunnel) sendFlowProgress(connID uint32, nextSequence uint64) {
	_ = t.sendFlowControl(calltunnel.MsgFlowProgress, connID, nextSequence, 0)
}

func (t *ParasiteTunnel) handleFlowControlMessage(frame []byte) bool {
	connID, msgType, ok := relayFrameIdentity(frame)
	if !ok || connID != calltunnel.ControlConnID || msgType < calltunnel.MsgFlowProgress || msgType > calltunnel.MsgFlowResume {
		return false
	}
	targetConnID, sequence, laneID, valid := decodeFlowControlPayload(frame[9:])
	if !valid || targetConnID == calltunnel.ControlConnID || laneID >= LaneCount && laneID != ^uint16(0) {
		return true
	}
	switch msgType {
	case calltunnel.MsgFlowProgress:
		t.trimFlowReplay(targetConnID, sequence)
	case calltunnel.MsgFlowFreeze, calltunnel.MsgFlowState:
		state := t.receiveFlows[targetConnID]
		if state == nil {
			t.receiveFlows[targetConnID] = &receiveFlowState{nextSequence: sequence, pending: make(map[uint64][]byte)}
		}
	case calltunnel.MsgFlowCommit:
		state := t.receiveFlows[targetConnID]
		if state == nil {
			state = &receiveFlowState{pending: make(map[uint64][]byte)}
			t.receiveFlows[targetConnID] = state
		}
		state.commitSequence = sequence
		state.commitLane = laneID
		state.commitPending = true
		t.ackFlowCommitIfReady(targetConnID, state)
	case calltunnel.MsgFlowCommitAck:
		key := flowMigrationAckKey{connID: targetConnID, sequence: sequence, laneID: laneID}
		if value, loaded := t.migrationAcks.LoadAndDelete(key); loaded {
			close(value.(chan struct{}))
		}
	case calltunnel.MsgFlowResume:
		// Resume is idempotent; sequence ordering has already been committed.
	}
	return true
}

func (t *ParasiteTunnel) ackFlowCommitIfReady(connID uint32, state *receiveFlowState) {
	if !state.commitPending || state.nextSequence < state.commitSequence {
		return
	}
	if t.sendFlowControl(calltunnel.MsgFlowCommitAck, connID, state.commitSequence, state.commitLane) {
		state.commitPending = false
	}
}

func (t *ParasiteTunnel) trimFlowReplay(connID uint32, nextSequence uint64) {
	t.sendMu.Lock()
	state := t.sendFlows[connID]
	t.sendMu.Unlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	trimmedBytes := 0
	index := 0
	for index < len(state.replay) && state.replay[index].sequence < nextSequence {
		trimmedBytes += len(state.replay[index].frame)
		state.replay[index] = flowReplayFrame{}
		index++
	}
	if index > 0 {
		state.replay = state.replay[index:]
		state.replayBytes -= trimmedBytes
		t.replayBytes.Add(-int64(trimmedBytes))
	}
	state.mu.Unlock()
}

func (t *ParasiteTunnel) migrateLaneFlows(laneID uint16, reason string) {
	t.sendMu.Lock()
	flows := make(map[uint32]*sendFlowState)
	for connID, state := range t.sendFlows {
		if state.laneOwner.Load() == uint32(laneID)+1 && !state.unordered {
			flows[connID] = state
		}
	}
	t.sendMu.Unlock()
	for connID, state := range flows {
		if !t.migrateOrderedFlow(connID, state, laneID, reason) && state.abortStarted.CompareAndSwap(false, true) {
			go t.finishOrderedFlowAbort(connID, "lane_flow_migration_failed")
		}
	}
}

func (t *ParasiteTunnel) migrateOrderedFlow(connID uint32, state *sendFlowState, sourceLane uint16, reason string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), t.sendStallTimeout)
	defer cancel()
	release, err := sharedTransportSupervisor.acquireMigration(ctx)
	if err != nil {
		return false
	}
	defer release()

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed || state.unordered || state.laneOwner.Load() != uint32(sourceLane)+1 {
		return true
	}
	target := t.migrationTarget(sourceLane)
	if target == nil {
		return false
	}
	progress := state.nextSequence
	if len(state.replay) > 0 {
		progress = state.replay[0].sequence
	}
	_ = t.sendFlowControl(calltunnel.MsgFlowFreeze, connID, progress, target.id)
	_ = t.sendFlowControl(calltunnel.MsgFlowState, connID, progress, target.id)
	state.laneID = target.id
	state.laneOwner.Store(uint32(target.id) + 1)
	bit := uint8(1 << target.id)
	if state.laneMask&bit == 0 {
		state.laneMask |= bit
		target.flowCount.Add(1)
	}
	for _, replay := range state.replay {
		encoded := encodeLaneFrameGeneration(0, connID, replay.sequence, replay.frame)
		if _, sent := t.sendEncoded(encoded, true, &target.id, true); !sent {
			return false
		}
	}
	key := flowMigrationAckKey{connID: connID, sequence: state.nextSequence, laneID: target.id}
	ack := make(chan struct{})
	t.migrationAcks.Store(key, ack)
	if !t.sendFlowControl(calltunnel.MsgFlowCommit, connID, state.nextSequence, target.id) {
		t.migrationAcks.Delete(key)
		return false
	}
	select {
	case <-ack:
	case <-ctx.Done():
		t.migrationAcks.Delete(key)
		return false
	}
	_ = t.sendFlowControl(calltunnel.MsgFlowResume, connID, state.nextSequence, target.id)
	t.recordEvent("lane_flow_migrated", "flow", reason, &target.id)
	return true
}

func (t *ParasiteTunnel) migrationTarget(sourceLane uint16) *kcpLane {
	var selected *kcpLane
	for _, lane := range t.lanes {
		if lane.id == sourceLane {
			continue
		}
		active, _ := lane.workerState()
		if !active {
			continue
		}
		lane.mu.Lock()
		healthy := lane.state == laneStateActive
		lane.mu.Unlock()
		if !healthy {
			continue
		}
		if selected == nil || lane.flowCount.Load() < selected.flowCount.Load() {
			selected = lane
		}
	}
	return selected
}
