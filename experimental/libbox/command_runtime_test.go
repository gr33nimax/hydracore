package libbox

import (
	"testing"

	"github.com/sagernet/sing-box/daemon"
)

func TestRuntimeSnapshotFromGRPC(t *testing.T) {
	t.Parallel()
	snapshot := runtimeSnapshotFromGRPC(&daemon.RuntimeSnapshot{
		SchemaVersion: 1,
		Sequence:      7,
		ObservedAt:    100,
		StartedAt:     50,
		Service:       &daemon.ServiceStatus{Status: daemon.ServiceStatus_STARTED},
		Status:        &daemon.Status{UplinkTotal: 12, DownlinkTotal: 34},
		Groups: &daemon.Groups{Group: []*daemon.Group{{
			Tag:      "auto",
			Selected: "proxy",
			Items:    []*daemon.GroupItem{{Tag: "proxy", Type: "vless"}},
		}}},
		ClashMode: &daemon.ClashModeStatus{ModeList: []string{"rule"}, CurrentMode: "rule"},
		UrlTestSessions: []*daemon.URLTestSession{{
			Id:        "urltest-1",
			GroupTag:  "auto",
			State:     daemon.URLTestSessionState_URL_TEST_SESSION_SUCCEEDED,
			StartedAt: 60,
			Completed: 1,
			Succeeded: 1,
			Results: []*daemon.URLTestResult{{
				OutboundTag: "proxy",
				DelayMillis: 42,
				Status:      "available",
			}},
		}},
	})
	if snapshot == nil || snapshot.SchemaVersion != 1 || snapshot.Sequence != 7 || snapshot.Service.Status != int32(daemon.ServiceStatus_STARTED) {
		t.Fatalf("unexpected snapshot conversion: %+v", snapshot)
	}
	groups := snapshot.Groups()
	if !groups.HasNext() || groups.Next().Tag != "auto" || groups.HasNext() {
		t.Fatal("runtime groups were not converted")
	}
	sessions := snapshot.URLTestSessions()
	if !sessions.HasNext() {
		t.Fatal("runtime URL test sessions were not converted")
	}
	session := sessions.Next()
	if session.State != URLTestSessionSucceeded || session.Results().Next().DelayMillis != 42 {
		t.Fatalf("unexpected URL test conversion: %+v", session)
	}
	if snapshot.ClashMode.ModeList().Next() != "rule" {
		t.Fatal("clash mode list was not converted")
	}
}

func TestRuntimeEventsFromGRPCPreservesResetAndTypedDelta(t *testing.T) {
	t.Parallel()
	events := runtimeEventsFromGRPC(&daemon.RuntimeEvents{
		Sequence: 9,
		Reset_:   true,
		Snapshot: &daemon.RuntimeSnapshot{SchemaVersion: 1},
		Events: []*daemon.RuntimeEvent{{
			Type:   daemon.RuntimeEventType_RUNTIME_EVENT_STATUS,
			Status: &daemon.Status{Goroutines: 3},
		}},
	})
	if events == nil || !events.Reset || events.Sequence != 9 || events.Snapshot == nil {
		t.Fatalf("unexpected event envelope: %+v", events)
	}
	iterator := events.Events()
	if !iterator.HasNext() {
		t.Fatal("missing typed runtime delta")
	}
	event := iterator.Next()
	if event.Type != RuntimeEventStatus || event.Status.Goroutines != 3 {
		t.Fatalf("unexpected typed runtime delta: %+v", event)
	}
}
