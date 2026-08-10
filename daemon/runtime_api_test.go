package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestNormalizeRuntimeEventInterval(t *testing.T) {
	t.Parallel()
	if actual := normalizeRuntimeEventInterval(1); actual != minimumRuntimeEventInterval {
		t.Fatalf("minimum interval was not enforced: %s", actual)
	}
	if actual := normalizeRuntimeEventInterval(0); actual != defaultRuntimeEventInterval {
		t.Fatalf("default interval mismatch: %s", actual)
	}
	if actual := normalizeRuntimeEventInterval(60_000); actual != maximumRuntimeEventInterval {
		t.Fatalf("maximum interval was not enforced: %s", actual)
	}
}

func TestRuntimeSnapshotIsCompleteWhileIdleAndRedactsFatalError(t *testing.T) {
	t.Parallel()
	service := NewStartedService(ServiceOptions{Context: context.Background()})
	defer service.Close()

	snapshot := service.readRuntimeSnapshot()
	if snapshot.SchemaVersion != runtimeSnapshotSchemaVersion || snapshot.Service == nil || snapshot.Status == nil || snapshot.Groups == nil || snapshot.ClashMode == nil {
		t.Fatalf("idle runtime snapshot is incomplete: %+v", snapshot)
	}
	if snapshot.Service.Status != ServiceStatus_IDLE || snapshot.StartedAt != 0 {
		t.Fatalf("unexpected idle state: %+v", snapshot)
	}

	service.serviceAccess.Lock()
	service.serviceStatus = &ServiceStatus{Status: ServiceStatus_FATAL, ErrorMessage: "password=do-not-expose"}
	service.serviceAccess.Unlock()
	snapshot = service.readRuntimeSnapshot()
	if snapshot.Service.ErrorMessage != "service failed; inspect local logs" {
		t.Fatalf("fatal error was not redacted: %q", snapshot.Service.ErrorMessage)
	}
}

func TestPopulateRuntimeTrafficRatesUsesElapsedTimeAndBothDirections(t *testing.T) {
	previous := &RuntimeSnapshot{
		ObservedAt: 1_000,
		Status: &Status{
			UplinkTotal:   100,
			DownlinkTotal: 200,
		},
	}
	current := &RuntimeSnapshot{
		ObservedAt: 1_500,
		Status: &Status{
			UplinkTotal:   350,
			DownlinkTotal: 700,
		},
	}

	populateRuntimeTrafficRates(previous, current)

	if current.Status.Uplink != 500 || current.Status.Downlink != 1_000 {
		t.Fatalf("unexpected runtime rates: uplink=%d downlink=%d", current.Status.Uplink, current.Status.Downlink)
	}
}

func TestPopulateRuntimeTrafficRatesRejectsCounterRegression(t *testing.T) {
	previous := &RuntimeSnapshot{
		ObservedAt: 1_000,
		Status:     &Status{UplinkTotal: 500, DownlinkTotal: 600},
	}
	current := &RuntimeSnapshot{
		ObservedAt: 2_000,
		Status:     &Status{UplinkTotal: 100, DownlinkTotal: 200},
	}

	populateRuntimeTrafficRates(previous, current)

	if current.Status.Uplink != 0 || current.Status.Downlink != 0 {
		t.Fatalf("counter regression produced rates: uplink=%d downlink=%d", current.Status.Uplink, current.Status.Downlink)
	}
}

func TestManagedURLTestSessionCloneAndRetention(t *testing.T) {
	t.Parallel()
	now := time.Now()
	session := &managedURLTestSession{
		id:          "urltest-1",
		groupTag:    "auto",
		state:       URLTestSessionState_URL_TEST_SESSION_SUCCEEDED,
		startedAt:   now,
		completedAt: now.Add(time.Second),
		total:       1,
		completed:   1,
		succeeded:   1,
		results: []*URLTestResult{{
			OutboundTag: "proxy",
			DelayMillis: 42,
			Status:      "available",
		}},
	}
	clone := cloneURLTestSessionLocked(session)
	clone.Results[0].Status = "changed"
	if session.results[0].Status != "available" {
		t.Fatal("session clone shares mutable result storage")
	}

	sessions := make(map[string]*managedURLTestSession)
	for index := 0; index < maximumRetainedURLTestSessions+3; index++ {
		id := string(rune('a' + index))
		sessions[id] = &managedURLTestSession{
			id:          id,
			state:       URLTestSessionState_URL_TEST_SESSION_SUCCEEDED,
			completedAt: now.Add(time.Duration(index) * time.Second),
		}
	}
	pruneURLTestSessionsLocked(sessions)
	if len(sessions) != maximumRetainedURLTestSessions {
		t.Fatalf("unexpected retained session count: %d", len(sessions))
	}
}

func TestCancelManagedURLTestSessionIsIdempotent(t *testing.T) {
	t.Parallel()
	service := NewStartedService(ServiceOptions{Context: context.Background()})
	defer service.Close()
	_, cancel := context.WithCancel(context.Background())
	session := &managedURLTestSession{
		id:        "urltest-1",
		groupTag:  "auto",
		state:     URLTestSessionState_URL_TEST_SESSION_RUNNING,
		startedAt: time.Now(),
		cancel:    cancel,
	}
	service.urlTestSessions[session.id] = session
	service.urlTestSessionByGroup[session.groupTag] = session.id

	first, err := service.cancelURLTestSession(session.id)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.cancelURLTestSession(session.id)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != URLTestSessionState_URL_TEST_SESSION_CANCELLED || second.State != first.State || second.CompletedAt == 0 {
		t.Fatalf("unexpected cancellation state: first=%+v second=%+v", first, second)
	}
}

func TestRuntimeEventStreamStartsWithResetSnapshot(t *testing.T) {
	t.Parallel()
	service := NewStartedService(ServiceOptions{Context: context.Background()})
	defer service.Close()
	ctx, cancel := context.WithCancel(context.Background())
	stream := &runtimeEventsTestStream{ctx: ctx, cancel: cancel}
	err := service.SubscribeRuntimeEvents(&RuntimeEventRequest{}, stream)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected stream termination: %v", err)
	}
	if len(stream.messages) != 1 || !stream.messages[0].Reset_ || stream.messages[0].Snapshot == nil {
		t.Fatalf("stream did not start with reset snapshot: %+v", stream.messages)
	}
}

type runtimeEventsTestStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	messages []*RuntimeEvents
}

func (s *runtimeEventsTestStream) Send(message *RuntimeEvents) error {
	s.messages = append(s.messages, message)
	s.cancel()
	return nil
}

func (s *runtimeEventsTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *runtimeEventsTestStream) SendHeader(metadata.MD) error { return nil }
func (s *runtimeEventsTestStream) SetTrailer(metadata.MD)       {}
func (s *runtimeEventsTestStream) Context() context.Context     { return s.ctx }
func (s *runtimeEventsTestStream) SendMsg(any) error            { return nil }
func (s *runtimeEventsTestStream) RecvMsg(any) error            { return nil }
