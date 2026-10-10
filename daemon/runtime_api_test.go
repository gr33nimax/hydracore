package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	HC "github.com/sagernet/sing-box/common/hydracore"
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
	HC.ResetRuntimeTransportState()
	t.Cleanup(HC.ResetRuntimeTransportState)
	service := NewStartedService(ServiceOptions{Context: context.Background()})
	defer service.Close()

	snapshot := service.readRuntimeSnapshot()
	if snapshot.SchemaVersion != runtimeSnapshotSchemaVersion || snapshot.Service == nil || snapshot.Status == nil || snapshot.Groups == nil || snapshot.ClashMode == nil || snapshot.TransportHealth == nil {
		t.Fatalf("idle runtime snapshot is incomplete: %+v", snapshot)
	}
	if snapshot.Service.Status != ServiceStatus_IDLE || snapshot.StartedAt != 0 {
		t.Fatalf("unexpected idle state: %+v", snapshot)
	}
	HC.PublishTransportHealth(7, HC.TransportHealthSnapshot{TransportTag: "call-vk", State: HC.TransportStateHealthy, ActiveLanes: 3})
	snapshot = service.readRuntimeSnapshot()
	if snapshot.TransportHealth.TransportTag != "call-vk" || snapshot.TransportHealth.RuntimeGeneration != 7 || snapshot.TransportHealth.ActiveLanes != 3 {
		t.Fatalf("transport health was not included: %+v", snapshot.TransportHealth)
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

func TestRuntimeEventStreamRetainsChangesDuringResetSend(t *testing.T) {
	t.Parallel()
	for connection := 0; connection < 2; connection++ {
		service := NewStartedService(ServiceOptions{Context: context.Background()})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream := &runtimeEventsTestStream{ctx: ctx, cancel: cancel}
		stream.onSend = func(message *RuntimeEvents) {
			if !message.Reset_ {
				cancel()
				return
			}
			// The first snapshot is already captured. Commit a change while Send
			// is still in progress and wait until its notification is delivered.
			// Without subscriptions established before Send, the stream loses it.
			probe, _, err := service.urlTestSessionObserver.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			defer service.urlTestSessionObserver.UnSubscribe(probe)
			service.urlTestSessionAccess.Lock()
			service.urlTestSessions["during-reset"] = &managedURLTestSession{
				id:       "during-reset",
				groupTag: "auto",
				state:    URLTestSessionState_URL_TEST_SESSION_RUNNING,
			}
			service.urlTestSessionAccess.Unlock()
			service.urlTestSessionSubscriber.Emit(struct{}{})
			select {
			case <-probe:
			case <-ctx.Done():
				t.Fatal("notification was not delivered")
			}
		}
		err := service.SubscribeRuntimeEvents(&RuntimeEventRequest{}, stream)
		cancel()
		service.Close()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("connection %d: unexpected stream termination: %v", connection+1, err)
		}
		if len(stream.messages) != 2 || len(stream.messages[0].Snapshot.UrlTestSessions) != 0 {
			t.Fatalf("connection %d: initial snapshot/change sequence mismatch: %+v", connection+1, stream.messages)
		}
		update := stream.messages[1]
		var sessionUpdate *RuntimeEvent
		for _, event := range update.Events {
			if event.Type == RuntimeEventType_RUNTIME_EVENT_URL_TEST_SESSIONS {
				sessionUpdate = event
			}
		}
		if update.Reset_ || update.Sequence != 2 || sessionUpdate == nil ||
			len(sessionUpdate.UrlTestSessions) != 1 || sessionUpdate.UrlTestSessions[0].Id != "during-reset" {
			t.Fatalf("connection %d: change during initial Send was not published: %+v", connection+1, update)
		}
	}
}

type runtimeEventsTestStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	messages []*RuntimeEvents
	onSend   func(*RuntimeEvents)
}

func (s *runtimeEventsTestStream) Send(message *RuntimeEvents) error {
	s.messages = append(s.messages, message)
	if s.onSend != nil {
		s.onSend(message)
	} else {
		s.cancel()
	}
	return nil
}

func (s *runtimeEventsTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *runtimeEventsTestStream) SendHeader(metadata.MD) error { return nil }
func (s *runtimeEventsTestStream) SetTrailer(metadata.MD)       {}
func (s *runtimeEventsTestStream) Context() context.Context     { return s.ctx }
func (s *runtimeEventsTestStream) SendMsg(any) error            { return nil }
func (s *runtimeEventsTestStream) RecvMsg(any) error            { return nil }

// Группы — самая дорогая часть снимка, поэтому подписчик передаёт уже собранное
// значение. Пока сервис не запущен, групп нет вообще, и переданное значение
// обязано быть проигнорировано: иначе снимок соврал бы про остановленный сервис.
func TestRuntimeSnapshotGroupsWhileNotStarted(t *testing.T) {
	HC.ResetRuntimeTransportState()
	t.Cleanup(HC.ResetRuntimeTransportState)
	service := NewStartedService(ServiceOptions{Context: context.Background()})
	defer service.Close()

	supplied := &Groups{Group: []*Group{{Tag: "select", Type: "selector"}}}
	snapshot := service.readRuntimeSnapshotWithGroups(supplied)
	if snapshot.Groups == supplied {
		t.Fatal("groups of a service that is not started must not be reused")
	}
	if snapshot.Groups == nil || len(snapshot.Groups.Group) != 0 {
		t.Fatalf("snapshot must carry an empty groups message: %+v", snapshot.Groups)
	}
	if service.readRuntimeSnapshotWithGroups(nil).Groups == nil {
		t.Fatal("snapshot must always carry a groups message")
	}
}

// processMemoryInUse обязан отдавать что-то осмысленное и не зависеть от
// runtime.ReadMemStats, который останавливает мир.
func TestProcessMemoryInUseIsPositiveAndRepeatable(t *testing.T) {
	first := processMemoryInUse()
	if first == 0 {
		t.Fatal("process memory reported zero")
	}
	if second := processMemoryInUse(); second == 0 {
		t.Fatal("second read reported zero")
	}
}
