package daemon

import (
	"context"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	runtimeSnapshotSchemaVersion = 1
	defaultRuntimeEventInterval  = time.Second
	minimumRuntimeEventInterval  = 250 * time.Millisecond
	maximumRuntimeEventInterval  = 30 * time.Second
)

func normalizeRuntimeEventInterval(milliseconds int64) time.Duration {
	if milliseconds <= 0 {
		return defaultRuntimeEventInterval
	}
	interval := time.Duration(milliseconds) * time.Millisecond
	if interval < minimumRuntimeEventInterval {
		return minimumRuntimeEventInterval
	}
	if interval > maximumRuntimeEventInterval {
		return maximumRuntimeEventInterval
	}
	return interval
}

func (s *StartedService) GetRuntimeSnapshot(context.Context, *emptypb.Empty) (*RuntimeSnapshot, error) {
	return s.readRuntimeSnapshot(), nil
}

func (s *StartedService) readRuntimeSnapshot() *RuntimeSnapshot {
	s.serviceAccess.RLock()
	serviceStatus := proto.Clone(s.serviceStatus).(*ServiceStatus)
	if serviceStatus.Status == ServiceStatus_FATAL && serviceStatus.ErrorMessage != "" {
		serviceStatus.ErrorMessage = "service failed; inspect local logs"
	}
	startedAt := s.startedAt
	isStarted := serviceStatus.Status == ServiceStatus_STARTED && s.instance != nil
	s.serviceAccess.RUnlock()

	snapshot := &RuntimeSnapshot{
		SchemaVersion:   runtimeSnapshotSchemaVersion,
		Sequence:        s.runtimeSequence.Add(1),
		ObservedAt:      time.Now().UnixMilli(),
		Service:         serviceStatus,
		Status:          s.readStatus(),
		Groups:          &Groups{},
		ClashMode:       &ClashModeStatus{},
		UrlTestSessions: s.readURLTestSessions(),
	}
	if !startedAt.IsZero() {
		snapshot.StartedAt = startedAt.UnixMilli()
	}
	if !isStarted {
		return snapshot
	}

	s.serviceAccess.RLock()
	if s.serviceStatus.Status == ServiceStatus_STARTED && s.instance != nil {
		snapshot.Groups = s.readGroups()
		if s.instance.clashServer != nil {
			snapshot.ClashMode = &ClashModeStatus{
				ModeList:    s.instance.clashServer.ModeList(),
				CurrentMode: s.instance.clashServer.Mode(),
			}
		}
	}
	s.serviceAccess.RUnlock()
	return snapshot
}

func (s *StartedService) SubscribeRuntimeEvents(request *RuntimeEventRequest, server grpc.ServerStreamingServer[RuntimeEvents]) error {
	if request == nil {
		request = &RuntimeEventRequest{}
	}
	previous := s.readRuntimeSnapshot()
	sequence := uint64(1)
	previous.Sequence = sequence
	if err := server.Send(&RuntimeEvents{
		Sequence: sequence,
		Reset_:   true,
		Snapshot: previous,
	}); err != nil {
		return err
	}

	ticker := time.NewTicker(normalizeRuntimeEventInterval(request.IntervalMillis))
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-server.Context().Done():
			return server.Context().Err()
		case <-ticker.C:
		}

		current := s.readRuntimeSnapshot()
		var events []*RuntimeEvent
		if !proto.Equal(previous.Service, current.Service) || previous.StartedAt != current.StartedAt {
			events = append(events, &RuntimeEvent{
				Type:      RuntimeEventType_RUNTIME_EVENT_SERVICE,
				Service:   current.Service,
				StartedAt: current.StartedAt,
			})
		}
		if !proto.Equal(previous.Status, current.Status) {
			events = append(events, &RuntimeEvent{Type: RuntimeEventType_RUNTIME_EVENT_STATUS, Status: current.Status})
		}
		if !proto.Equal(previous.Groups, current.Groups) {
			events = append(events, &RuntimeEvent{Type: RuntimeEventType_RUNTIME_EVENT_GROUPS, Groups: current.Groups})
		}
		if !proto.Equal(previous.ClashMode, current.ClashMode) {
			events = append(events, &RuntimeEvent{Type: RuntimeEventType_RUNTIME_EVENT_CLASH_MODE, ClashMode: current.ClashMode})
		}
		if !equalURLTestSessions(previous.UrlTestSessions, current.UrlTestSessions) {
			events = append(events, &RuntimeEvent{
				Type:            RuntimeEventType_RUNTIME_EVENT_URL_TEST_SESSIONS,
				UrlTestSessions: current.UrlTestSessions,
			})
		}
		previous = current
		if len(events) == 0 {
			continue
		}
		sequence++
		if err := server.Send(&RuntimeEvents{Sequence: sequence, Events: events}); err != nil {
			return err
		}
	}
}

func equalURLTestSessions(left []*URLTestSession, right []*URLTestSession) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func (s *StartedService) StartURLTest(_ context.Context, request *URLTestRequest) (*URLTestSession, error) {
	return s.startURLTest(request)
}

func (s *StartedService) GetURLTestSession(_ context.Context, request *URLTestSessionRequest) (*URLTestSession, error) {
	if request == nil {
		return nil, os.ErrInvalid
	}
	return s.getURLTestSession(request.Id)
}

func (s *StartedService) CancelURLTest(_ context.Context, request *URLTestSessionRequest) (*URLTestSession, error) {
	if request == nil {
		return nil, os.ErrInvalid
	}
	return s.cancelURLTestSession(request.Id)
}

func (s *StartedService) SubscribeURLTestEvents(request *URLTestEventRequest, server grpc.ServerStreamingServer[URLTestEvents]) error {
	if request == nil {
		request = &URLTestEventRequest{}
	}
	subscription, done, err := s.urlTestSessionObserver.Subscribe()
	if err != nil {
		return err
	}
	defer s.urlTestSessionObserver.UnSubscribe(subscription)
	sequence := uint64(1)
	if err = server.Send(&URLTestEvents{
		Sequence: sequence,
		Reset_:   true,
		Sessions: s.readURLTestSessions(),
	}); err != nil {
		return err
	}

	ticker := time.NewTicker(normalizeRuntimeEventInterval(request.IntervalMillis))
	defer ticker.Stop()
	dirty := false
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-server.Context().Done():
			return server.Context().Err()
		case <-done:
			return nil
		case <-subscription:
			dirty = true
		case <-ticker.C:
			if !dirty {
				continue
			}
			dirty = false
			sequence++
			if err = server.Send(&URLTestEvents{
				Sequence: sequence,
				Sessions: s.readURLTestSessions(),
			}); err != nil {
				return err
			}
		}
	}
}
