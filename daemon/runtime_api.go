package daemon

import (
	"context"
	"os"
	"time"

	HC "github.com/sagernet/sing-box/common/hydracore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	runtimeSnapshotSchemaVersion = 2
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
	return s.readRuntimeSnapshotWithGroups(nil)
}

// readRuntimeSnapshotWithGroups собирает снимок, переиспользуя список групп.
//
// Группы — самая дорогая часть снимка: обход всех outbound'ов, поиск истории
// url-теста на каждый элемент и чтение cache file на каждую группу. Меняются они
// только по событию (выбор сервера или завершившийся url-тест), поэтому
// подписчик, у которого это событие есть, передаёт сюда уже собранное значение,
// а nil означает «собрать заново».
func (s *StartedService) readRuntimeSnapshotWithGroups(groups *Groups) *RuntimeSnapshot {
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
		TransportHealth: runtimeTransportHealth(HC.CurrentTransportHealth()),
	}
	if !startedAt.IsZero() {
		snapshot.StartedAt = startedAt.UnixMilli()
	}
	if !isStarted {
		return snapshot
	}
	if groups != nil {
		snapshot.Groups = groups
	}

	s.serviceAccess.RLock()
	if s.serviceStatus.Status == ServiceStatus_STARTED && s.instance != nil {
		if groups == nil {
			snapshot.Groups = s.readGroups()
		}
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
	// Подписка на изменение групп: она есть у SubscribeGroups и покрывает и
	// выбор сервера, и завершившийся url-тест. Без неё цикл пересобирал группы
	// на каждый тик — обход всех outbound'ов, поиск истории на каждый элемент и
	// чтение cache file на каждую группу, раз в секунду всё время жизни туннеля.
	groupsChanged, groupsDone, err := s.urlTestObserver.Subscribe()
	if err != nil {
		return err
	}
	defer s.urlTestObserver.UnSubscribe(groupsChanged)

	previous := s.readRuntimeSnapshot()
	sequence := uint64(1)
	previous.Sequence = sequence
	if err = server.Send(&RuntimeEvents{
		Sequence: sequence,
		Reset_:   true,
		Snapshot: previous,
	}); err != nil {
		return err
	}
	cachedGroups := previous.Groups
	startedBefore := previous.Service.GetStatus() == ServiceStatus_STARTED

	ticker := time.NewTicker(normalizeRuntimeEventInterval(request.IntervalMillis))
	defer ticker.Stop()
	healthChanged := HC.TransportHealthChanged()
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-server.Context().Done():
			return server.Context().Err()
		case <-ticker.C:
		case <-healthChanged:
		case <-groupsChanged:
			cachedGroups = nil
		case <-groupsDone:
			return nil
		}

		healthChanged = HC.TransportHealthChanged()
		current := s.readRuntimeSnapshotWithGroups(cachedGroups)
		// Переход в STARTED поднимает группы, которых до него не было.
		startedNow := current.Service.GetStatus() == ServiceStatus_STARTED
		if startedNow != startedBefore {
			startedBefore = startedNow
			current = s.readRuntimeSnapshotWithGroups(nil)
		}
		cachedGroups = current.Groups
		populateRuntimeTrafficRates(previous, current)
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
		if current.Groups != previous.Groups && !proto.Equal(previous.Groups, current.Groups) {
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
		if !proto.Equal(previous.TransportHealth, current.TransportHealth) {
			events = append(events, &RuntimeEvent{
				Type:            RuntimeEventType_RUNTIME_EVENT_TRANSPORT_HEALTH,
				TransportHealth: current.TransportHealth,
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

func runtimeTransportHealth(health HC.TransportHealthSnapshot) *TransportHealth {
	result := &TransportHealth{
		TransportTag:            health.TransportTag,
		State:                   health.State,
		ActiveLanes:             health.ActiveLanes,
		TotalLanes:              health.TotalLanes,
		Demand:                  health.Demand,
		LastProgressAt:          health.LastProgressAt,
		LastAggregateProgressAt: health.LastAggregateProgressAt,
		LastInboundAt:           health.LastInboundAt,
		ObservedAt:              health.ObservedAt,
		Applicable:              health.Applicable,
		RuntimeGeneration:       health.RuntimeGeneration,
		NetworkGeneration:       health.NetworkGeneration,
	}
	if health.Failure != nil {
		result.Failure = &TransportFailure{
			Stage:            health.Failure.Stage,
			Kind:             health.Failure.Kind,
			Code:             health.Failure.Code,
			RetryAfterMillis: health.Failure.RetryAfterMS,
			ChallengeId:      health.Failure.ChallengeID,
			Domain:           health.Failure.Domain,
			Terminal:         health.Failure.Terminal,
		}
	}
	return result
}

func populateRuntimeTrafficRates(previous *RuntimeSnapshot, current *RuntimeSnapshot) {
	if previous == nil || current == nil || previous.Status == nil || current.Status == nil {
		return
	}
	elapsedMillis := current.ObservedAt - previous.ObservedAt
	if elapsedMillis <= 0 {
		return
	}
	current.Status.Uplink = runtimeBytesPerSecond(
		previous.Status.UplinkTotal,
		current.Status.UplinkTotal,
		elapsedMillis,
	)
	current.Status.Downlink = runtimeBytesPerSecond(
		previous.Status.DownlinkTotal,
		current.Status.DownlinkTotal,
		elapsedMillis,
	)
}

func runtimeBytesPerSecond(previousTotal int64, currentTotal int64, elapsedMillis int64) int64 {
	if currentTotal <= previousTotal || elapsedMillis <= 0 {
		return 0
	}
	return int64(float64(currentTotal-previousTotal) * float64(time.Second/time.Millisecond) / float64(elapsedMillis))
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
