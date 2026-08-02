package libbox

import (
	"context"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/daemon"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// StandaloneURLTestResult is the stable gomobile result returned by a
// pre-connect one-shot probe.
type StandaloneURLTestResult struct {
	Tag         string
	DelayMillis int64
	TimeSeconds int64
	Status      string
	Error       string
	ErrorCode   string
}

// StandaloneURLTestSession owns an isolated sing-box instance. It never opens
// a TUN or local inbound and may be run at most once.
type StandaloneURLTestSession struct {
	access sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	closed bool
	run    bool

	platformInterface PlatformInterface
}

func NewStandaloneURLTestSession(platformInterface PlatformInterface) (*StandaloneURLTestSession, error) {
	if platformInterface == nil {
		return nil, E.New("missing platform interface")
	}
	ctx, cancel := context.WithCancel(baseContext(platformInterface))
	return &StandaloneURLTestSession{
		ctx:               ctx,
		cancel:            cancel,
		done:              make(chan struct{}),
		platformInterface: platformInterface,
	}, nil
}

func (s *StandaloneURLTestSession) Run(
	configContent string,
	groupTag string,
	targetOutboundTag string,
	urlTestURL string,
	timeoutMillis int32,
	deadlineMillis int32,
) (*StandaloneURLTestResult, error) {
	s.access.Lock()
	if s.closed || s.run {
		s.access.Unlock()
		return nil, os.ErrClosed
	}
	s.run = true
	s.access.Unlock()

	defer func() {
		s.access.Lock()
		s.closed = true
		close(s.done)
		s.access.Unlock()
	}()

	platformWrapper := &platformInterfaceWrapper{
		iif:       s.platformInterface,
		useProcFS: s.platformInterface.UseProcFS(),
	}
	service.MustRegister[adapter.PlatformInterface](s.ctx, platformWrapper)
	startedService := daemon.NewStartedService(daemon.ServiceOptions{
		Context:           s.ctx,
		Handler:           standalonePlatformHandler{},
		Debug:             sDebug,
		LogMaxLines:       sLogMaxLines,
		OOMKiller:         memoryLimitEnabled,
		StandaloneURLTest: true,
	})
	defer startedService.Close()
	defer func() {
		if instance := startedService.Instance(); instance != nil {
			_ = instance.Close()
		}
	}()

	if err := startedService.StartOrReloadService(configContent, nil); err != nil {
		return nil, err
	}
	result, err := startedService.RunStandaloneURLTest(
		s.ctx,
		groupTag,
		targetOutboundTag,
		urlTestURL,
		timeoutMillis,
		deadlineMillis,
	)
	if err != nil {
		return nil, err
	}
	return &StandaloneURLTestResult{
		Tag:         result.Tag,
		DelayMillis: result.DelayMillis,
		TimeSeconds: result.TimeSeconds,
		Status:      result.Status,
		Error:       result.Error,
		ErrorCode:   result.ErrorCode,
	}, nil
}

// Close cancels an active probe and waits until all owned runtime resources
// have been released. It is safe to call repeatedly.
func (s *StandaloneURLTestSession) Close() {
	s.access.Lock()
	if !s.run {
		if !s.closed {
			s.closed = true
			s.cancel()
			close(s.done)
		}
		s.access.Unlock()
		return
	}
	done := s.done
	s.cancel()
	s.access.Unlock()
	<-done
}

type standalonePlatformHandler struct{}

func (standalonePlatformHandler) ServiceStop() error { return nil }

func (standalonePlatformHandler) ServiceReload() error { return nil }

func (standalonePlatformHandler) SystemProxyStatus() (*daemon.SystemProxyStatus, error) {
	return &daemon.SystemProxyStatus{}, nil
}

func (standalonePlatformHandler) SetSystemProxyEnabled(bool) error { return nil }

func (standalonePlatformHandler) WriteDebugMessage(string) {}
