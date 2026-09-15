package group

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	U "github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

type urlTestSelectionOutbound struct {
	tag string
}

func (o *urlTestSelectionOutbound) Type() string           { return "test" }
func (o *urlTestSelectionOutbound) Tag() string            { return o.tag }
func (o *urlTestSelectionOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o *urlTestSelectionOutbound) Dependencies() []string { return nil }
func (o *urlTestSelectionOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}
func (o *urlTestSelectionOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

type urlTestOutboundManager struct {
	adapter.OutboundManager
	outbound adapter.Outbound
}

func (m *urlTestOutboundManager) Outbound(string) (adapter.Outbound, bool) {
	return m.outbound, true
}

type cancellationObservingOutbound struct {
	urlTestSelectionOutbound
	started   chan struct{}
	cancelled chan struct{}
}

func (o *cancellationObservingOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	close(o.started)
	<-ctx.Done()
	close(o.cancelled)
	return nil, ctx.Err()
}

// countingOutbound records how many probes are inside DialContext at once, which is the
// only place a concurrency budget can be observed from outside the group.
type countingOutbound struct {
	urlTestSelectionOutbound
	access  *sync.Mutex
	running *int
	peak    *int
	dials   *int
}

func (o *countingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.access.Lock()
	*o.dials++
	*o.running++
	if *o.running > *o.peak {
		*o.peak = *o.running
	}
	o.access.Unlock()
	time.Sleep(50 * time.Millisecond)
	o.access.Lock()
	*o.running--
	o.access.Unlock()
	return nil, net.ErrClosed
}

func TestURLTestRequestCancellationStopsChildProbe(t *testing.T) {
	groupCtx, stopGroup := context.WithCancel(t.Context())
	groupCtx = service.ContextWithPtr(groupCtx, U.NewHistoryStorage())
	defer stopGroup()
	outbound := &cancellationObservingOutbound{
		urlTestSelectionOutbound: urlTestSelectionOutbound{tag: "blocking"},
		started:                  make(chan struct{}),
		cancelled:                make(chan struct{}),
	}
	manager := &urlTestOutboundManager{outbound: outbound}
	group, err := NewURLTestGroup(
		groupCtx,
		manager,
		log.NewNOPFactory().Logger(),
		[]adapter.Outbound{outbound},
		"http://example.invalid/",
		0,
		0,
		0,
		0,
		0,
		false,
	)
	require.NoError(t, err)

	requestCtx, cancelRequest := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		_, _ = group.URLTest(requestCtx)
		close(done)
	}()
	<-outbound.started
	cancelRequest()

	select {
	case <-outbound.cancelled:
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not reach the active URL-test probe")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("URL-test did not return after request cancellation")
	}
}

func TestURLTestProbeTimeoutBoundsTheChildProbe(t *testing.T) {
	groupCtx, stopGroup := context.WithCancel(t.Context())
	groupCtx = service.ContextWithPtr(groupCtx, U.NewHistoryStorage())
	defer stopGroup()
	outbound := &cancellationObservingOutbound{
		urlTestSelectionOutbound: urlTestSelectionOutbound{tag: "blocking"},
		started:                  make(chan struct{}),
		cancelled:                make(chan struct{}),
	}
	manager := &urlTestOutboundManager{outbound: outbound}
	probeTimeout := 100 * time.Millisecond
	group, err := NewURLTestGroup(
		groupCtx,
		manager,
		log.NewNOPFactory().Logger(),
		[]adapter.Outbound{outbound},
		"http://example.invalid/",
		0,
		0,
		0,
		probeTimeout,
		0,
		false,
	)
	require.NoError(t, err)

	started := time.Now()
	_, _ = group.URLTest(t.Context())

	select {
	case <-outbound.cancelled:
	case <-time.After(time.Second):
		t.Fatal("the probe timeout did not reach the child probe")
	}
	require.Less(t, time.Since(started), time.Second, "the group waited past its probe timeout")
	require.Equal(t, probeTimeout, group.probeTimeout)
}

func TestURLTestProbeRecordsUnavailableHistory(t *testing.T) {
	groupCtx, stopGroup := context.WithCancel(t.Context())
	history := U.NewHistoryStorage()
	groupCtx = service.ContextWithPtr(groupCtx, history)
	defer stopGroup()
	// Refusing every dial is what a dead server looks like to a probe.
	outbound := &urlTestSelectionOutbound{tag: "dead"}
	manager := &urlTestOutboundManager{outbound: outbound}
	group, err := NewURLTestGroup(
		groupCtx,
		manager,
		log.NewNOPFactory().Logger(),
		[]adapter.Outbound{outbound},
		"http://example.invalid/",
		0,
		0,
		0,
		0,
		0,
		false,
	)
	require.NoError(t, err)

	_, _ = group.URLTest(t.Context())

	// A failure has to be recorded rather than deleted: with no history at all the client keeps
	// showing the last good delay, because "nothing to report" and "the server is down" are
	// different facts and only one of them is true here.
	recorded := history.LoadURLTestHistory(RealTag(group.outbound, outbound))
	require.NotNil(t, recorded, "a failed probe left no history behind")
	require.Equal(t, adapter.URLTestStatusUnavailable, recorded.Status)
	require.Zero(t, recorded.Delay)
	require.NotEmpty(t, recorded.Error)
	require.False(t, adapter.URLTestHistoryIsAvailable(recorded))
}

func TestURLTestProbeConcurrencyLimitsParallelProbes(t *testing.T) {
	groupCtx, stopGroup := context.WithCancel(t.Context())
	groupCtx = service.ContextWithPtr(groupCtx, U.NewHistoryStorage())
	defer stopGroup()
	const probeCount = 4
	var access sync.Mutex
	running := 0
	peak := 0
	dials := 0
	counting := &countingOutbound{
		urlTestSelectionOutbound: urlTestSelectionOutbound{tag: "counting"},
		access:                   &access,
		running:                  &running,
		peak:                     &peak,
		dials:                    &dials,
	}
	outbounds := make([]adapter.Outbound, probeCount)
	for i := range outbounds {
		outbounds[i] = &urlTestSelectionOutbound{tag: fmt.Sprintf("probe-%d", i)}
	}
	manager := &urlTestOutboundManager{outbound: counting}
	group, err := NewURLTestGroup(
		groupCtx,
		manager,
		log.NewNOPFactory().Logger(),
		outbounds,
		"http://example.invalid/",
		0,
		0,
		0,
		0,
		2,
		false,
	)
	require.NoError(t, err)

	_, _ = group.URLTest(t.Context())

	access.Lock()
	observedPeak, observedDials := peak, dials
	access.Unlock()
	require.Equal(t, probeCount, observedDials, "not every member of the group was probed")
	require.LessOrEqual(t, observedPeak, 2, "more probes ran at once than the concurrency budget allows")
}

func TestURLTestProbeBudgetDefaults(t *testing.T) {
	group, err := NewURLTestGroup(
		service.ContextWithPtr(t.Context(), U.NewHistoryStorage()),
		&urlTestOutboundManager{outbound: &urlTestSelectionOutbound{tag: "default"}},
		log.NewNOPFactory().Logger(),
		[]adapter.Outbound{&urlTestSelectionOutbound{tag: "default"}},
		"http://example.invalid/",
		0,
		0,
		0,
		0,
		0,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, C.TCPTimeout, group.probeTimeout)
	require.Equal(t, 10, group.probeConcurrency)
}

func TestURLTestProbeBudgetRejectsImpossibleValues(t *testing.T) {
	// A negative concurrency used to reach the batch as a negative channel size, which
	// panics in a goroutine the start had already launched; the refusal has to come first.
	for _, budget := range []struct {
		timeout     time.Duration
		concurrency int
	}{
		{0, -1},
		{-time.Second, 0},
		{0, MaxURLTestProbeConcurrency + 1},
	} {
		_, err := NewURLTestGroup(
			t.Context(),
			&urlTestOutboundManager{outbound: &urlTestSelectionOutbound{tag: "rejected"}},
			log.NewNOPFactory().Logger(),
			[]adapter.Outbound{&urlTestSelectionOutbound{tag: "rejected"}},
			"http://example.invalid/",
			0,
			0,
			0,
			budget.timeout,
			budget.concurrency,
			false,
		)
		require.Error(t, err, "budget timeout=%v concurrency=%d must be refused", budget.timeout, budget.concurrency)
	}
}

func TestURLTestSelectionIgnoresUnavailableHistory(t *testing.T) {
	t.Parallel()

	unavailable := &urlTestSelectionOutbound{tag: "unavailable"}
	available := &urlTestSelectionOutbound{tag: "available"}
	history := U.NewHistoryStorage()
	history.StoreURLTestHistory(unavailable.Tag(), &adapter.URLTestHistory{
		Time:   time.Now(),
		Status: adapter.URLTestStatusUnavailable,
		Error:  "request timed out",
	})
	history.StoreURLTestHistory(available.Tag(), &adapter.URLTestHistory{
		Time:   time.Now(),
		Delay:  80,
		Status: adapter.URLTestStatusAvailable,
	})
	group := &URLTestGroup{
		outbounds:           []adapter.Outbound{available, unavailable},
		history:             history,
		tolerance:           50,
		interruptGroup:      interrupt.NewGroup(),
		selectedOutboundTCP: unavailable,
		selectedOutboundUDP: unavailable,
	}

	selected, availableHistory := group.Select(N.NetworkTCP)
	require.True(t, availableHistory)
	require.Same(t, available, selected)

	group.performUpdateCheck()
	selectedTCP, selectedUDP := group.selectedOutbounds()
	require.Same(t, available, selectedTCP)
	require.Same(t, available, selectedUDP)
}

func TestURLTestSelectionKeepsHealthyOutboundWithinTolerance(t *testing.T) {
	t.Parallel()

	selected := &urlTestSelectionOutbound{tag: "selected"}
	slightlyFaster := &urlTestSelectionOutbound{tag: "slightly-faster"}
	history := U.NewHistoryStorage()
	history.StoreURLTestHistory(selected.Tag(), &adapter.URLTestHistory{
		Time:   time.Now(),
		Delay:  100,
		Status: adapter.URLTestStatusAvailable,
	})
	history.StoreURLTestHistory(slightlyFaster.Tag(), &adapter.URLTestHistory{
		Time:   time.Now(),
		Delay:  70,
		Status: adapter.URLTestStatusAvailable,
	})
	group := &URLTestGroup{
		outbounds:           []adapter.Outbound{slightlyFaster, selected},
		history:             history,
		tolerance:           50,
		interruptGroup:      interrupt.NewGroup(),
		selectedOutboundTCP: selected,
	}

	outbound, hasHistory := group.Select(N.NetworkTCP)
	require.True(t, hasHistory)
	require.Same(t, selected, outbound)
}
