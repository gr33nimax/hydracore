package daemon

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
)

func TestNormalizeURLTestOptions(t *testing.T) {
	t.Parallel()
	options := normalizeURLTestOptions(&URLTestRequest{
		TimeoutMillis:  15_000,
		Concurrency:    100,
		DeadlineMillis: 60_000,
	})
	if options.timeout != 15*time.Second {
		t.Fatalf("unexpected timeout: %s", options.timeout)
	}
	if options.deadline != time.Minute {
		t.Fatalf("unexpected deadline: %s", options.deadline)
	}
	if options.concurrency != maximumURLTestConcurrency {
		t.Fatalf("concurrency was not clamped: %d", options.concurrency)
	}
}

func TestValidateURLTestLink(t *testing.T) {
	t.Parallel()
	for _, link := range []string{"", "http://example.com/generate_204", "https://example.com/health?probe=1"} {
		if err := validateURLTestLink(link); err != nil {
			t.Fatalf("valid link %q was rejected: %v", link, err)
		}
	}
	for _, link := range []string{"tcp://example.com:443", "https:///missing-host", "://broken"} {
		if err := validateURLTestLink(link); err == nil {
			t.Fatalf("invalid link %q was accepted", link)
		}
	}
}

func TestStandaloneURLTestSanitizesRuntimeOptions(t *testing.T) {
	t.Parallel()
	unifiedDelay := &option.UnifiedDelayOptions{Enabled: true}
	options := option.Options{
		Inbounds:  []option.Inbound{{}},
		Services:  []option.Service{{}},
		NTP:       &option.NTPOptions{},
		Providers: []option.Provider{{}},
		Experimental: &option.ExperimentalOptions{
			CacheFile:    &option.CacheFileOptions{Enabled: true},
			UnifiedDelay: unifiedDelay,
		},
	}

	sanitizeStandaloneURLTestOptions(&options)

	if options.Inbounds != nil || options.Services != nil || options.NTP != nil || options.Providers != nil {
		t.Fatal("standalone URL test retained listeners or background services")
	}
	if options.Experimental == nil || options.Experimental.UnifiedDelay != unifiedDelay {
		t.Fatal("standalone URL test discarded the safe unified-delay option")
	}
	if options.Experimental.CacheFile != nil {
		t.Fatal("standalone URL test retained experimental background services")
	}
}

func TestStandaloneURLTestProbeSuccess(t *testing.T) {
	t.Parallel()
	target := urlTestTarget{tag: "selected", outbound: &testURLTestOutbound{tag: "selected"}}
	result := runStandaloneURLTestProbe(
		context.Background(),
		target,
		normalizeURLTestOptions(&URLTestRequest{TimeoutMillis: 1_000}),
		func(context.Context, string, adapter.Outbound) (uint16, error) { return 42, nil },
	)
	if result.Tag != "selected" || result.DelayMillis != 42 || result.Status != "available" {
		t.Fatalf("unexpected standalone result: %+v", result)
	}
}

func TestStandaloneURLTestProbeCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	target := urlTestTarget{tag: "selected", outbound: &testURLTestOutbound{tag: "selected"}}
	result := runStandaloneURLTestProbe(
		ctx,
		target,
		normalizeURLTestOptions(&URLTestRequest{TimeoutMillis: 1_000}),
		func(ctx context.Context, _ string, _ adapter.Outbound) (uint16, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
	)
	if result.Status != "unavailable" || result.ErrorCode != "cancelled" {
		t.Fatalf("unexpected cancelled standalone result: %+v", result)
	}
}

type testURLTestOutbound struct {
	adapter.Outbound
	tag string
}

func (o *testURLTestOutbound) Tag() string { return o.tag }

type testURLTestGroup struct {
	adapter.Outbound
	tag          string
	now          string
	members      []string
	refreshCount atomic.Int32
}

func (g *testURLTestGroup) Tag() string              { return g.tag }
func (g *testURLTestGroup) Now() string              { return g.now }
func (g *testURLTestGroup) All() []string            { return append([]string(nil), g.members...) }
func (g *testURLTestGroup) RefreshURLTestSelection() { g.refreshCount.Add(1) }

type testURLTestOutboundManager struct {
	adapter.OutboundManager
	outbounds map[string]adapter.Outbound
}

func (m *testURLTestOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, loaded := m.outbounds[tag]
	return outbound, loaded
}

func TestCollectConcreteURLTestTargetsDeduplicatesNestedGroups(t *testing.T) {
	t.Parallel()
	first := &testURLTestOutbound{tag: "first"}
	second := &testURLTestOutbound{tag: "second"}
	manager := &testURLTestOutboundManager{outbounds: map[string]adapter.Outbound{
		"first":  first,
		"second": second,
		"nested": &testURLTestGroup{tag: "nested", members: []string{"first", "second"}},
		"lowest": &testURLTestGroup{tag: "lowest", now: "second", members: []string{"first", "second"}},
	}}
	targets, err := collectConcreteURLTestTargets(manager, []string{"nested", "lowest", "first"})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].tag != "first" || targets[1].tag != "second" {
		t.Fatalf("unexpected concrete targets: %#v", targets)
	}
	selected, err := resolveSelectedURLTestOutbound(manager, "lowest")
	if err != nil {
		t.Fatal(err)
	}
	if selected.Tag() != "second" {
		t.Fatalf("unexpected selected outbound: %q", selected.Tag())
	}
}

func TestCollectConcreteURLTestTargetsRejectsCycles(t *testing.T) {
	t.Parallel()
	manager := &testURLTestOutboundManager{outbounds: map[string]adapter.Outbound{
		"a": &testURLTestGroup{tag: "a", members: []string{"b"}},
		"b": &testURLTestGroup{tag: "b", members: []string{"a"}},
	}}
	if _, err := collectConcreteURLTestTargets(manager, []string{"a"}); err == nil {
		t.Fatal("cyclic group was accepted")
	}
}

func TestRefreshURLTestGroupSelectionsTraversesNestedGroupsOnce(t *testing.T) {
	t.Parallel()
	leaf := &testURLTestOutbound{tag: "leaf"}
	nested := &testURLTestGroup{tag: "nested", members: []string{"leaf"}}
	root := &testURLTestGroup{tag: "root", members: []string{"nested", "nested"}}
	manager := &testURLTestOutboundManager{outbounds: map[string]adapter.Outbound{
		"root":   root,
		"nested": nested,
		"leaf":   leaf,
	}}

	refreshURLTestGroupSelections(manager, "root")

	if root.refreshCount.Load() != 1 || nested.refreshCount.Load() != 1 {
		t.Fatalf("unexpected refresh counts: root=%d nested=%d", root.refreshCount.Load(), nested.refreshCount.Load())
	}
}

func TestRunURLTestTargetsBoundsConcurrency(t *testing.T) {
	t.Parallel()
	targets := make([]urlTestTarget, 24)
	for index := range targets {
		targets[index].tag = string(rune('a' + index))
	}
	var active atomic.Int32
	var maximum atomic.Int32
	var completed atomic.Int32
	probe := func(ctx context.Context, link string, outbound adapter.Outbound) (uint16, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return 25, nil
		}
	}
	runURLTestTargets(context.Background(), targets, urlTestSessionOptions{
		timeout:     time.Second,
		concurrency: 3,
	}, probe, func(target urlTestTarget, delay uint16, err error) {
		if err != nil || delay != 25 {
			t.Errorf("unexpected result for %s: delay=%d err=%v", target.tag, delay, err)
		}
		completed.Add(1)
	})
	if completed.Load() != int32(len(targets)) {
		t.Fatalf("completed %d of %d probes", completed.Load(), len(targets))
	}
	if maximum.Load() > 3 {
		t.Fatalf("maximum concurrency exceeded: %d", maximum.Load())
	}
}

func TestRunURLTestTargetsStopsAtDeadline(t *testing.T) {
	t.Parallel()
	targets := make([]urlTestTarget, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var started atomic.Int32
	probe := func(ctx context.Context, link string, outbound adapter.Outbound) (uint16, error) {
		started.Add(1)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	runURLTestTargets(ctx, targets, urlTestSessionOptions{
		timeout:     time.Second,
		concurrency: 2,
	}, probe, func(target urlTestTarget, delay uint16, err error) {})
	if started.Load() > 2 {
		t.Fatalf("deadline allowed queued probes to start: %d", started.Load())
	}
}

func TestClassifyURLTestError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "timeout", err: context.DeadlineExceeded, code: "timeout"},
		{name: "cancelled", err: context.Canceled, code: "cancelled"},
		{name: "dns", err: &net.DNSError{Err: "no such host", Name: "example.invalid"}, code: "dns"},
		{name: "eof", err: io.EOF, code: "eof"},
		{name: "tls", err: errors.New("tls: bad certificate"), code: "tls"},
		{name: "network", err: errors.New("connection refused"), code: "network"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, message := classifyURLTestError(test.err)
			if code != test.code || message == "" {
				t.Fatalf("unexpected classification: code=%q message=%q", code, message)
			}
		})
	}
}
