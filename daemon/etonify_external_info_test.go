package daemon

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func TestParseOutboundExternalInfo(t *testing.T) {
	t.Parallel()
	info, err := parseOutboundExternalInfo([]byte("fl=1\nip=2001:db8::1\nloc=se\n"))
	if err != nil {
		t.Fatal(err)
	}
	if info.ip != "2001:db8::1" || info.countryCode != "SE" {
		t.Fatalf("unexpected external info: %#v", info)
	}
}

func TestParseOutboundExternalInfoRejectsMissingIP(t *testing.T) {
	t.Parallel()
	if _, err := parseOutboundExternalInfo([]byte("loc=US\n")); err == nil {
		t.Fatal("missing IP must be rejected")
	}
}

func TestParseOutboundExternalInfoIgnoresUnknownCountry(t *testing.T) {
	t.Parallel()
	info, err := parseOutboundExternalInfo([]byte("ip=203.0.113.4\nloc=XX\n"))
	if err != nil {
		t.Fatal(err)
	}
	if info.countryCode != "" {
		t.Fatalf("unexpected country code: %q", info.countryCode)
	}
}

func TestParseOutboundExternalInfoIgnoresMalformedCountry(t *testing.T) {
	t.Parallel()
	info, err := parseOutboundExternalInfo([]byte("ip=203.0.113.4\nloc=1!\n"))
	if err != nil {
		t.Fatal(err)
	}
	if info.countryCode != "" {
		t.Fatalf("unexpected country code: %q", info.countryCode)
	}
}

func TestParsePlainExternalIP(t *testing.T) {
	t.Parallel()
	info, err := parsePlainExternalIP([]byte(" 2001:db8::2\n"))
	if err != nil {
		t.Fatal(err)
	}
	if info != (outboundExternalInfo{ip: "2001:db8::2"}) {
		t.Fatalf("unexpected plain external IP: %#v", info)
	}
	if _, err = parsePlainExternalIP([]byte("not-an-ip")); err == nil {
		t.Fatal("malformed plain external IP must be rejected")
	}
}

func TestFetchOutboundExternalInfoUsesFallbackInOrder(t *testing.T) {
	t.Parallel()
	var access sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		access.Lock()
		requests = append(requests, request.URL.Path)
		access.Unlock()
		switch request.URL.Path {
		case "/primary":
			http.Error(writer, "unavailable", http.StatusBadGateway)
		case "/fallback":
			_, _ = writer.Write([]byte("203.0.113.9\n"))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	sources := []outboundExternalInfoSource{
		{name: "primary", endpoint: server.URL + "/primary", parse: parseOutboundExternalInfo},
		{name: "fallback", endpoint: server.URL + "/fallback", parse: parsePlainExternalIP},
	}
	info, err := fetchOutboundExternalInfoFromSources(context.Background(), server.Client(), sources)
	if err != nil {
		t.Fatal(err)
	}
	if info != (outboundExternalInfo{ip: "203.0.113.9"}) {
		t.Fatalf("unexpected fallback result: %#v", info)
	}
	access.Lock()
	defer access.Unlock()
	if strings.Join(requests, ",") != "/primary,/fallback" {
		t.Fatalf("unexpected request order: %v", requests)
	}
}

func TestOutboundExternalInfoProductionSourcesUseHTTPS(t *testing.T) {
	t.Parallel()
	if len(outboundExternalInfoSources) < 2 {
		t.Fatalf("external info lookup needs an independent fallback source")
	}
	for _, source := range outboundExternalInfoSources {
		if !strings.HasPrefix(source.endpoint, "https://") {
			t.Fatalf("external info source %q is not HTTPS: %s", source.name, source.endpoint)
		}
	}
}

func TestFetchOutboundExternalInfoStopsAfterPrimarySuccess(t *testing.T) {
	t.Parallel()
	var fallbackCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/fallback" {
			fallbackCalls.Add(1)
		}
		_, _ = writer.Write([]byte("ip=203.0.113.10\nloc=FI\n"))
	}))
	defer server.Close()

	sources := []outboundExternalInfoSource{
		{name: "primary", endpoint: server.URL + "/primary", parse: parseOutboundExternalInfo},
		{name: "fallback", endpoint: server.URL + "/fallback", parse: parsePlainExternalIP},
	}
	info, err := fetchOutboundExternalInfoFromSources(context.Background(), server.Client(), sources)
	if err != nil {
		t.Fatal(err)
	}
	if info != (outboundExternalInfo{ip: "203.0.113.10", countryCode: "FI"}) {
		t.Fatalf("unexpected primary result: %#v", info)
	}
	if fallbackCalls.Load() != 0 {
		t.Fatalf("fallback was called %d times after primary success", fallbackCalls.Load())
	}
}

func TestFetchOutboundExternalInfoReportsAllSourceFailures(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/primary" {
			_, _ = writer.Write([]byte("not-a-trace"))
			return
		}
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	sources := []outboundExternalInfoSource{
		{name: "primary", endpoint: server.URL + "/primary", parse: parseOutboundExternalInfo},
		{name: "fallback", endpoint: server.URL + "/fallback", parse: parsePlainExternalIP},
	}
	_, err := fetchOutboundExternalInfoFromSources(context.Background(), server.Client(), sources)
	if err == nil || !strings.Contains(err.Error(), "primary:") || !strings.Contains(err.Error(), "fallback:") {
		t.Fatalf("unexpected combined lookup error: %v", err)
	}
}

func TestOutboundExternalInfoResolverSharesAndCachesLookup(t *testing.T) {
	t.Parallel()
	resolver := newOutboundExternalInfoResolver()
	var fetchCount atomic.Int32
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	resolver.fetch = func(ctx context.Context, instanceContext context.Context, outbound adapter.Outbound) (outboundExternalInfo, error) {
		if fetchCount.Add(1) == 1 {
			close(fetchStarted)
		}
		select {
		case <-ctx.Done():
			return outboundExternalInfo{}, ctx.Err()
		case <-releaseFetch:
			return outboundExternalInfo{ip: "203.0.113.8", countryCode: "SE"}, nil
		}
	}
	outbound := &testURLTestOutbound{tag: "proxy"}
	instanceContext, cancelInstance := context.WithCancel(context.Background())
	defer cancelInstance()

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := resolver.lookup(firstContext, instanceContext, outbound)
		firstResult <- err
	}()
	<-fetchStarted
	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter returned %v", err)
	}

	secondResult := make(chan struct {
		info outboundExternalInfo
		err  error
	}, 1)
	go func() {
		info, err := resolver.lookup(context.Background(), instanceContext, outbound)
		secondResult <- struct {
			info outboundExternalInfo
			err  error
		}{info: info, err: err}
	}()
	close(releaseFetch)
	result := <-secondResult
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.info.ip != "203.0.113.8" || result.info.countryCode != "SE" {
		t.Fatalf("unexpected lookup result: %#v", result.info)
	}
	if fetchCount.Load() != 1 {
		t.Fatalf("shared lookup fetched %d times", fetchCount.Load())
	}

	cached, err := resolver.lookup(context.Background(), instanceContext, outbound)
	if err != nil {
		t.Fatal(err)
	}
	if cached != result.info || fetchCount.Load() != 1 {
		t.Fatalf("cached lookup was not reused: info=%#v fetches=%d", cached, fetchCount.Load())
	}
}

func TestOutboundExternalInfoResolverHonorsInstanceCancellation(t *testing.T) {
	t.Parallel()
	resolver := newOutboundExternalInfoResolver()
	resolver.fetch = func(ctx context.Context, instanceContext context.Context, outbound adapter.Outbound) (outboundExternalInfo, error) {
		<-ctx.Done()
		return outboundExternalInfo{}, ctx.Err()
	}
	instanceContext, cancelInstance := context.WithCancel(context.Background())
	cancelInstance()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := resolver.lookup(ctx, instanceContext, &testURLTestOutbound{tag: "proxy"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("instance cancellation returned %v", err)
	}
}

func TestOutboundExternalInfoResolverFallsBackToRecentStaleCache(t *testing.T) {
	t.Parallel()
	resolver := newOutboundExternalInfoResolver()
	resolver.fetch = func(context.Context, context.Context, adapter.Outbound) (outboundExternalInfo, error) {
		return outboundExternalInfo{}, errors.New("all external info services failed")
	}
	want := outboundExternalInfo{ip: "203.0.113.11", countryCode: "SE"}
	resolver.store("proxy", want, time.Now().Add(-externalInfoCacheTTL-time.Second))

	got, err := resolver.lookup(context.Background(), context.Background(), &testURLTestOutbound{tag: "proxy"})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("unexpected stale cache result: got=%#v want=%#v", got, want)
	}
}

func TestOutboundExternalInfoResolverRejectsExpiredStaleCache(t *testing.T) {
	t.Parallel()
	resolver := newOutboundExternalInfoResolver()
	resolver.fetch = func(context.Context, context.Context, adapter.Outbound) (outboundExternalInfo, error) {
		return outboundExternalInfo{}, errors.New("all external info services failed")
	}
	resolver.store("proxy", outboundExternalInfo{ip: "203.0.113.12", countryCode: "SE"}, time.Now().Add(-externalInfoStaleTTL-time.Second))

	if _, err := resolver.lookup(context.Background(), context.Background(), &testURLTestOutbound{tag: "proxy"}); err == nil {
		t.Fatal("expired stale cache must not hide a lookup failure")
	}
}

func TestOutboundExternalInfoResolverPreservesCountryOnlyForSameIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		fetchedIP   string
		wantCountry string
	}{
		{name: "same IP", fetchedIP: "203.0.113.13", wantCountry: "FI"},
		{name: "changed IP", fetchedIP: "203.0.113.14", wantCountry: ""},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			resolver := newOutboundExternalInfoResolver()
			resolver.store("proxy", outboundExternalInfo{ip: "203.0.113.13", countryCode: "FI"}, time.Now().Add(-externalInfoCacheTTL-time.Second))
			resolver.fetch = func(context.Context, context.Context, adapter.Outbound) (outboundExternalInfo, error) {
				return outboundExternalInfo{ip: test.fetchedIP}, nil
			}
			info, err := resolver.lookup(context.Background(), context.Background(), &testURLTestOutbound{tag: "proxy"})
			if err != nil {
				t.Fatal(err)
			}
			if info.countryCode != test.wantCountry {
				t.Fatalf("unexpected country for %s: %q", test.fetchedIP, info.countryCode)
			}
		})
	}
}
