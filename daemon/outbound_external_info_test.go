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

func TestFetchOutboundExternalInfoRejectsOversizedResponse(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		huge := make([]byte, externalInfoMaxBytes+100)
		for i := range huge {
			huge[i] = 'a'
		}
		_, _ = writer.Write(huge)
	}))
	defer server.Close()

	sources := []outboundExternalInfoSource{
		{name: "oversized", endpoint: server.URL, parse: parsePlainExternalIP},
	}
	_, err := fetchOutboundExternalInfoFromSources(context.Background(), server.Client(), sources)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected oversized response error, got: %v", err)
	}
}

func TestFetchOutboundExternalInfoHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sources := []outboundExternalInfoSource{
		{name: "cancelled", endpoint: server.URL, parse: parsePlainExternalIP},
	}
	_, err := fetchOutboundExternalInfoFromSources(ctx, server.Client(), sources)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestLookupOutboundExternalInfoRejectsInvalidRequest(t *testing.T) {
	t.Parallel()
	service := &StartedService{}
	if _, err := service.LookupOutboundExternalInfo(context.Background(), nil); err == nil {
		t.Fatal("nil request must be rejected")
	}
	if _, err := service.LookupOutboundExternalInfo(context.Background(), &OutboundExternalInfoRequest{OutboundTag: "  "}); err == nil {
		t.Fatal("blank outbound tag must be rejected")
	}
}
