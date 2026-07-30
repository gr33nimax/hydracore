package daemon

import (
	"context"
	"errors"
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
