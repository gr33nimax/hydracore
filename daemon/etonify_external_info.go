package daemon

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/ntp"
	"golang.org/x/sync/singleflight"
)

const (
	externalInfoEndpoint = "https://cloudflare.com/cdn-cgi/trace"
	externalInfoTimeout  = 6 * time.Second
	externalInfoCacheTTL = 30 * time.Second
	externalInfoMaxBytes = 64 * 1024
)

type outboundExternalInfo struct {
	ip          string
	countryCode string
}

type outboundExternalInfoCacheEntry struct {
	info      outboundExternalInfo
	expiresAt time.Time
}

type outboundExternalInfoResolver struct {
	access   sync.Mutex
	cache    map[string]outboundExternalInfoCacheEntry
	requests singleflight.Group
	fetch    outboundExternalInfoFetcher
}

type outboundExternalInfoFetcher func(ctx context.Context, instanceContext context.Context, outbound adapter.Outbound) (outboundExternalInfo, error)

func newOutboundExternalInfoResolver() *outboundExternalInfoResolver {
	return &outboundExternalInfoResolver{
		cache: make(map[string]outboundExternalInfoCacheEntry),
		fetch: fetchOutboundExternalInfo,
	}
}

func (s *StartedService) LookupOutboundExternalInfo(ctx context.Context, request *OutboundExternalInfoRequest) (*OutboundExternalInfoResponse, error) {
	if request == nil || strings.TrimSpace(request.OutboundTag) == "" {
		return nil, os.ErrInvalid
	}
	s.serviceAccess.RLock()
	if s.serviceStatus.Status != ServiceStatus_STARTED || s.instance == nil {
		s.serviceAccess.RUnlock()
		return nil, os.ErrInvalid
	}
	boxService := s.instance

	outbound, err := resolveSelectedURLTestOutbound(boxService.instance.Outbound(), strings.TrimSpace(request.OutboundTag))
	s.serviceAccess.RUnlock()
	if err != nil {
		return nil, err
	}
	lookupContext, cancel := context.WithTimeout(ctx, externalInfoTimeout)
	stopInstanceCancellation := context.AfterFunc(boxService.ctx, cancel)
	defer stopInstanceCancellation()
	defer cancel()
	info, err := boxService.externalInfoResolver.lookup(lookupContext, boxService.ctx, outbound)
	if err != nil {
		return nil, err
	}
	return &OutboundExternalInfoResponse{Ip: info.ip, CountryCode: info.countryCode}, nil
}

func (r *outboundExternalInfoResolver) lookup(ctx context.Context, instanceContext context.Context, outbound adapter.Outbound) (outboundExternalInfo, error) {
	cacheKey := outbound.Tag()
	if cached, loaded := r.load(cacheKey, time.Now()); loaded {
		return cached, nil
	}
	resultChannel := r.requests.DoChan(cacheKey, func() (any, error) {
		if cached, loaded := r.load(cacheKey, time.Now()); loaded {
			return cached, nil
		}
		requestContext, cancel := context.WithTimeout(instanceContext, externalInfoTimeout)
		defer cancel()
		info, lookupErr := r.fetch(requestContext, instanceContext, outbound)
		if lookupErr != nil {
			return outboundExternalInfo{}, lookupErr
		}
		r.store(cacheKey, info, time.Now().Add(externalInfoCacheTTL))
		return info, nil
	})
	select {
	case <-ctx.Done():
		return outboundExternalInfo{}, ctx.Err()
	case result := <-resultChannel:
		if result.Err != nil {
			return outboundExternalInfo{}, result.Err
		}
		return result.Val.(outboundExternalInfo), nil
	}
}

func (r *outboundExternalInfoResolver) load(key string, now time.Time) (outboundExternalInfo, bool) {
	r.access.Lock()
	defer r.access.Unlock()
	entry, loaded := r.cache[key]
	if !loaded {
		return outboundExternalInfo{}, false
	}
	if !now.Before(entry.expiresAt) {
		delete(r.cache, key)
		return outboundExternalInfo{}, false
	}
	return entry.info, true
}

func (r *outboundExternalInfoResolver) store(key string, info outboundExternalInfo, expiresAt time.Time) {
	r.access.Lock()
	r.cache[key] = outboundExternalInfoCacheEntry{info: info, expiresAt: expiresAt}
	r.access.Unlock()
}

func fetchOutboundExternalInfo(ctx context.Context, instanceContext context.Context, outbound adapter.Outbound) (outboundExternalInfo, error) {
	resolveDialer := dialer.NewResolveDialer(instanceContext, outbound, true, "", adapter.DNSQueryOptions{}, 0)
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			return resolveDialer.DialContext(ctx, network, M.ParseSocksaddr(address))
		},
		TLSClientConfig: &tls.Config{
			Time:    ntp.TimeFuncFromContext(instanceContext),
			RootCAs: adapter.RootPoolFromContext(instanceContext),
		},
		DisableKeepAlives: true,
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, externalInfoEndpoint, nil)
	if err != nil {
		return outboundExternalInfo{}, err
	}
	request.Header.Set("Accept", "text/plain")
	request.Header.Set("User-Agent", "Etonify-Core")
	response, err := client.Do(request)
	if err != nil {
		return outboundExternalInfo{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return outboundExternalInfo{}, fmt.Errorf("external info service returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, externalInfoMaxBytes+1))
	if err != nil {
		return outboundExternalInfo{}, err
	}
	if len(body) > externalInfoMaxBytes {
		return outboundExternalInfo{}, fmt.Errorf("external info response is too large")
	}
	return parseOutboundExternalInfo(body)
}

func parseOutboundExternalInfo(content []byte) (outboundExternalInfo, error) {
	var info outboundExternalInfo
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if !found {
			continue
		}
		switch strings.TrimSpace(key) {
		case "ip":
			address, err := netip.ParseAddr(strings.TrimSpace(value))
			if err == nil {
				info.ip = address.String()
			}
		case "loc":
			countryCode := strings.ToUpper(strings.TrimSpace(value))
			if isValidCountryCode(countryCode) && countryCode != "XX" {
				info.countryCode = countryCode
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return outboundExternalInfo{}, err
	}
	if info.ip == "" {
		return outboundExternalInfo{}, fmt.Errorf("external info response does not contain a valid IP address")
	}
	return info, nil
}

func isValidCountryCode(countryCode string) bool {
	return len(countryCode) == 2 &&
		countryCode[0] >= 'A' && countryCode[0] <= 'Z' &&
		countryCode[1] >= 'A' && countryCode[1] <= 'Z'
}
