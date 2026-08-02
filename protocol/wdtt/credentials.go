// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/google/uuid"
	"golang.org/x/net/proxy"
)

const (
	vkConnectClientID     = "8093730"
	vkCallsAPIHost        = "api.vk.me"
	vkCallsAnonAPIVersion = "5.276"
	vkCallsUserAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
	maximumVKResponseSize = 1024 * 1024
	credentialLifetime    = 8 * time.Minute
)

var (
	errVKCaptchaRequired           = errors.New("VK anonymous call authentication requires captcha")
	errVKAccountCredentialsRequired = errors.New("HydraBox VK WebView credentials are required")
)

type contextDialer struct{ dialer coreDialer }

func (d *contextDialer) Dial(network string, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *contextDialer) DialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, parseDestination(address))
}

type credentialFetcher struct {
	dialer        coreDialer
	credentialRef string
	authMode      string

	mu      sync.Mutex
	entries map[string]*credentialEntry
}

type credentialEntry struct {
	credentials *turnCredentials
	expiresAt   time.Time
	fetching    bool
	ready       chan struct{}
}

func newCredentialFetcher(dialer coreDialer, credentialRef string, authMode string) *credentialFetcher {
	return &credentialFetcher{dialer: dialer, credentialRef: credentialRef, authMode: authMode, entries: make(map[string]*credentialEntry)}
}

func (f *credentialFetcher) get(ctx context.Context, hash string) (*turnCredentials, error) {
	if f.authMode == "account" || f.authMode == "auto" {
		if credentials, loaded := loadRuntimeAccountCredentials(f.credentialRef); loaded {
			return credentials, nil
		}
		if f.authMode == "account" {
			return nil, errVKAccountCredentialsRequired
		}
	}
	for {
		f.mu.Lock()
		entry := f.entries[hash]
		if entry != nil && entry.credentials != nil && time.Now().Before(entry.expiresAt) {
			credentials := cloneCredentials(entry.credentials)
			f.mu.Unlock()
			return credentials, nil
		}
		if entry != nil && entry.fetching {
			ready := entry.ready
			f.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
		entry = &credentialEntry{fetching: true, ready: make(chan struct{})}
		f.entries[hash] = entry
		f.mu.Unlock()

		credentials, err := fetchAnonymousTurnCredentials(ctx, f.dialer, hash)
		if errors.Is(err, errVKCaptchaRequired) && f.authMode == "auto" {
			if accountCredentials, loaded := loadRuntimeAccountCredentials(f.credentialRef); loaded {
				credentials = accountCredentials
				err = nil
			} else {
				err = errVKAccountCredentialsRequired
			}
		}
		f.mu.Lock()
		entry.fetching = false
		if err == nil {
			entry.credentials = cloneCredentials(credentials)
			entry.expiresAt = time.Now().Add(credentialLifetime)
		} else {
			delete(f.entries, hash)
		}
		close(entry.ready)
		f.mu.Unlock()
		return credentials, err
	}
}

func (f *credentialFetcher) invalidate(hash string) {
	f.mu.Lock()
	delete(f.entries, hash)
	f.mu.Unlock()
}

func cloneCredentials(credentials *turnCredentials) *turnCredentials {
	if credentials == nil {
		return nil
	}
	return &turnCredentials{
		username: credentials.username,
		password: credentials.password,
		urls:     append([]string(nil), credentials.urls...),
	}
}

func fetchAnonymousTurnCredentials(ctx context.Context, dialer coreDialer, hash string) (*turnCredentials, error) {
	directDialer := &contextDialer{dialer: dialer}
	client, err := tlsclient.NewHttpClient(
		tlsclient.NewNoopLogger(),
		tlsclient.WithTimeoutSeconds(20),
		tlsclient.WithClientProfile(profiles.Chrome_146),
		tlsclient.WithCookieJar(tlsclient.NewCookieJar()),
		tlsclient.WithNotFollowRedirects(),
		tlsclient.WithProxyDialerFactory(func(string, time.Duration, *net.TCPAddr, fhttp.Header, tlsclient.Logger) (proxy.ContextDialer, error) {
			return directDialer, nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create VK anonymous authentication client: %w", err)
	}
	defer client.CloseIdleConnections()

	deviceID := uuid.NewString()
	name, err := randomAnonymousName()
	if err != nil {
		return nil, err
	}
	joinLink := "https://vk.com/call/join/" + hash
	doRequest := func(step string, requestURL string) (map[string]any, error) {
		request, requestErr := fhttp.NewRequestWithContext(ctx, "POST", requestURL, bytes.NewReader(nil))
		if requestErr != nil {
			return nil, fmt.Errorf("prepare VK anonymous authentication %s: %w", step, requestErr)
		}
		request.Header.Set("User-Agent", vkCallsUserAgent)
		request.Header.Set("Accept", "*/*")
		request.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd")
		request.Header.Set("Accept-Language", "en-GB,en;q=0.9")
		response, requestErr := client.Do(request)
		if requestErr != nil {
			// tls-client errors can include the complete query string. Later VK
			// steps carry anonymous tokens and session credentials there, so only
			// the stable step name is allowed to escape into HydraCore logs.
			return nil, fmt.Errorf("VK anonymous authentication %s network request failed", step)
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("VK anonymous authentication %s returned HTTP %d", step, response.StatusCode)
		}
		body, requestErr := io.ReadAll(io.LimitReader(response.Body, maximumVKResponseSize+1))
		if requestErr != nil {
			return nil, fmt.Errorf("read VK anonymous authentication %s: %w", step, requestErr)
		}
		if len(body) > maximumVKResponseSize {
			return nil, fmt.Errorf("VK anonymous authentication %s response is too large", step)
		}
		var decoded map[string]any
		if requestErr = json.Unmarshal(body, &decoded); requestErr != nil {
			return nil, fmt.Errorf("decode VK anonymous authentication %s: %w", step, requestErr)
		}
		if requestErr = parseVKAPIError(decoded); requestErr != nil {
			return nil, fmt.Errorf("VK anonymous authentication %s: %w", step, requestErr)
		}
		return decoded, nil
	}

	response1, err := doRequest("auth.getAnonymToken", fmt.Sprintf(
		"https://%s/method/auth.getAnonymToken?v=%s&client_id=%s&link=%s&device_id=%s&anonymName=%s&lang=en",
		vkCallsAPIHost,
		vkCallsAnonAPIVersion,
		vkConnectClientID,
		url.QueryEscape(joinLink),
		url.QueryEscape(deviceID),
		url.QueryEscape(name),
	))
	if err != nil {
		return nil, err
	}
	anonymousToken, err := nestedString(response1, "response", "token")
	if err != nil {
		return nil, fmt.Errorf("parse VK anonymous token: %w", err)
	}

	response2, err := doRequest("messages.getCallPreview", fmt.Sprintf(
		"https://%s/method/messages.getCallPreview?v=%s&anonymous_token=%s&device_id=%s&extended=1&fields=first_name,last_name,photo_200&lang=en&link=%s",
		vkCallsAPIHost,
		vkCallsAnonAPIVersion,
		url.QueryEscape(anonymousToken),
		url.QueryEscape(deviceID),
		url.QueryEscape(joinLink),
	))
	if err != nil {
		return nil, err
	}
	userID, err := nestedNumber(response2, "response", "user_id")
	if err != nil {
		return nil, fmt.Errorf("parse VK anonymous user: %w", err)
	}
	secret, err := nestedString(response2, "response", "secret")
	if err != nil {
		return nil, fmt.Errorf("parse VK anonymous secret: %w", err)
	}

	response3, err := doRequest("messages.getAnonymCallToken", fmt.Sprintf(
		"https://%s/method/messages.getAnonymCallToken?v=%s&anonymous_token=%s&device_id=%s&link=%s&name=%s&user_id=%s&secret=%s&lang=en",
		vkCallsAPIHost,
		vkCallsAnonAPIVersion,
		url.QueryEscape(anonymousToken),
		url.QueryEscape(deviceID),
		url.QueryEscape(joinLink),
		url.QueryEscape(name),
		strconv.FormatInt(userID, 10),
		url.QueryEscape(secret),
	))
	if err != nil {
		return nil, err
	}
	okAnonymousToken, err := nestedString(response3, "response", "token")
	if err != nil {
		return nil, fmt.Errorf("parse VK Calls anonymous token: %w", err)
	}

	okDeviceID := uuid.NewString()
	response4, err := doRequest("auth.anonymLogin", "https://calls.okcdn.ru/fb.do?session_data="+
		url.QueryEscape(fmt.Sprintf(`{"version":2,"device_id":"%s","client_version":"1.0.1"}`, okDeviceID))+
		"&method=auth.anonymLogin&format=JSON&application_key=CGMMEJLGDIHBABABA")
	if err != nil {
		return nil, err
	}
	sessionKey, err := nestedString(response4, "session_key")
	if err != nil {
		return nil, fmt.Errorf("parse VK Calls session key: %w", err)
	}

	response5, err := doRequest("vchat.joinConversationByLink", fmt.Sprintf(
		"https://calls.okcdn.ru/fb.do?joinLink=%s&isVideo=false&protocolVersion=5&anonymToken=%s&method=vchat.joinConversationByLink&format=JSON&application_key=CGMMEJLGDIHBABABA&session_key=%s",
		url.QueryEscape(hash),
		url.QueryEscape(okAnonymousToken),
		url.QueryEscape(sessionKey),
	))
	if err != nil {
		return nil, err
	}
	if err = parseOKAPIError(response5); err != nil {
		return nil, fmt.Errorf("join VK anonymous call: %w", err)
	}
	username, err := nestedString(response5, "turn_server", "username")
	if err != nil {
		return nil, fmt.Errorf("parse VK TURN username: %w", err)
	}
	password, err := nestedString(response5, "turn_server", "credential")
	if err != nil {
		return nil, fmt.Errorf("parse VK TURN credential: %w", err)
	}
	turnURLs, err := parseTurnURLs(response5)
	if err != nil {
		return nil, err
	}
	return &turnCredentials{username: username, password: password, urls: turnURLs}, nil
}

func nestedString(value map[string]any, path ...string) (string, error) {
	var current any = value
	for _, key := range path {
		object, loaded := current.(map[string]any)
		if !loaded {
			return "", fmt.Errorf("expected object at %q", key)
		}
		current = object[key]
	}
	result, loaded := current.(string)
	if !loaded || result == "" {
		return "", fmt.Errorf("expected non-empty string")
	}
	return result, nil
}

func nestedNumber(value map[string]any, path ...string) (int64, error) {
	var current any = value
	for _, key := range path {
		object, loaded := current.(map[string]any)
		if !loaded {
			return 0, fmt.Errorf("expected object at %q", key)
		}
		current = object[key]
	}
	number, loaded := current.(float64)
	if !loaded || number <= 0 || number != float64(int64(number)) {
		return 0, fmt.Errorf("expected positive integer")
	}
	return int64(number), nil
}

func parseVKAPIError(response map[string]any) error {
	errorObject, loaded := response["error"].(map[string]any)
	if !loaded {
		return nil
	}
	code, _ := errorObject["error_code"].(float64)
	if int(code) == 14 {
		return errVKCaptchaRequired
	}
	// Remote error strings are deliberately not propagated: an upstream can
	// echo anonymous tokens or session material supplied in the query string.
	return fmt.Errorf("VK API error (code %d)", int(code))
}

func parseOKAPIError(response map[string]any) error {
	code, loaded := response["error_code"].(float64)
	if !loaded || code == 0 {
		return nil
	}
	return fmt.Errorf("OK Calls API error (code %d)", int(code))
}

func parseTurnURLs(response map[string]any) ([]string, error) {
	turnServer, loaded := response["turn_server"].(map[string]any)
	if !loaded {
		return nil, fmt.Errorf("VK TURN response is missing turn_server")
	}
	rawURLs, loaded := turnServer["urls"].([]any)
	if !loaded {
		return nil, fmt.Errorf("VK TURN response is missing relay URLs")
	}
	seen := make(map[string]struct{})
	addresses := make([]string, 0, len(rawURLs))
	for _, rawValue := range rawURLs {
		rawURL, loaded := rawValue.(string)
		if !loaded {
			continue
		}
		lower := strings.ToLower(rawURL)
		if !strings.HasPrefix(lower, "turn:") || strings.HasPrefix(lower, "turns:") {
			continue
		}
		addressAndQuery := rawURL[len("turn:"):]
		address, rawQuery, _ := strings.Cut(addressAndQuery, "?")
		if rawQuery != "" {
			query, queryErr := url.ParseQuery(rawQuery)
			if queryErr != nil || !strings.EqualFold(query.Get("transport"), "udp") {
				continue
			}
		}
		host, port, splitErr := net.SplitHostPort(address)
		if splitErr != nil || host == "" {
			continue
		}
		portNumber, parseErr := strconv.ParseUint(port, 10, 16)
		if parseErr != nil || portNumber == 0 {
			continue
		}
		normalized := net.JoinHostPort(host, strconv.FormatUint(portNumber, 10))
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		addresses = append(addresses, normalized)
		if len(addresses) == 16 {
			break
		}
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("VK TURN response contains no usable UDP relay")
	}
	return addresses, nil
}

func randomAnonymousName() (string, error) {
	firstNames := [...]string{"Alex", "Max", "Nikita", "Sasha", "Dima", "Misha", "Anna", "Mila"}
	lastNames := [...]string{"Ivanov", "Petrov", "Smirnov", "Volkov", "Sokolov", "Orlov"}
	var randomBytes [8]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return "", fmt.Errorf("generate VK anonymous identity: %w", err)
	}
	first := firstNames[binary.BigEndian.Uint32(randomBytes[0:4])%uint32(len(firstNames))]
	last := lastNames[binary.BigEndian.Uint32(randomBytes[4:8])%uint32(len(lastNames))]
	return first + " " + last, nil
}
