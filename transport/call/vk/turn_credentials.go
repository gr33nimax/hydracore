package vk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/sync/singleflight"
)

const turnCredentialCacheTTL = 8 * time.Minute

type cachedTURNCredentials struct {
	server  TurnServer
	expires time.Time
}

type TURNCredentialProvider struct {
	dialer N.Dialer
	logger logger.ContextLogger
	mu     sync.Mutex
	cache  map[string]cachedTURNCredentials
	group  singleflight.Group
}

func NewTURNCredentialProvider(dialer N.Dialer, log logger.ContextLogger) *TURNCredentialProvider {
	if log == nil {
		log = logger.NOP()
	}
	return &TURNCredentialProvider{
		dialer: dialer,
		logger: log,
		cache:  make(map[string]cachedTURNCredentials),
	}
}

func (p *TURNCredentialProvider) Fetch(ctx context.Context, joinLink string) (TurnServer, error) {
	if server, loaded := p.cached(joinLink); loaded {
		return server, nil
	}
	result := p.group.DoChan(joinLink, func() (any, error) {
		if server, loaded := p.cached(joinLink); loaded {
			return server, nil
		}
		server, err := FetchTURNCredentials(ctx, p.dialer, joinLink, "HydraCore", p.logger)
		if err != nil {
			return TurnServer{}, err
		}
		p.mu.Lock()
		p.cache[joinLink] = cachedTURNCredentials{server: cloneTurnServer(server), expires: time.Now().Add(turnCredentialCacheTTL)}
		p.mu.Unlock()
		return server, nil
	})
	select {
	case response := <-result:
		if response.Err != nil {
			return TurnServer{}, response.Err
		}
		return cloneTurnServer(response.Val.(TurnServer)), nil
	case <-ctx.Done():
		return TurnServer{}, ctx.Err()
	}
}

func (p *TURNCredentialProvider) cached(joinLink string) (TurnServer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, loaded := p.cache[joinLink]
	if !loaded || time.Now().After(entry.expires) {
		delete(p.cache, joinLink)
		return TurnServer{}, false
	}
	return cloneTurnServer(entry.server), true
}

func cloneTurnServer(server TurnServer) TurnServer {
	server.URLs = append([]string(nil), server.URLs...)
	return server
}

// FetchTURNCredentials joins only the VK signaling control plane. It does not
// create WebRTC media tracks or connect to the VK SFU; the returned TURN
// allocation is used to relay DTLS datagrams directly to the native VPS.
func FetchTURNCredentials(
	ctx context.Context,
	dialer N.Dialer,
	joinLink string,
	displayName string,
	log logger.ContextLogger,
) (TurnServer, error) {
	authJSON, err := RunVKAuthContext(ctx, dialer, joinLink, displayName, log)
	if err != nil {
		return TurnServer{}, fmt.Errorf("vk TURN auth: %w", err)
	}
	var params VKAuthParams
	if err = json.Unmarshal([]byte(authJSON), &params); err != nil {
		return TurnServer{}, errors.New("vk TURN auth returned invalid parameters")
	}
	if params.APIBaseURL == "" || params.SessionKey == "" || params.ApplicationKey == "" || params.JoinLink == "" {
		return TurnServer{}, errors.New("vk TURN auth returned incomplete parameters")
	}
	mediaSettings := `{"isAudioEnabled":false,"isVideoEnabled":true,"isScreenSharingEnabled":false}`
	body := url.Values{
		"method":          {"vchat.joinConversationByLink"},
		"session_key":     {params.SessionKey},
		"application_key": {params.ApplicationKey},
		"joinLink":        {params.JoinLink},
		"anonymToken":     {params.AnonymToken},
		"isVideo":         {"true"},
		"isAudio":         {"false"},
		"mediaSettings":   {mediaSettings},
		"format":          {"json"},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, params.APIBaseURL, strings.NewReader(body.Encode()))
	if err != nil {
		return TurnServer{}, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("User-Agent", common.UserAgent)
	client := common.HttpClient(dialer)
	client.Timeout = 20 * time.Second
	response, err := client.Do(request)
	if err != nil {
		return TurnServer{}, fmt.Errorf("vk TURN join: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return TurnServer{}, fmt.Errorf("vk TURN join returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return TurnServer{}, err
	}
	var joined VKJoinResponse
	if err = json.Unmarshal(raw, &joined); err != nil {
		return TurnServer{}, errors.New("vk TURN join returned invalid JSON")
	}
	server := TurnServer{
		URLs:       append([]string(nil), joined.TurnServer.URLs...),
		Username:   joined.TurnServer.Username,
		Credential: joined.TurnServer.Credential,
	}
	if len(server.URLs) == 0 || server.Username == "" || server.Credential == "" {
		return TurnServer{}, errors.New("vk TURN join returned incomplete credentials")
	}
	return server, nil
}
