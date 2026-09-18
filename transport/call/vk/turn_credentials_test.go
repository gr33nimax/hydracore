package vk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTURNCredentialProviderInvalidate(t *testing.T) {
	t.Parallel()
	provider := NewTURNCredentialProvider(nil, nil)
	joinLink := "https://vk.com/call/join/test"
	provider.cache[joinLink] = cachedTURNCredentials{
		server: TurnServer{
			URLs:       []string{"turn:example.invalid:3478"},
			Username:   "cached-user",
			Credential: "cached-secret",
		},
		expires: time.Now().Add(time.Minute),
	}

	server, loaded := provider.cached(joinLink)
	require.True(t, loaded)
	require.Equal(t, "cached-user", server.Username)

	provider.Invalidate(joinLink)
	_, loaded = provider.cached(joinLink)
	require.True(t, loaded)
	provider.Invalidate(joinLink)
	_, loaded = provider.cached(joinLink)
	require.True(t, loaded)
	provider.Invalidate(joinLink)
	_, loaded = provider.cached(joinLink)
	require.False(t, loaded)
}

func TestTURNCredentialProviderKeepsFreshCredentials(t *testing.T) {
	t.Parallel()
	provider := NewTURNCredentialProvider(nil, nil)
	joinLink := "https://vk.com/call/join/fresh"
	provider.cache[joinLink] = cachedTURNCredentials{
		server:       TurnServer{Username: "fresh-user"},
		expires:      time.Now().Add(time.Minute),
		refreshAfter: time.Now().Add(time.Minute),
	}

	provider.Invalidate(joinLink)
	provider.Invalidate(joinLink)
	provider.Invalidate(joinLink)
	server, loaded := provider.cached(joinLink)
	require.True(t, loaded)
	require.Equal(t, "fresh-user", server.Username)
}

func TestTURNCredentialProviderFloodControlCooldown(t *testing.T) {
	t.Parallel()
	provider := NewTURNCredentialProvider(nil, nil)
	provider.limiter = newVKControlPlaneLimiter()
	now := time.Now()
	provider.activateFloodControl(now)
	require.ErrorIs(t, provider.floodControlError(now.Add(time.Second)), ErrVKFloodControl)
	require.NoError(t, provider.floodControlError(now.Add(vkFloodControlCooldown)))
	provider.cache["cached"] = cachedTURNCredentials{
		server: TurnServer{Username: "still-valid"},
		expires: now.Add(time.Minute),
	}
	server, loaded := provider.cached("cached")
	require.True(t, loaded)
	require.Equal(t, "still-valid", server.Username)
	require.ErrorIs(t, provider.floodControlError(now.Add(time.Second)), ErrVKFloodControl)
}
