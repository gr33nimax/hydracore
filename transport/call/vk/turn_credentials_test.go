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
	require.False(t, loaded)
}
