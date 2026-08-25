package vk

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	HC "github.com/sagernet/sing-box/common/hydracore"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func TestSolveVKCaptchaPublishesChallengeAndReturnsToken(t *testing.T) {
	HC.ResetRuntimeTransportState()
	defer HC.ResetRuntimeTransportState()
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()

	result := make(chan struct {
		token string
		err   error
	}, 1)
	go func() {
		token, err := solveVKCaptcha(t.Context(), &vkCaptchaError{redirectURI: upstream.URL}, nil, logger.NOP())
		result <- struct {
			token string
			err   error
		}{token, err}
	}()

	var challenge *HC.RuntimeChallenge
	require.Eventually(t, func() bool {
		challenge = HC.CurrentRuntimeChallenge()
		return challenge != nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, HC.TransportStateWaitingUser, HC.CurrentTransportHealth().State)

	response, err := http.PostForm(challenge.URL+"local-captcha-result", url.Values{"token": {"success-token"}})
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	select {
	case outcome := <-result:
		require.NoError(t, outcome.err)
		require.Equal(t, "success-token", outcome.token)
	case <-time.After(time.Second):
		t.Fatal("captcha result was not delivered")
	}
	require.Nil(t, HC.CurrentRuntimeChallenge())
}

func TestSolveVKCaptchaSerializesInteractiveChallenges(t *testing.T) {
	HC.ResetRuntimeTransportState()
	defer HC.ResetRuntimeTransportState()
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()

	type outcome struct {
		token string
		err   error
	}
	results := make(chan outcome, 2)
	start := func() {
		go func() {
			token, err := solveVKCaptcha(t.Context(), &vkCaptchaError{redirectURI: upstream.URL}, nil, logger.NOP())
			results <- outcome{token: token, err: err}
		}()
	}
	start()
	var first *HC.RuntimeChallenge
	require.Eventually(t, func() bool {
		first = HC.CurrentRuntimeChallenge()
		return first != nil
	}, time.Second, 10*time.Millisecond)
	start()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, first.ID, HC.CurrentRuntimeChallenge().ID)

	response, err := http.PostForm(first.URL+"local-captcha-result", url.Values{"token": {"first"}})
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	firstOutcome := <-results
	require.NoError(t, firstOutcome.err)
	require.Equal(t, "first", firstOutcome.token)

	var second *HC.RuntimeChallenge
	require.Eventually(t, func() bool {
		second = HC.CurrentRuntimeChallenge()
		return second != nil && second.ID != first.ID
	}, time.Second, 10*time.Millisecond)
	response, err = http.PostForm(second.URL+"local-captcha-result", url.Values{"token": {"second"}})
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	secondOutcome := <-results
	require.NoError(t, secondOutcome.err)
	require.Equal(t, "second", secondOutcome.token)
}
