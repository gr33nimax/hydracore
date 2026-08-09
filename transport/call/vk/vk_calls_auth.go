package vk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

const (
	vkCallsClientID      = "8093730"
	vkCallsAPIVersion    = "5.276"
	vkCallsApplicationKey = "CGMMEJLGDIHBABABA"
	vkCallsAppVersion    = "1.0.1"
	vkCallsProtocolVersion = "5"
)

var (
	vkCallsAPIBaseURL = "https://api.vk.me/method"
	vkCallsOKBaseURL  = "https://calls.okcdn.ru/fb.do"
)

// RunVKAuth first uses the anonymous VK Calls flow used by current VK clients.
// It normally avoids the Smart Captcha gate hit by the legacy calls.* flow.
// The request sequence is adapted from qWDTT's GPL-3.0 implementation:
// https://github.com/SpaceNeuroX/proxy-turn-vk-android
func RunVKAuth(dialer N.Dialer, joinLink, displayName string, log logger.ContextLogger) (string, error) {
	return RunVKAuthContext(context.Background(), dialer, joinLink, displayName, log)
}

func RunVKAuthContext(ctx context.Context, dialer N.Dialer, joinLink, displayName string, log logger.ContextLogger) (string, error) {
	authJSON, err := runVKCallsAuth(ctx, dialer, joinLink, displayName, log)
	if err == nil {
		log.Info("vk-auth: authenticated via VK Calls path")
		return authJSON, nil
	}
	log.Warn(fmt.Sprintf("vk-auth: VK Calls path failed, falling back to legacy: %v", err))
	return runVKLegacyAuthContext(ctx, dialer, joinLink, displayName, log)
}

func runVKCallsAuth(ctx context.Context, dialer N.Dialer, joinLink, displayName string, log logger.ContextLogger) (string, error) {
	joinToken := extractJoinToken(joinLink)
	if joinToken == "" {
		return "", fmt.Errorf("empty VK call join token")
	}
	if displayName == "" {
		displayName = "Joiner"
	}

	client := common.HttpClient(dialer)
	client.Timeout = 20 * time.Second
	deviceID := uuid.NewString()
	canonicalJoinLink := "https://vk.com/call/join/" + joinToken

	log.Info("vk-auth: trying VK Calls anonymous path")
	step1, err := vkCallsPost(ctx, client, vkCallsAPIBaseURL+"/auth.getAnonymToken", url.Values{
		"v":         {vkCallsAPIVersion},
		"client_id": {vkCallsClientID},
		"link":      {canonicalJoinLink},
		"device_id": {deviceID},
		"anonymName": {displayName},
		"lang":      {"en"},
	})
	if err != nil {
		return "", fmt.Errorf("auth.getAnonymToken: %w", err)
	}
	if err := vkCallsResponseError(step1); err != nil {
		return "", fmt.Errorf("auth.getAnonymToken: %w", err)
	}
	anonymousToken, ok := vkCallsNestedString(step1, "response", "token")
	if !ok {
		return "", fmt.Errorf("auth.getAnonymToken: missing response.token")
	}

	step2, err := vkCallsPost(ctx, client, vkCallsAPIBaseURL+"/messages.getCallPreview", url.Values{
		"v":               {vkCallsAPIVersion},
		"anonymous_token": {anonymousToken},
		"device_id":       {deviceID},
		"extended":        {"1"},
		"fields":          {"first_name,last_name,photo_200"},
		"lang":            {"en"},
		"link":            {canonicalJoinLink},
	})
	if err != nil {
		return "", fmt.Errorf("messages.getCallPreview: %w", err)
	}
	if err := vkCallsResponseError(step2); err != nil {
		return "", fmt.Errorf("messages.getCallPreview: %w", err)
	}
	userID, ok := vkCallsNestedNumberString(step2, "response", "user_id")
	if !ok {
		return "", fmt.Errorf("messages.getCallPreview: missing response.user_id")
	}
	secret, ok := vkCallsNestedString(step2, "response", "secret")
	if !ok {
		return "", fmt.Errorf("messages.getCallPreview: missing response.secret")
	}

	step3, err := vkCallsPost(ctx, client, vkCallsAPIBaseURL+"/messages.getAnonymCallToken", url.Values{
		"v":               {vkCallsAPIVersion},
		"anonymous_token": {anonymousToken},
		"device_id":       {deviceID},
		"link":            {canonicalJoinLink},
		"name":            {displayName},
		"user_id":         {userID},
		"secret":          {secret},
		"lang":            {"en"},
	})
	if err != nil {
		return "", fmt.Errorf("messages.getAnonymCallToken: %w", err)
	}
	if err := vkCallsResponseError(step3); err != nil {
		return "", fmt.Errorf("messages.getAnonymCallToken: %w", err)
	}
	callToken, ok := vkCallsNestedString(step3, "response", "token")
	if !ok {
		return "", fmt.Errorf("messages.getAnonymCallToken: missing response.token")
	}

	sessionData, err := json.Marshal(map[string]interface{}{
		"version":        2,
		"device_id":      uuid.NewString(),
		"client_version": vkCallsAppVersion,
	})
	if err != nil {
		return "", fmt.Errorf("auth.anonymLogin session data: %w", err)
	}
	step4, err := vkCallsPost(ctx, client, vkCallsOKBaseURL, url.Values{
		"session_data":    {string(sessionData)},
		"method":          {"auth.anonymLogin"},
		"format":          {"JSON"},
		"application_key": {vkCallsApplicationKey},
	})
	if err != nil {
		return "", fmt.Errorf("auth.anonymLogin: %w", err)
	}
	if err := vkCallsOKResponseError(step4); err != nil {
		return "", fmt.Errorf("auth.anonymLogin: %w", err)
	}
	sessionKey, ok := vkCallsNestedString(step4, "session_key")
	if !ok {
		return "", fmt.Errorf("auth.anonymLogin: missing session_key")
	}

	result := VKAuthParams{
		SessionKey:      sessionKey,
		ApplicationKey:  vkCallsApplicationKey,
		APIBaseURL:      vkCallsOKBaseURL,
		JoinLink:        joinToken,
		AnonymToken:     callToken,
		AppVersion:      vkCallsAppVersion,
		ProtocolVersion: vkCallsProtocolVersion,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("encode VK Calls auth result: %w", err)
	}
	return string(encoded), nil
}

func vkCallsPost(ctx context.Context, client *http.Client, endpoint string, query url.Values) (map[string]interface{}, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	parsed.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", common.UserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-GB,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	return result, nil
}

func vkCallsResponseError(response map[string]interface{}) error {
	errorObject, ok := response["error"].(map[string]interface{})
	if !ok {
		return nil
	}
	code, _ := vkCallsNumberString(errorObject["error_code"])
	message, _ := errorObject["error_msg"].(string)
	if code == "14" {
		return fmt.Errorf("captcha required (error_code=14)")
	}
	if code == "" && message == "" {
		return nil
	}
	return fmt.Errorf("VK API error_code=%s message=%s", code, message)
}

func vkCallsOKResponseError(response map[string]interface{}) error {
	code, ok := vkCallsNumberString(response["error_code"])
	if !ok || code == "0" {
		return nil
	}
	message, _ := response["error_msg"].(string)
	return fmt.Errorf("OK API error_code=%s message=%s", code, message)
}

func vkCallsNestedString(value map[string]interface{}, keys ...string) (string, bool) {
	var current interface{} = value
	for _, key := range keys {
		object, ok := current.(map[string]interface{})
		if !ok {
			return "", false
		}
		current, ok = object[key]
		if !ok {
			return "", false
		}
	}
	result, ok := current.(string)
	return strings.TrimSpace(result), ok && strings.TrimSpace(result) != ""
}

func vkCallsNestedNumberString(value map[string]interface{}, keys ...string) (string, bool) {
	var current interface{} = value
	for _, key := range keys {
		object, ok := current.(map[string]interface{})
		if !ok {
			return "", false
		}
		current, ok = object[key]
		if !ok {
			return "", false
		}
	}
	return vkCallsNumberString(current)
}

func vkCallsNumberString(value interface{}) (string, bool) {
	switch typed := value.(type) {
	case float64:
		return strconv.FormatInt(int64(typed), 10), true
	case float32:
		return strconv.FormatInt(int64(typed), 10), true
	case int:
		return strconv.Itoa(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case json.Number:
		return typed.String(), true
	case string:
		trimmed := strings.TrimSpace(typed)
		return trimmed, trimmed != ""
	default:
		return "", false
	}
}
