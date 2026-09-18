package vk

import (
	"errors"
	"testing"
)

func TestVKCallsNestedValues(t *testing.T) {
	response := map[string]interface{}{
		"response": map[string]interface{}{
			"token":   " call-token ",
			"user_id": float64(123456),
		},
	}
	token, ok := vkCallsNestedString(response, "response", "token")
	if !ok || token != "call-token" {
		t.Fatalf("unexpected token: %q, %v", token, ok)
	}
	userID, ok := vkCallsNestedNumberString(response, "response", "user_id")
	if !ok || userID != "123456" {
		t.Fatalf("unexpected user id: %q, %v", userID, ok)
	}
}

func TestVKCallsResponseErrorDetectsFloodControl(t *testing.T) {
	err := vkCallsResponseError(map[string]interface{}{
		"error": map[string]interface{}{
			"error_code": float64(9),
			"error_msg":  "Flood control",
		},
	})
	if !errors.Is(err, ErrVKFloodControl) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVKCallsResponseErrorDetectsCaptcha(t *testing.T) {
	err := vkCallsResponseError(map[string]interface{}{
		"error": map[string]interface{}{
			"error_code": float64(14),
			"error_msg":  "Captcha needed",
		},
	})
	if !errors.Is(err, ErrVKCaptchaRequired) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseVKCaptchaErrorPreservesNumericRetryFields(t *testing.T) {
	captcha := parseVKCaptchaError(map[string]interface{}{
		"redirect_uri":    "https://id.vk.ru/captcha?session_token=test",
		"captcha_sid":     float64(42),
		"captcha_ts":      float64(7),
		"captcha_attempt": float64(3),
	})
	if captcha == nil {
		t.Fatal("captcha was not detected")
	}
	if captcha.captchaSid != "42" || captcha.captchaTs != "7" || captcha.captchaAttempt != "3" {
		t.Fatalf("unexpected captcha fields: %#v", captcha)
	}
}
