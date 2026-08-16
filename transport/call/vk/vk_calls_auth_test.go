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
	if err == nil || err.Error() != "captcha required (error_code=14)" {
		t.Fatalf("unexpected error: %v", err)
	}
}
