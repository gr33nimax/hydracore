package wdtt

import (
	"strings"
	"testing"
)

func TestNestedNumberAcceptsSignedVKUserIDs(t *testing.T) {
	for name, test := range map[string]struct {
		value any
		want  int64
	}{
		"positive JSON number": {value: float64(123), want: 123},
		"negative JSON number": {value: float64(-123), want: -123},
		"positive JSON string": {value: "123", want: 123},
		"negative JSON string": {value: "-123", want: -123},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := nestedNumber(map[string]any{"response": map[string]any{"user_id": test.value}}, "response", "user_id")
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("unexpected user ID: got %d, want %d", got, test.want)
			}
		})
	}
}

func TestNestedNumberRejectsInvalidVKUserIDs(t *testing.T) {
	for name, value := range map[string]any{
		"missing":         nil,
		"zero number":     float64(0),
		"fraction":        float64(1.5),
		"positive range":  float64(1 << 63),
		"negative range":  -float64(1<<63) - 2048,
		"zero string":     "0",
		"fraction string": "1.5",
		"overflow string": "9223372036854775808",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := nestedNumber(map[string]any{"response": map[string]any{"user_id": value}}, "response", "user_id"); err == nil {
				t.Fatal("expected invalid user ID to be rejected")
			}
		})
	}
}

func TestParseTurnURLsAcceptsUDPOnly(t *testing.T) {
	response := map[string]any{
		"turn_server": map[string]any{
			"urls": []any{
				"turn:192.0.2.1:3478?transport=udp",
				"turn:192.0.2.1:3478?transport=tcp",
				"turns:192.0.2.2:5349?transport=tcp",
				"turn:[2001:db8::1]:3478?transport=udp",
				"turn:192.0.2.1:3478?transport=udp",
			},
		},
	}
	addresses, err := parseTurnURLs(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 2 || addresses[0] != "192.0.2.1:3478" || addresses[1] != "[2001:db8::1]:3478" {
		t.Fatalf("unexpected TURN URLs: %#v", addresses)
	}
}

func TestParseVKAPIErrorRedactsCaptchaDetails(t *testing.T) {
	err := parseVKAPIError(map[string]any{
		"error": map[string]any{
			"error_code":  float64(14),
			"error_msg":   "captcha with remote metadata",
			"captcha_sid": "secret-session",
		},
	})
	if err != errVKCaptchaRequired {
		t.Fatalf("expected stable captcha error, got %v", err)
	}
}

func TestRemoteAPIErrorsDoNotExposeMessages(t *testing.T) {
	for name, err := range map[string]error{
		"VK": parseVKAPIError(map[string]any{
			"error": map[string]any{
				"error_code": float64(5),
				"error_msg":  "echoed-secret-token",
			},
		}),
		"OK Calls": parseOKAPIError(map[string]any{
			"error_code": float64(401),
			"error_msg":  "echoed-secret-token",
		}),
	} {
		if err == nil || strings.Contains(err.Error(), "echoed-secret-token") {
			t.Fatalf("%s error was not redacted: %v", name, err)
		}
	}
}
