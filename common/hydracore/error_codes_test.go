package hydracore

import (
	"regexp"
	"testing"
)

func TestAllErrorCodesAreUniqueAndSafe(t *testing.T) {
	codes := AllErrorCodes()
	if len(codes) == 0 {
		t.Fatal("error code dictionary is empty")
	}
	valid := regexp.MustCompile(`^[a-z0-9._]{3,64}$`)
	seen := make(map[string]struct{}, len(codes))
	for _, code := range codes {
		if !valid.MatchString(code) {
			t.Errorf("invalid error code %q", code)
		}
		if _, exists := seen[code]; exists {
			t.Errorf("duplicate error code %q", code)
		}
		seen[code] = struct{}{}
	}
}
