//go:build with_call

package libbox

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHydraSubscriptionJWEAcceptsVKParasiteRequiredFeature(t *testing.T) {
	content := strings.Replace(validHydraSubscriptionJSON(),
		`"features":["rmux"]`,
		`"features":["rmux","call","call_vk_parasite","call_vk_four_lane_kcp"]`,
		1,
	)
	key := make([]byte, 32)
	iv := []byte{0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab}
	envelope := sealHydraSubscriptionTestJWE(t, []byte(content), key, iv)
	keyValue := base64.RawURLEncoding.EncodeToString(key)

	validation := decodeValidationResult(t, HydraCoreValidateSubscriptionJWE(envelope, keyValue))
	require.True(t, validation.Valid, validation.Diagnostics)
}
