//go:build with_call

package call

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizedReadBuffer(t *testing.T) {
	t.Parallel()
	require.Equal(t, 32768, normalizedReadBuffer(0))
	require.Equal(t, 32768, normalizedReadBuffer(-1))
	require.Equal(t, 65536, normalizedReadBuffer(65536))
}
