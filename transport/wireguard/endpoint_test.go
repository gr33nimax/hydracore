package wireguard

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingFailingIPCSetter struct {
	config string
	err    error
}

func (s *recordingFailingIPCSetter) IpcSet(config string) error {
	s.config = config
	return s.err
}

func TestConfigureWireGuardDeviceRedactsSecretConfiguration(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("invalid UAPI input")
	setter := &recordingFailingIPCSetter{err: sentinel}
	config := "private_key=private-secret\npreshared_key=pre-shared-secret"

	err := configureWireGuardDevice(setter, config)
	require.ErrorIs(t, err, sentinel)
	require.Contains(t, err.Error(), "setup wireguard")
	require.Equal(t, config, setter.config)
	require.NotContains(t, err.Error(), "private-secret")
	require.NotContains(t, err.Error(), "pre-shared-secret")
}
