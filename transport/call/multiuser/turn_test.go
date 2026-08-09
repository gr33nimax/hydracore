package multiuser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTURNUDPURL(t *testing.T) {
	t.Parallel()
	address, err := parseTURNUDPURL("turn:relay.example.invalid:3478?transport=udp")
	require.NoError(t, err)
	require.Equal(t, "relay.example.invalid", address.Fqdn)
	require.Equal(t, uint16(3478), address.Port)
	address, err = parseTURNUDPURL("turn://192.0.2.10")
	require.NoError(t, err)
	require.Equal(t, uint16(3478), address.Port)
	_, err = parseTURNUDPURL("turns:relay.example.invalid:5349?transport=tcp")
	require.Error(t, err)
}
