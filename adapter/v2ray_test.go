package adapter

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResetV2RayClientTransportUsesReusableReset(t *testing.T) {
	transport := &resettableV2RayClientTransport{}

	ResetV2RayClientTransport(transport)

	require.Equal(t, 1, transport.resetCount)
	require.Zero(t, transport.closeCount)
}

func TestResetV2RayClientTransportFallsBackToClose(t *testing.T) {
	transport := &legacyV2RayClientTransport{}

	ResetV2RayClientTransport(transport)

	require.Equal(t, 1, transport.closeCount)
}

type legacyV2RayClientTransport struct {
	closeCount int
}

func (*legacyV2RayClientTransport) DialContext(context.Context) (net.Conn, error) {
	return nil, net.ErrClosed
}

func (t *legacyV2RayClientTransport) Close() error {
	t.closeCount++
	return nil
}

type resettableV2RayClientTransport struct {
	legacyV2RayClientTransport
	resetCount int
}

func (t *resettableV2RayClientTransport) Reset() {
	t.resetCount++
}

var _ V2RayClientTransport = (*legacyV2RayClientTransport)(nil)
var _ V2RayClientTransportResetter = (*resettableV2RayClientTransport)(nil)
