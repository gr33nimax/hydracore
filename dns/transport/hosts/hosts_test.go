package hosts_test

import (
	"net/netip"
	"os"
	"runtime"
	"testing"

	"github.com/sagernet/sing-box/dns/transport/hosts"

	"github.com/stretchr/testify/require"
)

func TestHosts(t *testing.T) {
	t.Parallel()
	require.Equal(t, []netip.Addr{netip.AddrFrom4([4]byte{127, 0, 0, 1}), netip.IPv6Loopback()}, hosts.NewFile("testdata/hosts").Lookup("localhost"))
	if runtime.GOOS == "windows" {
		// A stock Windows hosts file may intentionally leave localhost to the
		// DNS client and contain no active localhost record at all.
		_, err := os.Stat(hosts.DefaultPath)
		require.NoError(t, err)
	} else {
		require.NotEmpty(t, hosts.NewFile(hosts.DefaultPath).Lookup("localhost"))
	}
}
