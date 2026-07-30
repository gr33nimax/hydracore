package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAndroidSharedTagsCoverExtendedCore(t *testing.T) {
	required := []string{
		"with_gvisor",
		"with_quic",
		"with_dhcp",
		"with_wireguard",
		"with_utls",
		"with_acme",
		"with_clash_api",
		"with_manager",
		"with_admin_panel",
		"with_profiler",
		"with_v2ray_api",
		"with_masque",
		"with_mtproxy",
		"with_ccm",
		"with_ocm",
		"with_openvpn",
		"with_trusttunnel",
		"with_sudoku",
		"with_snell",
		"with_naive_outbound",
		"with_tailscale",
	}
	require.Subset(t, sharedTags, required)
}
