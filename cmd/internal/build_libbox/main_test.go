package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveBuildVersionPreservesExplicitReleaseIdentity(t *testing.T) {
	const expected = "v1.13.16-extended-hydracore.11-debug.16"
	actual := resolveBuildVersion(expected, func() (string, error) {
		return "1.13.16-extended-hydracore.11-debug.16", nil
	})
	require.Equal(t, expected, actual)
}

func TestResolveBuildVersionFallsBackToRepositoryTag(t *testing.T) {
	actual := resolveBuildVersion("", func() (string, error) {
		return "1.13.16-extended", nil
	})
	require.Equal(t, "1.13.16-extended", actual)

	actual = resolveBuildVersion("", func() (string, error) {
		return "", errors.New("no tag")
	})
	require.Equal(t, "unknown", actual)
}

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
