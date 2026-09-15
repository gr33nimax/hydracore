package hydracore

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContractJSON(t *testing.T) {
	var contract ProductContract
	require.NoError(t, json.Unmarshal([]byte(ContractJSON()), &contract))
	require.Equal(t, 1, contract.Version)
	require.Equal(t, "io.hydrabox.hydracore", contract.CoreID)
	require.NotEmpty(t, contract.Role)
	if contract.Role == "vps" {
		require.Equal(t, "vk_parasite", contract.CallsMode)
	}
}

// The pre-contract gate reads these places and refuses a core that answers none of them, so the
// legacy document has to agree with the build that produced it — including under the call tags
// CI builds the VPS runtime with, where that gate is what lets an older HYDRA install it.
func TestLegacyCapabilitiesAgreeWithTheBuild(t *testing.T) {
	var legacy LegacyCapabilities
	require.NoError(t, json.Unmarshal([]byte(LegacyCapabilitiesJSON()), &legacy))
	require.Equal(t, APIVersion, legacy.APIVersion)
	require.Equal(t, CoreID, legacy.Identity.CoreID)
	require.Equal(t, distributionRole, legacy.Identity.Role)
	require.Equal(t, SupportsCallMode("vk_parasite"), legacy.Features.CallVKParasite)
	require.Equal(t, len(callModes) > 0, legacy.Features.Call)
	require.Equal(t, callModes, legacy.Protocols.CallModes)
}
