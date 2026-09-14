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
