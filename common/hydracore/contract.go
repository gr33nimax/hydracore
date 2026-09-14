package hydracore

import "encoding/json"

const (
	APIVersion      = 2
	ContractVersion = 1
	ClientABI       = 1
	CoreID          = "io.hydrabox.hydracore"
)

type ProductContract struct {
	Version   int    `json:"contract_version"`
	CoreID    string `json:"core_id"`
	Role      string `json:"role"`
	CallsMode string `json:"calls_mode,omitempty"`
}

func Contract() ProductContract {
	contract := ProductContract{
		Version: ContractVersion,
		CoreID:  CoreID,
		Role:    distributionRole,
	}
	if SupportsCallMode("vk_parasite") {
		contract.CallsMode = "vk_parasite"
	}
	return contract
}

func ContractJSON() string {
	content, err := json.Marshal(Contract())
	if err != nil {
		return ""
	}
	return string(content)
}

func SupportsCallMode(mode string) bool {
	if mode != "vk_parasite" {
		mode = "p2p"
	}
	for _, supportedMode := range callModes {
		if supportedMode == mode {
			return true
		}
	}
	return false
}
