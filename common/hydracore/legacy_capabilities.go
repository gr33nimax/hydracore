package hydracore

import "encoding/json"

// The capability document HYDRA read before the product contract replaced it.
//
// It exists for one transition. A HYDRA older than the contract validates a core by asking
// `hydra capabilities --json` and reading `identity.role`, `features.call_vk_parasite` and
// `protocols.call_modes` out of the answer; without one it refuses to install this core at all.
// Since the successor of that same HYDRA requires this core, the two versions would deadlock
// every machine in the field — which is what an update on a running server did.
//
// Only what this build can state is filled in, and it comes from the same build tags the current
// contract reads: a key left out is a capability the old reader treats as absent, which is the
// safe direction. The document can be dropped when no pre-contract HYDRA remains.
type LegacyCapabilities struct {
	APIVersion int             `json:"api_version"`
	Identity   LegacyIdentity  `json:"identity"`
	Features   LegacyFeatures  `json:"features"`
	Protocols  LegacyProtocols `json:"protocols"`
}

type LegacyIdentity struct {
	CoreID   string `json:"core_id"`
	CoreName string `json:"core_name"`
	Role     string `json:"role"`
}

type LegacyFeatures struct {
	Call           bool `json:"call"`
	CallVKParasite bool `json:"call_vk_parasite"`
}

type LegacyProtocols struct {
	CallPlatforms []string `json:"call_platforms"`
	CallModes     []string `json:"call_modes"`
}

func LegacyCapabilitiesDocument() LegacyCapabilities {
	callCapable := len(callModes) > 0
	platforms := []string{}
	if callCapable {
		platforms = append(platforms, "vk")
	}
	return LegacyCapabilities{
		APIVersion: APIVersion,
		Identity: LegacyIdentity{
			CoreID:   CoreID,
			CoreName: "HydraCore",
			Role:     distributionRole,
		},
		Features: LegacyFeatures{
			Call:           callCapable,
			CallVKParasite: SupportsCallMode("vk_parasite"),
		},
		Protocols: LegacyProtocols{
			CallPlatforms: platforms,
			CallModes:     append([]string{}, callModes...),
		},
	}
}

func LegacyCapabilitiesJSON() string {
	content, err := json.Marshal(LegacyCapabilitiesDocument())
	if err != nil {
		return ""
	}
	return string(content)
}
