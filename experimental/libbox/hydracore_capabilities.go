package libbox

import (
	"encoding/json"

	H "github.com/sagernet/sing-box/common/hydracore"
)

const hydraCoreAPIVersion = H.APIVersion

type hydraCoreIdentity = H.Identity
type hydraCoreFeatureSet = H.FeatureSet
type hydraCoreProtocolSet = H.ProtocolSet
type hydraCoreRemotePolicy = H.RemotePolicy
type hydraCoreRuntimeContract = H.RuntimeContract
type hydraCoreCapabilitySet = H.CapabilitySet

func HydraCoreCapabilities() string {
	return H.CapabilitiesJSON()
}

func HydraCoreTransportState() string {
	payload := struct {
		SchemaVersion int                       `json:"schema_version"`
		Health        H.TransportHealthSnapshot `json:"health"`
		Challenge     *H.RuntimeChallenge       `json:"challenge,omitempty"`
	}{
		SchemaVersion: 2,
		Health:        H.CurrentTransportHealth(),
		Challenge:     H.CurrentRuntimeChallenge(),
	}
	content, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(content)
}

func HydraCoreCancelRuntimeChallenge(id string) bool {
	return H.CancelRuntimeChallenge(id)
}
