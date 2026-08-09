package libbox

import H "github.com/sagernet/sing-box/common/hydracore"

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
