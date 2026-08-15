//go:build (with_call && !with_call_client && !with_call_server) || (with_call_client && with_call_server)

package hydracore

const (
	callEnabled       = true
	callClientEnabled = true
	callServerEnabled = true
	distributionRole  = "combined"
	callWireMin       = 7
	callWireMax       = 7
)

var (
	callPlatforms = []string{"dion", "telemost", "vk", "wbstream"}
	callModes     = []string{"p2p", "vk_parasite"}
)
