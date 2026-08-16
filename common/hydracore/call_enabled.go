//go:build (with_call && !with_call_client && !with_call_server) || (with_call_client && with_call_server)

package hydracore

const (
	callEnabled       = true
	callClientEnabled = true
	callServerEnabled = true
	distributionRole  = "combined"
	callWireMin       = 8
	callWireMax       = 8
)

var (
	callPlatforms = []string{"dion", "telemost", "vk", "wbstream"}
	callModes     = []string{"p2p", "vk_parasite"}
)
