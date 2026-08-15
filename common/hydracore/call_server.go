//go:build with_call_server && !with_call_client

package hydracore

const (
	callEnabled       = true
	callClientEnabled = false
	callServerEnabled = true
	distributionRole  = "vps"
	callWireMin       = 6
	callWireMax       = 6
)

var (
	callPlatforms = []string{"vk"}
	callModes     = []string{"vk_parasite"}
)
