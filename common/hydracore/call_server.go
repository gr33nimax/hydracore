//go:build with_call_server && !with_call_client

package hydracore

const (
	callEnabled       = true
	callClientEnabled = false
	callServerEnabled = true
	distributionRole  = "vps"
	callWireMin       = 5
	callWireMax       = 5
)

var (
	callPlatforms = []string{"vk"}
	callModes     = []string{"vk_parasite"}
)
