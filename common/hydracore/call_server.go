//go:build with_call_server && !with_call_client

package hydracore

const (
	callEnabled       = true
	callClientEnabled = false
	callServerEnabled = true
	distributionRole  = "vps"
	callWireMin       = 1
	callWireMax       = 2
)

var (
	callPlatforms = []string{"vk"}
	callModes     = []string{"multi_user"}
)
