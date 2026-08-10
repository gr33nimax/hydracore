//go:build with_call_client && !with_call_server

package hydracore

const (
	callEnabled       = true
	callClientEnabled = true
	callServerEnabled = false
	distributionRole  = "client"
	callWireMin       = 2
	callWireMax       = 2
)

var (
	callPlatforms = []string{"vk"}
	callModes     = []string{"multi_user"}
)
