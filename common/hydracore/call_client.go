//go:build with_call_client && !with_call_server

package hydracore

const (
	callEnabled       = true
	callClientEnabled = true
	callServerEnabled = false
	distributionRole  = "client"
)

var (
	callPlatforms = []string{"vk"}
	callModes     = []string{"vk_parasite"}
)
