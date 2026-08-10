//go:build !with_call && !with_call_client && !with_call_server

package hydracore

const (
	callEnabled       = false
	callClientEnabled = false
	callServerEnabled = false
	distributionRole  = "base"
	callWireMin       = 0
	callWireMax       = 0
)

var (
	callPlatforms = []string{}
	callModes     = []string{}
)
