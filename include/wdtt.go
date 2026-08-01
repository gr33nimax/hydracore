//go:build with_wdtt

package include

import (
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/protocol/wdtt"
)

func registerWDTTEndpoint(registry *endpoint.Registry) {
	wdtt.RegisterEndpoint(registry)
}
