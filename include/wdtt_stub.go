//go:build !with_wdtt

package include

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func registerWDTTEndpoint(registry *endpoint.Registry) {
	endpoint.Register[option.WDTTEndpointOptions](registry, C.TypeWDTT, func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.WDTTEndpointOptions) (adapter.Endpoint, error) {
		return nil, E.New(`WDTT is not included in this build, rebuild with -tags with_wdtt`)
	})
}
