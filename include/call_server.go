//go:build with_call_server && !with_call_client

package include

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/call"
	E "github.com/sagernet/sing/common/exceptions"
)

func registerCallInbound(registry *inbound.Registry) {
	call.RegisterInbound(registry)
}

func registerCallOutbound(registry *outbound.Registry) {
	outbound.Register[option.CallOutboundOptions](registry, C.TypeCall, func(context.Context, adapter.Router, log.ContextLogger, string, option.CallOutboundOptions) (adapter.Outbound, error) {
		return nil, E.New("Call outbound is not included in the HydraCore VPS build")
	})
}
