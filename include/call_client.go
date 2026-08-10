//go:build with_call_client && !with_call_server

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
	inbound.Register[option.CallInboundOptions](registry, C.TypeCall, func(context.Context, adapter.Router, log.ContextLogger, string, option.CallInboundOptions) (adapter.Inbound, error) {
		return nil, E.New("Call inbound is not included in the HydraCore client build")
	})
}

func registerCallOutbound(registry *outbound.Registry) {
	call.RegisterOutbound(registry)
}
