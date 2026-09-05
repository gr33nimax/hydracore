// SPDX-License-Identifier: GPL-3.0-or-later

//go:build with_call_legacy

package call

import (
	"context"
	"fmt"

	"github.com/sagernet/sing-box/transport/call/dion"
	"github.com/sagernet/sing-box/transport/call/telemost"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/vk"
	"github.com/sagernet/sing-box/transport/call/wbstream"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
)

// connectLegacyPlatform поднимает мосты по платформам, которых нет в
// distribution contract: единственный поддерживаемый режим — vk_parasite.
// Здесь остались telemost, wbstream, VK P2P и dion; они существуют только под
// тегом with_call_legacy, потому что тянут за собой полный стек pion/webrtc.
func connectLegacyPlatform(
	ctx context.Context,
	cfg Config,
	readBuf int,
	cookieStr string,
	log logger.ContextLogger,
) (*Bridge, error) {
	switch cfg.Platform {
	case "telemost":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := telemost.ConnectCreator(ctx, cookieStr, cfg.JoinLink, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[telemost]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := telemost.ConnectJoiner(ctx, cfg.JoinLink, "", readBuf, cfg.Dialer, cfg.DNSRouter, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	case "wbstream":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := wbstream.ConnectCreator(ctx, cookieStr, cfg.JoinLink, cfg.Mode, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[wbstream]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := wbstream.ConnectJoiner(ctx, cfg.JoinLink, "", cfg.Mode, readBuf, cfg.Dialer, cfg.DNSRouter, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	case "vk":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := vk.ConnectCreator(ctx, cookieStr, cfg.JoinLink, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[vk]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := vk.ConnectJoiner(ctx, cfg.JoinLink, "", readBuf, cfg.Dialer, cfg.DNSRouter, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	case "dion":
		switch cfg.Role {
		case RoleCreator:
			relay, joinLink, err := dion.ConnectCreator(ctx, cookieStr, cfg.JoinLink, cfg.Email, cfg.Password, readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			log.Notice(fmt.Sprintf("call[dion]: join_link=%s", joinLink))
			return &Bridge{relay: relay}, nil
		case RoleJoiner:
			tun, err := dion.ConnectJoiner(ctx, cfg.JoinLink, "", readBuf, cfg.Dialer, log)
			if err != nil {
				return nil, err
			}
			relay := tunnel.NewRelayBridge(tun, "joiner", readBuf, cfg.Dialer, log)
			relay.MarkReady()
			return &Bridge{relay: relay}, nil
		}
	}
	return nil, E.New("call: unsupported platform ", cfg.Platform)
}
