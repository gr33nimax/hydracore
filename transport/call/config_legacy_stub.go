// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !with_call_legacy

package call

import (
	"context"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
)

// connectLegacyPlatform отказывает: сборка без with_call_legacy не содержит
// telemost, wbstream, VK P2P и dion, а с ними и стека pion/webrtc.
func connectLegacyPlatform(
	_ context.Context,
	cfg Config,
	_ int,
	_ string,
	_ logger.ContextLogger,
) (*Bridge, error) {
	return nil, E.New("call: unsupported platform ", cfg.Platform)
}
