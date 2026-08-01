// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"context"
	"net"

	M "github.com/sagernet/sing/common/metadata"
)

type coreDialer interface {
	DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

func parseDestination(address string) M.Socksaddr {
	return M.ParseSocksaddr(address)
}
