//go:build !linux

package vkparasite

import "net"

func packetSocketBufferSizes(net.PacketConn) (int, int, error) {
	return 0, 0, nil
}
