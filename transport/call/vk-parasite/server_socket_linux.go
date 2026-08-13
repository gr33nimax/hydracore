//go:build linux

package vkparasite

import (
	"errors"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func packetSocketBufferSizes(packetConn net.PacketConn) (int, int, error) {
	connection, supported := packetConn.(syscall.Conn)
	if !supported {
		return 0, 0, errors.New("syscall connection unavailable")
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var receiveBytes int
	var sendBytes int
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		receiveBytes, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
		if socketErr != nil {
			return
		}
		sendBytes, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
	})
	if err != nil {
		return 0, 0, err
	}
	if socketErr != nil {
		return 0, 0, socketErr
	}
	return receiveBytes, sendBytes, nil
}
