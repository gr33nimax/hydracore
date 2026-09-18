package vkparasite

import (
	"net"
)

type packetSocketBufferSetter interface {
	SetReadBuffer(bytes int) error
	SetWriteBuffer(bytes int) error
}

func (s *Server) configurePacketSocket(packetConn net.PacketConn) {
	setter, supported := packetConn.(packetSocketBufferSetter)
	if !supported {
		s.logger.Warn("call vk_parasite: UDP listener does not expose socket buffer controls")
		return
	}
	if err := setter.SetReadBuffer(s.options.UDPReceiveBufferBytes); err != nil {
		s.logger.Warn("call vk_parasite: set UDP receive buffer: ", err)
	}
	if err := setter.SetWriteBuffer(s.options.UDPSendBufferBytes); err != nil {
		s.logger.Warn("call vk_parasite: set UDP send buffer: ", err)
	}
}
