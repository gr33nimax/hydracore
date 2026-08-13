package vkparasite

import (
	"net"

	"github.com/sagernet/sing-box/transport/call/telemetry"
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
	receiveBytes, sendBytes, err := packetSocketBufferSizes(packetConn)
	if err != nil {
		s.logger.Warn("call vk_parasite: inspect effective UDP socket buffers: ", err)
		return
	}
	s.telemetry.metrics.Set(telemetry.UDPSocketReceiveBufferBytes, float64(receiveBytes))
	s.telemetry.metrics.Set(telemetry.UDPSocketSendBufferBytes, float64(sendBytes))
}
