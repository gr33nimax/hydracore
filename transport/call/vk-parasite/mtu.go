package vkparasite

// ai-generated: MTU budget constants and calculations for QUIC over DTLS/RTP/TURN
// Бюджет пути от QUIC-пакета до IP-датаграммы наружу.
//
//	IP + UDP до TURN                      28
//	TURN ChannelData                       4
//	RTP header                            12
//	ChaCha20-Poly1305 tag                 16
//	RTP padding (worst case)              24  = defaultRTPPadding
//	DTLS record header + tag              29
//	------------------------------------------
//	итого                                113
//
// При консервативном мобильном path MTU 1400 остаётся 1287 на QUIC-пакет,
// что выше обязательного для QUIC минимума 1200.
const (
	conservativePathMTU = 1400

	overheadIPUDP        = 28
	overheadTURNChannel  = 4
	overheadRTPHeader    = rtpHeaderLength
	overheadRTPAEAD      = 16
	overheadRTPPadding   = defaultRTPPadding
	overheadDTLSRecord   = 29

	pathOverheadTotal = overheadIPUDP + overheadTURNChannel +
		overheadRTPHeader + overheadRTPAEAD + overheadRTPPadding +
		overheadDTLSRecord

	// MTU для dtls.Config: сколько DTLS может положить в одну запись.
	dtlsMTU = conservativePathMTU - overheadIPUDP - overheadTURNChannel -
		overheadRTPHeader - overheadRTPAEAD - overheadRTPPadding

	// Размер QUIC-пакета. Ниже dtlsMTU на размер DTLS-записи, с запасом.
	quicPacketSize = 1250

	quicMinimumPacketSize = 1200
)
