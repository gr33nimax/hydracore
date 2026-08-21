package vkparasite

// Бюджет пути от QUIC-пакета до IP-датаграммы наружу.
//
//	IP + UDP до TURN                      28
//	TURN ChannelData                       4
//	RTP header + RFC 8285 extension       20  = rtpTotalHeaderLength
//	ChaCha20-Poly1305 tag                 16
//	RTP padding (worst case)              24  = defaultRTPPadding
//	DTLS record header + tag              29
//	------------------------------------------
//	итого                                121
//
// При консервативном мобильном path MTU 1400 остаётся 1279 на QUIC-пакет,
// что выше обязательного для QUIC минимума 1200.
//
// Значения здесь не должны расходиться с фактическим выводом rtpCodec.wrap:
// это проверяет TestWrappedPacketFitsPathMTU, который измеряет реальный
// пакет, а не пересчитывает те же константы.
const (
	conservativePathMTU = 1400

	overheadIPUDP        = 28
	overheadTURNChannel  = 4
	overheadRTPHeader    = rtpTotalHeaderLength
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
