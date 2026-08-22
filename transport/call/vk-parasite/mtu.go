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

	// Накладные внешнего QUIC-пакета над payload DATAGRAM-фрейма:
	//
	//	short header flags                  1
	//	destination connection ID (макс.)  20
	//	packet number                       4
	//	AEAD tag                           16
	//	DATAGRAM frame type + length        3
	//	--------------------------------------
	//	итого                              44
	overheadQUICHeader    = 1 + 20 + 4
	overheadQUICAEAD      = 16
	overheadDatagramFrame = 3

	// Бюджет payload одного DATAGRAM-фрейма.
	//
	// Проверено по sagernet/quic-go v0.59.0-sing-box-mod.4:
	//
	//  1. max_datagram_frame_size анонсируется как wire.MaxDatagramSize =
	//     16383 (connection.go), а quicConfig его не переопределяет. То есть
	//     транспортный параметр здесь ничего не ограничивает — вопреки
	//     распространённому допущению про обязательный минимум 1200.
	//  2. SendDatagram пропускает min(MaxDataLen(16383) = 16380,
	//     currentMTUEstimate). При DisablePathMTUDiscovery оценка остаётся
	//     estimateMaxPayloadSize(InitialPacketSize) = 1250-1-20-16 = 1213 и
	//     никогда не растёт: поднимает её только mtuDiscoverer.
	//  3. Эта оценка не учитывает номер пакета и заголовок самого фрейма,
	//     поэтому она не доказывает, что фрейм влезет в пакет. Не влезший
	//     фрейм packet_packer.go молча выбрасывает, оставив один debug-лог,
	//     то есть nil из SendDatagram отправку не гарантирует.
	//
	// Поэтому берём не порог SendDatagram, а физический бюджет: наибольший
	// payload, который влезает в quicPacketSize при худшем легальном
	// connection ID (20) и худшем номере пакета (4). Он заведомо ниже 1213,
	// что проверяет TestDatagramFrameFitsQUICPacket.
	maxDatagramFramePayload = quicPacketSize - overheadQUICHeader -
		overheadQUICAEAD - overheadDatagramFrame

	// Порог, на котором SendDatagram отдаёт DatagramTooLargeError:
	// estimateMaxPayloadSize из quic-go. Держим рядом, чтобы тест ловил уход
	// бюджета выше того, что quic-go вообще принимает.
	quicSendDatagramLimit = quicPacketSize - 1 - 20 - 16
)
