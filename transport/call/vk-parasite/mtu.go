package vkparasite

// Бюджет пути от QUIC-пакета до IP-датаграммы наружу.
//
//	IP + UDP до TURN                      28
//	TURN ChannelData                       4
//	RTP header + RFC 8285 extension       20  = rtpTotalHeaderLength
//	ChaCha20-Poly1305 tag                 16
//	RTP padding (worst case)               8  = defaultRTPPadding
//	------------------------------------------
//	итого                                 76
//
// При консервативном мобильном path MTU 1400 остаётся 1324 на QUIC-пакет.
//
// Слоя DTLS здесь больше нет. Он стоил 37 байт на пакет (13 record header +
// 8 explicit nonce + 16 tag — не 29, как считала прежняя версия этого файла) и
// один AES-GCM в каждую сторону, а наружу не давал ничего: DTLS работал поверх
// RTP-обёртки, то есть каждый его байт, включая handshake, уезжал внутри
// запечатанного ChaCha20-Poly1305 payload и ни одному наблюдателю в сети виден
// не был.
//
// Значения здесь не должны расходиться с фактическим выводом rtpCodec.wrap:
// это проверяет TestWrappedPacketFitsPathMTU, который измеряет реальный
// пакет, а не пересчитывает те же константы.
const (
	conservativePathMTU = 1400

	overheadIPUDP       = 28
	overheadTURNChannel = 4
	overheadRTPHeader   = rtpTotalHeaderLength
	overheadRTPAEAD     = 16
	overheadRTPPadding  = defaultRTPPadding

	pathOverheadTotal = overheadIPUDP + overheadTURNChannel +
		overheadRTPHeader + overheadRTPAEAD + overheadRTPPadding

	// Размер внешнего QUIC-пакета. Ниже бюджета пути с запасом в 4 байта.
	quicPacketSize = 1320

	quicMinimumPacketSize = 1200

	// Длина connection ID, которую выдаёт наш генератор.
	//
	// Она здесь не для экономии заголовка, а чтобы бюджет DATAGRAM-фрейма ниже
	// опирался на проверяемое значение вместо худшего легального 20.
	quicConnectionIDLength = 4

	// Накладные внешнего QUIC-пакета над payload DATAGRAM-фрейма:
	//
	//	short header flags                  1
	//	destination connection ID           4  = quicConnectionIDLength
	//	packet number (макс.)               4
	//	AEAD tag                           16
	//	DATAGRAM frame type + length        3
	//	--------------------------------------
	//	итого                              28
	overheadQUICHeader    = 1 + quicConnectionIDLength + 4
	overheadQUICAEAD      = 16
	overheadDatagramFrame = 3

	// Порог, на котором SendDatagram отдаёт DatagramTooLargeError.
	//
	// Проверено по sagernet/quic-go v0.59.0-sing-box-mod.4: SendDatagram
	// пропускает min(MaxDataLen(16383), currentMTUEstimate), а при
	// DisablePathMTUDiscovery оценка остаётся
	// estimateMaxPayloadSize(InitialPacketSize) = size-1-20-16 и никогда не
	// растёт. Оценка берёт худший легальный connection ID (20) независимо от
	// того, какой выдан на самом деле, поэтому она ниже физического бюджета — и
	// именно она ограничивает фрейм.
	quicSendDatagramLimit = quicPacketSize - 1 - 20 - 16

	// Бюджет payload одного DATAGRAM-фрейма.
	//
	// Это порог SendDatagram: он ниже физического места в пакете, которое
	// гарантирует quicConnectionIDLength. Оба неравенства держит
	// TestDatagramFrameFitsQUICPacket — просто пройти проверку quic-go
	// недостаточно, потому что не влезший фрейм packet_packer.go молча
	// выбрасывает, оставив один debug-лог.
	maxDatagramFramePayload = quicSendDatagramLimit
)
