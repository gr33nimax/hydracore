package device

import "testing"

func TestDeterminePacketTypeAndPadding_RandomTrailerTransportWinsOverInvalidHandshake(t *testing.T) {
	d := newBareTestDevice()
	d.randomTrailers.Store(true)
	d.paddings.init.Store(16)
	d.paddings.transport.Store(0)

	const transportHeader = ^uint32(0)
	var header UintRange
	header.FromUint32(0, transportHeader-1)
	d.headers.init.Store(header)
	header.FromUint32(transportHeader, transportHeader)
	d.headers.transport.Store(header)

	packet := make([]byte, MessageInitiationSize+32)
	fillDeterministic(packet)
	putType(packet, 0, transportHeader, zeroTypeHash[:])

	_, gotType, gotPadding := d.DeterminePacketTypeAndPadding(packet, zeroTypeHash[:])
	if gotType != MessageTransportType || gotPadding != 0 {
		t.Fatalf("type,padding = (%d,%d), want transport at offset 0; invalid H1 candidate must not drop transport", gotType, gotPadding)
	}
}

func TestDeterminePacketTypeAndPadding_RandomTrailerKeepsValidInitiation(t *testing.T) {
	d := newBareTestDevice()
	d.randomTrailers.Store(true)
	d.paddings.init.Store(16)
	d.paddings.transport.Store(0)

	var header UintRange
	header.FromUint32(100, 100)
	d.headers.init.Store(header)
	header.FromUint32(200, 200)
	d.headers.transport.Store(header)

	packet := make([]byte, 16+MessageInitiationSize+32)
	fillDeterministic(packet)
	putType(packet, 0, 200, zeroTypeHash[:])
	putType(packet, 16, 100, zeroTypeHash[:])

	var publicKey NoisePublicKey
	d.cookieChecker.Init(publicKey)
	var generator CookieGenerator
	generator.Init(publicKey)
	generator.AddMacs(packet[16 : 16+MessageInitiationSize])

	_, gotType, gotPadding := d.DeterminePacketTypeAndPadding(packet, zeroTypeHash[:])
	if gotType != MessageInitiationType || gotPadding != 16 {
		t.Fatalf("type,padding = (%d,%d), want valid initiation at offset 16", gotType, gotPadding)
	}
}
