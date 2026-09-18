// SPDX-FileCopyrightText: 2025-2026 SpaceNeuroX contributors
// SPDX-License-Identifier: MIT
//
// Adapted from proxy-turn-vk-android go_client/obfs.go and wrap.go at
// commit 40117047d71f0303504e276b18372c0626b94a35.

package vkparasite

import (
	"crypto/cipher"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	randv2 "math/rand/v2"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const (
	wrapKeyLength        = 32
	rtpHeaderLength      = 12
	rtpExtensionLength   = 8 // RFC 8285 One-Byte Header: 0xBEDE (2B) + len (2B) + ssrc-audio-level (4B)
	rtpTotalHeaderLength = rtpHeaderLength + rtpExtensionLength
	defaultRTPPadding    = 8
	maxCodecWireBuffer   = 1536
	maximumWirePacket    = 64 * 1024
)

type wireBuffer [maxCodecWireBuffer]byte

func deriveWrapKey(password string) ([wrapKeyLength]byte, error) {
	var key [wrapKeyLength]byte
	if password == "" {
		return key, errors.New("call vk_parasite: empty obfs_password")
	}
	reader := hkdf.New(
		sha256.New,
		[]byte(password),
		[]byte("WDTT-WRAP-v1"),
		[]byte("rtp-obfs/chacha20poly1305"),
	)
	if _, err := io.ReadFull(reader, key[:]); err != nil {
		return key, fmt.Errorf("derive RTP wrap key: %w", err)
	}
	return key, nil
}

type rtpCodec struct {
	aead        cipher.AEAD
	ssrc        uint32
	initialSeq  uint16
	initialTS   uint32
	payloadType byte
	startedAt   time.Time
	count       uint64
	prngMu      sync.Mutex
	prng        *randv2.ChaCha8
	pool        sync.Pool
}

func newRTPCodec(key [wrapKeyLength]byte) (*rtpCodec, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	var seed [10]byte
	if _, err = cryptorand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("initialize RTP wrapper: %w", err)
	}
	var prngSeed [32]byte
	if _, err = cryptorand.Read(prngSeed[:]); err != nil {
		return nil, fmt.Errorf("initialize RTP wrapper PRNG: %w", err)
	}
	codec := &rtpCodec{
		aead:        aead,
		ssrc:        binary.BigEndian.Uint32(seed[0:4]),
		initialSeq:  binary.BigEndian.Uint16(seed[4:6]),
		initialTS:   binary.BigEndian.Uint32(seed[6:10]),
		payloadType: 96 + byte(binary.BigEndian.Uint16(seed[4:6])%32),
		startedAt:   time.Now(),
		prng:        randv2.NewChaCha8(prngSeed),
	}
	codec.pool.New = func() any {
		return new(wireBuffer)
	}
	return codec, nil
}

func (c *rtpCodec) getBuffer() *wireBuffer {
	if buf := c.pool.Get(); buf != nil {
		return buf.(*wireBuffer)
	}
	return new(wireBuffer)
}

func (c *rtpCodec) putBuffer(buf *wireBuffer) {
	if buf != nil {
		c.pool.Put(buf)
	}
}

func (c *rtpCodec) wrap(payload []byte) ([]byte, *wireBuffer, error) {
	if len(payload) == 0 {
		return nil, nil, errors.New("RTP wrapper: empty payload")
	}
	if len(payload)+rtpTotalHeaderLength+chacha20poly1305.Overhead+defaultRTPPadding+1 > maximumWirePacket {
		return nil, nil, errors.New("RTP wrapper: payload too large")
	}

	c.prngMu.Lock()
	packetIndex := c.count
	c.count++
	elapsed := time.Since(c.startedAt)
	sec := uint64(elapsed / time.Second)
	nsec := uint64(elapsed % time.Second)
	ticks := sec*90_000 + nsec*90_000/uint64(time.Second)
	timestamp := c.initialTS + uint32(ticks)
	r := c.prng.Uint64()
	paddingLength := int(byte(r))%defaultRTPPadding + 1
	r >>= 8
	rem := 7
	totalLen := rtpTotalHeaderLength + len(payload) + chacha20poly1305.Overhead + paddingLength

	var out []byte
	var rawBuf *wireBuffer
	if totalLen <= maxCodecWireBuffer {
		rawBuf = c.getBuffer()
		out = rawBuf[:totalLen]
	} else {
		out = make([]byte, totalLen)
	}

	paddingStart := rtpTotalHeaderLength + len(payload) + chacha20poly1305.Overhead
	if paddingLength > 1 {
		padBytes := out[paddingStart : totalLen-1]
		for len(padBytes) > 0 {
			if rem == 0 {
				r = c.prng.Uint64()
				rem = 8
			}
			n := min(len(padBytes), rem)
			for i := 0; i < n; i++ {
				padBytes[i] = byte(r)
				r >>= 8
			}
			rem -= n
			padBytes = padBytes[n:]
		}
	}
	c.prngMu.Unlock()

	sequence := c.initialSeq + uint16(packetIndex)
	nonce := buildRTPNonce(c.ssrc, sequence, timestamp)

	out[0] = 0x80 | 0x20 | 0x10
	out[1] = c.payloadType
	binary.BigEndian.PutUint16(out[2:4], sequence)
	binary.BigEndian.PutUint32(out[4:8], timestamp)
	binary.BigEndian.PutUint32(out[8:12], c.ssrc)

	out[12] = 0xBE
	out[13] = 0xDE
	out[14] = 0x00
	out[15] = 0x01
	out[16] = 0x10
	out[17] = 0x00
	out[18] = 0x00
	out[19] = 0x00

	c.aead.Seal(out[rtpTotalHeaderLength:rtpTotalHeaderLength], nonce[:], payload, out[:rtpTotalHeaderLength])
	out[totalLen-1] = byte(paddingLength)
	return out, rawBuf, nil
}

// unwrap разбирает внешний пакет, складывая plaintext в dst.
//
// Раньше открытие AEAD шло в nil, то есть каждый принятый пакет стоил
// аллокации размером с пакет — на приёме это была самая крупная статья мусора
// во всём внешнем стеке. Теперь буфер даёт вызывающий: если его ёмкости
// хватает, приём не аллоцирует ничего. Недостаточная ёмкость остаётся
// корректной — Open аллоцирует сам, — но это защита, а не рабочий путь.
func (c *rtpCodec) unwrap(dst, wire []byte) ([]byte, error) {
	if len(wire) < rtpHeaderLength+chacha20poly1305.Overhead+1 || len(wire) > maximumWirePacket {
		return nil, errors.New("RTP wrapper: invalid packet length")
	}
	if wire[0]>>6 != 2 {
		return nil, errors.New("RTP wrapper: invalid version")
	}
	payloadType := wire[1] & 0x7f
	if payloadType < 96 || payloadType > 127 {
		return nil, errors.New("RTP wrapper: unexpected payload type")
	}
	headerLength := rtpHeaderLength
	if wire[0]&0x10 != 0 {
		if len(wire) < rtpHeaderLength+4 {
			return nil, errors.New("RTP wrapper: truncated extension header")
		}
		extWords := int(binary.BigEndian.Uint16(wire[rtpHeaderLength+2 : rtpHeaderLength+4]))
		headerLength = rtpHeaderLength + 4 + extWords*4
	}
	if len(wire) < headerLength+chacha20poly1305.Overhead+1 {
		return nil, errors.New("RTP wrapper: truncated header")
	}
	payloadEnd := len(wire)
	if wire[0]&0x20 != 0 {
		paddingLength := int(wire[len(wire)-1])
		if paddingLength == 0 || paddingLength > payloadEnd-headerLength-chacha20poly1305.Overhead {
			return nil, errors.New("RTP wrapper: invalid padding")
		}
		payloadEnd -= paddingLength
	}
	sequence := binary.BigEndian.Uint16(wire[2:4])
	timestamp := binary.BigEndian.Uint32(wire[4:8])
	ssrc := binary.BigEndian.Uint32(wire[8:12])
	nonce := buildRTPNonce(ssrc, sequence, timestamp)
	plain, err := c.aead.Open(dst[:0], nonce[:], wire[headerLength:payloadEnd], wire[:headerLength])
	if err != nil {
		return nil, errors.New("RTP wrapper: authentication failed")
	}
	return plain, nil
}

func buildRTPNonce(ssrc uint32, sequence uint16, timestamp uint32) [12]byte {
	var nonce [12]byte
	binary.BigEndian.PutUint32(nonce[0:4], ssrc)
	binary.BigEndian.PutUint16(nonce[4:6], sequence)
	binary.BigEndian.PutUint32(nonce[8:12], timestamp)
	return nonce
}
