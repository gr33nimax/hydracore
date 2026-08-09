// SPDX-FileCopyrightText: 2025-2026 SpaceNeuroX contributors
// SPDX-License-Identifier: MIT
//
// Adapted from proxy-turn-vk-android go_client/obfs.go and wrap.go at
// commit 40117047d71f0303504e276b18372c0626b94a35.

package multiuser

import (
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const (
	wrapKeyLength       = 32
	rtpHeaderLength     = 12
	rtpExtendedHdrLen   = 24
	defaultRTPPadding   = 24
	maximumWirePacket   = 64 * 1024
	rtpPayloadTypeAudio = 111
)

func deriveWrapKey(password string) ([wrapKeyLength]byte, error) {
	var key [wrapKeyLength]byte
	if password == "" {
		return key, errors.New("call multi_user: empty obfs_password")
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
	aead       cipher.AEAD
	ssrc       uint32
	initialSeq uint16
	initialTS  uint32
	count      uint64
	mu         sync.Mutex
}

func newRTPCodec(key [wrapKeyLength]byte) (*rtpCodec, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	var seed [10]byte
	if _, err = rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("initialize RTP wrapper: %w", err)
	}
	return &rtpCodec{
		aead:       aead,
		ssrc:       binary.BigEndian.Uint32(seed[0:4]),
		initialSeq: binary.BigEndian.Uint16(seed[4:6]),
		initialTS:  binary.BigEndian.Uint32(seed[6:10]),
	}, nil
}

func (c *rtpCodec) wrap(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("RTP wrapper: empty payload")
	}
	if len(payload)+rtpHeaderLength+chacha20poly1305.Overhead+defaultRTPPadding+1 > maximumWirePacket {
		return nil, errors.New("RTP wrapper: payload too large")
	}

	c.mu.Lock()
	packetIndex := c.count
	c.count++
	c.mu.Unlock()

	sequence := c.initialSeq + uint16(packetIndex)
	timestamp := c.initialTS + uint32(packetIndex)*960 + uint32(packetIndex>>16)
	nonce := buildRTPNonce(c.ssrc, sequence, timestamp)
	var randomPadding [1]byte
	if _, err := rand.Read(randomPadding[:]); err != nil {
		return nil, err
	}
	paddingLength := int(randomPadding[0])%defaultRTPPadding + 1
	out := make([]byte, rtpHeaderLength+len(payload)+chacha20poly1305.Overhead+paddingLength)
	out[0] = 0x80 | 0x20
	out[1] = rtpPayloadTypeAudio
	binary.BigEndian.PutUint16(out[2:4], sequence)
	binary.BigEndian.PutUint32(out[4:8], timestamp)
	binary.BigEndian.PutUint32(out[8:12], c.ssrc)
	sealed := c.aead.Seal(out[rtpHeaderLength:rtpHeaderLength], nonce[:], payload, out[:rtpHeaderLength])
	paddingStart := rtpHeaderLength + len(sealed)
	if paddingLength > 1 {
		if _, err := rand.Read(out[paddingStart : len(out)-1]); err != nil {
			return nil, err
		}
	}
	out[len(out)-1] = byte(paddingLength)
	return out, nil
}

func (c *rtpCodec) unwrap(wire []byte) ([]byte, error) {
	if len(wire) < rtpHeaderLength+chacha20poly1305.Overhead+1 || len(wire) > maximumWirePacket {
		return nil, errors.New("RTP wrapper: invalid packet length")
	}
	if wire[0]>>6 != 2 {
		return nil, errors.New("RTP wrapper: invalid version")
	}
	payloadType := wire[1] & 0x7f
	if payloadType != rtpPayloadTypeAudio && payloadType != 96 {
		return nil, errors.New("RTP wrapper: unexpected payload type")
	}
	headerLength := rtpHeaderLength
	if wire[0]&0x10 != 0 {
		headerLength = rtpExtendedHdrLen
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
	plain, err := c.aead.Open(nil, nonce[:], wire[headerLength:payloadEnd], wire[:headerLength])
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
