// SPDX-License-Identifier: MIT

package wdtt

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	hydrawrap "github.com/gr33nimax/hydra-wdtt/pkg/wrap"
	"golang.org/x/crypto/chacha20poly1305"
)

const wrapKeyLength = hydrawrap.KeySize

func deriveWrapKey(password string) ([]byte, error) {
	return hydrawrap.DeriveKey(password)
}

type obfsConfig struct {
	ssrc        uint32
	payloadType uint8
	paddingMax  int
	keyHint     *hydrawrap.KeyHint
}

func newObfsConfig(mode string, hints ...hydrawrap.KeyHint) (*obfsConfig, error) {
	var randomBytes [4]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return nil, fmt.Errorf("initialize WDTT obfuscation: %w", err)
	}
	config := &obfsConfig{
		ssrc:        binary.BigEndian.Uint32(randomBytes[:]),
		payloadType: 111,
		paddingMax:  24,
	}
	if strings.EqualFold(mode, "video") {
		config.payloadType = 96
		config.paddingMax = 60
	}
	if len(hints) > 0 {
		hint := hints[0]
		config.keyHint = &hint
	}
	return config, nil
}

type obfsState struct {
	mu      sync.Mutex
	initSeq uint16
	initTS  uint32
	count   uint64
}

func newObfsState() (*obfsState, error) {
	var randomBytes [6]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return nil, fmt.Errorf("initialize WDTT obfuscation state: %w", err)
	}
	return &obfsState{
		initSeq: binary.BigEndian.Uint16(randomBytes[0:2]),
		initTS:  binary.BigEndian.Uint32(randomBytes[2:6]),
	}, nil
}

func (s *obfsState) next() (uint16, uint32) {
	s.mu.Lock()
	count := s.count
	s.count++
	s.mu.Unlock()
	return s.initSeq + uint16(count), s.initTS + uint32(count)*960 + uint32(count>>16)
}

func buildNonce(ssrc uint32, sequence uint16, timestamp uint32) []byte {
	nonce := make([]byte, chacha20poly1305.NonceSize)
	binary.BigEndian.PutUint32(nonce[0:4], ssrc)
	binary.BigEndian.PutUint16(nonce[4:6], sequence)
	binary.BigEndian.PutUint32(nonce[8:12], timestamp)
	return nonce
}

func wrapPacket(aead cipher.AEAD, payload []byte, config *obfsConfig, state *obfsState) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("cannot wrap an empty WDTT packet")
	}
	sequence, timestamp := state.next()
	paddingRandom := 0
	if config.paddingMax > 0 {
		var randomByte [1]byte
		if _, err := rand.Read(randomByte[:]); err != nil {
			return nil, fmt.Errorf("generate WDTT padding: %w", err)
		}
		paddingRandom = int(randomByte[0]) % config.paddingMax
	}
	paddingLength := paddingRandom + 1
	if config.keyHint != nil {
		paddingLength += 10
	}
	output := make([]byte, 12+len(payload)+aead.Overhead()+paddingLength)
	output[0] = 0x80 | 0x20
	output[1] = config.payloadType & 0x7F
	binary.BigEndian.PutUint16(output[2:4], sequence)
	binary.BigEndian.PutUint32(output[4:8], timestamp)
	binary.BigEndian.PutUint32(output[8:12], config.ssrc)
	sealed := aead.Seal(output[12:12], buildNonce(config.ssrc, sequence, timestamp), payload, output[:12])
	paddingStart := 12 + len(sealed)
	if err := hydrawrap.FillRTPPadding(output[paddingStart:], config.keyHint); err != nil {
		return nil, fmt.Errorf("generate WDTT padding: %w", err)
	}
	return output, nil
}

func unwrapPacket(aead cipher.AEAD, wire []byte, destination []byte) (int, error) {
	if len(wire) < 12+aead.Overhead()+1 || wire[0]>>6 != 2 {
		return 0, fmt.Errorf("invalid WDTT RTP packet")
	}
	payloadType := wire[1] & 0x7F
	if payloadType != 111 && payloadType != 96 {
		return 0, fmt.Errorf("invalid WDTT RTP payload type")
	}
	payloadEnd := len(wire)
	if wire[0]&0x20 != 0 {
		paddingLength := int(wire[len(wire)-1])
		if paddingLength == 0 || paddingLength > payloadEnd-12 {
			return 0, fmt.Errorf("invalid WDTT RTP padding")
		}
		payloadEnd -= paddingLength
	}
	plainLength := payloadEnd - 12 - aead.Overhead()
	if plainLength <= 0 || plainLength > len(destination) {
		return 0, fmt.Errorf("invalid WDTT wrapped payload size")
	}
	sequence := binary.BigEndian.Uint16(wire[2:4])
	timestamp := binary.BigEndian.Uint32(wire[4:8])
	ssrc := binary.BigEndian.Uint32(wire[8:12])
	plain, err := aead.Open(destination[:0], buildNonce(ssrc, sequence, timestamp), wire[12:payloadEnd], wire[:12])
	if err != nil {
		return 0, fmt.Errorf("authenticate WDTT wrapped packet: %w", err)
	}
	return len(plain), nil
}

func newWrapAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != wrapKeyLength {
		return nil, fmt.Errorf("WDTT WRAP key must be %d bytes", wrapKeyLength)
	}
	return chacha20poly1305.New(key)
}
