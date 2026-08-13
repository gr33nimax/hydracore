package vkparasite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	authProtocolVersion   = 4
	maximumUserLength     = 64
	maximumPasswordLen    = 256
	maximumAuthFrameLen   = 4 + 1 + 16 + 4 + 2 + 2 + 8 + 1 + 2 + maximumUserLength + maximumPasswordLen
)

var (
	authMagic = [4]byte{'H', 'C', 'V', 'K'}
	ackMagic  = [4]byte{'H', 'A', 'C', 'K'}
)

type authRequest struct {
	ProtocolVersion byte
	SessionID       [16]byte
	Conv            uint32
	WorkerID        uint16
	WorkerTotal     uint16
	WorkerEpoch     uint64
	User            string
	Password        string
}

func encodeAuthRequest(request authRequest) ([]byte, error) {
	if err := validateAuthStrings(request.User, request.Password); err != nil {
		return nil, err
	}
	if request.SessionID == ([16]byte{}) {
		return nil, errors.New("call vk_parasite: zero session id")
	}
	if request.Conv == 0 || request.WorkerTotal != LaneCount || request.WorkerID >= LaneCount {
		return nil, errors.New("call vk_parasite: invalid worker auth metadata")
	}
	if request.WorkerEpoch == 0 {
		return nil, errors.New("call vk_parasite: zero worker epoch")
	}
	headerLength := 40
	frame := make([]byte, headerLength+len(request.User)+len(request.Password))
	copy(frame[0:4], authMagic[:])
	frame[4] = authProtocolVersion
	copy(frame[5:21], request.SessionID[:])
	binary.BigEndian.PutUint32(frame[21:25], request.Conv)
	binary.BigEndian.PutUint16(frame[25:27], request.WorkerID)
	binary.BigEndian.PutUint16(frame[27:29], request.WorkerTotal)
	binary.BigEndian.PutUint64(frame[29:37], request.WorkerEpoch)
	identityOffset := 37
	frame[identityOffset] = byte(len(request.User))
	binary.BigEndian.PutUint16(frame[identityOffset+1:identityOffset+3], uint16(len(request.Password)))
	copy(frame[identityOffset+3:identityOffset+3+len(request.User)], request.User)
	copy(frame[identityOffset+3+len(request.User):], request.Password)
	return frame, nil
}

func decodeAuthRequest(frame []byte) (authRequest, error) {
	var request authRequest
	if len(frame) < 40 || len(frame) > maximumAuthFrameLen {
		return request, errors.New("call vk_parasite: invalid auth frame length")
	}
	if !bytes.Equal(frame[0:4], authMagic[:]) || frame[4] != authProtocolVersion {
		return request, errors.New("call vk_parasite: unsupported auth frame")
	}
	request.ProtocolVersion = frame[4]
	copy(request.SessionID[:], frame[5:21])
	if request.SessionID == ([16]byte{}) {
		return request, errors.New("call vk_parasite: zero session id")
	}
	request.Conv = binary.BigEndian.Uint32(frame[21:25])
	request.WorkerID = binary.BigEndian.Uint16(frame[25:27])
	request.WorkerTotal = binary.BigEndian.Uint16(frame[27:29])
	request.WorkerEpoch = binary.BigEndian.Uint64(frame[29:37])
	if request.WorkerEpoch == 0 {
		return request, errors.New("call vk_parasite: zero worker epoch")
	}
	identityOffset := 37
	userLength := int(frame[identityOffset])
	passwordLength := int(binary.BigEndian.Uint16(frame[identityOffset+1 : identityOffset+3]))
	if userLength == 0 || userLength > maximumUserLength || passwordLength == 0 || passwordLength > maximumPasswordLen {
		return request, errors.New("call vk_parasite: invalid auth identity length")
	}
	headerLength := identityOffset + 3
	if len(frame) != headerLength+userLength+passwordLength {
		return request, errors.New("call vk_parasite: malformed auth frame")
	}
	request.User = string(frame[headerLength : headerLength+userLength])
	request.Password = string(frame[headerLength+userLength:])
	if err := validateAuthStrings(request.User, request.Password); err != nil {
		return request, err
	}
	if request.Conv == 0 || request.WorkerTotal != LaneCount || request.WorkerID >= LaneCount {
		return request, errors.New("call vk_parasite: invalid auth worker metadata")
	}
	return request, nil
}

func validateAuthStrings(user, password string) error {
	if len(user) == 0 || len(user) > maximumUserLength || !utf8.ValidString(user) {
		return fmt.Errorf("call vk_parasite: user must be valid UTF-8 and at most %d bytes", maximumUserLength)
	}
	if len(password) == 0 || len(password) > maximumPasswordLen || !utf8.ValidString(password) {
		return fmt.Errorf("call vk_parasite: password must be valid UTF-8 and at most %d bytes", maximumPasswordLen)
	}
	return nil
}

func encodeAuthAck(accepted bool, generation uint64) []byte {
	frame := make([]byte, 14)
	copy(frame[0:4], ackMagic[:])
	frame[4] = authProtocolVersion
	if accepted {
		frame[5] = 1
	}
	binary.BigEndian.PutUint64(frame[6:14], generation)
	return frame
}

func decodeAuthAck(frame []byte) (uint64, error) {
	if len(frame) != 14 || !bytes.Equal(frame[0:4], ackMagic[:]) || frame[4] != authProtocolVersion {
		return 0, errors.New("call vk_parasite: invalid server auth response")
	}
	if frame[5] != 1 {
		return 0, errors.New("call vk_parasite: authentication rejected")
	}
	generation := binary.BigEndian.Uint64(frame[6:14])
	if generation == 0 {
		return 0, errors.New("call vk_parasite: invalid server session generation")
	}
	return generation, nil
}
