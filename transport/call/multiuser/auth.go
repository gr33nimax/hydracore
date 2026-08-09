package multiuser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	authProtocolVersion = 1
	maximumUserLength   = 64
	maximumPasswordLen  = 256
	maximumAuthFrameLen = 4 + 1 + 16 + 4 + 2 + 2 + 1 + 2 + maximumUserLength + maximumPasswordLen
)

var (
	authMagic = [4]byte{'H', 'C', 'V', 'K'}
	ackMagic  = [4]byte{'H', 'A', 'C', 'K'}
)

type authRequest struct {
	SessionID  [16]byte
	Conv       uint32
	WorkerID   uint16
	WorkerTotal uint16
	User       string
	Password   string
}

func encodeAuthRequest(request authRequest) ([]byte, error) {
	if err := validateAuthStrings(request.User, request.Password); err != nil {
		return nil, err
	}
	if request.SessionID == ([16]byte{}) {
		return nil, errors.New("call multi_user: zero session id")
	}
	if request.Conv == 0 || request.WorkerTotal == 0 || request.WorkerID >= request.WorkerTotal {
		return nil, errors.New("call multi_user: invalid worker auth metadata")
	}
	frame := make([]byte, 32+len(request.User)+len(request.Password))
	copy(frame[0:4], authMagic[:])
	frame[4] = authProtocolVersion
	copy(frame[5:21], request.SessionID[:])
	binary.BigEndian.PutUint32(frame[21:25], request.Conv)
	binary.BigEndian.PutUint16(frame[25:27], request.WorkerID)
	binary.BigEndian.PutUint16(frame[27:29], request.WorkerTotal)
	frame[29] = byte(len(request.User))
	binary.BigEndian.PutUint16(frame[30:32], uint16(len(request.Password)))
	copy(frame[32:32+len(request.User)], request.User)
	copy(frame[32+len(request.User):], request.Password)
	return frame, nil
}

func decodeAuthRequest(frame []byte) (authRequest, error) {
	var request authRequest
	if len(frame) < 32 || len(frame) > maximumAuthFrameLen {
		return request, errors.New("call multi_user: invalid auth frame length")
	}
	if !bytes.Equal(frame[0:4], authMagic[:]) || frame[4] != authProtocolVersion {
		return request, errors.New("call multi_user: unsupported auth frame")
	}
	copy(request.SessionID[:], frame[5:21])
	if request.SessionID == ([16]byte{}) {
		return request, errors.New("call multi_user: zero session id")
	}
	request.Conv = binary.BigEndian.Uint32(frame[21:25])
	request.WorkerID = binary.BigEndian.Uint16(frame[25:27])
	request.WorkerTotal = binary.BigEndian.Uint16(frame[27:29])
	userLength := int(frame[29])
	passwordLength := int(binary.BigEndian.Uint16(frame[30:32]))
	if userLength == 0 || userLength > maximumUserLength || passwordLength == 0 || passwordLength > maximumPasswordLen {
		return request, errors.New("call multi_user: invalid auth identity length")
	}
	if len(frame) != 32+userLength+passwordLength {
		return request, errors.New("call multi_user: malformed auth frame")
	}
	request.User = string(frame[32 : 32+userLength])
	request.Password = string(frame[32+userLength:])
	if err := validateAuthStrings(request.User, request.Password); err != nil {
		return request, err
	}
	if request.Conv == 0 || request.WorkerTotal == 0 || request.WorkerID >= request.WorkerTotal {
		return request, errors.New("call multi_user: invalid auth worker metadata")
	}
	return request, nil
}

func validateAuthStrings(user, password string) error {
	if len(user) == 0 || len(user) > maximumUserLength || !utf8.ValidString(user) {
		return fmt.Errorf("call multi_user: user must be valid UTF-8 and at most %d bytes", maximumUserLength)
	}
	if len(password) == 0 || len(password) > maximumPasswordLen || !utf8.ValidString(password) {
		return fmt.Errorf("call multi_user: password must be valid UTF-8 and at most %d bytes", maximumPasswordLen)
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
		return 0, errors.New("call multi_user: invalid server auth response")
	}
	if frame[5] != 1 {
		return 0, errors.New("call multi_user: authentication rejected")
	}
	generation := binary.BigEndian.Uint64(frame[6:14])
	if generation == 0 {
		return 0, errors.New("call multi_user: invalid server session generation")
	}
	return generation, nil
}
