package vkparasite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	// Версия внешнего пути.
	//
	// 10 сняла слой DTLS: auth-фрейм и QUIC едут прямо в RTP-обёртке. Клиент
	// версии 9 с этим сервером физически не договорится, поэтому версия
	// поднята — чтобы несовпадение сборок отказывало явно, а не таймаутом.
	authProtocolVersion = 10
	maximumUserLength   = 64
	maximumPasswordLen  = 256
	maximumAuthFrameLen = 4 + 1 + 16 + 4 + 2 + 2 + 8 + 8 + 1 + 2 + maximumUserLength + maximumPasswordLen

	// Длина ack: magic, версия, accept-бит и восемь байт generation либо
	// причины отказа.
	authAckFrameLen    = 14
	CallCount          = 4
	DefaultWorkerCount = 4
	MaximumWorkerCount = 20
)

var (
	authMagic = [4]byte{'H', 'C', 'V', 'K'}
	ackMagic  = [4]byte{'H', 'A', 'C', 'K'}

	// errAuthRejected — отказ сервера, в отличие от «это вообще не ack».
	// Повторять отказ бессмысленно: пароль не изменится за интервал повтора.
	errAuthRejected = errors.New("call vk_parasite: authentication rejected")
)

type authRequest struct {
	ProtocolVersion byte
	SessionID       [16]byte
	Conv            uint32
	WorkerID        uint16
	WorkerTotal     uint16
	WorkerEpoch     uint64
	LaneGeneration  uint64
	User            string
	Password        string
}

func validWorkerMetadata(request authRequest) bool {
	return request.Conv != 0 &&
		supportedWorkerCount(request.WorkerTotal) &&
		request.WorkerID < request.WorkerTotal
}

func encodeAuthRequest(request authRequest) ([]byte, error) {
	if err := validateAuthStrings(request.User, request.Password); err != nil {
		return nil, err
	}
	if request.SessionID == ([16]byte{}) {
		return nil, errors.New("call vk_parasite: zero session id")
	}
	if !validWorkerMetadata(request) {
		return nil, errors.New("call vk_parasite: invalid worker auth metadata")
	}
	if request.WorkerEpoch == 0 {
		return nil, errors.New("call vk_parasite: zero worker epoch")
	}
	if request.LaneGeneration == 0 {
		return nil, errors.New("call vk_parasite: zero lane generation")
	}
	headerLength := 48
	frame := make([]byte, headerLength+len(request.User)+len(request.Password))
	copy(frame[0:4], authMagic[:])
	frame[4] = authProtocolVersion
	copy(frame[5:21], request.SessionID[:])
	binary.BigEndian.PutUint32(frame[21:25], request.Conv)
	binary.BigEndian.PutUint16(frame[25:27], request.WorkerID)
	binary.BigEndian.PutUint16(frame[27:29], request.WorkerTotal)
	binary.BigEndian.PutUint64(frame[29:37], request.WorkerEpoch)
	binary.BigEndian.PutUint64(frame[37:45], request.LaneGeneration)
	identityOffset := 45
	frame[identityOffset] = byte(len(request.User))
	binary.BigEndian.PutUint16(frame[identityOffset+1:identityOffset+3], uint16(len(request.Password)))
	copy(frame[identityOffset+3:identityOffset+3+len(request.User)], request.User)
	copy(frame[identityOffset+3+len(request.User):], request.Password)
	return frame, nil
}

func decodeAuthRequest(frame []byte) (authRequest, error) {
	var request authRequest
	if len(frame) < 48 || len(frame) > maximumAuthFrameLen {
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
	request.LaneGeneration = binary.BigEndian.Uint64(frame[37:45])
	if request.LaneGeneration == 0 {
		return request, errors.New("call vk_parasite: zero lane generation")
	}
	identityOffset := 45
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
	if !validWorkerMetadata(request) {
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

// Why the server refused a worker.
//
// The acknowledgement frame carries one accept bit, and the server used to collapse every
// refusal into it: a wrong password, a worker count the server will not host and a frame it
// could not parse all reached the client as "authentication rejected". They call for completely
// different actions, and telling them apart meant reading the server's own log — which on a real
// incident cost an afternoon.
//
// The reason travels in the byte that holds the top of the session generation, which is zero on
// every refusal, so the frame stays fourteen bytes at protocol version 10. An older client reads
// the accept bit and ignores this; an older server leaves it zero, which is Unspecified. Neither
// side needs to know about the other.
const (
	AuthRejectUnspecified byte = iota
	AuthRejectCredentials
	AuthRejectWorkerCount
	AuthRejectMalformed
	AuthRejectSession
)

func authRejectReasonName(reason byte) string {
	switch reason {
	case AuthRejectCredentials:
		return "user or password rejected"
	case AuthRejectWorkerCount:
		return "worker count refused by the server"
	case AuthRejectMalformed:
		return "malformed authentication request"
	case AuthRejectSession:
		return "session identity mismatch"
	default:
		return "reason not given"
	}
}

func encodeAuthAck(accepted bool, generation uint64, reason byte) []byte {
	frame := make([]byte, 14)
	copy(frame[0:4], ackMagic[:])
	frame[4] = authProtocolVersion
	if accepted {
		frame[5] = 1
		binary.BigEndian.PutUint64(frame[6:14], generation)

		return frame
	}
	frame[6] = reason

	return frame
}

func decodeAuthAck(frame []byte) (uint64, error) {
	if len(frame) != authAckFrameLen || !bytes.Equal(frame[0:4], ackMagic[:]) || frame[4] != authProtocolVersion {
		return 0, errors.New("call vk_parasite: invalid server auth response")
	}
	if frame[5] != 1 {
		return 0, fmt.Errorf("%w: %s", errAuthRejected, authRejectReasonName(frame[6]))
	}
	generation := binary.BigEndian.Uint64(frame[6:14])
	if generation == 0 {
		return 0, errors.New("call vk_parasite: invalid server session generation")
	}
	return generation, nil
}

// readAuthRequest читает auth-фрейм из потока.
//
// Длина фрейма записана в самом фрейме: фиксированные 48 байт заголовка несут
// длины имени и пароля. Поэтому кадрирование поверх потока не нужно — читаем
// заголовок, считаем остаток, читаем его.
func readAuthRequest(reader io.Reader) (authRequest, error) {
	const headerLength = 48
	frame := make([]byte, maximumAuthFrameLen)
	if _, err := io.ReadFull(reader, frame[:headerLength]); err != nil {
		return authRequest{}, err
	}
	userLength := int(frame[45])
	passwordLength := int(binary.BigEndian.Uint16(frame[46:48]))
	total := headerLength + userLength + passwordLength
	if total > maximumAuthFrameLen {
		return authRequest{}, errors.New("call vk_parasite: auth frame is too large")
	}
	if total > headerLength {
		if _, err := io.ReadFull(reader, frame[headerLength:total]); err != nil {
			return authRequest{}, err
		}
	}
	return decodeAuthRequest(frame[:total])
}
