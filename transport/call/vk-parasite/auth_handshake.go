package vkparasite

import (
	"context"
	"io"
	"time"

	"github.com/sagernet/quic-go"
)

// exchangeAuth аутентифицирует worker'а на первом потоке QUIC-соединения.
//
// Раньше auth-фрейм ехал отдельной датаграммой поверх обёртки, а до этого — по
// DTLS. Датаграмма ненадёжна, поэтому фрейм приходилось повторять до дедлайна, а
// сервер держал последний ack, чтобы ответить на повтор. Поток надёжен и
// упорядочен, так что вся эта механика не нужна: одна запись, одно чтение.
//
// Соединение открывается до аутентификации. Пускать в него посторонних всё равно
// нечем: пакет без ключа обёртки не проходит внешний AEAD и до QUIC не доходит.
func exchangeAuth(ctx context.Context, conn *quic.Conn, request []byte, timeout time.Duration) (uint64, error) {
	streamCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(streamCtx)
	if err != nil {
		return 0, err
	}
	defer func() {
		stream.CancelRead(0)
		_ = stream.Close()
	}()
	if err = stream.SetDeadline(time.Now().Add(timeout)); err != nil {
		return 0, err
	}
	if _, err = stream.Write(request); err != nil {
		return 0, err
	}
	var ack [authAckFrameLen]byte
	if _, err = io.ReadFull(stream, ack[:]); err != nil {
		return 0, err
	}
	return decodeAuthAck(ack[:])
}
