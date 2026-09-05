package vkparasite

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// Интервал между повторами auth-фрейма. Совпадает с FlightInterval, который
// прежде задавал темп повторов DTLS-handshake.
const authRetryInterval = 500 * time.Millisecond

var errAuthTimeout = errors.New("call vk_parasite: inner auth timed out")

// exchangeAuth посылает auth-фрейм и ждёт ack на том же пакетном соединении.
//
// Раньше эту фазу несла DTLS-сессия, которая переспрашивала потерянные пакеты
// сама. Обёртка ненадёжна, поэтому запрос повторяется до дедлайна: на мобильной
// сети потерять первый пакет — обычное дело, а отказ стоит целого пути и его
// backoff. Сервер запоминает ответ и переотправляет его на повтор, так что
// потеря ack тоже лечится.
//
// Пакеты, которые не разбираются как ack, пропускаются: на этом соединении
// ничего другого быть не должно, но чужой пакет не повод рвать дозвон.
func exchangeAuth(
	ctx context.Context,
	conn net.PacketConn,
	remote net.Addr,
	request []byte,
	timeout time.Duration,
) (uint64, error) {
	deadline := time.Now().Add(timeout)
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	ack := make([]byte, maximumAuthFrameLen)
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		now := time.Now()
		if !now.Before(deadline) {
			return 0, errAuthTimeout
		}
		if _, err := conn.WriteTo(request, remote); err != nil {
			return 0, fmt.Errorf("write inner auth: %w", err)
		}
		attemptDeadline := now.Add(authRetryInterval)
		if attemptDeadline.After(deadline) {
			attemptDeadline = deadline
		}
		if err := conn.SetReadDeadline(attemptDeadline); err != nil {
			return 0, err
		}
		for {
			n, _, err := conn.ReadFrom(ack)
			if err != nil {
				var timeoutErr net.Error
				if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
					break
				}
				return 0, fmt.Errorf("read inner auth: %w", err)
			}
			generation, decodeErr := decodeAuthAck(ack[:n])
			if decodeErr == nil {
				return generation, nil
			}
			if isTerminalAuthAck(decodeErr) {
				return 0, decodeErr
			}
		}
	}
}

// isTerminalAuthAck отличает отказ сервера от пакета, который просто не ack.
//
// Отказ повторять бессмысленно: пароль не изменится за 500 мс. Всё остальное —
// мусор на соединении, его нужно пропустить и продолжать ждать.
func isTerminalAuthAck(err error) bool {
	return errors.Is(err, errAuthRejected)
}
