// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/log"
)

type transport struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      log.ContextLogger
	dialer      coreDialer
	peer        *net.UDPAddr
	localConn   net.PacketConn
	localPort   uint16
	deviceID    string
	password    string
	hashes      []string
	workerCount int
	obfsMode    string
	wrapKey     []byte

	dispatcher             *dispatcher
	credentialFetcher      *credentialFetcher
	configurationCh        chan string
	initializationErrorCh  chan error
	configurationClaimed   atomic.Bool
	configurationDelivered atomic.Bool
	waitGroup              sync.WaitGroup
	closeOnce              sync.Once
}

func newTransport(
	ctx context.Context,
	logger log.ContextLogger,
	dialer coreDialer,
	peer *net.UDPAddr,
	localConn net.PacketConn,
	localPort uint16,
	deviceID string,
	password string,
	hashes []string,
	workerCount int,
	obfsMode string,
) (*transport, error) {
	wrapKey, err := deriveWrapKey(password)
	if err != nil {
		return nil, err
	}
	transportContext, cancel := context.WithCancel(ctx)
	t := &transport{
		ctx:                   transportContext,
		cancel:                cancel,
		logger:                logger,
		dialer:                dialer,
		peer:                  peer,
		localConn:             localConn,
		localPort:             localPort,
		deviceID:              deviceID,
		password:              password,
		hashes:                append([]string(nil), hashes...),
		workerCount:           workerCount,
		obfsMode:              obfsMode,
		wrapKey:               wrapKey,
		credentialFetcher:     newCredentialFetcher(dialer),
		configurationCh:       make(chan string, 1),
		initializationErrorCh: make(chan error, 1),
	}
	t.dispatcher = newDispatcher(transportContext, localConn)
	return t, nil
}

func (t *transport) start() {
	for workerID := 0; workerID < t.workerCount; workerID++ {
		t.waitGroup.Add(1)
		go t.runWorker(workerID, t.hashes[workerID%len(t.hashes)])
	}
}

func (t *transport) waitConfiguration(ctx context.Context) (string, error) {
	select {
	case configuration := <-t.configurationCh:
		t.configurationDelivered.Store(true)
		return configuration, nil
	case err := <-t.initializationErrorCh:
		return "", err
	case <-ctx.Done():
		return "", context.Cause(ctx)
	case <-t.ctx.Done():
		return "", context.Cause(t.ctx)
	}
}

func (t *transport) runWorker(workerID int, hash string) {
	defer t.waitGroup.Done()
	if workerID > 0 {
		select {
		case <-time.After(time.Duration(workerID%9) * 500 * time.Millisecond):
		case <-t.ctx.Done():
			return
		}
	}
	for attempt := 0; ; attempt++ {
		if t.ctx.Err() != nil {
			return
		}
		credentials, err := t.credentialFetcher.get(t.ctx, hash)
		if err != nil {
			if errors.Is(err, errVKCaptchaRequired) {
				t.reportInitializationError(err)
				return
			}
			t.logger.WarnContext(t.ctx, "WDTT worker ", workerID, " could not acquire anonymous TURN credentials: ", err)
			if !t.waitRetry(workerID, attempt, false) {
				return
			}
			continue
		}

		requestConfiguration := !t.configurationDelivered.Load() && t.configurationClaimed.CompareAndSwap(false, true)
		err = runSession(
			t.ctx,
			workerID,
			t.dialer,
			t.peer,
			credentials,
			t.wrapKey,
			t.obfsMode,
			t.dispatcher,
			t.localPort,
			t.deviceID,
			t.password,
			requestConfiguration,
			t.configurationCh,
		)
		if requestConfiguration && !t.configurationDelivered.Load() {
			t.configurationClaimed.Store(false)
		}
		if t.ctx.Err() != nil {
			return
		}
		if err != nil {
			lowerError := strings.ToLower(err.Error())
			if strings.Contains(lowerError, "wdtt authentication failed") {
				t.reportInitializationError(err)
				return
			}
			credentialsExpired := strings.Contains(lowerError, "turn") &&
				(strings.Contains(lowerError, "credential") ||
					strings.Contains(lowerError, "stale nonce") ||
					strings.Contains(lowerError, "attribute not found") ||
					strings.Contains(lowerError, "error 508"))
			if credentialsExpired {
				t.credentialFetcher.invalidate(hash)
			}
			t.logger.WarnContext(t.ctx, "WDTT worker ", workerID, " disconnected: ", err)
			quota := strings.Contains(lowerError, "quota") || strings.Contains(lowerError, "486")
			if !t.waitRetry(workerID, attempt, quota) {
				return
			}
		} else if !t.waitRetry(workerID, attempt, false) {
			// A clean session shutdown is unusual but must not become a hot loop.
			return
		}
	}
}

func (t *transport) waitRetry(workerID int, attempt int, quota bool) bool {
	delay := time.Duration(5+(workerID+attempt)%11) * time.Second
	if quota {
		delay = time.Duration(30+(workerID+attempt)%31) * time.Second
	}
	select {
	case <-time.After(delay):
		return true
	case <-t.ctx.Done():
		return false
	}
}

func (t *transport) reportInitializationError(err error) {
	select {
	case t.initializationErrorCh <- err:
	default:
	}
}

func (t *transport) close() {
	t.closeOnce.Do(func() {
		t.cancel()
		t.dispatcher.close()
		_ = t.localConn.Close()
		t.waitGroup.Wait()
		for index := range t.wrapKey {
			t.wrapKey[index] = 0
		}
	})
}
