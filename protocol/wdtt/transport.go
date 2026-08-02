// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gr33nimax/hydra-wdtt/pkg/access"
	"github.com/gr33nimax/hydra-wdtt/pkg/workers"
	hydrawrap "github.com/gr33nimax/hydra-wdtt/pkg/wrap"
	"github.com/sagernet/sing-box/log"
)

const workerGenerationDrain = 5 * time.Second

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

type leaseSnapshot struct {
	token        string
	expiresAt    time.Time
	refreshAfter time.Time
}

type workerGeneration struct {
	id         int
	ctx        context.Context
	cancel     context.CancelFunc
	ready      chan struct{}
	readyCount atomic.Int32
	readyOnce  sync.Once
}

func newWorkerGeneration(parent context.Context, id int) *workerGeneration {
	generationContext, cancel := context.WithCancel(parent)
	return &workerGeneration{id: id, ctx: generationContext, cancel: cancel, ready: make(chan struct{})}
}

func (generation *workerGeneration) markReady() {
	if generation.readyCount.Add(1) >= workers.Minimum {
		generation.readyOnce.Do(func() { close(generation.ready) })
	}
}

type transport struct {
	ctx           context.Context
	cancel        context.CancelFunc
	logger        log.ContextLogger
	dialer        coreDialer
	peer          *net.UDPAddr
	localConn     net.PacketConn
	localPort     uint16
	deviceID      string
	credentialRef string
	runtimeID     string
	grantToken    string
	legacy        bool
	hashes        []string
	workerCount   int
	obfsMode      string

	dispatcher             *dispatcher
	credentialFetcher      *credentialFetcher
	configurationCh        chan sessionConfiguration
	initializationErrorCh  chan error
	configurationClaimed   atomic.Bool
	configurationDelivered atomic.Bool

	leaseMu      sync.RWMutex
	lease        *leaseSnapshot
	leaseReady   chan struct{}
	leaseReadyDo sync.Once

	generationMu sync.Mutex
	generations  map[int]*workerGeneration
	active       int

	waitGroup sync.WaitGroup
	closeOnce sync.Once
}

func newTransport(
	ctx context.Context,
	logger log.ContextLogger,
	dialer coreDialer,
	peer *net.UDPAddr,
	localConn net.PacketConn,
	localPort uint16,
	deviceID string,
	credentialRef string,
	credentialSecret string,
	hashes []string,
	workerCount int,
	obfsMode string,
	vkAuth string,
) (*transport, error) {
	if _, err := hydrawrap.DeriveKey(credentialSecret); err != nil {
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
		credentialRef:         credentialRef,
		runtimeID:             uuid.NewString(),
		grantToken:            credentialSecret,
		legacy:                credentialRef == "",
		hashes:                append([]string(nil), hashes...),
		workerCount:           workerCount,
		obfsMode:              obfsMode,
		credentialFetcher:     newCredentialFetcher(dialer, credentialRef, vkAuth),
		configurationCh:       make(chan sessionConfiguration, 1),
		initializationErrorCh: make(chan error, 1),
		leaseReady:            make(chan struct{}),
		generations:           make(map[int]*workerGeneration),
		active:                1,
	}
	t.dispatcher = newDispatcher(transportContext, localConn)
	return t, nil
}

func (t *transport) start() {
	initial := newWorkerGeneration(t.ctx, 1)
	t.generationMu.Lock()
	t.generations[initial.id] = initial
	t.generationMu.Unlock()
	t.startGeneration(initial, true)
	if !t.legacy {
		t.waitGroup.Add(1)
		go t.rotationLoop()
	}
}

func (t *transport) startGeneration(generation *workerGeneration, initial bool) {
	for workerID := 0; workerID < t.workerCount; workerID++ {
		t.waitGroup.Add(1)
		go func(workerID int, hash string) {
			defer t.waitGroup.Done()
			t.runWorker(generation, workerID, hash, initial)
		}(workerID, t.hashes[workerID%len(t.hashes)])
	}
}

func (t *transport) waitConfiguration(ctx context.Context) (string, error) {
	select {
	case configuration := <-t.configurationCh:
		if configuration.lease != nil {
			t.installLease(*configuration.lease)
		}
		t.configurationDelivered.Store(true)
		return configuration.content, nil
	case err := <-t.initializationErrorCh:
		return "", err
	case <-ctx.Done():
		return "", context.Cause(ctx)
	case <-t.ctx.Done():
		return "", context.Cause(t.ctx)
	}
}

func (t *transport) runWorker(generation *workerGeneration, workerID int, hash string, initial bool) {
	if workerID > 0 {
		select {
		case <-time.After(time.Duration(workerID%workers.PerGroup) * 500 * time.Millisecond):
		case <-generation.ctx.Done():
			return
		}
	}
	var readyOnce sync.Once
	for attempt := 0; ; attempt++ {
		if generation.ctx.Err() != nil {
			return
		}

		purpose := sessionPurposeAuthenticate
		requestConfiguration := initial && !t.configurationDelivered.Load() && t.configurationClaimed.CompareAndSwap(false, true)
		if requestConfiguration {
			purpose = sessionPurposeConfigure
		}
		authorization, authErr := t.workerAuthorization(generation.ctx, requestConfiguration)
		if authErr != nil {
			if requestConfiguration {
				t.configurationClaimed.Store(false)
			}
			return
		}
		wrapKey, err := hydrawrap.DeriveKey(authorization.token)
		if err != nil {
			t.reportInitializationError(err)
			return
		}
		keyHint := hydrawrap.HintForSecret(authorization.token)
		credentials, err := t.credentialFetcher.get(generation.ctx, hash)
		if err != nil {
			if requestConfiguration {
				t.configurationClaimed.Store(false)
			}
			if errors.Is(err, errVKCaptchaRequired) || errors.Is(err, errVKAccountCredentialsRequired) {
				t.reportInitializationError(annotateCredentialChallenge(err, t.credentialRef))
				return
			}
			t.logger.WarnContext(generation.ctx, "WDTT worker ", workerID, " could not acquire VK TURN credentials: ", err)
			if !t.waitRetry(generation.ctx, workerID, attempt, false) {
				return
			}
			continue
		}

		err = runSession(
			generation.ctx,
			workerID,
			t.dialer,
			t.peer,
			credentials,
			wrapKey,
			keyHint,
			t.obfsMode,
			t.dispatcher,
			t.localPort,
			authorization,
			purpose,
			generation.id,
			t.configurationCh,
			nil,
			func() { readyOnce.Do(generation.markReady) },
		)
		zeroBytes(wrapKey)
		if requestConfiguration && !t.configurationDelivered.Load() {
			t.configurationClaimed.Store(false)
		}
		if generation.ctx.Err() != nil {
			return
		}
		if err != nil {
			lowerError := strings.ToLower(err.Error())
			if strings.Contains(lowerError, "authentication failed") && !t.configurationDelivered.Load() {
				t.reportInitializationError(err)
				return
			}
			credentialsExpired := strings.Contains(lowerError, "turn") &&
				(strings.Contains(lowerError, "credential") || strings.Contains(lowerError, "stale nonce") || strings.Contains(lowerError, "attribute not found") || strings.Contains(lowerError, "error 508"))
			if credentialsExpired {
				t.credentialFetcher.invalidate(hash)
			}
			t.logger.WarnContext(generation.ctx, "WDTT worker ", workerID, " disconnected: ", err)
			quota := strings.Contains(lowerError, "quota") || strings.Contains(lowerError, "486") || strings.Contains(lowerError, "worker_quota")
			if !t.waitRetry(generation.ctx, workerID, attempt, quota) {
				return
			}
		} else if !t.waitRetry(generation.ctx, workerID, attempt, false) {
			return
		}
	}
}

func annotateCredentialChallenge(err error, credentialRef string) error {
	if err == nil || credentialRef == "" {
		return err
	}
	return fmt.Errorf("%w for credential_ref %q", err, credentialRef)
}

func (t *transport) workerAuthorization(ctx context.Context, requestConfiguration bool) (sessionAuthorization, error) {
	if t.legacy || requestConfiguration {
		return sessionAuthorization{
			credentialRef: t.credentialRef,
			deviceID:      t.deviceID,
			token:         t.grantToken,
			workerCount:   t.workerCount,
			runtimeID:     t.runtimeID,
			legacy:        t.legacy,
		}, nil
	}
	select {
	case <-t.leaseReady:
	case <-ctx.Done():
		return sessionAuthorization{}, context.Cause(ctx)
	}
	t.leaseMu.RLock()
	lease := t.lease
	t.leaseMu.RUnlock()
	if lease == nil || lease.token == "" {
		return sessionAuthorization{}, errors.New("Hydra WDTT session lease is unavailable")
	}
	return sessionAuthorization{
		credentialRef: t.credentialRef,
		deviceID:      t.deviceID,
		token:         lease.token,
		workerCount:   t.workerCount,
		runtimeID:     t.runtimeID,
	}, nil
}

func (t *transport) installLease(lease access.IssuedLease) {
	now := time.Now()
	expiresAt := time.Unix(lease.ExpiresAt, 0)
	refreshAfter := time.Unix(lease.RefreshAfter, 0)
	if !expiresAt.After(now) {
		expiresAt = now.Add(access.SessionTTL)
	}
	if !refreshAfter.After(now) || !refreshAfter.Before(expiresAt) {
		refreshAfter = now.Add(access.SessionRefreshAfter)
	}
	t.leaseMu.Lock()
	previous := t.lease
	t.lease = &leaseSnapshot{token: lease.Token, expiresAt: expiresAt, refreshAfter: refreshAfter}
	t.leaseMu.Unlock()
	if previous != nil {
		previous.token = ""
	}
	t.leaseReadyDo.Do(func() { close(t.leaseReady) })
}

func (t *transport) currentLease() *leaseSnapshot {
	t.leaseMu.RLock()
	defer t.leaseMu.RUnlock()
	if t.lease == nil {
		return nil
	}
	return &leaseSnapshot{token: t.lease.token, expiresAt: t.lease.expiresAt, refreshAfter: t.lease.refreshAfter}
}

func (t *transport) rotationLoop() {
	defer t.waitGroup.Done()
	select {
	case <-t.leaseReady:
	case <-t.ctx.Done():
		return
	}
	for {
		lease := t.currentLease()
		if lease == nil {
			return
		}
		delay := time.Until(lease.refreshAfter)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-t.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}

		newLease, err := t.renewLease(lease)
		if err != nil {
			t.logger.WarnContext(t.ctx, "Hydra WDTT lease renewal failed: ", err)
			select {
			case <-time.After(15 * time.Second):
				continue
			case <-t.ctx.Done():
				return
			}
		}
		t.installLease(newLease)
		if err := t.rotateGeneration(); err != nil {
			t.logger.WarnContext(t.ctx, "Hydra WDTT worker generation rotation failed: ", err)
		}
	}
}

func (t *transport) renewLease(lease *leaseSnapshot) (access.IssuedLease, error) {
	renewContext, cancel := context.WithTimeout(t.ctx, startupTimeout)
	defer cancel()
	credentials, err := t.credentialFetcher.get(renewContext, t.hashes[0])
	if err != nil {
		return access.IssuedLease{}, err
	}
	wrapKey, err := hydrawrap.DeriveKey(lease.token)
	if err != nil {
		return access.IssuedLease{}, err
	}
	defer zeroBytes(wrapKey)
	renewalCh := make(chan access.IssuedLease, 1)
	authorization := sessionAuthorization{
		credentialRef: t.credentialRef,
		deviceID:      t.deviceID,
		token:         lease.token,
		workerCount:   t.workerCount,
		runtimeID:     t.runtimeID,
	}
	if err := runSession(
		renewContext,
		-1,
		t.dialer,
		t.peer,
		credentials,
		wrapKey,
		hydrawrap.HintForSecret(lease.token),
		t.obfsMode,
		t.dispatcher,
		t.localPort,
		authorization,
		sessionPurposeRenew,
		0,
		nil,
		renewalCh,
		nil,
	); err != nil {
		return access.IssuedLease{}, err
	}
	select {
	case renewed := <-renewalCh:
		return renewed, nil
	default:
		return access.IssuedLease{}, errors.New("Hydra WDTT lease renewal returned no result")
	}
}

func (t *transport) rotateGeneration() error {
	t.generationMu.Lock()
	oldGeneration := t.generations[t.active]
	newID := t.active + 1
	newGeneration := newWorkerGeneration(t.ctx, newID)
	t.generations[newID] = newGeneration
	t.generationMu.Unlock()

	t.startGeneration(newGeneration, false)
	warmup := time.NewTimer(startupTimeout)
	defer warmup.Stop()
	select {
	case <-newGeneration.ready:
	case <-warmup.C:
		newGeneration.cancel()
		return errors.New("new Hydra WDTT generation did not reach nine ready workers")
	case <-t.ctx.Done():
		newGeneration.cancel()
		return context.Cause(t.ctx)
	}

	t.dispatcher.activateGeneration(newID)
	t.generationMu.Lock()
	t.active = newID
	t.generationMu.Unlock()
	t.logger.InfoContext(t.ctx, "Hydra WDTT switched to worker generation ", newID, " after ", workers.Minimum, " workers became ready")

	select {
	case <-time.After(workerGenerationDrain):
	case <-t.ctx.Done():
	}
	if oldGeneration != nil {
		oldGeneration.cancel()
	}
	return nil
}

func (t *transport) waitRetry(ctx context.Context, workerID int, attempt int, quota bool) bool {
	delay := time.Duration(5+(workerID+attempt)%11) * time.Second
	if quota {
		delay = time.Duration(30+(workerID+attempt)%31) * time.Second
	}
	select {
	case <-time.After(delay):
		return true
	case <-ctx.Done():
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
		t.grantToken = ""
		t.leaseMu.Lock()
		if t.lease != nil {
			t.lease.token = ""
			t.lease = nil
		}
		t.leaseMu.Unlock()
	})
}
