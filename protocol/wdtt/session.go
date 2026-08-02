// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cbeuw/connutil"
	"github.com/gr33nimax/hydra-wdtt/pkg/access"
	hydrawrap "github.com/gr33nimax/hydra-wdtt/pkg/wrap"
	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	"github.com/pion/logging"
	"github.com/pion/transport/v4/stdnet"
	"github.com/pion/turn/v5"
)

const (
	wdttReadBufferSize = 2048
	turnSocketBuffer   = 625 * 1024
	sessionIdleTimeout = 30 * time.Minute
	keepaliveInterval  = 15 * time.Second
	keepaliveByte      = 0xFF
)

var (
	handshakeSemaphore   = make(chan struct{}, 3)
	errConfigUnavailable = errors.New("WDTT server has not issued a WireGuard configuration yet")
)

type turnCredentials struct {
	username string
	password string
	urls     []string
}

type sessionPurpose uint8

const (
	sessionPurposeConfigure sessionPurpose = iota
	sessionPurposeAuthenticate
	sessionPurposeRenew
)

type sessionAuthorization struct {
	credentialRef string
	deviceID      string
	token         string
	workerCount   int
	runtimeID     string
	legacy        bool
}

type sessionConfiguration struct {
	content string
	lease   *access.IssuedLease
}

type silentLoggerFactory struct{}

func (*silentLoggerFactory) NewLogger(string) logging.LeveledLogger { return &silentLogger{} }

type silentLogger struct{}

func (*silentLogger) Trace(string)                  {}
func (*silentLogger) Tracef(string, ...interface{}) {}
func (*silentLogger) Debug(string)                  {}
func (*silentLogger) Debugf(string, ...interface{}) {}
func (*silentLogger) Info(string)                   {}
func (*silentLogger) Infof(string, ...interface{})  {}
func (*silentLogger) Warn(string)                   {}
func (*silentLogger) Warnf(string, ...interface{})  {}
func (*silentLogger) Error(string)                  {}
func (*silentLogger) Errorf(string, ...interface{}) {}

type connectedPacketConn struct{ net.Conn }

func (c *connectedPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	n, err := c.Read(buffer)
	return n, c.RemoteAddr(), err
}

func (c *connectedPacketConn) WriteTo(buffer []byte, _ net.Addr) (int, error) {
	return c.Write(buffer)
}

func runSession(
	ctx context.Context,
	sessionID int,
	turnDialer coreDialer,
	peer *net.UDPAddr,
	credentials *turnCredentials,
	wrapKey []byte,
	keyHint hydrawrap.KeyHint,
	obfsMode string,
	dispatcher *dispatcher,
	localPort uint16,
	authorization sessionAuthorization,
	purpose sessionPurpose,
	generation int,
	configurationCh chan<- sessionConfiguration,
	renewalCh chan<- access.IssuedLease,
	ready func(),
) error {
	if len(credentials.urls) == 0 {
		return fmt.Errorf("WDTT TURN credentials contain no UDP relay")
	}
	turnAddress := credentials.urls[sessionID%len(credentials.urls)]
	turnConnection, err := turnDialer.DialContext(ctx, "udp", parseDestination(turnAddress))
	if err != nil {
		return fmt.Errorf("dial WDTT TURN relay: %w", err)
	}
	defer turnConnection.Close()
	turnServerAddress, err := numericRemoteAddress(turnConnection.RemoteAddr())
	if err != nil {
		return err
	}
	if bufferSetter, loaded := turnConnection.(interface {
		SetReadBuffer(int) error
		SetWriteBuffer(int) error
	}); loaded {
		_ = bufferSetter.SetReadBuffer(turnSocketBuffer)
		_ = bufferSetter.SetWriteBuffer(turnSocketBuffer)
	}
	packetConnection := &connectedPacketConn{Conn: turnConnection}
	addressFamily := turn.RequestedAddressFamilyIPv6
	if peer.IP.To4() != nil {
		addressFamily = turn.RequestedAddressFamilyIPv4
	}
	turnClient, err := turn.NewClient(&turn.ClientConfig{
		// Pion resolves these fields through its Net implementation. Supplying
		// the numeric address selected by HydraCore's dialer prevents a second
		// system-DNS lookup from bypassing the core resolver/protect boundary.
		STUNServerAddr:         turnServerAddress,
		TURNServerAddr:         turnServerAddress,
		Conn:                   packetConnection,
		Net:                    new(stdnet.Net),
		Username:               credentials.username,
		Password:               credentials.password,
		RequestedAddressFamily: addressFamily,
		LoggerFactory:          &silentLoggerFactory{},
	})
	if err != nil {
		return fmt.Errorf("create WDTT TURN client: %w", err)
	}
	defer turnClient.Close()
	if err = turnClient.Listen(); err != nil {
		return fmt.Errorf("start WDTT TURN client: %w", err)
	}
	relay, err := turnClient.Allocate()
	if err != nil {
		return fmt.Errorf("allocate WDTT TURN relay: %w", err)
	}
	defer relay.Close()

	sessionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	pipeRelay, pipeDTLS := connutil.AsyncPacketPipe()
	defer pipeRelay.Close()
	defer pipeDTLS.Close()

	aead, err := newWrapAEAD(wrapKey)
	if err != nil {
		return err
	}
	obfuscation, err := newObfsConfig(obfsMode, keyHint)
	if err != nil {
		return err
	}
	obfuscationState, err := newObfsState()
	if err != nil {
		return err
	}

	var relayWaitGroup sync.WaitGroup
	relayWaitGroup.Add(3)
	go func() {
		defer relayWaitGroup.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sessionContext.Done():
				return
			case <-ticker.C:
				turnClient.SendBindingRequest()
			}
		}
	}()
	go func() {
		defer relayWaitGroup.Done()
		defer cancel()
		wire := make([]byte, wdttReadBufferSize+128)
		plain := make([]byte, wdttReadBufferSize)
		for {
			n, _, readErr := relay.ReadFrom(wire)
			if readErr != nil {
				return
			}
			plainLength, unwrapErr := unwrapPacket(aead, wire[:n], plain)
			if unwrapErr != nil {
				continue
			}
			if _, writeErr := pipeRelay.WriteTo(plain[:plainLength], peer); writeErr != nil {
				return
			}
		}
	}()
	go func() {
		defer relayWaitGroup.Done()
		defer cancel()
		plain := make([]byte, wdttReadBufferSize)
		for {
			n, _, readErr := pipeRelay.ReadFrom(plain)
			if readErr != nil {
				return
			}
			wire, wrapErr := wrapPacket(aead, plain[:n], obfuscation, obfuscationState)
			if wrapErr != nil {
				return
			}
			if _, writeErr := relay.WriteTo(wire, peer); writeErr != nil {
				return
			}
		}
	}()
	stopRelay := context.AfterFunc(sessionContext, func() {
		_ = relay.SetDeadline(time.Now())
		_ = pipeRelay.SetDeadline(time.Now())
	})
	defer stopRelay()
	defer func() {
		cancel()
		_ = pipeRelay.Close()
		relayWaitGroup.Wait()
	}()

	certificate, err := selfsign.GenerateSelfSigned()
	if err != nil {
		return fmt.Errorf("generate WDTT DTLS certificate: %w", err)
	}
	select {
	case handshakeSemaphore <- struct{}{}:
	case <-sessionContext.Done():
		return context.Cause(sessionContext)
	}
	handshakeSlotHeld := true
	defer func() {
		if handshakeSlotHeld {
			<-handshakeSemaphore
		}
	}()
	dtlsConfig := &dtls.Config{
		Certificates: []tls.Certificate{certificate},
		// WDTT authenticates at the application layer and intentionally uses
		// an ephemeral self-signed DTLS certificate for protocol compatibility.
		InsecureSkipVerify:    true, //nolint:gosec
		ExtendedMasterSecret:  dtls.RequireExtendedMasterSecret,
		CipherSuites:          []dtls.CipherSuiteID{dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		ConnectionIDGenerator: dtls.OnlySendCIDGenerator(),
		MTU:                   1100,
	}
	dtlsConnection, err := dtls.Client(pipeDTLS, peer, dtlsConfig)
	if err != nil {
		return fmt.Errorf("create WDTT DTLS client: %w", err)
	}
	defer dtlsConnection.Close()
	handshakeContext, handshakeCancel := context.WithTimeout(sessionContext, 50*time.Second)
	err = dtlsConnection.HandshakeContext(handshakeContext)
	handshakeCancel()
	<-handshakeSemaphore
	handshakeSlotHeld = false
	if err != nil {
		return fmt.Errorf("complete WDTT DTLS handshake: %w", err)
	}

	switch purpose {
	case sessionPurposeConfigure:
		var configuration sessionConfiguration
		if authorization.legacy {
			configuration.content, err = requestConfig(dtlsConnection, localPort, authorization.deviceID, authorization.token)
		} else {
			var lease access.IssuedLease
			configuration.content, lease, err = requestHydraConfig(dtlsConnection, localPort, authorization.deviceID, authorization.credentialRef, authorization.token, authorization.workerCount, authorization.runtimeID, generation, sessionID+1)
			configuration.lease = &lease
		}
		if err != nil {
			return err
		}
		if configuration.content == "" {
			return errConfigUnavailable
		}
		select {
		case configurationCh <- configuration:
		case <-sessionContext.Done():
			return context.Cause(sessionContext)
		}
	case sessionPurposeAuthenticate:
		if authorization.legacy {
			err = sendAuth(dtlsConnection, authorization.deviceID, authorization.token)
		} else {
			err = sendHydraAuth(dtlsConnection, authorization.deviceID, authorization.credentialRef, authorization.token, authorization.workerCount, authorization.runtimeID, generation, sessionID+1)
		}
		if err != nil {
			return err
		}
	case sessionPurposeRenew:
		lease, renewErr := renewHydraLease(dtlsConnection, authorization.deviceID, authorization.credentialRef, authorization.token, authorization.workerCount, authorization.runtimeID)
		if renewErr != nil {
			return renewErr
		}
		select {
		case renewalCh <- lease:
			return nil
		case <-sessionContext.Done():
			return context.Cause(sessionContext)
		}
	}

	slot := dispatcher.register(sessionID, generation)
	defer func() {
		dispatcher.unregister(slot)
		for {
			select {
			case packet := <-slot.sendCh:
				releasePacket(packet)
			default:
				return
			}
		}
	}()
	if ready != nil {
		ready()
	}
	stopDTLS := context.AfterFunc(sessionContext, func() { _ = dtlsConnection.SetDeadline(time.Now()) })
	defer stopDTLS()

	errCh := make(chan error, 3)
	go func() {
		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sessionContext.Done():
				return
			case <-ticker.C:
				_ = dtlsConnection.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, keepaliveErr := dtlsConnection.Write([]byte{keepaliveByte}); keepaliveErr != nil {
					errCh <- keepaliveErr
					return
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-sessionContext.Done():
				return
			case packet := <-slot.sendCh:
				_ = dtlsConnection.SetWriteDeadline(time.Now().Add(sessionIdleTimeout))
				_, writeErr := dtlsConnection.Write(packet)
				releasePacket(packet)
				if writeErr != nil {
					errCh <- writeErr
					return
				}
			}
		}
	}()
	go func() {
		buffer := make([]byte, wdttReadBufferSize)
		for {
			_ = dtlsConnection.SetReadDeadline(time.Now().Add(sessionIdleTimeout))
			n, readErr := dtlsConnection.Read(buffer)
			if readErr != nil {
				if networkError, loaded := readErr.(net.Error); loaded && networkError.Timeout() && sessionContext.Err() == nil {
					continue
				}
				errCh <- readErr
				return
			}
			if n == 1 && buffer[0] == keepaliveByte {
				continue
			}
			packet := acquirePacket(n)
			copy(packet, buffer[:n])
			select {
			case dispatcher.returnCh <- packet:
			case <-sessionContext.Done():
				releasePacket(packet)
				return
			}
		}
	}()

	select {
	case <-sessionContext.Done():
		return context.Cause(sessionContext)
	case sessionErr := <-errCh:
		if sessionErr == nil || errors.Is(sessionErr, net.ErrClosed) {
			return nil
		}
		if strings.Contains(strings.ToLower(sessionErr.Error()), "closed") && sessionContext.Err() != nil {
			return context.Cause(sessionContext)
		}
		return fmt.Errorf("WDTT session ended: %w", sessionErr)
	}
}

func numericRemoteAddress(address net.Addr) (string, error) {
	if address == nil {
		return "", fmt.Errorf("WDTT TURN connection has no remote address")
	}
	host, port, err := net.SplitHostPort(address.String())
	if err != nil {
		return "", fmt.Errorf("WDTT TURN connection returned an invalid remote address")
	}
	if net.ParseIP(host) == nil {
		return "", fmt.Errorf("WDTT TURN connection did not expose a numeric remote address")
	}
	return net.JoinHostPort(host, port), nil
}
