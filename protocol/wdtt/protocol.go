// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gr33nimax/hydra-wdtt/pkg/access"
	"github.com/gr33nimax/hydra-wdtt/pkg/control"
)

const initialConfigReadTimeout = 45 * time.Second

func requestConfig(conn net.Conn, localPort uint16, deviceID string, password string) (string, error) {
	payload := fmt.Sprintf("GETCONF:%d|%s|%s", localPort, deviceID, password)
	if _, err := conn.Write([]byte(payload)); err != nil {
		return "", fmt.Errorf("send WDTT configuration request: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(initialConfigReadTimeout)); err != nil {
		return "", fmt.Errorf("set WDTT configuration deadline: %w", err)
	}
	defer conn.SetReadDeadline(time.Time{})
	buffer := make([]byte, 16*1024+1)
	n, err := conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("read WDTT configuration: %w", err)
	}
	if n > 16*1024 {
		return "", fmt.Errorf("WDTT configuration response is too large")
	}
	response := string(buffer[:n])
	if response == "NOCONF" {
		return "", nil
	}
	if strings.HasPrefix(response, "DENIED:") {
		reason := strings.TrimSpace(strings.TrimPrefix(response, "DENIED:"))
		switch reason {
		case "wrong_password":
			return "", fmt.Errorf("WDTT authentication failed: wrong password")
		case "expired":
			return "", fmt.Errorf("WDTT authentication failed: subscription credential expired")
		case "device_mismatch":
			return "", fmt.Errorf("WDTT authentication failed: credential is bound to another device")
		default:
			return "", fmt.Errorf("WDTT authentication failed")
		}
	}
	return response, nil
}

func sendAuth(conn net.Conn, deviceID string, password string) error {
	if _, err := fmt.Fprintf(conn, "AUTH:%s|%s", deviceID, password); err != nil {
		return fmt.Errorf("send WDTT authentication: %w", err)
	}
	return nil
}

func requestHydraConfig(conn net.Conn, localPort uint16, deviceID string, credentialRef string, token string, workerCount int, runtimeID string, generation int, workerSlot int) (string, access.IssuedLease, error) {
	packet, err := control.EncodeRequest(control.OperationGetConfig, control.Request{
		DeviceID:      deviceID,
		CredentialRef: credentialRef,
		Token:         token,
		LocalPort:     int(localPort),
		Workers:       workerCount,
		RuntimeID:     runtimeID,
		Generation:    generation,
		WorkerSlot:    workerSlot,
		Features:      []string{control.FeatureKeyHint, control.FeatureLease, control.FeatureHotRotate, control.FeatureWorkerTakeover},
	})
	if err != nil {
		return "", access.IssuedLease{}, fmt.Errorf("encode Hydra WDTT configuration request: %w", err)
	}
	response, err := exchangeHydraControl(conn, packet)
	if err != nil {
		return "", access.IssuedLease{}, err
	}
	if response.Config == "" || response.Lease == nil {
		return "", access.IssuedLease{}, fmt.Errorf("Hydra WDTT server returned an incomplete configuration response")
	}
	return response.Config, *response.Lease, nil
}

func sendHydraAuth(conn net.Conn, deviceID string, credentialRef string, leaseToken string, workerCount int, runtimeID string, generation int, workerSlot int) error {
	packet, err := control.EncodeRequest(control.OperationAuth, control.Request{
		DeviceID:      deviceID,
		CredentialRef: credentialRef,
		Token:         leaseToken,
		Workers:       workerCount,
		RuntimeID:     runtimeID,
		Generation:    generation,
		WorkerSlot:    workerSlot,
		Features:      []string{control.FeatureKeyHint, control.FeatureLease, control.FeatureHotRotate, control.FeatureWorkerTakeover},
	})
	if err != nil {
		return fmt.Errorf("encode Hydra WDTT authentication: %w", err)
	}
	if _, err := exchangeHydraControl(conn, packet); err != nil {
		return fmt.Errorf("confirm Hydra WDTT authentication: %w", err)
	}
	return nil
}

func renewHydraLease(conn net.Conn, deviceID string, credentialRef string, leaseToken string, workerCount int, runtimeID string) (access.IssuedLease, error) {
	packet, err := control.EncodeRequest(control.OperationRenew, control.Request{
		DeviceID:      deviceID,
		CredentialRef: credentialRef,
		Token:         leaseToken,
		Workers:       workerCount,
		RuntimeID:     runtimeID,
		Features:      []string{control.FeatureKeyHint, control.FeatureLease, control.FeatureHotRotate, control.FeatureWorkerTakeover},
	})
	if err != nil {
		return access.IssuedLease{}, fmt.Errorf("encode Hydra WDTT lease renewal: %w", err)
	}
	response, err := exchangeHydraControl(conn, packet)
	if err != nil {
		return access.IssuedLease{}, err
	}
	if response.Lease == nil {
		return access.IssuedLease{}, fmt.Errorf("Hydra WDTT server returned no renewed lease")
	}
	return *response.Lease, nil
}

func exchangeHydraControl(conn net.Conn, request []byte) (control.Response, error) {
	if _, err := conn.Write(request); err != nil {
		return control.Response{}, fmt.Errorf("send Hydra WDTT control request: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(initialConfigReadTimeout)); err != nil {
		return control.Response{}, fmt.Errorf("set Hydra WDTT control deadline: %w", err)
	}
	defer conn.SetReadDeadline(time.Time{})
	buffer := make([]byte, 20*1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return control.Response{}, fmt.Errorf("read Hydra WDTT control response: %w", err)
	}
	response, matched, err := control.DecodeResponse(buffer[:n])
	if err != nil {
		return control.Response{}, fmt.Errorf("decode Hydra WDTT control response: %w", err)
	}
	if !matched {
		return control.Response{}, fmt.Errorf("Hydra WDTT server returned a legacy response")
	}
	if response.Error != "" {
		return control.Response{}, fmt.Errorf("Hydra WDTT authentication failed: %s", response.Error)
	}
	return response, nil
}
