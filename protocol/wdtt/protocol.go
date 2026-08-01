// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"fmt"
	"net"
	"strings"
	"time"
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
