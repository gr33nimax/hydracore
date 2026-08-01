// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"encoding/base64"
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/sagernet/sing/common/json/badoption"
)

const (
	defaultMTU = 1280
	minMTU     = 576
	maxMTU     = 1500
)

type wireGuardConfig struct {
	privateKey string
	addresses  badoption.Listable[netip.Prefix]
	publicKey  string
	mtu        uint32
}

func parseWireGuardConfig(content string) (*wireGuardConfig, error) {
	if len(content) == 0 || len(content) > 16*1024 {
		return nil, fmt.Errorf("WDTT WireGuard config has an invalid size")
	}
	config := &wireGuardConfig{mtu: defaultMTU}
	section := ""
	peerCount := 0
	seenInterface := false
	seenFields := make(map[string]struct{})
	for lineNumber, rawLine := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			switch section {
			case "interface":
				if seenInterface {
					return nil, fmt.Errorf("WDTT WireGuard config contains multiple interface sections")
				}
				seenInterface = true
			case "peer":
				peerCount++
				if peerCount > 1 {
					return nil, fmt.Errorf("WDTT WireGuard config must contain exactly one peer")
				}
			default:
				return nil, fmt.Errorf("WDTT WireGuard config contains unsupported section at line %d", lineNumber+1)
			}
			continue
		}
		key, value, loaded := strings.Cut(line, "=")
		if !loaded || section == "" {
			return nil, fmt.Errorf("WDTT WireGuard config contains malformed line %d", lineNumber+1)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		fieldKey := section + "." + key
		if _, duplicate := seenFields[fieldKey]; duplicate {
			return nil, fmt.Errorf("WDTT WireGuard config contains duplicate %q directive", fieldKey)
		}
		seenFields[fieldKey] = struct{}{}
		switch section {
		case "interface":
			switch key {
			case "privatekey":
				if err := validateWireGuardKey(value); err != nil {
					return nil, fmt.Errorf("WDTT WireGuard private key is invalid: %w", err)
				}
				config.privateKey = value
			case "address":
				for _, rawAddress := range strings.Split(value, ",") {
					prefix, err := parseInterfacePrefix(strings.TrimSpace(rawAddress))
					if err != nil {
						return nil, fmt.Errorf("WDTT WireGuard address is invalid: %w", err)
					}
					config.addresses = append(config.addresses, prefix)
				}
			case "mtu":
				mtu, err := strconv.Atoi(value)
				if err != nil || mtu < minMTU || mtu > maxMTU {
					return nil, fmt.Errorf("WDTT WireGuard MTU must be between %d and %d", minMTU, maxMTU)
				}
				config.mtu = uint32(mtu)
			case "dns":
				// DNS remains owned by HydraBox. The server value is deliberately ignored.
			default:
				return nil, fmt.Errorf("WDTT WireGuard interface directive %q is not allowed", key)
			}
		case "peer":
			switch key {
			case "publickey":
				if err := validateWireGuardKey(value); err != nil {
					return nil, fmt.Errorf("WDTT WireGuard peer key is invalid: %w", err)
				}
				config.publicKey = value
			case "allowedips":
				for _, rawPrefix := range strings.Split(value, ",") {
					if _, err := netip.ParsePrefix(strings.TrimSpace(rawPrefix)); err != nil {
						return nil, fmt.Errorf("WDTT WireGuard allowed IP is invalid")
					}
				}
			case "endpoint":
				// The server endpoint is untrusted input. HydraCore always replaces it
				// with its own loopback bridge before constructing WireGuard.
				if value == "" {
					return nil, fmt.Errorf("WDTT WireGuard endpoint is empty")
				}
			case "persistentkeepalive":
				keepalive, err := strconv.Atoi(value)
				if err != nil || keepalive < 0 || keepalive > 120 {
					return nil, fmt.Errorf("WDTT WireGuard keepalive is invalid")
				}
			default:
				return nil, fmt.Errorf("WDTT WireGuard peer directive %q is not allowed", key)
			}
		}
	}
	if !seenInterface || config.privateKey == "" || config.publicKey == "" || len(config.addresses) == 0 || peerCount != 1 {
		return nil, fmt.Errorf("WDTT WireGuard config is incomplete")
	}
	return config, nil
}

func validateWireGuardKey(value string) error {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return err
	}
	if len(decoded) != 32 {
		return fmt.Errorf("key must decode to 32 bytes")
	}
	return nil
}

func parseInterfacePrefix(value string) (netip.Prefix, error) {
	if prefix, err := netip.ParsePrefix(value); err == nil {
		return prefix, nil
	}
	address, err := netip.ParseAddr(value)
	if err != nil {
		return netip.Prefix{}, err
	}
	bits := 128
	if address.Is4() {
		bits = 32
	}
	return netip.PrefixFrom(address, bits), nil
}
