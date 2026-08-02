package wdtt

import (
	"strings"
	"testing"
)

const testPrivateKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
const testPublicKey = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="

func TestParseWireGuardConfigAllowlist(t *testing.T) {
	config, err := parseWireGuardConfig(strings.Join([]string{
		"[Interface]",
		"PrivateKey = " + testPrivateKey,
		"Address = 10.77.0.2/32, fd00:77::2/128",
		"DNS = 1.1.1.1",
		"MTU = 1280",
		"[Peer]",
		"PublicKey = " + testPublicKey,
		"AllowedIPs = 0.0.0.0/0, ::/0",
		"Endpoint = 198.51.100.4:51820",
		"PersistentKeepalive = 25",
	}, "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if config.privateKey != testPrivateKey || config.publicKey != testPublicKey {
		t.Fatal("WireGuard keys were not parsed")
	}
	if len(config.addresses) != 2 || config.mtu != 1280 {
		t.Fatalf("unexpected parsed config: addresses=%d mtu=%d", len(config.addresses), config.mtu)
	}
}

func TestParseWireGuardConfigRejectsExecutableDirectives(t *testing.T) {
	content := strings.Join([]string{
		"[Interface]",
		"PrivateKey = " + testPrivateKey,
		"Address = 10.77.0.2/32",
		"PostUp = touch /tmp/not-allowed",
		"[Peer]",
		"PublicKey = " + testPublicKey,
		"AllowedIPs = 0.0.0.0/0",
		"Endpoint = 127.0.0.1:9000",
	}, "\n")
	if _, err := parseWireGuardConfig(content); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected allowlist rejection, got %v", err)
	}
}

func TestParseWireGuardConfigRejectsMultiplePeers(t *testing.T) {
	content := strings.Join([]string{
		"[Interface]",
		"PrivateKey = " + testPrivateKey,
		"Address = 10.77.0.2/32",
		"[Peer]",
		"PublicKey = " + testPublicKey,
		"Endpoint = 127.0.0.1:9000",
		"[Peer]",
		"PublicKey = " + testPublicKey,
		"Endpoint = 127.0.0.1:9001",
	}, "\n")
	if _, err := parseWireGuardConfig(content); err == nil {
		t.Fatal("expected multiple peers to be rejected")
	}
}

func TestParseWireGuardConfigRejectsDuplicateAuthority(t *testing.T) {
	content := strings.Join([]string{
		"[Interface]",
		"PrivateKey = " + testPrivateKey,
		"PrivateKey = " + testPublicKey,
		"Address = 10.77.0.2/32",
		"[Peer]",
		"PublicKey = " + testPublicKey,
		"AllowedIPs = 0.0.0.0/0",
		"Endpoint = 127.0.0.1:9000",
	}, "\n")
	if _, err := parseWireGuardConfig(content); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate authority to be rejected, got %v", err)
	}
}
