package vless

import (
	"context"
	"net"
	"testing"

	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

const testUserID = "9f1c6db7-2c40-4c62-9db5-65ce35f50f2f"

func TestProtocolClientEncryptionIsOptIn(t *testing.T) {
	for _, config := range []string{"", "none"} {
		client, err := newProtocolClient(context.Background(), testUserID, "", config, logger.NOP())
		if err != nil {
			t.Fatal(err)
		}
		if client.encryption != nil {
			t.Fatalf("encryption initialized for %q", config)
		}
	}
}

func TestProtocolClientRejectsInvalidEncryption(t *testing.T) {
	if _, err := newProtocolClient(context.Background(), testUserID, "", "invalid", logger.NOP()); err == nil {
		t.Fatal("expected invalid encryption error")
	}
}

func TestDialEncryptedVisionUsesEncryptionLayerForBothArguments(t *testing.T) {
	encryptedConn, peerConn := net.Pipe()
	t.Cleanup(func() {
		_ = encryptedConn.Close()
		_ = peerConn.Close()
	})
	destination := M.Socksaddr{Fqdn: "example.com", Port: 443}

	called := false
	visionConn, err := dialEncryptedVision(
		func(conn net.Conn, baseConn net.Conn, gotDestination M.Socksaddr, canSplice bool) (net.Conn, error) {
			called = true
			if conn != encryptedConn {
				t.Fatal("Vision protocol connection is not the encrypted layer")
			}
			if baseConn != encryptedConn {
				t.Fatal("Vision base connection is not the encrypted layer")
			}
			if gotDestination != destination {
				t.Fatalf("unexpected destination: %v", gotDestination)
			}
			if canSplice {
				t.Fatal("Vision splice must stay disabled with authenticated encryption")
			}
			return conn, nil
		},
		encryptedConn,
		destination,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("Vision dialer was not called")
	}
	if visionConn != encryptedConn {
		t.Fatal("unexpected Vision connection")
	}
}
