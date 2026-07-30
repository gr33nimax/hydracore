package vless

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/vless/encryption"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

const testUserID = "9f1c6db7-2c40-4c62-9db5-65ce35f50f2f"

type opaqueProtocolConn struct {
	net.Conn
}

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

func TestProtocolClientVisionUsesEncryptionLayer(t *testing.T) {
	privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	const padding = "100-35-35"
	config := "mlkem768x25519plus.native.1rtt." + padding + "." +
		base64.RawURLEncoding.EncodeToString(privateKey.PublicKey().Bytes())

	for _, opaqueTransport := range []bool{false, true} {
		name := "raw"
		if opaqueTransport {
			name = "opaque-transport"
		}
		t.Run(name, func(t *testing.T) {
			server := &encryption.ServerInstance{}
			if err := server.Init([][]byte{privateKey.Bytes()}, 0, 0, 0, padding); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Close() })

			ctx := encryption.OverrideUseAES(context.Background(), true)
			client, err := newProtocolClient(
				ctx,
				testUserID,
				"xtls-rprx-vision",
				config,
				logger.NOP(),
			)
			if err != nil {
				t.Fatal(err)
			}

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })

			serverDone := make(chan error, 1)
			go func() {
				serverConn, acceptErr := listener.Accept()
				if acceptErr != nil {
					serverDone <- acceptErr
					return
				}
				defer serverConn.Close()
				_ = serverConn.SetDeadline(time.Now().Add(5 * time.Second))
				encryptedConn, handshakeErr := server.Handshake(serverConn, nil)
				if encryptedConn != nil {
					_ = encryptedConn.Close()
				}
				serverDone <- handshakeErr
			}()

			clientConn, err := net.DialTimeout(
				"tcp",
				listener.Addr().String(),
				5*time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = clientConn.Close() })
			_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
			var transportConn net.Conn = clientConn
			if opaqueTransport {
				transportConn = &opaqueProtocolConn{Conn: clientConn}
			}

			visionConn, err := client.DialEarlyConn(
				ctx,
				transportConn,
				M.Socksaddr{Fqdn: "example.com", Port: 443},
			)
			if err != nil {
				t.Fatal(err)
			}
			if visionConn == nil {
				t.Fatal("expected Vision connection")
			}
			_ = visionConn.Close()

			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server handshake did not finish")
			}
		})
	}
}
