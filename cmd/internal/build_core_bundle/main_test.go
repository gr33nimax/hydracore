package main

import (
	"archive/zip"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractLibrariesEmitsOnlySupportedABIs(t *testing.T) {
	directory := t.TempDir()
	aar := filepath.Join(directory, "libbox.aar")
	file, err := os.Create(aar)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	for _, abi := range []string{"arm64-v8a", "armeabi-v7a", "x86", "x86_64"} {
		entry, createErr := writer.Create("jni/" + abi + "/libbox.so")
		if createErr != nil {
			t.Fatal(createErr)
		}
		_, _ = entry.Write([]byte("native-" + abi))
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	artifacts, err := extractLibraries(aar, directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 3 {
		t.Fatalf("got %d artifacts, want 3", len(artifacts))
	}
	for _, item := range artifacts {
		if item.ABI == "x86" {
			t.Fatal("x86 must not be distributed by HydraBox 1.0")
		}
		if _, statErr := os.Stat(filepath.Join(directory, item.AssetName)); statErr != nil {
			t.Fatal(statErr)
		}
	}
}

func TestSignUsesRawManifestBytes(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("{\"schemaVersion\":1}\n")
	signature, err := sign(content, base64.StdEncoding.EncodeToString(privateKey.Seed()))
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, content, signature) {
		t.Fatal("signature does not verify")
	}
	if ed25519.Verify(publicKey, bytes.TrimSpace(content), signature) {
		t.Fatal("signature must cover the exact raw bytes")
	}
}
