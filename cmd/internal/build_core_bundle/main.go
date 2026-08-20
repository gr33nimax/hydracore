package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	distributionID = "io.hydrabox.hydracore"
	manifestName   = "hydracore-bundle-manifest-v1.json"
	signatureName  = "hydracore-bundle-manifest-v1.sig"
	maximumSOBytes = 256 * 1024 * 1024
)

type schemaRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type artifact struct {
	ABI       string `json:"abi"`
	AssetName string `json:"assetName"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
	MinSDK    int    `json:"minSdk"`
}

type manifest struct {
	SchemaVersion         int         `json:"schemaVersion"`
	DistributionID        string      `json:"distributionId"`
	ReleaseSequence       int64       `json:"releaseSequence"`
	Version               string      `json:"version"`
	SourceCommit          string      `json:"sourceCommit"`
	UpstreamCommit        string      `json:"upstreamCommit"`
	PublishedAt           string      `json:"publishedAt"`
	CoreAPIMajor          int         `json:"coreApiMajor"`
	CoreAPIMinor          int         `json:"coreApiMinor"`
	RuntimeSnapshotSchema schemaRange `json:"runtimeSnapshotSchema"`
	RuntimeEventSchema    schemaRange `json:"runtimeEventSchema"`
	ConfigSchema          schemaRange `json:"configSchema"`
	SubscriptionSchema    schemaRange `json:"subscriptionSchema"`
	CapabilitiesSHA256    string      `json:"capabilitiesSha256"`
	KeyID                 string      `json:"keyId"`
	Artifacts             []artifact  `json:"artifacts"`
}

var (
	aarPath         string
	capabilities    string
	outputDirectory string
	version         string
	sourceCommit    string
	upstreamCommit  string
	publishedAt     string
	keyID           string
	privateKey      string
	releaseSequence int64
)

func init() {
	flag.StringVar(&aarPath, "aar", "libbox.aar", "HydraCore Android AAR")
	flag.StringVar(&capabilities, "capabilities", "", "client capabilities JSON")
	flag.StringVar(&outputDirectory, "out", "dist", "bundle output directory")
	flag.StringVar(&version, "version", "", "HydraCore version")
	flag.StringVar(&sourceCommit, "source-commit", "", "HydraCore source commit")
	flag.StringVar(&upstreamCommit, "upstream-commit", "", "upstream source commit")
	flag.StringVar(&publishedAt, "published-at", "", "RFC3339 publication time")
	flag.StringVar(&keyID, "key-id", "", "embedded public key id")
	flag.StringVar(&privateKey, "private-key", "", "base64 Ed25519 seed/private key; optional")
	flag.Int64Var(&releaseSequence, "release-sequence", 0, "monotonic release sequence")
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "build core bundle:", err)
		os.Exit(1)
	}
}

func run() error {
	if releaseSequence <= 0 {
		return fmt.Errorf("release-sequence must be positive")
	}
	if !safeToken(version, 127) || !safeToken(keyID, 64) {
		return fmt.Errorf("version or key-id is invalid")
	}
	if !fullCommit(sourceCommit) || !fullCommit(upstreamCommit) {
		return fmt.Errorf("source commits must be lowercase full Git hashes")
	}
	published, err := time.Parse(time.RFC3339, publishedAt)
	if err != nil {
		return fmt.Errorf("published-at: %w", err)
	}
	capabilityBytes, err := os.ReadFile(capabilities)
	if err != nil {
		return fmt.Errorf("read capabilities: %w", err)
	}
	if !json.Valid(capabilityBytes) {
		return fmt.Errorf("capabilities are not valid JSON")
	}
	if err = os.MkdirAll(outputDirectory, 0o755); err != nil {
		return err
	}
	artifacts, err := extractLibraries(aarPath, outputDirectory)
	if err != nil {
		return err
	}
	capabilityDigest := sha256.Sum256(capabilityBytes)
	document := manifest{
		SchemaVersion:         1,
		DistributionID:        distributionID,
		ReleaseSequence:       releaseSequence,
		Version:               version,
		SourceCommit:          sourceCommit,
		UpstreamCommit:        upstreamCommit,
		PublishedAt:           published.UTC().Format(time.RFC3339),
		CoreAPIMajor:          2,
		CoreAPIMinor:          0,
		RuntimeSnapshotSchema: schemaRange{Min: 1, Max: 1},
		RuntimeEventSchema:    schemaRange{Min: 1, Max: 1},
		ConfigSchema:          schemaRange{Min: 1, Max: 1},
		SubscriptionSchema:    schemaRange{Min: 2, Max: 2},
		CapabilitiesSHA256:    hex.EncodeToString(capabilityDigest[:]),
		KeyID:                 keyID,
		Artifacts:             artifacts,
	}
	manifestBytes, err := json.Marshal(document)
	if err != nil {
		return err
	}
	manifestBytes = append(manifestBytes, '\n')
	manifestPath := filepath.Join(outputDirectory, manifestName)
	if err = os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		return err
	}
	if privateKey != "" {
		signature, signErr := sign(manifestBytes, privateKey)
		if signErr != nil {
			return signErr
		}
		if err = os.WriteFile(filepath.Join(outputDirectory, signatureName), signature, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func extractLibraries(path string, output string) ([]artifact, error) {
	archive, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer archive.Close()
	supported := map[string]bool{
		"arm64-v8a": true, "armeabi-v7a": true, "x86_64": true,
	}
	var result []artifact
	for _, entry := range archive.File {
		parts := strings.Split(entry.Name, "/")
		if len(parts) != 3 || parts[0] != "jni" || parts[2] != "libbox.so" ||
			!supported[parts[1]] {
			continue
		}
		if entry.UncompressedSize64 == 0 || entry.UncompressedSize64 > maximumSOBytes {
			return nil, fmt.Errorf("invalid %s size", entry.Name)
		}
		name := "hydracore-android-" + parts[1] + "-libbox.so"
		destination := filepath.Join(output, name)
		digest, size, copyErr := copyEntry(entry, destination)
		if copyErr != nil {
			return nil, copyErr
		}
		result = append(result, artifact{
			ABI: parts[1], AssetName: name, SizeBytes: size, SHA256: digest, MinSDK: 26,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ABI < result[j].ABI })
	if len(result) != len(supported) {
		return nil, fmt.Errorf("AAR must contain arm64-v8a, armeabi-v7a, and x86_64")
	}
	return result, nil
}

func copyEntry(entry *zip.File, destination string) (string, int64, error) {
	input, err := entry.Open()
	if err != nil {
		return "", 0, err
	}
	defer input.Close()
	temporary := destination + ".tmp"
	output, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(output, digest), io.LimitReader(input, maximumSOBytes+1))
	if syncErr := output.Sync(); copyErr == nil {
		copyErr = syncErr
	}
	if closeErr := output.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(temporary)
		return "", 0, copyErr
	}
	if size > maximumSOBytes || uint64(size) != entry.UncompressedSize64 {
		_ = os.Remove(temporary)
		return "", 0, fmt.Errorf("native artifact size mismatch")
	}
	if err = os.Rename(temporary, destination); err != nil {
		_ = os.Remove(temporary)
		return "", 0, err
	}
	return hex.EncodeToString(digest.Sum(nil)), size, nil
}

func sign(content []byte, encoded string) ([]byte, error) {
	value, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode Ed25519 private key: %w", err)
	}
	var key ed25519.PrivateKey
	switch len(value) {
	case ed25519.SeedSize:
		key = ed25519.NewKeyFromSeed(value)
	case ed25519.PrivateKeySize:
		key = ed25519.PrivateKey(value)
	default:
		return nil, fmt.Errorf("Ed25519 private key must contain %s or %s bytes",
			strconv.Itoa(ed25519.SeedSize), strconv.Itoa(ed25519.PrivateKeySize))
	}
	return ed25519.Sign(key, content), nil
}

func safeToken(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("._+-", character) {
			continue
		}
		return false
	}
	return true
}

func fullCommit(value string) bool {
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
