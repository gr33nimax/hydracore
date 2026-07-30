//go:build linux

package xhttp

import (
	"os"
	"testing"
)

func platformOpenFileDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
