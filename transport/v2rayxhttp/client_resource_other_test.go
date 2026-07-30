//go:build !linux

package xhttp

import "testing"

func platformOpenFileDescriptorCount(*testing.T) int {
	return -1
}
