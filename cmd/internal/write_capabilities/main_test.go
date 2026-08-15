package main

import (
	"strings"
	"testing"

	H "github.com/sagernet/sing-box/common/hydracore"
)

func TestWriteCapabilitiesMatchesNativeBytes(t *testing.T) {
	var output strings.Builder
	if err := writeCapabilities(&output); err != nil {
		t.Fatal(err)
	}
	written := output.String()
	if written != H.CapabilitiesJSON() {
		t.Fatal("capability writer changed the native capability bytes")
	}
	if strings.HasSuffix(written, "\n") {
		t.Fatal("capability writer appended a newline")
	}
}
