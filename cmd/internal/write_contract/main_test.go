package main

import (
	"strings"
	"testing"

	H "github.com/sagernet/sing-box/common/hydracore"
)

func TestWriteContractMatchesNativeBytes(t *testing.T) {
	var output strings.Builder
	if err := writeContract(&output); err != nil {
		t.Fatal(err)
	}
	written := output.String()
	if written != H.ContractJSON() {
		t.Fatal("contract writer changed the native contract bytes")
	}
	if strings.HasSuffix(written, "\n") {
		t.Fatal("capability writer appended a newline")
	}
}
