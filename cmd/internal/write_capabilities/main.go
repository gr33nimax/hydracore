package main

import (
	"fmt"
	"os"

	H "github.com/sagernet/sing-box/common/hydracore"
)

// This command intentionally imports only the capability registry. Using the
// full sing-box CLI here would link Android-only badlinkname surfaces into a
// Linux host helper after the AAR itself has already been built successfully.
func main() {
	capabilities := H.CapabilitiesJSON()
	if capabilities == "" {
		fmt.Fprintln(os.Stderr, "HydraCore capabilities could not be encoded")
		os.Exit(1)
	}
	fmt.Println(capabilities)
}
