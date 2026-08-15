package main

import (
	"fmt"
	"io"
	"os"

	H "github.com/sagernet/sing-box/common/hydracore"
)

// This command intentionally imports only the capability registry. Using the
// full sing-box CLI here would link Android-only badlinkname surfaces into a
// Linux host helper after the AAR itself has already been built successfully.
func main() {
	if err := writeCapabilities(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeCapabilities(output io.Writer) error {
	capabilities := H.CapabilitiesJSON()
	if capabilities == "" {
		return fmt.Errorf("HydraCore capabilities could not be encoded")
	}
	// The bundle manifest signs this file's exact bytes. Keep them identical to
	// Libbox.HydraCoreCapabilities(), which returns the JSON without a newline.
	_, err := io.WriteString(output, capabilities)
	return err
}
