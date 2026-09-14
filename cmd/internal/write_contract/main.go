package main

import (
	"fmt"
	"io"
	"os"

	H "github.com/sagernet/sing-box/common/hydracore"
)

func main() {
	if err := writeContract(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeContract(output io.Writer) error {
	contract := H.ContractJSON()
	if contract == "" {
		return fmt.Errorf("HydraCore contract could not be encoded")
	}
	_, err := io.WriteString(output, contract)
	return err
}
