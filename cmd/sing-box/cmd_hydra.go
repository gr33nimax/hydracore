package main

import (
	"fmt"

	H "github.com/sagernet/sing-box/common/hydracore"

	"github.com/spf13/cobra"
)

var hydraCommand = &cobra.Command{
	Use:   "hydra",
	Short: "HydraCore runtime metadata",
}

var hydraContractCommand = &cobra.Command{
	Use:   "contract",
	Short: "Print the HydraCore product contract",
	Args:  cobra.NoArgs,
	RunE: func(command *cobra.Command, _ []string) error {
		if !hydraContractJSON {
			return fmt.Errorf("contract output requires --json")
		}
		_, err := fmt.Fprintln(command.OutOrStdout(), H.ContractJSON())
		return err
	},
}

var hydraContractJSON bool

func init() {
	hydraContractCommand.Flags().BoolVar(&hydraContractJSON, "json", false, "print JSON product contract")
	hydraCommand.AddCommand(hydraContractCommand)
	mainCommand.AddCommand(hydraCommand)
}
