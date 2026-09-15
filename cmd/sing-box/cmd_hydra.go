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

// The capability document the contract replaced. A HYDRA older than the contract asks for it and
// refuses to install a core that does not answer, so this is what lets an update reach a machine
// whose HYDRA has not been updated yet. See common/hydracore/legacy_capabilities.go.
var hydraCapabilitiesCommand = &cobra.Command{
	Use:   "capabilities",
	Short: "Print the capability document HYDRA read before the product contract",
	Args:  cobra.NoArgs,
	RunE: func(command *cobra.Command, _ []string) error {
		if !hydraCapabilitiesJSON {
			return fmt.Errorf("capabilities output requires --json")
		}
		_, err := fmt.Fprintln(command.OutOrStdout(), H.LegacyCapabilitiesJSON())
		return err
	},
}

var hydraCapabilitiesJSON bool

func init() {
	hydraContractCommand.Flags().BoolVar(&hydraContractJSON, "json", false, "print JSON product contract")
	hydraCapabilitiesCommand.Flags().BoolVar(&hydraCapabilitiesJSON, "json", false, "print JSON capability document")
	hydraCommand.AddCommand(hydraContractCommand)
	hydraCommand.AddCommand(hydraCapabilitiesCommand)
	mainCommand.AddCommand(hydraCommand)
}
