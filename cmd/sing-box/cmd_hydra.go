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

var hydraCapabilitiesCommand = &cobra.Command{
	Use:   "capabilities",
	Short: "Print the HydraCore capability contract",
	Args:  cobra.NoArgs,
	RunE: func(command *cobra.Command, _ []string) error {
		if !hydraCapabilitiesJSON {
			return fmt.Errorf("capabilities output requires --json")
		}
		_, err := fmt.Fprintln(command.OutOrStdout(), H.CapabilitiesJSON())
		return err
	},
}

var hydraCapabilitiesJSON bool

func init() {
	hydraCapabilitiesCommand.Flags().BoolVar(&hydraCapabilitiesJSON, "json", false, "print JSON capability data")
	hydraCommand.AddCommand(hydraCapabilitiesCommand)
	mainCommand.AddCommand(hydraCommand)
}
