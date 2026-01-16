// Package main is the main entrypoint for dbwtool
package main

// cobra and viper are used to create a uniform interface on CLI and configuration file.
import (
	"fmt"
	"os"
	"strings"

	"github.com/pgvillage-tools/dbtwool/internal/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile    string
	globalArgs = []string{
		"cfgFile",
	}
)

// requireSubcommand returns an error if no sub command is provided
// This was copied from skopeo, which copied it from podman: `github.com/containers/podman/cmd/podman/validate/args.go
// Some small style changes to match skopeo were applied, but try to apply any
// bugfixes there first.
func requireSubcommand(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		suggestions := cmd.SuggestionsFor(args[0])
		if len(suggestions) == 0 {
			return fmt.Errorf("unrecognized command `%[1]s %[2]s`\nTry '%[1]s --help' for more information", cmd.CommandPath(), args[0])
		}
		return fmt.Errorf("unrecognized command `%[1]s %[2]s`\n\nDid you mean this?\n\t%[3]s\n\nTry '%[1]s --help' for more information", cmd.CommandPath(), args[0], strings.Join(suggestions, "\n\t"))
	}
	return fmt.Errorf("missing command '%[1]s COMMAND'\nTry '%[1]s --help' for more information", cmd.CommandPath())
}

// createApp returns either a validly formed command for main() to run, or
// an error. Initializes a cobra command structure using the settings from the
// configuration file. Override the default location with -c,--cfgFile).
// Override the target pg_hba.conf file with -f, --hbaFile
func createApp() *cobra.Command {
	cobra.OnInitialize(initConfig)
	rootCmd := &cobra.Command{
		Use:               "dbtwool",
		Short:             "Run tests against DB2 and PostgreSQL",
		Long:              `Lorem ipsum ...`,
		RunE:              requireSubcommand,
		CompletionOptions: cobra.CompletionOptions{},
		TraverseChildren:  true,
		Version:           version.GetAppVersion(),
		//SilenceErrors: true,
		//SilenceUsage: true,
	}

	viper.AddConfigPath(viper.GetString("cfgFile"))
	err := viper.ReadInConfig()
	if err == nil {
		fmt.Printf("dbtwool is reading config from this config file: %s", viper.ConfigFileUsed())
	}

	rootCmd.AddCommand(
		consistencyCommand(),
	)
	return rootCmd
}

// Execute the fully formed pgcustodian command and handle any errors.
func main() {
	initLogger("")
	rootCmd := createApp()
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
	log.Info("finished")
}

// Read settings as key value pairs from the ".dbtwool" config file in the home directory.
// This is (obscurely) referenced from the "createApp" function above.
// TODO would this be clearer if moved above createApp?
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".dbtwool")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
