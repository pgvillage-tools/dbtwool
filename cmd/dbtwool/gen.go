package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func genCommand() *cobra.Command {
	var genArgs args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("gen:" + genArgs.GetString("spread"))
			fmt.Println("gen:" + genArgs.GetString("bytesize"))
		},
	}

	genArgs = allArgs.commandArgs(genCommand, append(globalArgs, "spread", "bytesize", "table"))
	return genCommand
}
