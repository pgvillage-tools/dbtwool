package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func stageCommand() *cobra.Command {
	var stageArgs args
	stageCommand := &cobra.Command{
		Use:   "stage",
		Short: "create tables",
		Long:  "Create the necessary schema and table(s)",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("stage:" + stageArgs.GetString("table"))
		},
	}

	stageArgs = allArgs.commandArgs(stageCommand, append(globalArgs, "table"))

	return stageCommand
}
