package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func lobPerformanceCommand() *cobra.Command {
	var lobPerformanceArgs args
	lobPerformanceCommand := &cobra.Command{
		Use:   "lob-performance",
		Short: "test db performance with large objects",
		Long:  "Use this command to create a testenvironment, create a workload, and execute a performance test for large objects.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("lob-performance:" + lobPerformanceArgs.GetString("datasource"))
			fmt.Println("lob-performance:" + lobPerformanceArgs.GetString("table"))
		},
	}

	lobPerformanceCommand.AddCommand(
		stageCommand(),
		genCommand(),
		testExecutionCommand(),
	)

	lobPerformanceArgs = allArgs.commandArgs(lobPerformanceCommand, append(globalArgs,
		"datasource", "table"))

	return lobPerformanceCommand
}
