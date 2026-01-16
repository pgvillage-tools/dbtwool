package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func ruPerformanceCommand() *cobra.Command {
	var ruPerformanceArgs args
	ruPerformanceCommand := &cobra.Command{
		Use:   "ru-performance",
		Short: "test db performance with read uncommitted isolation level",
		Long:  "Use this command to create a testenvironment, create a workload, and execute a performance test for read uncommitted isolation level.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("ru-performance:" + ruPerformanceArgs.GetString("datasource"))
			fmt.Println("ru-performance:" + ruPerformanceArgs.GetString("table"))
		},
	}

	ruPerformanceCommand.AddCommand(
		stageCommand(),
		genCommand(),
		testExecutionCommand(),
	)

	return ruPerformanceCommand
}
