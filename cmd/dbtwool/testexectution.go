package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func testExecutionCommand() *cobra.Command {
	var testExecutionArgs args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("test:" + testExecutionArgs.GetString("param"))
			fmt.Println("test:" + testExecutionArgs.GetString("table"))
		},
	}

	testExecutionArgs = allArgs.commandArgs(testExecutionCommand, append(globalArgs, "parallel", "table"))

	return testExecutionCommand
}
