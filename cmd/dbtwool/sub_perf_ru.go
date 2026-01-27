package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func ruCommand() *cobra.Command {
	ruPerformanceCommand := &cobra.Command{
		Use:   "ru-performance",
		Short: "test db performance with read uncommitted isolation level",
		Long: strings.Join([]string{
			"Use this command to create a testenvironment",
			"create a workload",
			"and execute a performance test for read uncommitted isolation level.",
		}, ", "),
		RunE: requireSubcommand,
	}

	ruPerformanceCommand.AddCommand(
		ruStageCommand(),
		ruGenCommand(),
		ruTestCommand(),
	)

	return ruPerformanceCommand
}
func ruGenCommand() *cobra.Command {
	var genCmdArgs args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			for _, element := range genCmdArgs.GetStringSlice("spread") {
				fmt.Println("gen:" + element)
			}
			fmt.Println("gen:" + genCmdArgs.GetString("byteSize"))
		},
	}

	genCmdArgs = allArgs.commandArgs(genCommand, append(globalArgs, "spread", "byteSize", "table"))
	return genCommand
}

func ruStageCommand() *cobra.Command {
	var stageCmdArgs args
	stageCommand := &cobra.Command{
		Use:   "stage",
		Short: "create tables",
		Long:  "Create the necessary schema and table(s)",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("stage:" + stageCmdArgs.GetString("table"))
		},
	}

	stageCmdArgs = allArgs.commandArgs(stageCommand, append(globalArgs, "table"))

	return stageCommand
}

func ruTestCommand() *cobra.Command {
	var testCmdArgs args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("test: %d\n", testCmdArgs.GetUint("parallel"))
			fmt.Printf("test: %s\n", testCmdArgs.GetString("table"))
		},
	}

	testCmdArgs = allArgs.commandArgs(testExecutionCommand, append(globalArgs, "parallel", "table"))

	return testExecutionCommand
}
