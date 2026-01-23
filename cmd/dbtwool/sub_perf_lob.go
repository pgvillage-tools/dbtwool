package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func lobPerformanceCommand() *cobra.Command {
	lobPerformanceCommand := &cobra.Command{
		Use:   "lob-performance",
		Short: "test db performance with large objects",
		Long: "Use this command to create a test environment, " +
			"create a workload, " +
			"and execute a performance test for large objects.",
		RunE: requireSubcommand,
	}

	lobPerformanceCommand.AddCommand(
		lobStageCommand(),
		lobGenCommand(),
		lobTestCommand(),
	)

	return lobPerformanceCommand
}

func lobGenCommand() *cobra.Command {
	var genArgs args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			for _, element := range genArgs.GetStringSlice("spread") {
				fmt.Println("gen:" + element)
			}
			fmt.Println("gen:" + genArgs.GetString("bytesize"))
		},
	}

	genArgs = allArgs.commandArgs(genCommand, append(globalArgs, "spread", "bytesize", "table"))
	return genCommand
}

func lobStageCommand() *cobra.Command {
	var stageArgs args
	stageCommand := &cobra.Command{
		Use:   "stage",
		Short: "create tables",
		Long:  "Create the necessary schema and table(s)",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("stage:" + stageArgs.GetString("table"))
			fmt.Println("stage:" + stageArgs.GetString("datasource"))
			fmt.Println("stage:" + stageArgs.GetString("cfgFile"))
		},
	}

	stageArgs = allArgs.commandArgs(stageCommand, append(globalArgs, "table", "datasource"))

	return stageCommand
}

func lobTestCommand() *cobra.Command {
	var testExecutionArgs args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("lob-performance test: %d\n", testExecutionArgs.GetUint("parallel"))
			fmt.Printf("lob-performance test: %s\n", testExecutionArgs.GetString("table"))
			spread := testExecutionArgs.GetStringSlice("spread")
			for i, v := range spread {
				fmt.Printf("lob-performance test: #%d, %s\n", i, v)
			}
		},
	}

	testExecutionArgs = allArgs.commandArgs(testExecutionCommand, append(globalArgs, "parallel", "table", "spread"))

	return testExecutionCommand
}
