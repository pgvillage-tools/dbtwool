package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	db2 "github.com/pgvillage-tools/dbtwool/pkg/db2client"
	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/ruperformance"
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

func ruStageCommand() *cobra.Command {
	var stageArgs args
	stageCommand := &cobra.Command{
		Use:   "stage",
		Short: "create tables",
		Long:  "Create the necessary schema and table(s)",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(stageArgs.GetString(ArgTable))

			if err == nil {
				params := db2.NewDB2ConnparamsFromEnv()
				db2Client := db2.NewClient(params)
				if stageErr := ruperformance.Stage(
					context.Background(),
					dbclient.DB2,
					&db2Client,
					schema, table); stageErr != nil {
					fmt.Printf("An error occurred while staging RU performance: %v", stageErr)
					return
				}
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	stageArgs = allArgs.commandArgs(stageCommand, append(globalArgs, ArgTable))

	return stageCommand
}

func ruGenCommand() *cobra.Command {
	var genArgs args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(genArgs.GetString(ArgTable))

			if err == nil {
				params := db2.NewDB2ConnparamsFromEnv()
				db2Client := db2.NewClient(params)

				ruperformance.Generate(
					context.Background(),
					dbclient.DB2,
					&db2Client,
					schema,
					table,
					int64(genArgs.GetUint(ArgNumOfRows)))
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}
	genArgs = allArgs.commandArgs(genCommand, append(globalArgs, ArgTable, ArgNumOfRows))
	return genCommand
}

func ruTestCommand() *cobra.Command {
	var testExecutionArgs args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, tableParseErr := parseSchemaTable(testExecutionArgs.GetString(ArgTable))
			if tableParseErr != nil {
				fmt.Printf("An error occurred while parsing the schema + table: %v", tableParseErr)
			}

			iLevel, isolationParseErr := strconv.Atoi(testExecutionArgs.GetString(ArgIsolationLevel))

			if isolationParseErr == nil {
				params := db2.NewDB2ConnparamsFromEnv()
				db2Client := db2.NewClient(params)

				err := ruperformance.ExecuteTest(
					context.Background(),
					dbclient.DB2,
					&db2Client,
					schema,
					table,
					int(testExecutionArgs.GetUint(ArgWarmupTime)),
					int(testExecutionArgs.GetUint(ArgExecutionTime)),
					db2.GetIsolationLevel(iLevel))
				if err != nil {
					fmt.Printf("An error occurred while trying to execute the RU performance test: %v", err)
				}
			} else {
				fmt.Printf("An error occurred while parsing the isolation level: %v", isolationParseErr)
			}
		},
	}

	testExecutionArgs = allArgs.commandArgs(
		testExecutionCommand,
		append(globalArgs, ArgTable, ArgWarmupTime, ArgExecutionTime, ArgIsolationLevel))

	return testExecutionCommand
}
