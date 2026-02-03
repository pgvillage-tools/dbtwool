package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pgvillage-tools/dbtwool/internal/arguments"
	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/pg"
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
	var stageArgs arguments.Args
	stageCommand := &cobra.Command{
		Use:   "stage",
		Short: "create tables",
		Long:  "Create the necessary schema and table(s)",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(stageArgs.GetString(arguments.ArgTable))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				ruperformance.Stage(context.Background(), dbclient.Postgres, &postgresClient, schema, table)
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	stageArgs = arguments.AllArgs.CommandArgs(stageCommand, append(globalArgs, arguments.ArgTable))

	return stageCommand
}

func ruGenCommand() *cobra.Command {
	var genArgs arguments.Args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			// not used yet
			schema, table, err := parseSchemaTable(genArgs.GetString(arguments.ArgTable))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				ruperformance.Generate(
					context.Background(),
					dbclient.Postgres,
					&postgresClient,
					schema,
					table,
					int64(genArgs.GetUint(arguments.ArgNumOfRows)))
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	genArgs = arguments.AllArgs.CommandArgs(genCommand,
		// revive:disable-next-line
		append(globalArgs, arguments.ArgTable, arguments.ArgNumOfRows))
	return genCommand
}

func ruTestCommand() *cobra.Command {
	var testExecutionArgs arguments.Args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, tableParseErr := parseSchemaTable(testExecutionArgs.GetString(arguments.ArgTable))
			if tableParseErr != nil {
				fmt.Printf("An error occurred while parsing the schema + table: %v", tableParseErr)
			}

			iLevel, err := strconv.Atoi(testExecutionArgs.GetString(arguments.ArgIsolationLevel))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				err := ruperformance.ExecuteTest(
					context.Background(),
					dbclient.Postgres,
					&postgresClient,
					schema,
					table,
					int(testExecutionArgs.GetUint(arguments.ArgWarmupTime)),
					int(testExecutionArgs.GetUint(arguments.ArgExecutionTime)),
					pg.GetIsolationLevel(iLevel))
				if err != nil {
					fmt.Printf("An error occurred while trying to execute the RU performance test: %v", err)
				}
			} else {
				fmt.Printf("An error occurred while parsing the isolation level: %v", err)
			}
		},
	}

	testExecutionArgs = arguments.AllArgs.CommandArgs(
		testExecutionCommand,
		append(globalArgs,
			arguments.ArgTable,
			arguments.ArgWarmupTime,
			arguments.ArgExecutionTime,
			arguments.ArgIsolationLevel))

	return testExecutionCommand
}
