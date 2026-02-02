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
			schema, table, err := parseSchemaTable(stageArgs.GetString("table"))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				ruperformance.Stage(context.Background(), dbclient.Postgres, &postgresClient, schema, table)
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	stageArgs = arguments.AllArgs.CommandArgs(stageCommand, append(globalArgs, "table"))

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
			schema, table, err := parseSchemaTable(genArgs.GetString("table"))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				ruperformance.Generate(
					context.Background(),
					dbclient.Postgres,
					&postgresClient,
					schema,
					table,
					int64(genArgs.GetUint("numOfRows")))
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	genArgs = arguments.AllArgs.CommandArgs(genCommand,
		// revive:disable-next-line
		append(globalArgs, "table", "numOfRows"))
	return genCommand
}
func ruTestCommand() *cobra.Command {
	var testExecutionArgs arguments.Args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, tableParseErr := parseSchemaTable(testExecutionArgs.GetString("table"))
			if tableParseErr != nil {
				fmt.Printf("An error occurred while parsing the schema + table: %v", tableParseErr)
			}

			iLevel, err := strconv.Atoi(testExecutionArgs.GetString("isolationLevel"))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				err := ruperformance.ExecuteTest(
					context.Background(),
					dbclient.Postgres,
					&postgresClient,
					schema,
					table,
					int(testExecutionArgs.GetUint("warmupTime")),
					int(testExecutionArgs.GetUint("executionTime")),
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
		append(globalArgs, "table", "warmupTime", "executionTime", "isolationLevel"))

	return testExecutionCommand
}
