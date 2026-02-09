package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pgvillage-tools/dbtwool/internal/arguments"
	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/lobperformance"
	"github.com/pgvillage-tools/dbtwool/pkg/pg"
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

func lobStageCommand() *cobra.Command {
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

				lobperformance.Stage(context.Background(), dbclient.Postgres, &postgresClient, schema, table)
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	stageArgs = arguments.AllArgs.CommandArgs(stageCommand, append(globalArgs, arguments.ArgTable))

	return stageCommand
}

func lobGenCommand() *cobra.Command {
	var genArgs arguments.Args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(genArgs.GetString(arguments.ArgTable))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				lobperformance.GenerateBulk(
					context.Background(),
					dbclient.Postgres,
					&postgresClient,
					schema,
					table,
					genArgs.GetStringSlice(arguments.ArgSpread),
					int64(genArgs.GetUint(arguments.ArgEmptyLobs)),
					genArgs.GetString(arguments.ArgByteSize),
					int(genArgs.GetUint(arguments.ArgBatchSize)),
					genArgs.GetString(arguments.ArgLobType))
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	genArgs = arguments.AllArgs.CommandArgs(
		genCommand,
		append(globalArgs,
			arguments.ArgSpread,
			arguments.ArgByteSize,
			arguments.ArgTable,
			arguments.ArgEmptyLobs,
			arguments.ArgLobType,
			arguments.ArgBatchSize))
	return genCommand
}

func lobTestCommand() *cobra.Command {
	var testExecutionArgs arguments.Args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(testExecutionArgs.GetString(arguments.ArgTable))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				err := lobperformance.ExecuteTest(
					context.Background(),
					dbclient.Postgres,
					&postgresClient,
					schema,
					table,
					testExecutionArgs.GetString(arguments.ArgRandomizerSeed),
					int(testExecutionArgs.GetUint(arguments.ArgParallel)),
					int(testExecutionArgs.GetUint(arguments.ArgWarmupTime)),
					int(testExecutionArgs.GetUint(arguments.ArgExecutionTime)),
					testExecutionArgs.GetString(arguments.ArgReadMode),
					testExecutionArgs.GetString(arguments.ArgLobType))

				if err != nil {
					fmt.Printf("An error occurred while trying to execute the LOB performance test: %v", err)
				}
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %v", err)
			}
		},
	}

	testExecutionArgs = arguments.AllArgs.CommandArgs(
		testExecutionCommand,
		append(
			globalArgs,
			arguments.ArgTable,
			arguments.ArgRandomizerSeed,
			arguments.ArgParallel,
			arguments.ArgWarmupTime,
			arguments.ArgExecutionTime,
			arguments.ArgReadMode,
			arguments.ArgLobType))

	return testExecutionCommand
}

func parseSchemaTable(fullName string) (schema string, table string, err error) {
	if fullName == "" {
		return "", "", errors.New("table name cannot be empty")
	}

	if strings.Contains(fullName, ".") {
		parts := strings.SplitN(fullName, ".", 2)
		schema = parts[0]
		table = parts[1]

		if schema == "" || table == "" {
			return "", "", fmt.Errorf("invalid table name %q, expected schema.table", fullName)
		}

		return schema, table, nil
	}
	return "dbtwooltests", fullName, nil
}
