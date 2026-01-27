package main

import (
	"context"
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
			schema, table, err := parseSchemaTable(stageArgs.GetString("table"))

			if err == nil {
				params := pg.ConnParamsFromEnv()

				postgresClient := pg.NewClient(params)

				lobperformance.LobPerformanceStage(dbclient.RdbmsPostgres, context.Background(), &postgresClient, schema, table)
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %e", err)
			}
		},
	}

	stageArgs = arguments.AllArgs.CommandArgs(stageCommand, append(globalArgs, "table"))

	return stageCommand
}

func lobGenCommand() *cobra.Command {
	var genArgs arguments.Args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("lob-performance test: %s\n", genArgs.GetString("randomizerSeed"))

			//not used yet
			schema, table, err := parseSchemaTable(genArgs.GetString("table"))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				lobperformance.LobPerformanceGenerate(
					dbclient.RdbmsPostgres,
					context.Background(),
					&postgresClient,
					schema,
					table,
					genArgs.GetStringSlice("spread"),
					int64(genArgs.GetUint("emptyLobs")),
					genArgs.GetString("byteSize"),
					genArgs.GetString("lobType"))
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %e", err)
			}
		},
	}

	genArgs = arguments.AllArgs.CommandArgs(genCommand, append(globalArgs, "spread", "byteSize", "table", "emptyLobs", "lobType", "randomizerSeed"))
	return genCommand
}

func lobTestCommand() *cobra.Command {
	var testExecutionArgs arguments.Args
	testExecutionCommand := &cobra.Command{
		Use:   "test",
		Short: "run the test",
		Long:  "Use this command to run the test on the earlier created data.",
		Run: func(_ *cobra.Command, _ []string) {

			schema, table, err := parseSchemaTable(testExecutionArgs.GetString("table"))

			if err == nil {
				params := pg.ConnParamsFromEnv()
				postgresClient := pg.NewClient(params)

				err := lobperformance.LobPerformanceExecuteTest(
					dbclient.RdbmsPostgres,
					context.Background(),
					&postgresClient,
					schema,
					table,
					testExecutionArgs.GetString("randomizerSeed"),
					int(testExecutionArgs.GetUint("parallel")),
					int(testExecutionArgs.GetUint("warmupTime")),
					int(testExecutionArgs.GetUint("executionTime")),
					testExecutionArgs.GetString("readMode"),
					testExecutionArgs.GetString("lobType"))

				if err != nil {
					fmt.Printf("An error occurred while trying to execute the LOB performance test: %e", err)
				}
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %e", err)
			}
		},
	}

	testExecutionArgs = arguments.AllArgs.CommandArgs(testExecutionCommand,
		append(globalArgs, "table", "randomizerSeed", "parallel", "warmupTime", "executionTime", "readMode", "lobType"))

	return testExecutionCommand
}

func parseSchemaTable(fullName string) (schema string, table string, err error) {
	if fullName == "" {
		return "", "", fmt.Errorf("table name cannot be empty")
	}

	if strings.Contains(fullName, ".") {
		parts := strings.SplitN(fullName, ".", 2)
		schema = parts[0]
		table = parts[1]

		if schema == "" || table == "" {
			return "", "", fmt.Errorf("invalid table name %q, expected schema.table", fullName)
		}

		return schema, table, nil
	} else {
		return "dbtwooltests", fullName, nil
	}
}
