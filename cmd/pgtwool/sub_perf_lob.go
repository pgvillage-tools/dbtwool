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
			fmt.Println("stage:" + stageArgs.GetString("table"))
			fmt.Println("stage:" + stageArgs.GetString("cfgFile"))

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
			schema, table, err := parseSchemaTable(genArgs.GetString("table"))

			//func LobPerformanceGenerate(dbType dbclient.Rdbms, ctx context.Context, client dbinterface.Client, schemaName string, tableName string, spread []string, emptyLobs int64, byteSize string, lobType string) {

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

	genArgs = arguments.AllArgs.CommandArgs(genCommand, append(globalArgs, "spread", "byteSize", "table", "emptyLobs", "lobType"))
	return genCommand
}

func lobTestCommand() *cobra.Command {
	var testExecutionArgs arguments.Args
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

	testExecutionArgs = arguments.AllArgs.CommandArgs(testExecutionCommand,
		append(globalArgs, "parallel", "table", "spread"))

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
