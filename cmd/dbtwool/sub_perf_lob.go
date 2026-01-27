package main

import (
	"context"
	"fmt"
	"strings"

	db2 "github.com/pgvillage-tools/dbtwool/pkg/db2client"
	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/lobperformance"
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
	var stageArgs args
	stageCommand := &cobra.Command{
		Use:   "stage",
		Short: "create tables",
		Long:  "Create the necessary schema and table(s)",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(stageArgs.GetString("table"))

			if err == nil {
				params := db2.NewDB2ConnparamsFromEnv()
				db2Client := db2.NewClient(params)
				lobperformance.LobPerformanceStage(dbclient.RdbmsDB2, context.Background(), &db2Client, schema, table)
			} else {
				fmt.Printf("An error occurred while parsing the schema + table: %e", err)
			}
		},
	}

	stageArgs = allArgs.commandArgs(stageCommand, append(globalArgs, "table"))

	return stageCommand
}

func lobGenCommand() *cobra.Command {
	var genArgs args
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "generate all the things",
		Long:  "Use this command to generate data to test with.",
		Run: func(_ *cobra.Command, _ []string) {
			schema, table, err := parseSchemaTable(genArgs.GetString("table"))

			if err == nil {
				params := db2.NewDB2ConnparamsFromEnv()
				db2Client := db2.NewClient(params)

				lobperformance.LobPerformanceGenerate(
					dbclient.RdbmsDB2,
					context.Background(),
					&db2Client,
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

	genArgs = allArgs.commandArgs(genCommand, append(globalArgs, "spread", "byteSize", "table", "emptyLobs", "lobType"))
	return genCommand
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
