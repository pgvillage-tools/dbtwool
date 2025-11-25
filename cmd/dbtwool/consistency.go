package main

import (
	"context"

	"github.com/pgvillage-tools/dbtwool/pkg/db2client"
	"github.com/spf13/cobra"
)

func consistencyCommand() *cobra.Command {
	var consistencyArgs args
	consistencyCommand := &cobra.Command{
		Use:   "consistency",
		Short: "Run a consistency test.",
		Long:  `Use this command to test consistency with different transaction isolation levels.`,
		Run: func(_ *cobra.Command, _ []string) {
			isolationLevel := consistencyArgs.GetString("isolationLevel")
			if isolationLevel == "" {
				log.Info("Isolation level set to default.")
			}

			cl := db2client.NewClient(db2client.ConnParamsFromEnv())
			cl.ConsistencyTest(
				context.Background(),
				"SELECT AVG(price) AS avgprice FROM gotest.products;",
				db2client.IsolationLevel(isolationLevel),
				"SELECT * FROM gotest.products FOR UPDATE;",
				"UPDATE gotest.products SET price = 5000 where product_id = 1;",
			)
		},
	}

	consistencyArgs = allArgs.commandArgs(consistencyCommand, append(globalArgs,
		"isolationLevel",
	))
	return consistencyCommand
}
