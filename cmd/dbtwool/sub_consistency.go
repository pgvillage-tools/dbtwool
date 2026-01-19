package main

import (
	"context"
	"strconv"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/spf13/cobra"
)

func consistencyCommand() *cobra.Command {
	var consistencyArgs args
	consistencyCommand := &cobra.Command{
		Use:   "consistency",
		Short: "Run a consistency test.",
		Long:  `Use this command to test consistency with different transaction isolation levels.`,
		Run: func(_ *cobra.Command, _ []string) {
			isolationLevel, error := strconv.Atoi(consistencyArgs.GetString("isolationLevel"))
			if error != nil {
				isolationLevel = 1
			}

			var db2Params dbclient.ConnParams = dbclient.NewDb2ConnparamsFromEnv()

			cl1 := dbclient.NewClient(db2Params, dbclient.RdbmsDB2)
			cl1.ConsistencyTest(
				context.Background(),
				"SELECT AVG(price) AS avgprice FROM gotest.products;",
				isolationLevel,
				"SELECT * FROM gotest.products FOR UPDATE;",
				"UPDATE gotest.products SET price = 5000 where product_id = 1;",
			)

			var pgParams dbclient.ConnParams = dbclient.NewPgConnParamsFromEnv()

			c2 := dbclient.NewClient(pgParams, dbclient.RdbmsPostgres)
			c2.ConsistencyTest(
				context.Background(),
				"SELECT AVG(price) AS avgprice FROM gotest.products;",
				isolationLevel,
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
