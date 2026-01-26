package main

import (
	"context"
	"strconv"

	db2 "github.com/pgvillage-tools/dbtwool/pkg/db2client"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/spf13/cobra"
)

func consistencyCommand() *cobra.Command {
	var consistencyArgs args
	consistencyCommand := &cobra.Command{
		Use:   "consistency",
		Short: "Run a consistency test.",
		Long:  `Use this command to test consistency with different transaction isolation levels.`,
		Run: func(_ *cobra.Command, _ []string) {
			iLevel, err := strconv.Atoi(consistencyArgs.GetString("isolationLevel"))
			if err != nil {
				log.Info("Warning: invalid isolationLevel, defaulting to 1")
				iLevel = 1
			}

			cl1 := db2.NewClient(db2.NewDB2ConnparamsFromEnv())
			dbinterface.ConsistencyTest(
				context.Background(),
				&cl1,
				"SELECT AVG(price) AS avgprice FROM gotest.products;",
				db2.GetIsolationLevel(iLevel),
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
