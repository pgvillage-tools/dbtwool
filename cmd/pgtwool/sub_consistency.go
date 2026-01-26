package main

import (
	"context"
	"strconv"

	"github.com/pgvillage-tools/dbtwool/internal/arguments"
	db "github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/pgvillage-tools/dbtwool/pkg/pg"
	"github.com/spf13/cobra"
)

func consistencyCommand() *cobra.Command {
	var consistencyArgs arguments.Args
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
			isolationLevel := pg.GetIsolationLevel(iLevel)

			params := pg.ConnParamsFromEnv()

			cl1 := pg.NewClient(params)
			db.ConsistencyTest(
				context.Background(),
				&cl1,
				"SELECT AVG(price) AS avgprice FROM gotest.products;",
				isolationLevel,
				"SELECT * FROM gotest.products FOR UPDATE;",
				"UPDATE gotest.products SET price = 5000 where product_id = 1;",
			)
		},
	}

	consistencyArgs = arguments.AllArgs.CommandArgs(consistencyCommand, append(globalArgs,
		"isolationLevel",
	))
	return consistencyCommand
}
