package lobperformance

import (
	"context"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/rs/zerolog/log"
)

// Stage is the main handler for the staging phase of the LOB tests
func Stage(ctx context.Context, dbType dbclient.RDBMS, client dbinterface.Client,
	schemaName string, tableName string) {
	var logger = log.With().Logger()

	logger.Info().Msg("Initiating connection pool.")
	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %v", poolErr)
	}

	logger.Info().Msg("Connecting to database.")
	conn, connectErr1 := pool.Connect(ctx) //
	if connectErr1 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %v", connectErr1)
	}
	defer conn.Close(ctx)

	logger.Info().Msg("Starting transaction")
	if err := conn.Begin(ctx); err != nil {
		logger.Fatal().Msgf("error during begin transaction on connection: %v", err)
	}

	var dbHelper DBHelper

	if dbType == dbclient.DB2 {
		dbHelper = DB2Helper{schemaName: schemaName, tableName: tableName}
	} else {
		dbHelper = PGHelper{schemaName: schemaName, tableName: tableName}
	}

	logger.Info().Msg("Executing create schema")

	if rowsAltered, err := conn.Execute(ctx, dbHelper.CreateSchemaSQL()); err != nil {
		logger.Warn().Msgf("Error while trying to create the schema: %v", err)
	} else {
		logger.Info().Msgf("Rows altered: %v", rowsAltered)
	}

	logger.Info().Msg("Executing create table")
	if rowsAltered, err := conn.Execute(ctx, dbHelper.CreateTableSQL()); err != nil {
		logger.Fatal().Msgf("Error while creating the table: %v", err)
	} else {
		logger.Info().Msgf("Rows altered: %v", rowsAltered)
	}

	conn.Commit(ctx)

	logger.Info().Msg("Closing connection")
	conn.Close(ctx)
}
