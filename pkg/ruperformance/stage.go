package ruperformance

import (
	"context"
	"fmt"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/rs/zerolog/log"
)

// Stage is the main handler for the staging phase of the LOB tests
func Stage(ctx context.Context, dbType dbclient.RDBMS, client dbinterface.Client,
	schemaName string, tableName string) (errorResult error) {
	var logger = log.With().Logger()

	logger.Info().Msg("Initiating connection pool.")
	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %v", poolErr)
	}

	logger.Info().Msg("Connecting to database.")
	conn, connectErr := pool.Connect(ctx)
	if connectErr != nil {
		return fmt.Errorf("connect error: %w", connectErr)
	}
	defer conn.Close(ctx)

	logger.Info().Msg("Starting transaction")
	if beginErr := conn.Begin(ctx); beginErr != nil {
		return fmt.Errorf("error during begin transaction: %w", beginErr)
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
	if rowsAltered, execErr := conn.Execute(ctx, dbHelper.CreateTableSQL()); execErr != nil {
		return fmt.Errorf("error during create table: %w", execErr) // revive:disable-next-line
	} else {
		logger.Info().Msgf("Rows altered: %v", rowsAltered)
	}

	logger.Info().Msg("Executing create index")

	if rowsAltered, execErr := conn.Execute(ctx, dbHelper.CreateIndexSQL()); execErr != nil {
		return fmt.Errorf("error while creating the index: %w", execErr) // revive:disable-next-line
	} else {
		logger.Info().Msgf("Rows altered: %v", rowsAltered)
	}

	if commitErr := conn.Commit(ctx); commitErr != nil {
		return fmt.Errorf("error while committing transaction: %w", commitErr)
	}

	logger.Info().Msg("Closing connection")

	return nil
}
