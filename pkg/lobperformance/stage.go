package lobperformance

import (
	"context"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
)

func LobPerformanceStage(db dbclient.Rdbms, schemaName string, tableName string) {
	//connection based on dbclient

	var connParams dbclient.ConnParams

	if db == dbclient.RdbmsDB2 {
		connParams = dbclient.NewDb2ConnparamsFromEnv()
	} else {
		connParams = dbclient.NewPgConnParamsFromEnv()
	}

	logger.Info().Msg("Connecting to database...")
	//execute script based on db and tablename
	client := dbclient.NewClient(connParams, db)

	pool, err := client.Pool()

	if err != nil {
		logger.Fatal().Msgf("Error while creating connection pool %v", err)
	}

	conn, connErr := pool.Connect(context.Background())
	if connErr != nil {
		logger.Fatal().Msgf("error %v", err)
	}

	logger.Info().Msg("Starting transaction")

	if err := conn.Begin(); err != nil {
		logger.Fatal().Msgf("error during begin transaction on connection: %v", err)
	}

	db2Helper := Db2Helper{schemaName: schemaName, tableName: tableName}

	logger.Info().Msg("Executing create schema")
	if rowsAltered, err := db2Helper.EnsureSchema(conn); err != nil {
		logger.Fatal().Msgf("Error while creating the schema: %v", err)
	} else {
		logger.Info().Msgf("Rows altered: %v", rowsAltered)
	}

	conn.Commit()

	logger.Info().Msg("Executing create table")
	if rowsAltered, err := conn.Execute(db2Helper.CreateTableSql()); err != nil {
		logger.Fatal().Msgf("Error while creating the table: %v", err)
	} else {
		logger.Info().Msgf("Rows altered: %v", rowsAltered)
	}

	logger.Info().Msg("Closing connection")
	conn.Close()
}
