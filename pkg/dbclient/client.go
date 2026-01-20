// Package dbclient holds all code to connect to db2
package dbclient

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// importing db drivers so that database/sql can use it
	_ "github.com/ibmdb/go_ibm_db"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Client is the main object to connect to DB2
type Client struct {
	ConnectParams ConnParams
	pool          *Pool
	rdbms         RDBMS
}

// NewClient returns a new Client
func NewClient(connectionParams ConnParams, rdbms RDBMS) Client {
	return Client{
		ConnectParams: connectionParams,
		rdbms:         rdbms,
	}
}

// Pool will connect to DB2 and return a new DB2 pool
func (cl *Client) Pool() (*Pool, error) {
	if cl.pool != nil {
		return cl.pool, nil
	}

	driver, ok := cl.rdbms.DriverName()
	if !ok {
		logger.Fatal().Msgf("Cannot start a connection. Driver not supported: %s", driver)
	}

	pool, err := sql.Open(driver, cl.ConnectParams.GetConnString())
	if err != nil {
		return nil, err
	} else if err = pool.Ping(); err != nil {
		return nil, err
	} else if _, err := pool.Query(GetTestQuery(cl.rdbms)); err != nil {
		return nil, err
	}
	cl.pool = &Pool{pool: pool}
	return cl.pool, nil
}

// ConsistencyTest runs a consistency test against DB2
func (cl Client) ConsistencyTest(
	ctx context.Context,
	olapQuery string,
	isolationLevel int,
	oltpLockQuery string,
	oltpUpdateQuery string,
) {
	pool, poolErr := cl.Pool()
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %e", poolErr)
	}

	// Get 2 dedicated physical connections from the pool
	conn1, connectErr1 := pool.Connect(ctx) //
	if connectErr1 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %e", connectErr1)
	}
	defer conn1.Close()

	conn2, connectErr2 := pool.Connect(ctx)
	if connectErr2 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %e", connectErr2)
	}
	defer conn2.Close()

	start := time.Now()

	logSinceElapsed := func(formatted string, args ...any) {
		elapsed := time.Since(start).Milliseconds()
		logger.Info().Int64("elapsed (ms)", elapsed).Msgf(formatted, args...)
	}

	isolationLevelQuery := GetIsolationLevelQuery(cl.rdbms, isolationLevel)
	logger.Info().Msgf("CONN2: %s", isolationLevelQuery)
	conn2.Execute(isolationLevelQuery)

	logSinceElapsed("T1: BEGIN;")
	if err := conn1.Begin(); err != nil {
		logger.Fatal().Msgf("error during begin transaction on connection 1: %v", err)
	}

	if row, err := conn1.QueryOneRow(olapQuery); err != nil {
		logger.Fatal().Msgf("error during fetch of olap query: %v", err)
	} else {
		logger.Info().Msgf("T1: result: %v", row)
	}

	// Lock rows
	logSinceElapsed(fmt.Sprintf("T1: %s", oltpLockQuery))
	if _, err := conn1.Execute(oltpLockQuery); err != nil {
		logger.Fatal().Err(err)
	}

	// Try select
	go func() {
		logSinceElapsed("T2: BEGIN;")
		if err := conn2.Begin(); err != nil {
			logger.Fatal().Msgf("error during begin transaction: %v", err)
		}

		logSinceElapsed(fmt.Sprintf("T2: %s", olapQuery))
		if row, err := conn2.QueryOneRow(olapQuery); err != nil {
			logger.Fatal().Msgf("error during fetch of olap query: %v", err)
		} else {
			logger.Info().Msgf("T2: result: %v", row)
		}
		conn2.Commit()
	}()

	// Update
	logSinceElapsed(fmt.Sprintf("T1: %s", oltpUpdateQuery))
	if _, err := conn1.Execute(oltpUpdateQuery); err != nil {
		logger.Fatal().Err(err)
	}

	logSinceElapsed("T1: sleeping 10s');")
	time.Sleep(10 * time.Second)

	logSinceElapsed("T1: COMMIT;")
	if err := conn1.Commit(); err != nil {
		logger.Fatal().Err(err)
	}
}
