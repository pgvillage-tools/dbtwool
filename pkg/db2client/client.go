// Package db2client holds all code to connect to db2
package db2client

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// importing ibm db2 driver so that database/sql can use it
	_ "github.com/ibmdb/go_ibm_db"
)

// Client is the main object to connect to DB2
type Client struct {
	ConnectParams ConnParams
	pool          *Pool
}

// NewClient returns a new Client
func NewClient(connectionParams ConnParams) Client {
	return Client{
		ConnectParams: connectionParams,
	}
}

// Pool will connect to DB2 and return a new DB2 pool
func (cl *Client) Pool() (*Pool, error) {
	if cl.pool != nil {
		return cl.pool, nil
	}

	pool, err := sql.Open("go_ibm_db", cl.ConnectParams.String())
	if err != nil {
		return nil, err
	} else if err = pool.Ping(); err != nil {
		return nil, err
	} else if _, err := pool.Query("SELECT 1 FROM SYSIBM.SYSDUMMY1"); err != nil {
		return nil, err
	}
	cl.pool = &Pool{pool: pool}
	return cl.pool, nil
}

// ConsistencyTest runs a consistency test against DB2
func (cl Client) ConsistencyTest(
	ctx context.Context,
	olapQuery string,
	isolationLevel IsolationLevel,
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
		logger.Fatal().Msgf("connect error for connection 1: %e", connectErr1)
	}
	defer conn2.Close()

	start := time.Now()

	logSinceElapsed := func(formatted string, args ...any) {
		elapsed := time.Since(start).Milliseconds()
		logger.Info().Int64("elapsed (ms)", elapsed).Msgf(formatted, args...)
	}

	//Lock row
	fmt.Printf("CONN2: SET CURRENT ISOLATION %s;\n", isolationLevel)
	conn2.SetIsolationLevel(isolationLevel)

	logSinceElapsed("T1: BEGIN;")
	if err := conn1.Begin(); err != nil {
		logger.Fatal().Msgf("error during begin transaction on connection 1: %v", err)
	}

	if row, err := conn1.QueryOneRow(olapQuery); err != nil {
		logger.Fatal().Msgf("error during fetch of olap query: %v", err)
	} else {
		fmt.Println("T1: result: %v", row)
	}

	logSinceElapsed("T1: SELECT * FROM gotest.products FOR UPDATE;")
	if _, err := conn1.Execute(oltpLockQuery); err != nil {
		logger.Fatal().Err(err)
	}

	//Try select
	go func() {
		logSinceElapsed("T2: BEGIN;")
		if err := conn2.Begin(); err != nil {
			logger.Fatal().Msgf("error during begin transaction: %v", err)
		}

		logSinceElapsed("T2: SELECT AVG(price) AS avgprice FROM gotest.products;")
		if row, err := conn2.QueryOneRow(olapQuery); err != nil {
			logger.Fatal().Msgf("error during fetch of olap query: %v", err)
		} else {
			fmt.Println("T2: result: %v", row)
		}
		conn2.Commit()
	}()

	//Update
	logSinceElapsed("T1: UPDATE gotest.products SET price = 5000 where product_id = 1;")
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
