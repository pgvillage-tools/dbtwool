// Package db2client holds all code to connect to db2
package db2client

import (
	"context"
	"database/sql"

	// importing db drivers so that database/sql can use it
	_ "github.com/ibmdb/go_ibm_db"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

const db2TestQuery = "SELECT 1 FROM SYSIBM.SYSDUMMY1"

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
func (cl *Client) Pool(_ context.Context) (dbinterface.Pool, error) {
	if cl.pool != nil {
		return cl.pool, nil
	}

	pool, err := sql.Open("go_ibm_db", cl.ConnectParams.GetConnString())
	if err != nil {
		return nil, err
	} else if err = pool.Ping(); err != nil {
		return nil, err
	} else if _, err := pool.Query(db2TestQuery); err != nil {
		return nil, err
	}
	cl.pool = &Pool{pool: pool}
	return cl.pool, nil
}
