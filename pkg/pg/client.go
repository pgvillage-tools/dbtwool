// Package pg holds all code to connect to db2
package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

const (
	pgTestQuery = "SELECT 1;"
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

// Pool will connect to DB2 and return a new PostgreSQL pool
func (cl *Client) Pool(ctx context.Context) (dbinterface.Pool, error) {
	if cl.pool != nil {
		return *cl.pool, nil
	}

	pool, err := pgxpool.New(ctx, cl.ConnectParams.GetConnString())
	if err != nil {
		return Pool{}, err
	} else if err = pool.Ping(ctx); err != nil {
		return Pool{}, err
	} else if _, err := pool.Query(ctx, pgTestQuery); err != nil {
		return Pool{}, err
	}
	cl.pool = &Pool{pool: pool}
	return *cl.pool, nil
}
