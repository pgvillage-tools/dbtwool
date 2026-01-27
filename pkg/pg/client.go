// Package pg holds all code to connect to PostgreSQL
package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// Client is the main object to connect to PostgreSQL
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

// Pool will connect to PostgreSQL and return a new PostgreSQL pool
func (cl *Client) Pool(ctx context.Context) (dbinterface.Pool, error) {
	if cl.pool == nil {
		pool, err := pgxpool.New(ctx, cl.ConnectParams.GetConnString())
		if err != nil {
			return Pool{}, err
		}
		cl.pool = &Pool{pool: pool}
	}
	if err := cl.pool.validate(ctx); err != nil {
		cl.pool = nil
		return Pool{}, err
	}
	return *cl.pool, nil
}
