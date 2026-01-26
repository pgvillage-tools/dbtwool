// Package pg holds all code to connect to db2
package pg

import (
	"context"
	"fmt"

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

func (c *Connection) ExecuteWithPayload(ctx context.Context, sql string, payload any, args ...any) (int64, error) {
	if c.tx == nil {
		return 0, fmt.Errorf("ExecuteWithPayload requires an active transaction; call Begin() first")
	}

	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, payload)

	ct, err := c.conn.Exec(ctx, sql, allArgs...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
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
