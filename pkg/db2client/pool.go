package db2client

import (
	"context"
	"database/sql"
	"errors"

	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// Pool is a wrapper around sql.DB, so we can add methods on top of it
type Pool struct {
	pool *sql.DB
}

// Connect will create and return a new connection in the pool
func (p Pool) Connect(ctx context.Context) (dbinterface.Connection, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Connection{conn: conn}, nil
}

// ExecuteWithPayload executes a query with adding a payload
func (c *Connection) ExecuteWithPayload(ctx context.Context, qry string, payload any, args ...any) (int64, error) {
	if c.tx == nil {
		return 0, errors.New("ExecuteWithPayload requires an active transaction; call Begin() first")
	}

	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, payload)

	ct, err := c.conn.ExecContext(ctx, qry, allArgs...)
	if err != nil {
		return 0, err
	}

	return ct.RowsAffected()
}
