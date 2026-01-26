package db2client

import (
	"context"
	"database/sql"
	"fmt"

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

func (c *Connection) ExecuteWithPayload(ctx context.Context, sql string, payload any, args ...any) (int64, error) {
	if c.tx == nil {
		return 0, fmt.Errorf("ExecuteWithPayload requires an active transaction; call Begin() first")
	}

	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, payload)

	ct, err := c.conn.ExecContext(ctx, sql, allArgs...)
	if err != nil {
		return 0, err
	}

	return ct.RowsAffected()
}
