package db2client

import (
	"context"
	"database/sql"

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
