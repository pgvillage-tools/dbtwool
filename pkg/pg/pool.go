package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// Pool is a wrapper around sql.DB, so we can add methods on top of it
type Pool struct {
	pool *pgxpool.Pool
}

// Connect will create and return a new connection in the pool
func (p Pool) Connect(ctx context.Context) (dbinterface.Connection, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	pgConn := &Connection{conn: conn.Conn()}
	return pgConn, nil
}
