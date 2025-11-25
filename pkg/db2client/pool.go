package db2client

import (
	"context"
	"database/sql"
)

type Pool struct {
	pool sql.DB
}

func (p Pool) Connect(ctx context.Context) (*Connection, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Connection{ctx: ctx, conn: *conn}, nil
}
