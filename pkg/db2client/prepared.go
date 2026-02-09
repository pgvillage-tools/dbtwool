package db2client

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

type db2PreparedStmt struct {
	stmt *sql.Stmt
}

func (s *db2PreparedStmt) ExecWithPayload(ctx context.Context, payload any, args ...any) (int64, error) {
	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, payload)

	res, err := s.stmt.ExecContext(ctx, allArgs...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *db2PreparedStmt) Close(_ context.Context) error {
	return s.stmt.Close()
}

func (c *Connection) PrepareInTx(ctx context.Context, sqlText string) (dbinterface.PreparedStatement, error) {
	if c.tx == nil {
		return nil, fmt.Errorf("PrepareInTx requires active transaction")
	}
	st, err := c.tx.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	return &db2PreparedStmt{stmt: st}, nil
}
