package db2client

import (
	"context"
	"database/sql"
	"errors"

	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

type db2PreparedStmt struct {
	stmt *sql.Stmt
}

// ExecWithPayload is a db2 implementation of using the prepared statement with a payload.
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

// Close closes the prepared statement
func (s *db2PreparedStmt) Close(_ context.Context) error {
	return s.stmt.Close()
}

// PrepareInTx is used to prepare a statement on the current transaction in the current connection.
func (c *Connection) PrepareInTx(ctx context.Context, sqlText string) (dbinterface.PreparedStatement, error) {
	if c.tx == nil {
		return nil, errors.New("PrepareInTx requires active transaction")
	}
	st, err := c.tx.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	return &db2PreparedStmt{stmt: st}, nil
}
