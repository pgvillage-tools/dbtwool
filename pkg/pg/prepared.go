package pg

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

type pgPreparedStmt struct {
	tx   pgx.Tx
	name string
}

// ExecWithPayload: Execute with payload on prepared statements
func (s *pgPreparedStmt) ExecWithPayload(ctx context.Context, payload any, args ...any) (int64, error) {
	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, payload)

	ct, err := s.tx.Exec(ctx, s.name, allArgs...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// Close: Closing prepared statements is not for PostgreSQL
func (s *pgPreparedStmt) Close(_ context.Context) error {
	// not implemented
	return nil
}

// PrepareInTx: Prepare in transaction implementation for PostgreSQL
func (c *Connection) PrepareInTx(ctx context.Context, sqlText string) (dbinterface.PreparedStatement, error) {
	if c.tx == nil {
		return nil, fmt.Errorf("PrepareInTx requires active transaction")
	}
	// Name should be stable per connection+SQL.
	name := stmtNameForSQL(sqlText)

	_, err := c.conn.Prepare(ctx, name, sqlText)
	if err != nil {
		return nil, err
	}
	return &pgPreparedStmt{tx: c.tx, name: name}, nil
}

func stmtNameForSQL(sql string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return fmt.Sprintf("stmt_%x", h.Sum64())
}
