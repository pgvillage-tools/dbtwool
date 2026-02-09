package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

func (c *Connection) InsertLOBRowsBulk(ctx context.Context, schema, table string, rows []dbinterface.LobRow) (int64, int64, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}

	// Use a tx for COPY
	if err := c.Begin(ctx); err != nil {
		return 0, 0, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = c.Rollback(ctx)
		}
	}()

	src := &pgLobRowSource{rows: rows}
	cols := []string{"tenant_id", "doc_type", "payload_bin", "payload_text"}

	n, err := c.tx.CopyFrom(ctx, pgx.Identifier{schema, table}, cols, src)
	if err != nil {
		return 0, 0, err
	}

	var totalBytes, bytesCalcErr = calculateTotalBytes(rows)
	if bytesCalcErr != nil {
		return 0, 0, bytesCalcErr
	}

	if err := c.Commit(ctx); err != nil {
		return 0, 0, err
	}
	committed = true

	return n, totalBytes, nil
}

type pgLobRowSource struct {
	rows []dbinterface.LobRow
	i    int
}

func (s *pgLobRowSource) Next() bool { return s.i < len(s.rows) }

func (s *pgLobRowSource) Values() ([]any, error) {
	r := s.rows[s.i]
	s.i++

	var bin any = nil
	var txt any = nil

	switch r.LobType {
	case "blob", "bytea":
		bin = r.Payload
	case "clob", "text":
		txt = r.Payload
	default:
		return nil, fmt.Errorf("unsupported lobType %q", r.LobType)
	}

	return []any{r.TenantID, r.DocType, bin, txt}, nil
}

func (s *pgLobRowSource) Err() error { return nil }

func calculateTotalBytes(rows []dbinterface.LobRow) (int64, error) {
	var totalBytes int64 = 0
	var err error = nil

	for _, r := range rows {
		switch v := r.Payload.(type) {
		case []byte:
			totalBytes += int64(len(v))
		case string:
			totalBytes += int64(len(v))
		case nil:
		default:
			err = fmt.Errorf("unexpected payload type %T for lobType=%q", r.Payload, r.LobType)
		}
	}

	return totalBytes, err

}
