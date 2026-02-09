package dbinterface

import "context"

type LobRow struct {
	TenantID int
	DocType  string
	LobType  string // "blob"/"clob"
	Payload  any    // []byte or string
}

type BulkInserter interface {
	// InsertLOBRowsBulk inserts rows using a DB-specific bulk path (PG COPY, DB2 LOAD).
	// Returns rows inserted, bytes inserted.
	InsertLOBRowsBulk(ctx context.Context, schema, table string, rows []LobRow) (int64, int64, error)
}
