package dbinterface

import "context"

// LobRow is used to contain the data to insert into the lobperformance test table
type LobRow struct {
	TenantID int
	DocType  string
	LobType  string // "blob"/"clob"
	Payload  any    // []byte or string
}

// BulkInserter is an interface which is separate because not every RDMS has a good option for this.
type BulkInserter interface {
	// InsertLOBRowsBulk inserts rows using a DB-specific bulk path (PG COPY, DB2 LOAD).
	// Returns rows inserted, bytes inserted.
	InsertLOBRowsBulk(ctx context.Context, schema, table string, rows []LobRow) (int64, int64, error)
}
