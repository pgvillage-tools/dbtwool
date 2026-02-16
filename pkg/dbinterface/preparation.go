package dbinterface

import "context"

// TxPreparer interface
// Implement only if connection supports prepared statements within the active transaction.
type TxPreparer interface {
	PrepareInTx(ctx context.Context, sql string) (PreparedStatement, error)
}

// PreparedStatement is a DB-agnostic prepared statement handle.
type PreparedStatement interface {
	ExecWithPayload(ctx context.Context, payload any, args ...any) (int64, error)
	Close(ctx context.Context) error
}
