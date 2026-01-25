// Package dbinterface acts as an interface on database connections, as well as a placeholder for
// functions that should work on all types of rdbms'es
package dbinterface

import "context"

// Client represents any database client (either DB2, PostgreSQL or in the future something else)
type Client interface {
	Pool(context.Context) (Pool, error)
}

// Pool represents any database connection pool (for DB2 connections, PostgreSQL connections, and in the others too)
type Pool interface {
	Connect(context.Context) (Connection, error)
}

// Connection represents any database connection (for DB2 connections, PostgreSQL connections, and in the others too)
type Connection interface {
	Close(context.Context) error
	Begin(context.Context) error
	Commit(context.Context) error
	Execute(context.Context, string) (int64, error)
	SetIsolationLevel(context.Context, IsolationLevel) error
	QueryOneRow(context.Context, string, ...any) (map[string]any, error)
}

// IsolationLevel can be different for RDBMS, so we have an Enum per RDBMS driver.
// All we need is a string value
type IsolationLevel interface {
	AsQuery() string
}
