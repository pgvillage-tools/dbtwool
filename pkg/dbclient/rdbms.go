// Package dbclient holds a helper to derive RDBMS type
package dbclient

import (
	"strings"
)

// RDBMS defines an ENUM
type RDBMS string

const (
	// DB2 means DB2
	DB2 RDBMS = "db2"
	// Postgres means PostgreSQL
	Postgres RDBMS = "pg"
)

// Drivers defines all available driver names to rdbms names
var Drivers = map[RDBMS]string{
	DB2:      "go_ibm_db",
	Postgres: "pgx",
}

// DriverName is a helper to get the driver name safely
func (r RDBMS) DriverName() (string, bool) {
	d, ok := Drivers[r]
	return d, ok
}

// GetRDBMSFromString returns the RDBMS belonging to a RDBMS string
func GetRDBMSFromString(rdbmsText string) RDBMS {
	var rdbms RDBMS

	switch strings.ToLower(rdbmsText) {
	case "postgresql":
	case "postgres":
	case "pg":
		rdbms = Postgres
	case "ibmdb":
	case "db2":
		rdbms = DB2
	default:
		rdbms = Postgres
	}

	return rdbms
}
