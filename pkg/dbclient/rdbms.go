package dbclient

import (
	"strings"
)

type Rdbms string

const (
	RdbmsDB2      Rdbms = "db2"
	RdbmsPostgres Rdbms = "pg"
)

// Map driver names to rdbms names
var RdbmsDrivers = map[Rdbms]string{
	RdbmsDB2:      "go_ibm_db",
	RdbmsPostgres: "pgx",
}

// Helper to get the driver name safely
func (r Rdbms) DriverName() (string, bool) {
	d, ok := RdbmsDrivers[r]
	return d, ok
}

func GetRdbmsFromString(rdbmsText string) Rdbms {
	var rdbms Rdbms

	switch strings.ToLower(rdbmsText) {
	case "postgresql":
	case "postgres":
	case "pg":
		rdbms = RdbmsPostgres
	case "ibmdb":
	case "db2":
		rdbms = RdbmsDB2
	default:
		rdbms = RdbmsPostgres
	}

	return rdbms
}
