package dbclient

// RDBMS is an enum defining what RDBMS is in use
type RDBMS string

const (
	// RDBMSDB2 defines a DB2 RDBMS
	RDBMSDB2 RDBMS = "db2"
	// RDBMSPostgres defines a PostgreSQL RDBMS
	RDBMSPostgres RDBMS = "postgres"
)

// RDBMSDrivers maps an RDBMS enum to its driver name
var RDBMSDrivers = map[RDBMS]string{
	RDBMSDB2:      "go_ibm_db",
	RDBMSPostgres: "pgx",
}

// DriverName converts an RDBMS enum to a driver name as a string
func (r RDBMS) DriverName() (string, bool) {
	d, ok := RDBMSDrivers[r]
	return d, ok
}
