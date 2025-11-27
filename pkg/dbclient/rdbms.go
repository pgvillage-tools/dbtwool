package dbclient

type Rdbms string

const (
	RdbmsDB2      Rdbms = "db2"
	RdbmsPostgres Rdbms = "postgres"
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
