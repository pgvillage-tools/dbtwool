package dbclient

// QueryHelper is used to get rdbms specific queries for basic functions
type QueryHelper struct {
}

const (
	pgTestQuery                      = "SELECT 1;"
	db2TestQuery                     = "SELECT 1 FROM SYSIBM.SYSDUMMY1"
	baseSetIsolationLevelSQLDb2      = "SET CURRENT ISOLATION "
	baseSetIsolationLevelSQLPostgres = "SET TRANSACTION ISOLATION LEVEL "
	db2UncommittedRead               = "UR"
	db2ReadStability                 = "RS"
	db2CursorStability               = "CS"
	db2RepeatableRead                = "RR"
	pgReadCommitted                  = "READ COMMITTED"
	pgRepeatableRead                 = "REPEATABLE READ"
	pgSerializable                   = "SERIALIZABLE"
)

// GetIsolationLevelQuery can be used to return a query to retrieve the
// isolation level for a specific RDBMS
func GetIsolationLevelQuery(rdbms RDBMS, isolationLevel int) string {
	switch rdbms {
	case RDBMSDB2:
		return baseSetIsolationLevelSQLDb2 + getDb2IsolationLevelString(isolationLevel)
	default:
		return baseSetIsolationLevelSQLPostgres + getPostgresIsolationLevelString(isolationLevel)
	}
}

// GetTestQuery can be used to retrieve a test query specific to the rdbms
func GetTestQuery(rdbms RDBMS) string {
	if rdbms == RDBMSDB2 {
		return db2TestQuery
	}
	return pgTestQuery
}

// returns nearest isolation level if provided level is not supported.
func getPostgresIsolationLevelString(isolationLevel int) string {
	if isolationLevel <= 1 { // postgres does not support uncommitted reads
		return pgReadCommitted
	} else if isolationLevel == 2 {
		return pgRepeatableRead
	}
	return pgSerializable
}

// returns nearest isolation level if provided level is not supported.
func getDb2IsolationLevelString(isolationLevel int) string {
	if isolationLevel <= 0 {
		return db2UncommittedRead
	} else if isolationLevel == 1 {
		return db2CursorStability
	} else if isolationLevel == 2 {
		return db2ReadStability
	}
	return db2RepeatableRead
}
