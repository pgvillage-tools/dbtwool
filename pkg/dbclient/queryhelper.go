package dbclient

// QueryHelper is used to get rdbms specific queries for basic functions
type QueryHelper struct {
}

const (
	pgTestQuery                      = "SELECT 1;"
	db2TestQuery                     = "SELECT 1 FROM SYSIBM.SYSDUMMY1"
	baseSetIsolationLevelSqlDb2      = "SET CURRENT ISOLATION "
	baseSetIsolationLevelSqlPostgres = "SET TRANSACTION ISOLATION LEVEL "
	db2UncommittedRead               = "UR"
	db2ReadStability                 = "RS"
	db2CursorStability               = "CS"
	db2RepeatableRead                = "RR"
	pgReadCommitted                  = "READ COMMITTED"
	pgRepeatableRead                 = "REPEATABLE READ"
	pgSerializable                   = "SERIALIZABLE"
)

func GetIsolationLevelQuery(rdbms Rdbms, isolationLevel int) string {
	switch rdbms {
	case RdbmsDB2:
		return baseSetIsolationLevelSqlDb2 + getDb2IsolationLevelString(isolationLevel)
	default:
		return baseSetIsolationLevelSqlPostgres + getPostgresIsolationLevelString(isolationLevel)
	}
}

func GetTestQuery(rdbms Rdbms) string {
	if rdbms == RdbmsDB2 {
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
	} else { // if isolationLevel >= 3
		return pgSerializable
	}
}

// returns nearest isolation level if provided level is not supported.
func getDb2IsolationLevelString(isolationLevel int) string {
	if isolationLevel <= 0 {
		return db2UncommittedRead
	} else if isolationLevel == 1 {
		return db2CursorStability
	} else if isolationLevel == 2 {
		return db2ReadStability
	} else { // if isolationLevel >= 3
		return db2RepeatableRead
	}
}
