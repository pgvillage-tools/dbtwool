package lobperformance

// DBHelper is an interface to help returning queries for a specific RDBMS type
type DBHelper interface {
	CreateSchemaSQL() string
	CreateTableSQL() string
	CreateInsertLOBRowBaseSQL(string) (string, error)
	SelectReadLOBByIDSQL(lobType string) (string, error)
	SelectMinMaxIDSQL() string
	PayloadColumnForLOBType(lobType string) string
}
