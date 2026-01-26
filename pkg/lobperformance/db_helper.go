package lobperformance

type DbHelper interface {
	CreateSchemaSql() string
	CreateTableSql() string
	InsertOneRowSql(lobType string) (string, error)
}
