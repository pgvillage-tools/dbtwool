package lobperformance

type DbHelper interface {
	CreateSchemaSql() string
	CreateTableSql() string
	CreateInsertLobRowBaseSql(string) (string, error)
}
