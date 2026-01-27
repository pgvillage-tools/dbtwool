package lobperformance

type DbHelper interface {
	CreateSchemaSql() string
	CreateTableSql() string
	CreateInsertLobRowBaseSql(string) (string, error)
	SelectReadLobByIdSql(lobType string) (string, error)
	SelectMinMaxIdSql() string
	PayloadColumnForLobType(lobType string) string
}
