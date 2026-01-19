package lobperformance

import "fmt"

type PgHelper struct {
	schemaName string
	tableName  string
}

func (helper PgHelper) CreateSchemaSql() string {
	return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v;", helper.schemaName)
}

func (helper PgHelper) CreateTableSql() string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v.%v;", helper.schemaName, helper.tableName)
}
