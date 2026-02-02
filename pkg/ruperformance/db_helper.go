package ruperformance

// DBHelper is an interface to help returning queries for a specific RDBMS type
type DBHelper interface {
	CreateSchemaSQL() string
	CreateTableSQL() string
	CreateIndexSQL() string
	CreateInserSQLPrefix() string
}
