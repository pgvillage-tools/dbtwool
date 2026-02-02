package ruperformance

import (
	"fmt"
)

// DB2Helper is a helper to return queries for a specific RDBMS type
type DB2Helper struct {
	schemaName string
	tableName  string
}

// CreateSchemaSQL returns an RDBMS specific query to create a schema
func (helper DB2Helper) CreateSchemaSQL() string {
	sql := fmt.Sprintf(`
CREATE SCHEMA %v;
`, helper.schemaName)

	logger.Debug().Msg(sql)
	return sql
}

// CreateTableSQL returns a CREATE TABLE query for DB2
func (helper DB2Helper) CreateTableSQL() string {
	sql := fmt.Sprintf(`
CREATE TABLE %s.%s (
    acct_id     INTEGER NOT NULL,
    txn_ts      TIMESTAMP NOT NULL,
    amount      DECIMAL(12,2) NOT NULL,
    descr      CHAR(100) NOT NULL
)
ORGANIZE BY ROW;`, helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}

// CreateIndexSQL returns a CREATE INDEX query for DB2
func (helper DB2Helper) CreateIndexSQL() string {
	sql := fmt.Sprintf(`
CREATE INDEX index_account_transaction_acct
    ON %v.%v (acct_id, txn_ts);`, helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}

func (helper DB2Helper) CreateInserSQLPrefix() string {
	sql := fmt.Sprintf("INSERT INTO %s.%s (acct_id, txn_ts, amount, filler) VALUES (?, ?, ?, ?)", helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}
