package ruperformance

import (
	"fmt"
)

// PGHelper is a helper for generating queries for PostgreSQL
type PGHelper struct {
	schemaName string
	tableName  string
}

// CreateSchemaSQL returns a schema query
func (helper PGHelper) CreateSchemaSQL() string {
	sql := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %v;
`, helper.schemaName)

	logger.Debug().Msg(sql)
	return sql
}

// CreateTableSQL returns a CREATE TABLE query for PostgreSQL
func (helper PGHelper) CreateTableSQL() string {
	sql := fmt.Sprintf(`
CREATE TABLE %v.%v (
    acct_id   INTEGER NOT NULL,
    txn_ts    TIMESTAMPTZ NOT NULL,
    amount    NUMERIC(12,2) NOT NULL,
    descr     VARCHAR(100) NOT NULL
);`, helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}

// CreateIndexSQL returns a CREATE INDEX query for PostgreSQL
func (helper PGHelper) CreateIndexSQL() string {
	sql := fmt.Sprintf(`
CREATE INDEX index_account_transaction_acct
    ON %v.%v (acct_id, txn_ts);`, helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}

func (helper PGHelper) CreateInserSQLPrefix() string {
	sql := fmt.Sprintf("INSERT INTO %s.%s (acct_id, txn_ts, amount, filler) VALUES ($1, $2, $3, $4)", helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}
