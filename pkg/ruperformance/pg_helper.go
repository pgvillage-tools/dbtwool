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

// CreateTableSQL returns a CREATE Index query for PostgreSQL
func (helper PGHelper) CreateIndexSQL() string {
	sql := fmt.Sprintf(`
CREATE INDEX index_account_transaction_acct_%v
    ON %v.%v (acct_id, txn_ts);`, helper.tableName, helper.schemaName, helper.tableName)
	logger.Debug().Msg(sql)
	return sql
}

func (helper PGHelper) CreateOlapSQL() string {
	return fmt.Sprintf(`
SELECT COUNT(*) AS cnt, SUM(amount) AS total_amt
FROM   %s.%s
WHERE  acct_id BETWEEN 1 AND 50
  AND  txn_ts >= (CURRENT_TIMESTAMP - INTERVAL '30 minutes')
`, helper.schemaName, helper.tableName)
}

func (helper PGHelper) CreateOltpSQL(id int64) string {
	return fmt.Sprintf(`
UPDATE %s.%s
   SET amount = amount + 1.00
 WHERE acct_id = %d
   AND txn_ts >= (CURRENT_TIMESTAMP - INTERVAL '30 minutes')
`, helper.schemaName, helper.tableName, id)
}
