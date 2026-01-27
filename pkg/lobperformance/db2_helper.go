// Package lobperformance handles all work regarding LOB performance tests
package lobperformance

import (
	"fmt"
	"strings"
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
CREATE TABLE %v.%v (
  ID            BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  TENANT_ID     INTEGER NOT NULL,
  CREATED_AT    TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
  UPDATED_AT    TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
  DOC_TYPE      VARCHAR(64) NOT NULL,
  PAYLOAD_BIN   BLOB(50M),
  PAYLOAD_TEXT  CLOB(50M),
  CONSTRAINT PK_LOB_PERF PRIMARY KEY (ID)
);`, helper.schemaName, helper.tableName)

	logger.Debug().Msg(sql)
	return sql
}

// CreateInsertLOBRowBaseSQL returns an INSERT LOB query
func (helper DB2Helper) CreateInsertLOBRowBaseSQL(lobType string) (string, error) {
	col := helper.PayloadColumnForLOBType(lobType)
	if col == "" {
		return "", fmt.Errorf("unsupported lobType %q", lobType)
	}
	sql := fmt.Sprintf(`
INSERT INTO %v.%v (tenant_id, doc_type, %v)
VALUES (?, ?, ?);`, helper.schemaName, helper.tableName, col)

	logger.Debug().Msg(sql)
	return sql, nil
}

// SelectMinMaxIDSQL returns a query for the min and max ID's in a table
func (helper DB2Helper) SelectMinMaxIDSQL() string {
	sql := fmt.Sprintf(`
SELECT
  MIN(id) AS min_id,
  MAX(id) AS max_id
FROM %v.%v;
`, helper.schemaName, helper.tableName)

	logger.Debug().Msg(sql)
	return sql
}

// SelectReadLOBByIDSQL returns the query to return a LOB
func (helper DB2Helper) SelectReadLOBByIDSQL(lobType string) (string, error) {
	col := helper.PayloadColumnForLOBType(lobType)
	if col == "" {
		return "", fmt.Errorf("unsupported lobType %q", lobType)
	}

	// DB2 uses '?' parameter markers
	sql := fmt.Sprintf(`
SELECT %v
FROM %v.%v
WHERE id = ?;
`, col, helper.schemaName, helper.tableName)

	logger.Debug().Msg(sql)
	return sql, nil
}

// PayloadColumnForLOBType returns the payload type for a specific RDBMS
func (helper DB2Helper) PayloadColumnForLOBType(lobType string) string {
	switch strings.ToLower(lobType) {
	case "clob", "text":
		return "payload_text"
	case "blob", "bytea":
		return "payload_bin"
	default:
		return ""
	}
}
