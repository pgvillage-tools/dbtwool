package lobperformance

import (
	"fmt"
	"strings"
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

// CreateTableSQL returns a table query to be used for CLOB data
func (helper PGHelper) CreateTableSQL() string {
	sql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %v.%v (
  id            bigserial PRIMARY KEY,
  tenant_id     integer NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  doc_type      text NOT NULL,
  payload_bin   bytea,
  payload_text  text
);`, helper.schemaName, helper.tableName)

	logger.Debug().Msg(sql)
	return sql
}

// CreateInsertLOBRowBaseSQL returns a query for inserting LOB data
func (helper PGHelper) CreateInsertLOBRowBaseSQL(lobType string) (string, error) {
	col := helper.PayloadColumnForLOBType(lobType)
	if col == "" {
		return "", fmt.Errorf("unsupported lobType %q", lobType)
	}
	sql := fmt.Sprintf(`
INSERT INTO %v.%v (tenant_id, doc_type, %v)
VALUES ($1, $2, $3);`, helper.schemaName, helper.tableName, col)

	logger.Debug().Msg(sql)
	return sql, nil
}

// SelectMinMaxIDSQL returns a query to fetch the min and max id of a table
func (helper PGHelper) SelectMinMaxIDSQL() string {
	sql := fmt.Sprintf(`
SELECT
  MIN(id) AS min_id,
  MAX(id) AS max_id
FROM %v.%v;
`, helper.schemaName, helper.tableName)

	logger.Debug().Msg(sql)
	return sql
}

// SelectReadLOBByIDSQL returns a query to fetch a LOB
func (helper PGHelper) SelectReadLOBByIDSQL(lobType string) (string, error) {
	col := helper.PayloadColumnForLOBType(lobType)
	if col == "" {
		return "", fmt.Errorf("unsupported lobType %q", lobType)
	}

	sql := fmt.Sprintf(`
SELECT %v
FROM %v.%v
WHERE id = $1;
`, col, helper.schemaName, helper.tableName)

	logger.Debug().Msg(sql)
	return sql, nil
}

// PayloadColumnForLOBType returns the payload type
func (helper PGHelper) PayloadColumnForLOBType(lobType string) string {
	switch strings.ToLower(lobType) {
	case "clob", "text":
		return "payload_text"
	case "blob", "bytea":
		return "payload_bin"
	default:
		return ""
	}
}
