package lobperformance

import (
	"fmt"
	"strings"
)

type PgHelper struct {
	schemaName string
	tableName  string
}

func (helper PgHelper) CreateSchemaSql() string {
	sql := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %v;
`, helper.schemaName)

	logger.Debug().Msg(sql)
	return sql
}

func (helper PgHelper) CreateTableSql() string {
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

func (helper PgHelper) InsertOneRowSql(lobType string) (string, error) {
	col := payloadColumnForLobType(lobType)
	if col == "" {
		return "", fmt.Errorf("unsupported lobType %q", lobType)
	}

	// payload is parameter $3
	sql := fmt.Sprintf(`
INSERT INTO %v.%v (tenant_id, doc_type, %v)
VALUES ($1, $2, $3);`, helper.schemaName, helper.tableName, col)

	logger.Debug().Msg(sql)
	return sql, nil
}

func payloadColumnForLobType(lobType string) string {
	switch strings.ToLower(lobType) {
	case "clob", "text":
		return "payload_text"
	case "blob", "bytea":
		return "payload_bin"
	default:
		return ""
	}
}
