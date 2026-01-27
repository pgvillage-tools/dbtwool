package lobperformance

import (
	"fmt"
	"strings"
)

type Db2Helper struct {
	schemaName string
	tableName  string
}

func (helper Db2Helper) CreateSchemaSql() string {
	sql := fmt.Sprintf(`
CREATE SCHEMA %v;
`, helper.schemaName)

	logger.Debug().Msg(sql)
	return sql
}

func (helper Db2Helper) CreateTableSql() string {
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

func (helper Db2Helper) CreateInsertLobRowBaseSql(lobType string) (string, error) {
	col := payloadColumnForLobTypeDb2(lobType)
	if col == "" {
		return "", fmt.Errorf("unsupported lobType %q", lobType)
	}
	sql := fmt.Sprintf(`
INSERT INTO %v.%v (tenant_id, doc_type, %v)
VALUES (?, ?, ?);`, helper.schemaName, helper.tableName, col)

	logger.Debug().Msg(sql)
	return sql, nil
}

func payloadColumnForLobTypeDb2(lobType string) string {
	switch strings.ToLower(lobType) {
	case "clob", "text":
		return "payload_text"
	case "blob", "bytea":
		return "payload_bin"
	default:
		return ""
	}
}
