package lobperformance

import (
	"fmt"
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

func (helper Db2Helper) InsertOneRowSql(lobType string) (string, error) {
	return "", nil
}
