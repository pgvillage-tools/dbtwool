package lobperformance

import "fmt"

type Db2Helper struct {
	schemaName string
	tableName  string
}

func (helper Db2Helper) CreateSchemaSql() string {
	return fmt.Sprintf(`
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
    EXECUTE IMMEDIATE 'CREATE SCHEMA %s';
END
`, helper.schemaName)
}

func (helper Db2Helper) CreateTableSql() string {
	return fmt.Sprintf("CREATE TABLE %v.%v columns (, , , ,);", helper.schemaName, helper.tableName)
}
