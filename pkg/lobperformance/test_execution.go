package lobperformance

import (
	"context"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

func lobPerformanceExecuteTest(dbType dbclient.Rdbms, ctx context.Context, client dbinterface.Client, schemaName string, tableName string, seed string, parallel int, warmuptime int, executiontime int) {
	// work in progress
}
