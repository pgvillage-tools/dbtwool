package db2client

import (
	"fmt"
	"os"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
)

// ConnParams objects define connection parameters for a DB2 connection
type ConnParams map[string]string

func (cp ConnParams) String() string {
	var l []string
	for key, value := range cp {
		l = append(l, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(l, ";")
}

// ConnParamsFromEnv generates a new default ConnParams from env variables with defaults
func ConnParamsFromEnv() ConnParams {
	return ConnParams{
		"host":   utils.GetEnv("DB2_HOST", "db2"),
		"port":   utils.GetEnv("DB2_PORT", "50000"),
		"dbname": utils.GetEnv("DB2_DATABASE", "sample"),
		"uid":    utils.GetEnv("DB2_USER", "db2inst1"),
		"pwd":    os.Getenv("DB2_PASSWORD"),
	}
}
