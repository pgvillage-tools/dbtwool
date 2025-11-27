package dbclient

import (
	"fmt"
	"os"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
)

// ConnParams objects define connection parameters for a DB2 connection
type Db2ConnParams map[string]string

func (cp Db2ConnParams) GetConnString() string {
	var l []string
	for key, value := range cp {
		l = append(l, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(l, ";")
}

// ConnParamsFromEnv generates a new default ConnParams from env variables with defaults
func NewDb2ConnparamsFromEnv() ConnParams {
	return Db2ConnParams{
		"HOSTNAME": utils.GetEnv("DB2_HOST", "db2"),
		"PORT":     utils.GetEnv("DB2_PORT", "50000"),
		"DATABASE": utils.GetEnv("DB2_DATABASE", "sample"),
		"UID":      utils.GetEnv("DB2_USER", "db2inst1"),
		"PWD":      os.Getenv("DB2_PASSWORD"),
		"PROTOCOL": utils.GetEnv("DB2_PROTOCOL", "TCPIP"),
	}
}
