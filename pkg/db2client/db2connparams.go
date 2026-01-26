package db2client

import (
	"fmt"
	"os"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
)

// DB2ConnParams objects define connection parameters for a DB2 connection
type DB2ConnParams map[string]string

// GetConnString builds and returns a string that can be used to connect to DB2
func (cp DB2ConnParams) GetConnString() string {
	var l []string
	for key, value := range cp {
		l = append(l, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(l, ";")
}

// NewDB2ConnparamsFromEnv generates a new default ConnParams from env variables with defaults
func NewDB2ConnparamsFromEnv() ConnParams {
	return DB2ConnParams{
		"HOSTNAME": utils.GetEnv("DB2_HOST", "db2"),
		"PORT":     utils.GetEnv("DB2_PORT", "50000"),
		"DATABASE": utils.GetEnv("DB2_DATABASE", "sample"),
		"UID":      utils.GetEnv("DB2_USER", "db2inst1"),
		"PWD":      os.Getenv("DB2_PASSWORD"),
		"PROTOCOL": utils.GetEnv("DB2_PROTOCOL", "TCPIP"),
	}
}
