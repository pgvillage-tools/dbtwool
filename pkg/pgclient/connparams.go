package pgclient

import (
	"fmt"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
)

// ConnParams objects define connection parameters for a DB2 connection
type ConnParams struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
	SslMode  string
}

func (cp ConnParams) GetConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		cp.Host,
		cp.Port,
		cp.Database,
		cp.User,
		cp.Password,
		cp.SslMode,
	)
}

// ConnParamsFromEnv generates a new default ConnParams from env variables with defaults
func ConnParamsFromEnv() ConnParams {
	return ConnParams{
		Host:     utils.GetEnv("PGHOST", "localhost"),
		Port:     utils.GetEnv("PGPORT", "5432"),
		Database: utils.GetEnv("PGDATABASE", "postgres"),
		User:     utils.GetEnv("PGUSER", "postgres"),
		Password: utils.GetEnv("PGPASSWORD", "postgres"),
		SslMode:  "disable",
	}
}
