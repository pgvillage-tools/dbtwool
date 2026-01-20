package dbclient

import (
	"fmt"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
)

// PgConnParams objects define connection parameters for a DB2 connection
type PgConnParams struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
	SslMode  string
}

// GetConnString builds and returns a string that can be used to connect to PostgreSQL
func (cp PgConnParams) GetConnString() string {
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

// NewPgConnParamsFromEnv generates a new default ConnParams from env variables with defaults
func NewPgConnParamsFromEnv() ConnParams {
	return PgConnParams{
		Host:     utils.GetEnv("PGHOST", "localhost"),
		Port:     utils.GetEnv("PGPORT", "5432"),
		Database: utils.GetEnv("PGDATABASE", "postgres"),
		User:     utils.GetEnv("PGUSER", "postgres"),
		Password: utils.GetEnv("PGPASSWORD", "postgres"),
		SslMode:  "disable",
	}
}
