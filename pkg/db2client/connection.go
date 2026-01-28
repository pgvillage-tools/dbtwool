package db2client

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// Connection is a wrapper over sql.Conn, so that we can add methods
type Connection struct {
	conn *sql.Conn
	tx   *sql.Tx
}

// Close closes the connection
func (c *Connection) Close(_ context.Context) error {
	return c.conn.Close()
}

// SetIsolationLevel can be used to change the isolation level on a connection
func (c *Connection) SetIsolationLevel(ctx context.Context, isoLevel dbinterface.IsolationLevel) error {
	qryIsoLevel := isoLevel.AsQuery()
	logger.Info().Msgf("CONN2: %s", qryIsoLevel)
	_, err := c.Execute(ctx, qryIsoLevel)
	return err
}

// Execute will execute a query and return number of affected rows
func (c *Connection) Execute(ctx context.Context, query string) (int64, error) {
	r, err := c.conn.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// Query will execute a query and return a list of maps where every list item is a row and every map item is a column
func (c *Connection) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := c.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rowsToMaps(rows)
}

// QueryOneRow executes a query and expects one row, or fails. On success it returns the row.
func (c *Connection) QueryOneRow(ctx context.Context, query string, args ...any) (map[string]any, error) {
	rows, queryErr := c.Query(ctx, query, args...)
	if queryErr != nil {
		return nil, fmt.Errorf("error while executing olap query %v", queryErr)
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("expected 1 row on olap query: %v", queryErr)
	}
	return rows[0], nil
}

// Begin starts a transaction. In this case there is a one-on-one relation between the transaction and the connection
func (c *Connection) Begin(ctx context.Context) error {
	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	c.tx = tx
	return nil
}

// Commit will commit the connection
func (c *Connection) Commit(_ context.Context) error {
	if c.tx != nil {
		if err := c.tx.Commit(); err != nil {
			return err
		}
		c.tx = nil
	}
	return nil
}

// Rollback will rollback the transaction
func (c *Connection) Rollback(_ context.Context) error {
	if c.tx != nil {
		if err := c.tx.Rollback(); err != nil {
			return err
		}
		c.tx = nil
	}
	return nil
}

func rowsToMaps(rows *sql.Rows) ([]map[string]any, error) {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]any

	for rows.Next() {
		values := make([]any, len(cols))
		scanArgs := make([]any, len(cols))

		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		rowMap := make(map[string]any, len(cols))
		for i, colName := range cols {
			key := strings.ToLower(colName) // Makes uppercase column names not break everything
			val := values[i]

			if b, ok := val.([]byte); ok {
				rowMap[key] = string(b)
			} else {
				rowMap[key] = val
			}
		}
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	return results, nil
}
