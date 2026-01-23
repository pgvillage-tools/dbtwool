package dbclient

import (
	"context"
	"database/sql"
	"fmt"
)

// Connection is a wrapper over sql.Conn, so that we can add methods
type Connection struct {
	ctx  context.Context
	conn *sql.Conn
	tx   *sql.Tx
}

// Close closes the connection
func (c *Connection) Close() error {
	return c.conn.Close()
}

// Execute will execute a query and return number of affected rows
func (c *Connection) Execute(query string) (int64, error) {
	r, err := c.conn.ExecContext(c.ctx, query)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// Query will execute a query and return a list of maps where every list item is a row and every map item is a column
func (c *Connection) Query(query string, args ...any) ([]map[string]any, error) {
	rows, err := c.conn.QueryContext(c.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rowsToMaps(rows)
}

// QueryOneRow executes a query and expects one row, or fails. On success it returns the row.
func (c *Connection) QueryOneRow(query string, args ...any) (map[string]any, error) {
	rows, queryErr := c.Query(query, args...)
	if queryErr != nil {
		logger.Fatal().Msgf("error while executing olap query %v", queryErr)
	}
	if len(rows) != 1 {
		logger.Fatal().Msgf("expected 1 row on olap query: %v", queryErr)
	}
	return rows[0], nil
}

// Begin starts a transaction. In this case there is a one-on-one relation between the transaction and the connection
func (c *Connection) Begin() error {
	tx, err := c.conn.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	c.tx = tx
	return nil
}

// Commit will commit the connection
func (c *Connection) Commit() error {
	if c.tx != nil {
		if err := c.tx.Commit(); err != nil {
			return err
		}
		c.tx = nil
	}
	return nil
}

// Rollback will rollback the transaction
func (c *Connection) Rollback() error {
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
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = val
			}
		}
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	return results, nil
}
