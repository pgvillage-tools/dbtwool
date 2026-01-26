package pg

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// Connection is a wrapper over sql.Conn, so that we can add methods
type Connection struct {
	conn *pgx.Conn
	tx   *pgx.Tx
}

// Close closes the connection
func (c *Connection) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
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
	r, err := c.conn.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected(), nil
}

// Query will execute a query and return a list of maps where every list item is a row and every map item is a column
func (c *Connection) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rowsToMaps(rows)
}

// QueryOneRow executes a query and expects one row, or fails. On success it returns the row.
func (c *Connection) QueryOneRow(ctx context.Context, query string, args ...any) (map[string]any, error) {
	rows, queryErr := c.Query(ctx, query, args...)
	if queryErr != nil {
		logger.Fatal().Msgf("error while executing olap query %v", queryErr)
	}
	if len(rows) != 1 {
		logger.Fatal().Msgf("expected 1 row on olap query: %v", queryErr)
	}
	return rows[0], nil
}

// Begin starts a transaction. In this case there is a one-on-one relation between the transaction and the connection
func (c *Connection) Begin(ctx context.Context) error {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return err
	}
	c.tx = &tx
	return nil
}

// Commit will commit the connection
func (c *Connection) Commit(ctx context.Context) error {
	if c.tx != nil {
		tx := *c.tx
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		c.tx = nil
	}
	return nil
}

// Rollback will rollback the transaction
func (c *Connection) Rollback(ctx context.Context) error {
	if c.tx != nil {
		tx := *c.tx
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		c.tx = nil
	}
	return nil
}

func rowsToMaps(rows pgx.Rows) ([]map[string]any, error) {
	defer rows.Close()

	var result []map[string]any

	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columnNames[i] = fd.Name
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		rowMap := map[string]any{}
		for i, colName := range columnNames {
			rowMap[colName] = values[i]
		}

		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
