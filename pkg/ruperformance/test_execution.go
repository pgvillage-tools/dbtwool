package ruperformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// ExecuteTest runs a mixed OLTP (updates) + OLAP (aggregate reads) workload.
// It reports how many OLAP queries completed during the measurement interval.
func ExecuteTest(
	ctx context.Context,
	dbType dbclient.RDBMS,
	client dbinterface.Client,
	schemaName string,
	tableName string,
	warmupTimeSec int,
	executionTimeSec int,
	readIsolation dbinterface.IsolationLevel, // e.g. UR or CS for DB2
) error {
	logger := log.With().
		Str("schema", schemaName).
		Str("table", tableName).
		Int("warmup_s", warmupTimeSec).
		Int("execution_s", executionTimeSec).
		Logger()

	if warmupTimeSec <= 0 {
		return errors.New("warmupTimeSec must be > 0")
	}
	if executionTimeSec <= 0 {
		return errors.New("executionTimeSec must be > 0")
	}

	pool, err := client.Pool(ctx)
	if err != nil {
		return fmt.Errorf("failed to init pool: %w", err)
	}

	var dbHelper DBHelper

	if dbType == dbclient.DB2 {
		dbHelper = DB2Helper{schemaName: schemaName, tableName: tableName}
	} else {
		dbHelper = PGHelper{schemaName: schemaName, tableName: tableName}
	}

	olapSql := dbHelper.CreateOlapSQL()

	// Total test context: warmup + measurement
	totalDur := time.Duration(warmupTimeSec+executionTimeSec) * time.Second
	totalCtx, cancelTotal := context.WithTimeout(ctx, totalDur)
	defer cancelTotal()

	// Measurement starts after warmup
	warmupCtx, cancelWarmup := context.WithTimeout(ctx, time.Duration(warmupTimeSec)*time.Second)
	defer cancelWarmup()

	// Two separate connections: one for OLTP, one for OLAP.
	oltpConn, err := pool.Connect(totalCtx)
	if err != nil {
		return fmt.Errorf("failed to connect for oltp: %w", err)
	}
	defer oltpConn.Close(totalCtx)

	olapConn, err := pool.Connect(totalCtx)
	if err != nil {
		return fmt.Errorf("failed to connect for olap: %w", err)
	}
	defer olapConn.Close(totalCtx)

	// Set read isolation level on OLAP connection.
	if err := olapConn.SetIsolationLevel(totalCtx, readIsolation); err != nil {
		return fmt.Errorf("failed to set isolation on olap conn: %w", err)
	}

	var measuring atomic.Int32
	var olapCompleted atomic.Int64
	var oltpOps atomic.Int64

	var startTime time.Time
	var startOnce sync.Once

	errCh := make(chan error, 2)

	// OLTP worker: continuously update & commit.
	go func() {
		var step int64
		for {
			if totalCtx.Err() != nil {
				errCh <- nil
				return
			}

			// each statement in its own transaction (keeps log growth controlled and produces churn)
			if err := oltpConn.Begin(totalCtx); err != nil {
				if totalCtx.Err() != nil {
					errCh <- nil
					return
				}
				errCh <- fmt.Errorf("oltp begin failed: %w", err)
				return
			}

			sql := dbHelper.CreateOltpSQL(step)
			step++

			if _, err := oltpConn.Execute(totalCtx, sql); err != nil {
				_ = oltpConn.Rollback(totalCtx)
				if totalCtx.Err() != nil {
					errCh <- nil
					return
				}
				errCh <- fmt.Errorf("oltp execute failed: %w", err)
				return
			}

			if err := oltpConn.Commit(totalCtx); err != nil {
				if totalCtx.Err() != nil {
					errCh <- nil
					return
				}
				errCh <- fmt.Errorf("oltp commit failed: %w", err)
				return
			}

			oltpOps.Add(1)
		}
	}()

	// OLAP worker: repeatedly run aggregation query; count completions only during measurement.
	go func() {
		for {
			if totalCtx.Err() != nil {
				errCh <- nil
				return
			}

			_, err := olapConn.QueryOneRow(totalCtx, olapSql)
			if err != nil {
				if totalCtx.Err() != nil {
					errCh <- nil
					return
				}
				errCh <- fmt.Errorf("olap query failed: %w", err)
				return
			}

			if measuring.Load() == 1 {
				olapCompleted.Add(1)
				startOnce.Do(func() { startTime = time.Now() })
			}
		}
	}()

	// Wait warmup, then begin measurement.
	<-warmupCtx.Done()
	measuring.Store(1)

	// Wait until total duration ends.
	<-totalCtx.Done()

	// Collect worker results.
	var firstErr error
	for i := 0; i < 2; i++ {
		if wErr := <-errCh; wErr != nil && firstErr == nil {
			firstErr = wErr
		}
	}
	if firstErr != nil {
		return firstErr
	}

	// Reporting
	if startTime.IsZero() {
		startTime = time.Now().Add(-time.Duration(executionTimeSec) * time.Second)
	}
	elapsed := time.Since(startTime)
	if elapsed <= 0 {
		elapsed = time.Duration(executionTimeSec) * time.Second
	}

	olap := olapCompleted.Load()
	oltp := oltpOps.Load()

	logger.Info().
		Int64("oltp_ops", oltp).
		Int64("olap_completed", olap).
		Float64("olap_per_sec", float64(olap)/elapsed.Seconds()).
		Msg("Isolation read performance test finished")

	return nil
}
