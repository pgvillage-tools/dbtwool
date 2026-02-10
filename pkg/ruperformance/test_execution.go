package ruperformance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

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
	readIsolation dbinterface.IsolationLevel,
) error {
	if err := validateTimes(warmupTimeSec, executionTimeSec); err != nil {
		return err
	}

	logger := testLogger(schemaName, tableName, warmupTimeSec, executionTimeSec)

	pool, err := client.Pool(ctx)
	if err != nil {
		return fmt.Errorf("failed to init pool: %w", err)
	}

	dbHelper := getDBHelper(dbType, schemaName, tableName)
	olapSQL := dbHelper.CreateOlapSQL()

	totalCtx, cancelTotal := context.WithTimeout(
		ctx,
		time.Duration(warmupTimeSec+executionTimeSec)*time.Second,
	)
	defer cancelTotal()

	warmupCtx, cancelWarmup := context.WithTimeout(
		totalCtx,
		time.Duration(warmupTimeSec)*time.Second,
	)
	defer cancelWarmup()

	oltpConn, closeOLTP, err := connectConn(totalCtx, pool)
	if err != nil {
		return fmt.Errorf("failed to connect for oltp: %w", err)
	}
	defer closeOLTP()

	olapConn, closeOLAP, err := connectConn(totalCtx, pool)
	if err != nil {
		return fmt.Errorf("failed to connect for olap: %w", err)
	}
	defer closeOLAP()

	if err := olapConn.SetIsolationLevel(totalCtx, readIsolation); err != nil {
		return fmt.Errorf("failed to set isolation on olap conn: %w", err)
	}

	var metrics testMetrics

	g, gctx := errgroup.WithContext(totalCtx)
	g.Go(func() error { return runOLTPWorkerErr(gctx, dbHelper, oltpConn, &metrics) })
	g.Go(func() error { return runOLAPWorkerErr(gctx, olapConn, olapSQL, &metrics) })

	<-warmupCtx.Done()
	metrics.measuring.Store(1)

	<-totalCtx.Done()

	if err := g.Wait(); err != nil {
		return err
	}

	logResults(logger, &metrics, executionTimeSec)
	return nil
}

func runOLTPWorkerErr(ctx context.Context, dbHelper DBHelper, conn dbinterface.Connection, m *testMetrics) error {
	var step int64
	for {
		if ctx.Err() != nil {
			return nil
		}

		if err := conn.Begin(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("oltp begin failed: %w", err)
		}

		sql := dbHelper.CreateOltpSQL(step)
		step++

		if _, err := conn.Execute(ctx, sql); err != nil {
			_ = conn.Rollback(ctx)
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("oltp execute failed: %w", err)
		}

		if err := conn.Commit(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("oltp commit failed: %w", err)
		}

		m.oltpOps.Add(1)
	}
}

func runOLAPWorkerErr(ctx context.Context, conn dbinterface.Connection, olapSQL string, m *testMetrics) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		if _, err := conn.QueryOneRow(ctx, olapSQL); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("olap query failed: %w", err)
		}

		if m.measuring.Load() == 1 {
			m.olapCompleted.Add(1)
			m.markStart()
		}
	}
}

// getDBHelper makes ExecuteTest just a bit shorter
func getDBHelper(rdbms dbclient.RDBMS, schemaName, tableName string) DBHelper {
	if rdbms == dbclient.DB2 {
		return DB2Helper{schemaName: schemaName, tableName: tableName}
	}
	return PGHelper{schemaName: schemaName, tableName: tableName}
}

func connectConn(ctx context.Context, pool dbinterface.Pool) (dbinterface.Connection, func(), error) {
	conn, err := pool.Connect(ctx)
	if err != nil {
		return nil, func() {}, err
	}
	cleanup := func() { conn.Close(ctx) }
	return conn, cleanup, nil
}

func validateTimes(warmupTimeSec, executionTimeSec int) error {
	if warmupTimeSec <= 0 {
		return errors.New("warmupTimeSec must be > 0")
	}
	if executionTimeSec <= 0 {
		return errors.New("executionTimeSec must be > 0")
	}
	return nil
}

func testLogger(schemaName, tableName string, warmupTimeSec, executionTimeSec int) zerolog.Logger {
	return log.With().
		Str("schema", schemaName).
		Str("table", tableName).
		Int("warmup_s", warmupTimeSec).
		Int("execution_s", executionTimeSec).
		Logger()
}

func logResults(
	logger zerolog.Logger,
	metrics *testMetrics,
	executionTimeSec int,
) {
	start := metrics.startTimeOrFallback(executionTimeSec)
	elapsed := time.Since(start)
	if elapsed <= 0 {
		elapsed = time.Duration(executionTimeSec) * time.Second
	}

	olap := metrics.olapCompleted.Load()
	oltp := metrics.oltpOps.Load()

	logger.Info().
		Int64("oltp_ops", oltp).
		Int64("olap_completed", olap).
		Float64("olap_per_sec", float64(olap)/elapsed.Seconds()).
		Msg("Isolation read performance test finished")
}
