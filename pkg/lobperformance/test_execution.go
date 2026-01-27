package lobperformance

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

func LobPerformanceExecuteTest(
	dbType dbclient.Rdbms,
	ctx context.Context,
	client dbinterface.Client,
	schemaName string,
	tableName string,
	seed string,
	parallel int,
	warmupTime int,
	executionTime int,
	readMode string,
	lobType string,
) error {
	//This dbhelper logic should be moved to things outside this package. Where client specific logic lives
	var dbHelper DbHelper = nil

	if dbType == dbclient.RdbmsDB2 {
		dbHelper = Db2Helper{schemaName: schemaName, tableName: tableName}
	} else {
		dbHelper = PgHelper{schemaName: schemaName, tableName: tableName}
	}

	logger := log.With().
		Str("schema", schemaName).
		Str("table", tableName).
		Str("read_mode", readMode).
		Str("lob_type", lobType).
		Logger()
	if parallel <= 0 {
		return fmt.Errorf("parallel must be > 0")
	}
	if warmupTime <= 0 {
		warmupTime = 10
	}
	if executionTime <= 0 {
		executionTime = 10
	}

	seedInt, err := strconv.ParseInt(seed, 10, 64)
	if err != nil {
		return fmt.Errorf("seed must be an integer (got %q): %w", seed, err)
	}

	col := dbHelper.PayloadColumnForLobType(lobType)
	if col == "" {
		return fmt.Errorf("failed to determine column to select from based on lobType: %s", lobType)
	}

	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		return fmt.Errorf("failed to init pool: %w", poolErr)
	}

	metaConn, err := pool.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect for metadata query: %w", err)
	}
	defer metaConn.Close(ctx)

	boundsSQL := dbHelper.SelectMinMaxIdSql()

	bounds, err := metaConn.QueryOneRow(ctx, boundsSQL)
	if err != nil {
		return fmt.Errorf("failed to query min/max(id): %w", err)
	}

	minID, err := getIntFromAnyNumberOutput(bounds["min_id"])
	if err != nil {
		return fmt.Errorf("failed to parse min_id: %w", err)
	}

	maxID, err := getIntFromAnyNumberOutput(bounds["max_id"])
	if err != nil {
		return fmt.Errorf("failed to parse max_id: %w", err)
	}

	if minID <= 0 || maxID <= 0 || maxID < minID {
		return fmt.Errorf("no rows to test: min(id)=%d max(id)=%d", minID, maxID)
	}

	readSQL, err := dbHelper.SelectReadLobByIdSql(lobType)
	if err != nil {
		return err
	}

	logger.Info().Msgf("Starting read test with parallel=%d (max_id=%d)", parallel, maxID)

	// Warmup ctx: warmupTime
	warmupCtx, cancelWarmup := context.WithTimeout(ctx, time.Duration(warmupTime)*time.Second)
	defer cancelWarmup()

	// Total ctx: warmup + execution
	totalCtx, cancelTotal := context.WithTimeout(ctx, time.Duration(warmupTime+executionTime)*time.Second)
	defer cancelTotal()

	// RNG shared across workers (as you described).
	rg, err := NewRandGenerator(int(minID), int(maxID), RandMode(readMode), seedInt+int64(parallel)) // +p to vary per p but remain reproducible
	if err != nil {
		return err
	}
	safeRng := NewSafeRandGenerator(rg)

	var measuring atomic.Int32 // 0 warmup, 1 measure
	var readCount atomic.Int64

	var startTime time.Time
	var startOnce sync.Once

	// Worker function: each worker uses its own DB connection
	worker := func(workerID int) error {
		conn, err := pool.Connect(totalCtx)
		if err != nil {
			return fmt.Errorf("worker %d connect failed: %w", workerID, err)
		}
		defer conn.Close(totalCtx)

		for {
			select {
			case <-totalCtx.Done():
				return nil
			default:
			}

			id := safeRng.NextRand()
			row, qErr := conn.QueryOneRow(totalCtx, readSQL, int64(id))
			if qErr != nil {
				// If we're stopping due to context timeout/cancel, that's normal.
				if totalCtx.Err() != nil {
					return nil
				}
				return fmt.Errorf("worker %d read failed (id=%d): %w", workerID, id, qErr)
			}

			v, ok := row[col]
			if !ok {
				return fmt.Errorf("worker %d: column %q not found in result", workerID, col)
			}

			// Touch the bytes so the LOB is actually materialized
			switch t := v.(type) {
			case []byte:
				if len(t) > 0 {
					_ = t[0]
				}
			case string:
				if len(t) > 0 {
					_ = t[0]
				}
			case nil:
			default:
				_ = fmt.Sprintf("%v", t)
			}

			// Count only after warmup
			if measuring.Load() == 1 {
				readCount.Add(1)
				startOnce.Do(func() { startTime = time.Now() })
			}
		}
	}

	// Launch workers
	errCh := make(chan error, parallel)
	for i := 0; i < parallel; i++ {
		workerID := i
		go func() {
			errCh <- worker(workerID)
		}()
	}

	// Wait for warmup to end, then start measuring
	<-warmupCtx.Done()
	measuring.Store(1)

	// Wait for total (warmup+execution) to end
	<-totalCtx.Done()

	// Collect worker errors
	var firstErr error
	for i := 0; i < parallel; i++ {
		if wErr := <-errCh; wErr != nil && firstErr == nil {
			firstErr = wErr
		}
	}
	if firstErr != nil {
		return firstErr
	}

	reads := readCount.Load()

	if startTime.IsZero() {
		startTime = time.Now().Add(-time.Duration(executionTime) * time.Second)
	}

	elapsed := time.Since(startTime)
	if elapsed <= 0 {
		elapsed = time.Duration(executionTime) * time.Second
	}

	readsPerSec := float64(reads) / elapsed.Seconds()

	logger.Info().
		Int("parallel", parallel).
		Int64("reads", reads).
		Float64("reads_per_sec", readsPerSec).
		Str("column", col).
		Msg("Read test finished")

	return nil
}

func getIntFromAnyNumberOutput(number any) (int64, error) {
	var foundNumber int64 = 0

	switch v := number.(type) {
	case int64:
		foundNumber = v
	case int32:
		foundNumber = int64(v)
	case int:
		foundNumber = int64(v)
	case []byte:
		parsed, pErr := strconv.ParseInt(string(v), 10, 64)
		if pErr != nil {
			return 0, fmt.Errorf("failed to parse max_id: %w", pErr)
		}
		foundNumber = parsed
	case string:
		parsed, pErr := strconv.ParseInt(v, 10, 64)
		if pErr != nil {
			return 0, fmt.Errorf("failed to parse max_id: %w", pErr)
		}
		foundNumber = parsed
	default:
		return 0, fmt.Errorf("unexpected max_id type %T (%v)", number, number)
	}

	return foundNumber, nil
}
