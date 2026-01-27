package lobperformance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// ExecuteTest executes the performance test
func ExecuteTest(
	ctx context.Context,
	dbType dbclient.RDBMS,
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

	dbHelper := newDBHelper(dbType, schemaName, tableName)

	logger := log.With().
		Str("schema", schemaName).
		Str("table", tableName).
		Str("read_mode", readMode).
		Str("lob_type", lobType).
		Logger()

	parallel, warmupTime, executionTime, err := normalizeArgs(parallel, warmupTime, executionTime)
	if err != nil {
		return err
	}

	seedInt, err := parseSeed(seed)
	if err != nil {
		return err
	}

	col := dbHelper.PayloadColumnForLOBType(lobType)
	if col == "" {
		return fmt.Errorf("failed to determine column to select from based on lobType: %s", lobType)
	}

	pool, err := client.Pool(ctx)
	if err != nil {
		return fmt.Errorf("failed to init pool: %w", err)
	}

	metaConn, err := pool.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect for metadata query: %w", err)
	}
	defer metaConn.Close(ctx)

	minID, maxID, err := fetchMinMaxIDs(ctx, metaConn, dbHelper)
	if err != nil {
		return err
	}

	readSQL, err := dbHelper.SelectReadLOBByIDSQL(lobType)
	if err != nil {
		return err
	}

	logger.Info().Msgf("Starting read test with parallel=%d (max_id=%d)", parallel, maxID)

	startTime, reads, err := runReaders(
		ctx,
		pool,
		readSQL,
		col,
		int(minID),
		int(maxID),
		readMode,
		seedInt+int64(parallel),
		parallel,
		warmupTime,
		executionTime,
	)
	if err != nil {
		return err
	}

	readsPerSec := computeReadsPerSec(startTime, reads, executionTime)

	logger.Info().
		Int("parallel", parallel).
		Int64("reads", reads).
		Float64("reads_per_sec", readsPerSec).
		Str("column", col).
		Msg("Read test finished")

	return nil
}

func newDBHelper(dbType dbclient.RDBMS, schema, table string) DBHelper {
	if dbType == dbclient.DB2 {
		return DB2Helper{schemaName: schema, tableName: table}
	}
	return PGHelper{schemaName: schema, tableName: table}
}

func normalizeArgs(parallel, warmupTime, executionTime int) (int, int, int, error) {
	if parallel <= 0 {
		return 0, 0, 0, errors.New("parallel must be > 0")
	}
	if warmupTime <= 0 {
		warmupTime = defaultWarmupTime
	}
	if executionTime <= 0 {
		executionTime = defaultExecutionTime
	}
	return parallel, warmupTime, executionTime, nil
}

func parseSeed(seed string) (int64, error) {
	seedInt, err := strconv.ParseInt(seed, decimalSystem, bitSize64)
	if err != nil {
		return 0, fmt.Errorf("seed must be an integer (got %q): %w", seed, err)
	}
	return seedInt, nil
}

func fetchMinMaxIDs(ctx context.Context, conn dbinterface.Connection, helper DBHelper) (int64, int64, error) {
	bounds, err := conn.QueryOneRow(ctx, helper.SelectMinMaxIDSQL())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query min/max(id): %w", err)
	}

	minID, err := getIntFromAnyNumberOutput(bounds["min_id"])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse min_id: %w", err)
	}
	maxID, err := getIntFromAnyNumberOutput(bounds["max_id"])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse max_id: %w", err)
	}

	if minID <= 0 || maxID <= 0 || maxID < minID {
		return 0, 0, fmt.Errorf("no rows to test: min(id)=%d max(id)=%d", minID, maxID)
	}

	return minID, maxID, nil
}

func runReaders(
	parent context.Context,
	pool dbinterface.Pool,
	readSQL string,
	col string,
	minID int,
	maxID int,
	readMode string,
	rngSeed int64,
	parallel int,
	warmupTime int,
	executionTime int,
) (time.Time, int64, error) {

	// Warmup ctx
	warmupCtx, cancelWarmup := context.WithTimeout(parent, time.Duration(warmupTime)*time.Second)
	defer cancelWarmup()

	// Total ctx
	totalCtx, cancelTotal := context.WithTimeout(parent, time.Duration(warmupTime+executionTime)*time.Second)
	defer cancelTotal()

	rg, err := NewRandGenerator(minID, maxID, RandMode(readMode), rngSeed)
	if err != nil {
		return time.Time{}, 0, err
	}
	safeRng := NewSafeRandGenerator(rg)

	var measuring atomic.Int32
	var readCount atomic.Int64
	var startTime time.Time
	var startOnce sync.Once

	worker := func(workerID int) error {
		conn, err := pool.Connect(totalCtx)
		if err != nil {
			return fmt.Errorf("worker %d connect failed: %w", workerID, err)
		}
		defer conn.Close(totalCtx)

		for {
			if err := totalCtx.Err(); err != nil {
				return nil
			}

			id := safeRng.NextRand()
			row, qErr := conn.QueryOneRow(totalCtx, readSQL, int64(id))
			if qErr != nil {
				if totalCtx.Err() != nil {
					return nil
				}
				return fmt.Errorf("worker %d read failed (id=%d): %w", workerID, id, qErr)
			}

			v, ok := row[col]
			if !ok {
				return fmt.Errorf("worker %d: column %q not found in result", workerID, col)
			}

			touchValue(v)

			if measuring.Load() == 1 {
				readCount.Add(1)
				startOnce.Do(func() { startTime = time.Now() })
			}
		}
	}

	errCh := make(chan error, parallel)
	for i := 0; i < parallel; i++ {
		workerID := i
		go func() { errCh <- worker(workerID) }()
	}

	<-warmupCtx.Done()
	measuring.Store(1)

	<-totalCtx.Done()

	var firstErr error
	for i := 0; i < parallel; i++ {
		if wErr := <-errCh; wErr != nil && firstErr == nil {
			firstErr = wErr
		}
	}
	if firstErr != nil {
		return time.Time{}, 0, firstErr
	}

	return startTime, readCount.Load(), nil
}

func touchValue(v any) {
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
		return
	default:
		_ = fmt.Sprintf("%v", t)
	}
}

func computeReadsPerSec(startTime time.Time, reads int64, executionTime int) float64 {
	if startTime.IsZero() {
		startTime = time.Now().Add(-time.Duration(executionTime) * time.Second)
	}
	elapsed := time.Since(startTime)
	if elapsed <= 0 {
		elapsed = time.Duration(executionTime) * time.Second
	}
	return float64(reads) / elapsed.Seconds()
}

func getIntFromAnyNumberOutput(number any) (int64, error) {
	var foundNumber int64

	switch v := number.(type) {
	case int64:
		foundNumber = v
	case int32:
		foundNumber = int64(v)
	case int:
		foundNumber = int64(v)
	case []byte:
		parsed, pErr := strconv.ParseInt(string(v), decimalSystem, bitSize64)
		if pErr != nil {
			return 0, fmt.Errorf("failed to parse max_id: %w", pErr)
		}
		foundNumber = parsed
	case string:
		parsed, pErr := strconv.ParseInt(v, decimalSystem, bitSize64)
		if pErr != nil {
			return 0, fmt.Errorf("failed to parse max_id: %w", pErr)
		}
		foundNumber = parsed
	default:
		return 0, fmt.Errorf("unexpected max_id type %T (%v)", number, number)
	}

	return foundNumber, nil
}
