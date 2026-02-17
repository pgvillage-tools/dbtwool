package lobperformance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

	minID, maxID, err := fetchMinMaxIDs(ctx, metaConn, dbHelper)
	if err != nil {
		return err
	}

	readSQL, err := dbHelper.SelectReadLOBByIDSQL(lobType)
	if err != nil {
		return err
	}

	metaConn.Close(ctx)

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
	logger.Info().Msgf("Acquiring %v connections from pool.", parallel)
	conns, err := openWorkerConns(parent, pool, parallel, 60*time.Second)
	if err != nil {
		return time.Time{}, 0, err
	}
	defer closeAll(parent, conns)

	logger.Info().Msg("Acquiring connections finished.")

	warmupCtx, totalCtx, cancel := makeWarmupAndTotalContexts(parent, warmupTime, executionTime)
	defer cancel()

	safeRng, err := buildSafeRng(minID, maxID, readMode, rngSeed)
	if err != nil {
		return time.Time{}, 0, err
	}

	var measuring atomic.Int32
	var readCount atomic.Int64
	var startTime atomic.Value // stores time.Time

	logger.Info().Msg("Starting workers.")
	errCh := startWorkers(parallel, conns, func(workerID int, conn dbinterface.Connection) error {
		return readerWorkerLoop(totalCtx, workerID, conn, safeRng, readSQL, col, &measuring, &readCount)
	})

	<-warmupCtx.Done()
	logger.Info().Msg("Warmup finished. Starting measurements.")
	measuring.Store(1)
	startTime.Store(time.Now())

	<-totalCtx.Done()

	if firstErr := collectFirstError(errCh, parallel); firstErr != nil {
		return time.Time{}, 0, firstErr
	}
	return resolveStartTime(&startTime, executionTime), readCount.Load(), nil
}

func openWorkerConns(
	parent context.Context,
	pool dbinterface.Pool,
	parallel int,
	timeout time.Duration,
) ([]dbinterface.Connection, error) {
	conns := make([]dbinterface.Connection, 0, parallel)
	for i := 0; i < parallel; i++ {
		connectCtx, cancel := context.WithTimeout(parent, timeout)
		conn, err := pool.Connect(connectCtx)
		cancel()
		if err != nil {
			closeAll(parent, conns)
			return nil, fmt.Errorf("worker %d connect failed: %w", i, err)
		}
		conns = append(conns, conn)
	}
	return conns, nil
}

func closeAll(ctx context.Context, conns []dbinterface.Connection) {
	for _, c := range conns {
		_ = c.Close(ctx)
	}
}

func makeWarmupAndTotalContexts(
	parent context.Context,
	warmupTime int,
	executionTime int,
) (context.Context, context.Context, func()) {
	warmupCtx, cancelWarmup := context.WithTimeout(parent, time.Duration(warmupTime)*time.Second)
	totalCtx, cancelTotal := context.WithTimeout(parent, time.Duration(warmupTime+executionTime)*time.Second)

	cancel := func() {
		cancelWarmup()
		cancelTotal()
	}
	return warmupCtx, totalCtx, cancel
}

func buildSafeRng(minID, maxID int, readMode string, rngSeed int64) (*SafeRandGenerator, error) {
	rg, err := NewRandGenerator(minID, maxID, RandMode(readMode), rngSeed)
	if err != nil {
		return nil, err
	}
	return NewSafeRandGenerator(rg), nil
}

func startWorkers(
	parallel int,
	conns []dbinterface.Connection,
	fn func(workerID int, conn dbinterface.Connection) error,
) <-chan error {
	errCh := make(chan error, parallel)
	for i := 0; i < parallel; i++ {
		workerID := i
		conn := conns[i]
		go func() { errCh <- fn(workerID, conn) }()
	}
	return errCh
}

func readerWorkerLoop(
	ctx context.Context,
	workerID int,
	conn dbinterface.Connection,
	safeRng *SafeRandGenerator,
	readSQL string,
	col string,
	measuring *atomic.Int32,
	readCount *atomic.Int64,
) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		id := safeRng.NextRand()
		row, qErr := conn.QueryOneRow(ctx, readSQL, int64(id))
		if qErr != nil {
			if ctx.Err() != nil {
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
		}
	}
}

func collectFirstError(errCh <-chan error, parallel int) error {
	var firstErr error
	for i := 0; i < parallel; i++ {
		if wErr := <-errCh; wErr != nil && firstErr == nil {
			firstErr = wErr
		}
	}
	return firstErr
}

func resolveStartTime(startTime *atomic.Value, executionTime int) time.Time {
	if stAny := startTime.Load(); stAny != nil {
		if st, ok := stAny.(time.Time); ok {
			return st
		}
	}
	return time.Now().Add(-time.Duration(executionTime) * time.Second)
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
