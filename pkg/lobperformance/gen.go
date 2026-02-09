package lobperformance

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/rs/zerolog/log"
)

// LOBRowPlan defines a record for a LOB table row
type LOBRowPlan struct {
	RowIndex int64 // 0..N-1
	TenantID int
	LobType  string // "clob", "blob", ...
	LobBytes int64  // exact size to generate for this row
	DocType  string
}

// Generate generates LOB data
func Generate(ctx context.Context, dbType dbclient.RDBMS, client dbinterface.Client, schemaName string,
	tableName string, spread []string, emptyLobs int64, byteSize string, batchSize int, lobType string) {
	var logger = log.With().Logger()
	logger.Info().Msg("Initiating connection pool.")
	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %v", poolErr)
	}

	logger.Info().Msg("Connecting to database.")
	conn, connectErr1 := pool.Connect(ctx) //
	if connectErr1 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %v", connectErr1)
	}
	defer conn.Close(ctx)

	dbHelper := initDBHelper(dbType, schemaName, tableName)

	// Interpret byte size
	totalBytes, err := ParseByteSize(byteSize)
	if err != nil {
		logger.Fatal().Msgf("Cannot parse bytes from byteSize argument: %v", err)
	}

	logger.Info().Msgf("Totalbytes set to %v", totalBytes)

	var buckets = createSpreadBuckets(spread)

	logger.Info().Msg("Building LOB generation plan")
	plan, err := BuildLOBPlan(totalBytes, lobType, buckets, int64(emptyLobs))
	if err != nil {
		logger.Fatal().Msgf("Something went wrong building the LOB generation plan: %v", err)
	}

	totalNumOfRows := len(plan)
	logger.Info().Msgf("Building LOB generation plan finished. %v rows will be inserted.", totalNumOfRows)
	logger.Info().Msgf("Batch size set to %v", batchSize)

	insertSQL, err := dbHelper.CreateInsertLOBRowBaseSQL(lobType)
	if err != nil {
		logger.Fatal().Msgf("Could not establish base SQL insert query: %v", err)
	}
	const randomSeed = 12345
	idx := ShuffledIndices(len(plan), randomSeed)
	total := len(idx)
	startedAt := time.Now()

	for start := 0; start < len(idx); start += batchSize {
		end := min(start+batchSize, len(idx))

		batch := make([]LOBRowPlan, 0, end-start)
		for _, k := range idx[start:end] {
			batch = append(batch, plan[k])
		}

		// "done" after this batch completes successfully
		doneAfter := end

		pct := progressPct(doneAfter, total)
		eta := estimateRemaining(startedAt, doneAfter, total)

		logger.Info().Msgf(
			"Inserting LOBs %d to %d of %d (%.3f%%, ETA %s)",
			start+1, end, totalNumOfRows, pct, eta.Truncate(time.Second),
		)

		err := processLobBatch(ctx, conn, batch, start/batchSize, insertSQL)
		if err != nil {
			logger.Fatal().Msgf("Something went wrong while processing the LOB batch %v", err)
		}
	}
}

func processLobBatch(
	ctx context.Context,
	conn dbinterface.Connection,
	batch []LOBRowPlan,
	batchIndex int,
	insertSQL string,
) error {
	if len(batch) == 0 {
		return nil
	}

	// Basic batch-level validation
	lobType := batch[0].LobType
	for i, row := range batch {
		if row.LobType != lobType {
			return fmt.Errorf(
				"mixed lob types in batch %d at position %d: %q vs %q",
				batchIndex, i, lobType, row.LobType,
			)
		}
		if row.LobBytes < 0 {
			return fmt.Errorf("negative lob size in batch %d at row %d", batchIndex, row.RowIndex)
		}
	}

	// Begin one transaction for the entire batch
	if err := conn.Begin(ctx); err != nil {
		return fmt.Errorf("begin batch tx failed: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = conn.Rollback(ctx)
		}
	}()

	var (
		rowsAltered int64
		totalBytes  int64
	)

	// Prefer prepared statement if available
	var preparedStatement dbinterface.PreparedStatement
	if tp, ok := any(conn).(dbinterface.TxPreparer); ok {
		s, err := tp.PrepareInTx(ctx, insertSQL)
		if err != nil {
			return fmt.Errorf("prepare failed for batch %d: %w", batchIndex, err)
		}
		preparedStatement = s
		defer func() { _ = preparedStatement.Close(ctx) }()
	}

	for _, row := range batch {
		payload, err := createLobPayload(row.LobType, row.LobBytes)
		if err != nil {
			return fmt.Errorf("create payload failed for row_index=%d: %w", row.RowIndex, err)
		}

		var ra int64
		if preparedStatement != nil {
			ra, err = preparedStatement.ExecWithPayload(ctx, payload, row.TenantID, row.DocType)
		} else {
			ra, err = conn.ExecuteWithPayload(ctx, insertSQL, payload, row.TenantID, row.DocType)
		}
		if err != nil {
			return fmt.Errorf("insert failed for row_index=%d: %w", row.RowIndex, err)
		}

		rowsAltered += ra
		totalBytes += row.LobBytes
	}

	if err := conn.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch tx failed: %w", err)
	}
	committed = true

	logger.Debug().
		Int("batch_index", batchIndex).
		Int("rows", len(batch)).
		Int64("rows_altered", rowsAltered).
		Int64("lob_bytes", totalBytes).
		Str("lob_type", lobType).
		Msg("Inserted LOB batch finished")

	return nil
}

func createSpreadBuckets(spread []string) []SpreadBucket {
	var buckets []SpreadBucket
	for _, s := range spread {
		b, err := ParseSpread(s)
		if err != nil {
			logger.Fatal().Msgf("Cannot parse spread argument: %v", err)
		}
		buckets = append(buckets, b)
	}

	return buckets
}

func createLobPayload(lobType string, size int64) (any, error) {
	if size < 0 {
		return nil, errors.New("lob size must be >= 0")
	}

	if size == 0 {
		switch strings.ToLower(lobType) {
		case "clob", "text":
			return "", nil
		case "blob", "bytea":
			return []byte{}, nil
		default:
			return nil, fmt.Errorf("unsupported lobType %q", lobType)
		}
	}

	switch strings.ToLower(lobType) {
	case "clob", "text":
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = 'a'
		}
		encryptInPlace(buf)
		asciiEncodeInPlace(buf)
		return string(buf), nil

	case "blob", "bytea":
		b := make([]byte, size)
		for i := range b {
			b[i] = byte(i)
		}
		encryptInPlace(b)
		return b, nil

	default:
		return nil, fmt.Errorf("unsupported lobType %q", lobType)
	}
}

func encryptInPlace(buf []byte) {
	var counter uint64
	offset := 0

	for offset < len(buf) {
		h := sha256.Sum256(uint64ToBytes(counter))
		counter++

		for i := 0; i < len(h) && offset < len(buf); i++ {
			buf[offset] ^= h[i]
			offset++
		}
	}
}

func uint64ToBytes(v uint64) []byte {
	return []byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	}
}

func asciiEncodeInPlace(buf []byte) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	const alphabetMask = byte(len(alphabet) - 1)
	for i := range buf {
		buf[i] = alphabet[buf[i]&alphabetMask]
	}
}

func progressPct(done, total int) float64 {
	if total <= 0 {
		return maxPercentFloat
	}
	return (float64(done) / float64(total)) * maxPercentFloat
}

func estimateRemaining(start time.Time, done, total int) time.Duration {
	if done <= 0 || total <= done {
		return 0
	}

	elapsed := time.Since(start).Seconds()
	rate := float64(done) / elapsed // rows per second

	if rate <= 0 {
		return 0
	}

	remaining := float64(total - done)
	etaSeconds := remaining / rate

	return time.Duration(etaSeconds * float64(time.Second))
}

func initDBHelper(dbType dbclient.RDBMS, schemaName, tableName string) DBHelper {
	if dbType == dbclient.DB2 {
		return DB2Helper{schemaName: schemaName, tableName: tableName}
	}
	return PGHelper{schemaName: schemaName, tableName: tableName}
}
