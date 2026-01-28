package lobperformance

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

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
func Generate(ctx context.Context, dbType dbclient.RDBMS, client dbinterface.Client,
	schemaName string, tableName string, spread []string, emptyLobs int64, byteSize string,
	lobType string) {
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

	var dbHelper DBHelper
	if dbType == dbclient.DB2 {
		dbHelper = DB2Helper{schemaName: schemaName, tableName: tableName}
	} else {
		dbHelper = PGHelper{schemaName: schemaName, tableName: tableName}
	}

	// Interpret byte size
	totalBytes, err := ParseByteSize(byteSize)
	if err != nil {
		logger.Fatal().Msgf("Cannot parse bytes from byteSize argument: %v", err)
	}

	logger.Info().Msgf("Totalbytes set to %v", totalBytes)

	var buckets []SpreadBucket
	for _, s := range spread {
		b, err := ParseSpread(s)
		if err != nil {
			logger.Fatal().Msgf("Cannot parse spread argument: %v", err)
		}
		buckets = append(buckets, b)
	}

	logger.Info().Msg("Building LOB generation plan")
	plan, err := BuildLOBPlan(totalBytes, lobType, buckets, int64(emptyLobs))
	if err != nil {
		logger.Fatal().Msgf("Something went wrong building the LOB generation plan: %v", err)
	}

	logger.Info().Msgf("Building LOB generation plan finished. %v rows will be inserted.", len(plan))
	const batchSize = 100
	logger.Info().Msgf("Batch size set to %v", batchSize)

	insertSQL, err := dbHelper.CreateInsertLOBRowBaseSQL(lobType)
	if err != nil {
		logger.Fatal().Msgf("Could not establish base SQL insert query: %v", err)
	}
	const randomSeed = 12345
	idx := ShuffledIndices(len(plan), randomSeed)

	total := len(idx)

	for start := 0; start < len(idx); start += batchSize {
		end := min(start+batchSize, len(idx))

		batch := make([]LOBRowPlan, 0, end-start)
		for _, k := range idx[start:end] {
			batch = append(batch, plan[k])
		}

		logger.Info().Msgf("Inserting LOBs %v to %v out of %v total", start+1, end, total)
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
				batchIndex,
				i,
				lobType,
				row.LobType,
			)
		}
		if row.LobBytes < 0 {
			return fmt.Errorf(
				"negative lob size in batch %d at row %d",
				batchIndex,
				row.RowIndex,
			)
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

	var rowsAltered int64
	var totalBytes int64

	for _, row := range batch {
		payload, err := createLobPayload(row.LobType, row.LobBytes)
		if err != nil {
			return fmt.Errorf("create payload failed for row_index=%d: %w", row.RowIndex, err)
		}

		ra, err := conn.ExecuteWithPayload(ctx, insertSQL, payload, row.TenantID, row.DocType)
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
		plain := make([]byte, size)
		for i := range plain {
			plain[i] = 'a'
		}
		encryptInPlace(plain)
		return string(plain), nil

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
