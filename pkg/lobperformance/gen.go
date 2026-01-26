package lobperformance

import (
	"context"
	"fmt"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/rs/zerolog/log"
)

type LobRowPlan struct {
	RowIndex int64  // 0..N-1
	LobType  string // "clob", "blob", ...
	LobBytes int64  // exact size to generate for this row
	TenantID int
	DocType  string
}

func LobPerformanceGenerate(dbType dbclient.Rdbms, ctx context.Context, client dbinterface.Client, schemaName string, tableName string, spread []string, emptyLobs int64, byteSize string, lobType string) {
	var logger = log.With().Logger()

	logger.Info().Msg("Initiating connection pool.")
	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %e", poolErr)
	}

	logger.Info().Msg("Connecting to database.")
	conn, connectErr1 := pool.Connect(ctx) //
	if connectErr1 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %e", connectErr1)
	}
	defer conn.Close(ctx)

	var dbHelper DbHelper = nil

	if dbType == dbclient.RdbmsDB2 {
		dbHelper = Db2Helper{schemaName: schemaName, tableName: tableName}
	} else {
		dbHelper = PgHelper{schemaName: schemaName, tableName: tableName}
	}

	// Interpret byte size
	totalBytes, err := ParseByteSize(byteSize)
	if err != nil {
		logger.Fatal().Msgf("Cannot parse bytes from byteSize argument: %v", err)
	}

	var buckets []SpreadBucket

	for _, s := range spread {
		b, err := ParseSpread(s)
		if err != nil {
			logger.Fatal().Msgf("Cannot parse spread argument: %v", err)
		}
		buckets = append(buckets, b)
	}

	plan, err := BuildLobPlan(totalBytes, lobType, buckets, int64(emptyLobs))
	if err != nil {
		logger.Fatal().Msg("Something went wrong building the LOB generation plan")
	}

	batchSize := 100

	idx := ShuffledIndices(len(plan), 12345)

	for start := 0; start < len(idx); start += batchSize {
		end := min(start+batchSize, len(idx))

		batch := make([]LobRowPlan, 0, end-start)
		for _, k := range idx[start:end] {
			batch = append(batch, plan[k])
		}

		processLobBatch(ctx, dbHelper, conn, batch, start/batchSize)
	}
}

func processLobBatch(
	ctx context.Context,
	dbHelper DbHelper,
	conn dbinterface.Connection,
	batch []LobRowPlan,
	batchIndex int,
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

	insertSQL, err := dbHelper.InsertOneRowSql(lobType)
	if err != nil {
		return err
	}

	const tenantID = 1
	const docType = "lob-performance"

	fixedArgs := []any{tenantID, docType}
	payloads := make([]any, 0, len(batch))

	var totalBytes int64
	for _, row := range batch {
		payload, err := createLobPayload(lobType, row.LobBytes)
		if err != nil {
			return fmt.Errorf("create payload failed for row_index=%d: %w", row.RowIndex, err)
		}

		payloads = append(payloads, payload)
		totalBytes += row.LobBytes
	}

	rowsAltered, err := conn.ExecuteBatchWithPayloads(ctx, insertSQL, fixedArgs, payloads)
	if err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	logger.Debug().
		Int("batch_index", batchIndex).
		Int("rows", len(batch)).
		Int64("lob_bytes", totalBytes).
		Int64("rows_altered", rowsAltered).
		Str("lob_type", lobType).
		Msg("Inserted LOB batch")

	return nil
}

func createLobPayload(lobType string, size int64) (any, error) {
	if size < 0 {
		return nil, fmt.Errorf("lob size must be >= 0")
	}

	switch strings.ToLower(lobType) {
	case "clob", "text":
		// ASCII => len == bytes
		return strings.Repeat("a", int(size)), nil
	case "blob", "bytea":
		b := make([]byte, size)
		for i := int64(0); i < size; i++ {
			b[i] = byte(i)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported lobType %q", lobType)
	}
}
