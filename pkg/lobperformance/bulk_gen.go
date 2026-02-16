package lobperformance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

// GenerateBulk generates LOB data and inserts using the bulk path (COPY/LOAD) via processLobRowsBatchBulk.
// It builds LobRow payloads per batch (instead of passing LOBRowPlan into the DB layer).
func GenerateBulk(
	ctx context.Context,
	dbType dbclient.RDBMS,
	client dbinterface.Client,
	schemaName, tableName string,
	spread []string,
	emptyLobs int64,
	byteSize string,
	batchSize int,
	lobType string,
) {
	conn := mustConnect(ctx, client)
	defer conn.Close(ctx)

	totalBytes := mustParseByteSize(byteSize)
	buckets := createSpreadBuckets(spread)

	plan := mustBuildLOBPlan(totalBytes, lobType, buckets, emptyLobs)
	logger.Info().Msgf("Plan built: %d rows; batch size %d", len(plan), batchSize)

	const randomSeed = 12345
	idx := ShuffledIndices(len(plan), randomSeed)
	startedAt := time.Now()

	for b, start := 0, 0; start < len(idx); b, start = b+1, start+batchSize {
		end := min(start+batchSize, len(idx))
		rows := mustBuildBatchRows(plan, idx[start:end])

		doneAfter := end
		logger.Info().Msgf(
			"Inserting LOBs %d to %d of %d (%.3f%%, ETA %s)",
			start+1, end, len(plan),
			progressPct(doneAfter, len(idx)),
			estimateRemaining(startedAt, doneAfter, len(idx)).Truncate(time.Second),
		)

		if err := processLobRowsBatchBulk(ctx, conn, schemaName, tableName, rows, b); err != nil {
			logger.Fatal().Msgf("Something went wrong while processing the bulk LOB batch: %v", err)
		}
	}
}

// --- small helpers used by GenerateBulk ---
func mustConnect(ctx context.Context, client dbinterface.Client) dbinterface.Connection {
	logger.Info().Msg("Initiating connection pool.")
	pool, err := client.Pool(ctx)
	if err != nil {
		logger.Fatal().Msgf("Failed to connect: %v", err)
	}

	logger.Info().Msg("Connecting to database.")
	conn, err := pool.Connect(ctx)
	if err != nil {
		logger.Fatal().Msgf("connect error: %v", err)
	}
	return conn
}

func mustParseByteSize(byteSize string) int64 {
	totalBytes, err := ParseByteSize(byteSize)
	if err != nil {
		logger.Fatal().Msgf("Cannot parse bytes from byteSize argument: %v", err)
	}
	logger.Info().Msgf("Totalbytes set to %v", totalBytes)
	return totalBytes
}

func mustBuildLOBPlan(totalBytes int64, lobType string, buckets []SpreadBucket, emptyLobs int64) []LOBRowPlan {
	logger.Info().Msg("Building LOB generation plan")
	plan, err := BuildLOBPlan(totalBytes, lobType, buckets, emptyLobs)
	if err != nil {
		logger.Fatal().Msgf("Something went wrong building the LOB generation plan: %v", err)
	}
	return plan
}

func mustBuildBatchRows(plan []LOBRowPlan, batchIdx []int) []dbinterface.LobRow {
	rows := make([]dbinterface.LobRow, 0, len(batchIdx))
	for _, k := range batchIdx {
		p := plan[k]
		payload, err := createLobPayload(p.LobType, p.LobBytes)
		if err != nil {
			logger.Fatal().Msgf("create payload failed for row_index=%d: %v", p.RowIndex, err)
		}
		rows = append(rows, dbinterface.LobRow{
			TenantID: p.TenantID,
			DocType:  p.DocType,
			LobType:  strings.ToLower(p.LobType),
			Payload:  payload,
		})
	}
	return rows
}

func processLobRowsBatchBulk(
	ctx context.Context,
	conn dbinterface.Connection,
	schema, table string,
	rows []dbinterface.LobRow,
	batchIndex int,
) error {
	bi, ok := any(conn).(dbinterface.BulkInserter)
	if !ok {
		return errors.New("bulk mode requested but connection does not support bulk insert")
	}

	insRows, insBytes, err := bi.InsertLOBRowsBulk(ctx, schema, table, rows)
	if err != nil {
		return fmt.Errorf("bulk insert failed (batch %d): %w", batchIndex, err)
	}

	logger.Debug().
		Int("batch_index", batchIndex).
		Int("rows", len(rows)).
		Int64("rows_altered", insRows).
		Int64("lob_bytes", insBytes).
		Msg("Bulk LOB batch finished")

	return nil
}
