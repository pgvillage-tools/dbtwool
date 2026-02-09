package lobperformance

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/rs/zerolog/log"
)

// GenerateBulk generates LOB data and inserts using the bulk path (COPY/LOAD) via processLobRowsBatchBulk.
// It builds LobRow payloads per batch (instead of passing LOBRowPlan into the DB layer).
func GenerateBulk(
	ctx context.Context,
	dbType dbclient.RDBMS,
	client dbinterface.Client,
	schemaName string,
	tableName string,
	spread []string,
	emptyLobs int64,
	byteSize string,
	batchSize int,
	lobType string,
) {
	var logger = log.With().Logger()

	logger.Info().Msg("Initiating connection pool.")
	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %v", poolErr)
	}

	logger.Info().Msg("Connecting to database.")
	conn, connectErr1 := pool.Connect(ctx)
	if connectErr1 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %v", connectErr1)
	}
	defer conn.Close(ctx)

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

	const randomSeed = 12345
	idx := ShuffledIndices(len(plan), randomSeed)
	total := len(idx)
	startedAt := time.Now()

	for start := 0; start < len(idx); start += batchSize {
		end := min(start+batchSize, len(idx))
		rows := make([]dbinterface.LobRow, 0, end-start)

		for _, k := range idx[start:end] {
			p := plan[k]

			payload, err := createLobPayload(p.LobType, p.LobBytes)
			if err != nil {
				logger.Fatal().Msgf("create payload failed for row_index=%d: %v", p.RowIndex, err)
			}

			rows = append(rows, dbinterface.LobRow{
				TenantID: p.TenantID,
				DocType:  p.DocType,
				LobType:  strings.ToLower(p.LobType),
				Payload:  payload, // []byte or string
			})
		}

		// "done" after this batch completes successfully
		doneAfter := end
		pct := progressPct(doneAfter, total)
		eta := estimateRemaining(startedAt, doneAfter, total)

		logger.Info().Msgf(
			"Inserting LOBs %d to %d of %d (%.3f%%, ETA %s)",
			start+1, end, totalNumOfRows, pct, eta.Truncate(time.Second),
		)

		err := processLobRowsBatchBulk(ctx, conn, schemaName, tableName, rows, start/batchSize)
		if err != nil {
			logger.Fatal().Msgf("Something went wrong while processing the bulk LOB batch: %v", err)
		}
	}
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
		return fmt.Errorf("bulk mode requested but connection does not support bulk insert")
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
