package ruperformance

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pgvillage-tools/dbtwool/pkg/dbclient"
	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
	"github.com/rs/zerolog/log"
)

// AcctTxnRowPlan defines a record for acct_txn table row
type AcctTxnRowPlan struct {
	RowIndex    int64
	AcctID      int
	TxnTS       time.Time
	Amount      float64 // decmial
	Description string  // always 100 long
}

// Generate actually generates data and writes it to the database based on db, and data generation arguments.
func Generate(
	ctx context.Context,
	dbType dbclient.RDBMS,
	client dbinterface.Client,
	schemaName string,
	tableName string,
	numRows int64,
) {
	logger := log.With().Str("cmd", "gen").Logger()

	logger.Info().Msg("Initiating connection pool.")
	pool, poolErr := client.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %v", poolErr)
	}

	logger.Info().Msg("Connecting to database.")
	conn, err := pool.Connect(ctx)
	if err != nil {
		logger.Fatal().Msgf("connect error: %v", err)
	}
	defer conn.Close(ctx)

	if numRows <= 0 {
		logger.Fatal().Msg("numRows must be > 0")
	}

	const batchSize = 100
	logger.Info().Msgf("Generating %d rows into %s.%s (batchSize=%d)", numRows, schemaName, tableName, batchSize)
	insertPrefix := fmt.Sprintf("INSERT INTO %s.%s (acct_id, txn_ts, amount, descr) VALUES ", schemaName, tableName)

	// Stable base so runs are comparable.
	baseTS := time.Now().UTC().Truncate(time.Second)
	const seed uint64 = 0xC0FFEE12345

	total := int(numRows)
	for start := 0; start < total; start += batchSize {
		end := minInt(start+batchSize, total)

		// one tx per batch
		if err := conn.Begin(ctx); err != nil {
			logger.Fatal().Msgf("begin failed: %v", err)
		}

		committed := false
		// should probably be separate function:
		func() {
			defer func() {
				if !committed {
					_ = conn.Rollback(ctx)
				}
			}()

			sql := buildInsertSQL(dbType, insertPrefix, baseTS, seed, start, end)

			if _, err := conn.Execute(ctx, sql); err != nil {
				logger.Error().Msgf("batch insert failed (rows %d..%d): %v", start+1, end, err)
				return
			}

			if err := conn.Commit(ctx); err != nil {
				logger.Error().Msgf("commit failed (batch starting %d): %v", start, err)
				return
			}
			committed = true
		}()

		if !committed {
			logger.Fatal().Msg("aborting due to batch failure")
		}

		logger.Info().Msgf("Inserted rows %d..%d of %d", start+1, end, total)
	}

	logger.Info().Msg("Generate transactions completed.")
}

func buildInsertSQL(
	dbType dbclient.RDBMS,
	insertPrefix string,
	baseTS time.Time,
	seed uint64,
	start, end int,
) string {
	var sb strings.Builder
	sb.Grow((end - start) * stringBufferallocation)
	sb.WriteString(insertPrefix)

	for i := start; i < end; i++ {
		rowIdx := int64(i)

		acctID := pickSkewedAcctID(seed, rowIdx)
		ts := baseTS.Add(time.Duration(rowIdx) * time.Millisecond)
		amt := pickAmount(seed, rowIdx)
		descr := generateDescription(descriptionLength, seed, rowIdx)

		tsLit := timestampLiteral(dbType, ts)
		amtLit := formatAmountLiteral(amt)
		descrLit := sqlStringLiteral(descr)

		if i > start {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("(%d, %s, %s, %s)", acctID, tsLit, amtLit, descrLit))
	}

	return sb.String()
}

// functions to make literals for building the insert statements (sadly some rdbms specific logic heres)
func timestampLiteral(dbType dbclient.RDBMS, t time.Time) string {
	if dbType == dbclient.DB2 {
		return fmt.Sprintf("TIMESTAMP('%s')", t.UTC().Format("2006-01-02-15.04.05"))
	}
	return fmt.Sprintf("TIMESTAMP '%s'", t.UTC().Format("2006-01-02 15:04:05"))
}

func formatAmountLiteral(v float64) string {
	return fmt.Sprintf("%.2f", v)
}

func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Makes most of the transactions be on the same 50 accounts so the RU permance scenario is recreated better. (hot data)
func pickSkewedAcctID(seed uint64, rowIdx int64) int {
	u := hashU64(seed, uint64(rowIdx), 0)
	p := int(u % 100)

	if p < hotAccountPercentage {
		return 1 + int(hashU64(seed, uint64(rowIdx), 1)%hotAccountCount)
	}
	return hotAccountCount + int(hashU64(seed, uint64(rowIdx), 2)%(totalAccountCount-hotAccountCount))
}

func pickAmount(seed uint64, rowIdx int64) float64 {
	u := hashU64(seed, uint64(rowIdx), 3)
	v := int64(u%(2*amountRangeHalfCents+1)) - amountRangeHalfCents
	amt := float64(v) / amountScale

	if math.Abs(amt) < amountZeroThreshold {
		amt = 0
	}
	return amt
}

// generateDescription makes a 100-char ASCII
func generateDescription(n int, seed uint64, rowIdx int64) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, n)
	var counter uint64
	offset := 0
	for offset < len(buf) {
		h := sha256.Sum256(uint64ToBytes(hashU64(seed, uint64(rowIdx), counter)))
		counter++
		for i := 0; i < len(h) && offset < len(buf); i++ {
			buf[offset] = h[i]
			offset++
		}
	}
	asciiEncodeInPlace(buf)
	return string(buf)
}

// hashU64 returns a deterministic 64-bit value
func hashU64(seed uint64, row uint64, salt uint64) uint64 {
	var b [24]byte
	binary.BigEndian.PutUint64(b[0:8], seed)
	binary.BigEndian.PutUint64(b[8:16], row)
	binary.BigEndian.PutUint64(b[16:24], salt)
	h := sha256.Sum256(b[:])
	return binary.BigEndian.Uint64(h[0:8])
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
