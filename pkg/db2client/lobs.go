package db2client

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/dbinterface"
)

const (
	defaultFilePerm = 0o640
	tokenBytes      = 8
)

// InsertLOBRowsBulk writes files to the file system and executes LOAD commnands
// with DB2's SYSPROC.ADMIN_CMD to insert large objects.
func (c *Connection) InsertLOBRowsBulk(
	ctx context.Context,
	schema, table string,
	rows []dbinterface.LobRow,
) (int64, int64, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}

	baseDir := lobBaseDir(schema, table)
	if err := ensureDirAccessible(baseDir); err != nil {
		return 0, 0, err
	}

	batch, err := newLOBLoadBatch(baseDir, schema, table, rows)
	if err != nil {
		return 0, 0, err
	}

	totalBytes, err := calculateTotalBytes(rows)
	if err != nil {
		return 0, 0, err
	}

	if err := batch.writeFiles(rows); err != nil {
		_ = batch.close()
		return 0, 0, err
	}
	if err := batch.close(); err != nil {
		return 0, 0, err
	}

	if err := c.runDB2Load(ctx, batch); err != nil {
		return 0, 0, err
	}

	if err := cleanupFiles(batch.delFilePath, batch.lobFilePath); err != nil {
		return int64(len(rows)), totalBytes, fmt.Errorf("load succeeded but cleanup failed: %w", err)
	}

	return int64(len(rows)), totalBytes, nil
}

// --- batch orchestration ---

type lobLoadBatch struct {
	baseDir     string
	schema      string
	table       string
	lobType     string
	payloadCol  string
	lobFileName string
	lobFilePath string
	delFilePath string

	lf   *os.File
	df   *os.File
	delW *bufio.Writer
}

func newLOBLoadBatch(baseDir, schema, table string, rows []dbinterface.LobRow) (*lobLoadBatch, error) {
	lobType, payloadCol, err := resolveBatchLOBType(rows)
	if err != nil {
		return nil, err
	}

	token, err := newHexToken(tokenBytes)
	if err != nil {
		return nil, fmt.Errorf("token generation failed: %w", err)
	}

	lobFileName := fmt.Sprintf("db2exp_%s.001", token)
	b := &lobLoadBatch{
		baseDir:     baseDir,
		schema:      schema,
		table:       table,
		lobType:     lobType,
		payloadCol:  payloadCol,
		lobFileName: lobFileName,
		lobFilePath: filepath.Join(baseDir, lobFileName),
		delFilePath: filepath.Join(baseDir, fmt.Sprintf("data_%s.del", token)),
	}

	if err := b.openFiles(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *lobLoadBatch) openFiles() error {
	lf, err := os.OpenFile(b.lobFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		return fmt.Errorf("open lob container failed: %w", err)
	}
	df, err := os.OpenFile(b.delFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		_ = lf.Close()
		return fmt.Errorf("open del file failed: %w", err)
	}

	b.lf = lf
	b.df = df
	b.delW = bufio.NewWriterSize(df, 1<<20) // 1MB
	return nil
}

func (b *lobLoadBatch) writeFiles(rows []dbinterface.LobRow) error {
	var offset int64
	for i, r := range rows {
		if err := validateLOBRow(i, b.lobType, r); err != nil {
			return err
		}

		payloadBytes, err := payloadToBytes(b.lobType, r.Payload)
		if err != nil {
			return fmt.Errorf("payload conversion failed at row %d: %w", i, err)
		}

		if err := writeAll(b.lf, payloadBytes, i); err != nil {
			return err
		}

		length := int64(len(payloadBytes))
		lls := fmt.Sprintf("%s.%d.%d/", b.lobFileName, offset, length)
		if err := writeDELLine(b.delW, int64(r.TenantID), r.DocType, lls, i); err != nil {
			return err
		}
		offset += length
	}
	return nil
}

func (b *lobLoadBatch) close() error {
	var firstErr error

	if b.delW != nil {
		if err := b.delW.Flush(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("flush del file failed: %w", err)
		}
	}

	if b.df != nil {
		if err := b.df.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close del file failed: %w", err)
		}
	}

	if b.lf != nil {
		if err := b.lf.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close lob container failed: %w", err)
		}
	}

	return firstErr
}

func (c *Connection) runDB2Load(ctx context.Context, b *lobLoadBatch) error {
	loadCmd := fmt.Sprintf(
		"LOAD FROM %s OF DEL LOBS FROM %s MODIFIED BY COLDEL| LOBSINFILE INSERT INTO %s.%s (tenant_id, doc_type, %s)",
		b.delFilePath,
		b.baseDir,
		b.schema, b.table,
		b.payloadCol,
	)

	_, err := c.conn.ExecContext(ctx, "CALL SYSPROC.ADMIN_CMD(?)", loadCmd)
	if err != nil {
		return fmt.Errorf("ADMIN_CMD LOAD failed: %w", err)
	}
	return nil
}

// --- small helpers ---

func lobBaseDir(schema, table string) string {
	return filepath.Join("/tmp/dbtwoollobgen", fmt.Sprintf("%s.%s", schema, table))
}

func ensureDirAccessible(dir string) error {
	st, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("base dir %q not accessible: %w", dir, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("base path %q is not a directory", dir)
	}
	return nil
}

func resolveBatchLOBType(rows []dbinterface.LobRow) (string, string, error) {
	lobType := strings.ToLower(rows[0].LobType)
	payloadCol := db2PayloadColumnForLOBType(lobType)
	if payloadCol == "" {
		return "", "", fmt.Errorf("unsupported lobType %q", rows[0].LobType)
	}
	return lobType, payloadCol, nil
}

func validateLOBRow(i int, batchLobType string, r dbinterface.LobRow) error {
	rt := strings.ToLower(r.LobType)
	if rt != batchLobType {
		return fmt.Errorf("mixed lob types in bulk batch: %q vs %q at row %d", batchLobType, rt, i)
	}
	if strings.TrimSpace(r.DocType) == "" {
		return fmt.Errorf("doc_type must be non-empty (row %d) because target column is NOT NULL", i)
	}
	return nil
}

func writeAll(w io.Writer, b []byte, rowIdx int) error {
	if len(b) == 0 {
		return nil
	}
	n, err := w.Write(b)
	if err != nil {
		return fmt.Errorf("write lob container failed at row %d: %w", rowIdx, err)
	}
	if n != len(b) {
		return fmt.Errorf("short write to lob container at row %d: wrote %d, expected %d", rowIdx, n, len(b))
	}
	return nil
}

func writeDELLine(w *bufio.Writer, tenantID int64, docType, lls string, rowIdx int) error {
	_, err := fmt.Fprintf(w, "%d|%s|%s\n", tenantID, escapeDelField(docType), lls)
	if err != nil {
		return fmt.Errorf("write del failed at row %d: %w", rowIdx, err)
	}
	return nil
}

// --- existing helpers (unchanged) ---

func db2PayloadColumnForLOBType(lobType string) string {
	switch strings.ToLower(lobType) {
	case "blob", "bytea":
		return "payload_bin"
	case "clob", "text":
		return "payload_text"
	default:
		return ""
	}
}

func payloadToBytes(lobType string, payload any) ([]byte, error) {
	switch strings.ToLower(lobType) {
	case "blob", "bytea":
		if payload == nil {
			return []byte{}, nil
		}
		b, ok := payload.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte for blob payload, got %T", payload)
		}
		return b, nil

	case "clob", "text":
		if payload == nil {
			return []byte{}, nil
		}
		s, ok := payload.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for clob payload, got %T", payload)
		}
		return []byte(s), nil

	default:
		return nil, fmt.Errorf("unsupported lobType %q", lobType)
	}
}

func escapeDelField(v string) string {
	v = strings.ReplaceAll(v, "\r", " ")
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "|", "/")
	return v
}

func cleanupFiles(paths ...string) error {
	var firstErr error
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func newHexToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func calculateTotalBytes(rows []dbinterface.LobRow) (int64, error) {
	var totalBytes int64
	for _, r := range rows {
		switch v := r.Payload.(type) {
		case []byte:
			totalBytes += int64(len(v))
		case string:
			totalBytes += int64(len(v))
		case nil:
			// treat as 0
		default:
			return totalBytes, fmt.Errorf("unexpected payload type %T for lobType=%q", r.Payload, r.LobType)
		}
	}
	return totalBytes, nil
}
