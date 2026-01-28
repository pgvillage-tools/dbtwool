package pg

import (
	"errors"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// mockRows implements pgx.Rows interface for testing purposes
type mockRows struct {
	fieldDescriptions []pgconn.FieldDescription
	rows              [][]any
	currentIndex      int
	err               error
	closeCalled       bool
}

func (m *mockRows) FieldDescriptions() []pgconn.FieldDescription {
	return m.fieldDescriptions
}

func (m *mockRows) Next() bool {
	if m.currentIndex < len(m.rows) {
		m.currentIndex++
		return true
	}
	return false
}

func (m *mockRows) Values() ([]any, error) {
	if m.currentIndex > 0 && m.currentIndex <= len(m.rows) {
		return m.rows[m.currentIndex-1], nil
	}
	return nil, errors.New("no more rows or index out of bounds")
}

func (m *mockRows) Err() error {
	return m.err
}

func (m *mockRows) Close() {
	m.closeCalled = true
}

func (m *mockRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{} // Initialize as empty struct
}

func (m *mockRows) Conn() *pgx.Conn {
	return nil
}

func (m *mockRows) Read() error {
	return nil
}

func (m *mockRows) Tag() pgconn.CommandTag {
	return pgconn.CommandTag{} // Initialize as empty struct
}

func (m *mockRows) RawValues() [][]byte {
	return nil
}

func (m *mockRows) Scan(_ ...any) error {
	return nil
}

func (m *mockRows) RowTo(_ any) error {
	return nil
}

var _ = Describe("Connection", func() {
	Context("RowsToMaps", func() {
		It("should convert empty rows to an empty slice of maps", func() {
			mock := &mockRows{
				fieldDescriptions: []pgconn.FieldDescription{
					{Name: "col1", DataTypeOID: pgtype.Int4OID},
				},
				rows: [][]any{},
			}
			result, err := rowsToMaps(mock)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
			Expect(mock.closeCalled).To(BeTrue())
		})

		It("should convert rows with a single row and multiple columns to a slice of maps", func() {
			mock := &mockRows{
				fieldDescriptions: []pgconn.FieldDescription{
					{Name: "id", DataTypeOID: pgtype.Int4OID},
					{Name: "name", DataTypeOID: pgtype.TextOID},
				},
				rows: [][]any{
					{1, "test1"},
				},
			}
			result, err := rowsToMaps(mock)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveKeyWithValue("id", 1))
			Expect(result[0]).To(HaveKeyWithValue("name", "test1"))
			Expect(mock.closeCalled).To(BeTrue())
		})

		It("should convert rows with multiple rows and multiple columns to a slice of maps", func() {
			mock := &mockRows{
				fieldDescriptions: []pgconn.FieldDescription{
					{Name: "id", DataTypeOID: pgtype.Int4OID},
					{Name: "name", DataTypeOID: pgtype.TextOID},
				},
				rows: [][]any{
					{1, "test1"},
					{2, "test2"},
				},
			}
			result, err := rowsToMaps(mock)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect(result[0]).To(HaveKeyWithValue("id", 1))
			Expect(result[0]).To(HaveKeyWithValue("name", "test1"))
			Expect(result[1]).To(HaveKeyWithValue("id", 2))
			Expect(result[1]).To(HaveKeyWithValue("name", "test2"))
			Expect(mock.closeCalled).To(BeTrue())
		})

		It("should return an error if rows.Err() returns an error", func() {
			expectedErr := errors.New("rows iteration error")
			mock := &mockRows{
				fieldDescriptions: []pgconn.FieldDescription{
					{Name: "col1", DataTypeOID: pgtype.Int4OID},
				},
				rows: [][]any{
					{1},
				},
				err: expectedErr,
			}
			result, err := rowsToMaps(mock)
			Expect(err).To(MatchError(expectedErr))
			Expect(result).To(BeNil())
			Expect(mock.closeCalled).To(BeTrue())
		})

		It("should handle different data types", func() {
			mock := &mockRows{
				fieldDescriptions: []pgconn.FieldDescription{
					{Name: "id", DataTypeOID: pgtype.Int4OID},
					{Name: "name", DataTypeOID: pgtype.TextOID},
					{Name: "active", DataTypeOID: pgtype.BoolOID},
					{Name: "value", DataTypeOID: pgtype.Float8OID},
				},
				rows: [][]any{
					{1, "row1", true, 1.23},
				},
			}
			result, err := rowsToMaps(mock)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveKeyWithValue("id", 1))
			Expect(result[0]).To(HaveKeyWithValue("name", "row1"))
			Expect(result[0]).To(HaveKeyWithValue("active", true))
			Expect(result[0]).To(HaveKeyWithValue("value", 1.23))
			Expect(mock.closeCalled).To(BeTrue())
		})
	})
})
