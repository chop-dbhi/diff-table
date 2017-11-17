package difftable

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
)

var bom = []byte{0xef, 0xbb, 0xbf}

// uniReader wraps an io.Reader to replace carriage returns with newlines.
// This is used with the csv.Reader so it can properly delimit lines.
type uniReader struct {
	r io.Reader
}

func (r *uniReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)

	// Detect and remove BOM.
	if bytes.HasPrefix(buf, bom) {
		copy(buf, buf[len(bom):])
		n -= len(bom)
	}

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}

func (r *uniReader) Close() error {
	if rc, ok := r.r.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

func NewCSVReader(r io.Reader, d rune) *csv.Reader {
	cr := csv.NewReader(&uniReader{r})
	cr.Comma = d
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true
	cr.ReuseRecord = true
	return cr
}

func CSVTable(cr *csv.Reader, key []string) (Table, error) {
	cols, err := cr.Read()
	if err != nil {
		return nil, err
	}

	// Create map of column name to index in the array.
	colMap := make(map[string]int, len(cols))
	colTypes := make(map[string]string, len(cols))

	for i, c := range cols {
		colMap[c] = i
		colTypes[c] = "string"
	}

	return &csvTable{
		rows:     cr,
		key:      key,
		colLen:   len(cols),
		colMap:   colMap,
		colTypes: colTypes,
	}, nil
}

type csvTable struct {
	rows *csv.Reader
	key  []string

	colLen   int
	colMap   map[string]int
	colTypes map[string]string

	row []string
}

func (t *csvTable) Key() []string {
	return t.key
}

func (t *csvTable) Cols() map[string]string {
	return t.colTypes
}

func (t *csvTable) Row() Row {
	return &csvRow{
		colMap: t.colMap,
		row:    t.row,
	}
}

func (t *csvTable) Next() (bool, error) {
	t.row = nil

	row, err := t.rows.Read()
	if err != nil {
		// Done.
		if err == io.EOF {
			return false, nil
		}

		return false, err
	}

	if len(row) != t.colLen {
		return false, fmt.Errorf("expected %d columns, got %d", t.colLen, len(row))
	}

	t.row = row

	return true, nil
}

type csvRow struct {
	// Unused by unsorted CSV table.
	key    string
	colMap map[string]int
	row    []string
}

func (r *csvRow) Bytes(col string) []byte {
	i, ok := r.colMap[col]
	if !ok {
		return nil
	}

	return []byte(r.row[i])
}

func (r *csvRow) Value(col string) interface{} {
	i, ok := r.colMap[col]
	if !ok {
		return nil
	}

	return r.row[i]
}
