package difftable

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strings"
)

// csvRows implements sort.Interface.
type csvRows []*csvRow

func (r csvRows) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r csvRows) Len() int {
	return len(r)
}

func (r csvRows) Less(i, j int) bool {
	return strings.Compare(r[i].key, r[j].key) == -1
}

func copySlice(s []string) []string {
	c := make([]string, len(s))
	copy(c, s)
	return c
}

func UnsortedCSVTable(cr *csv.Reader, key []string) (Table, error) {
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

	keyLen := len(key)
	keyIdx := make([]int, keyLen)
	for i, k := range key {
		keyIdx[i] = colMap[k]
	}

	makeKey := func(r []string) string {
		k := make([]string, keyLen)
		for i, x := range keyIdx {
			k[i] = r[x]
		}
		return strings.Join(k, "|")
	}

	var rows csvRows
	for {
		r, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		rows = append(rows, &csvRow{
			colMap: colMap,
			key:    makeKey(r),
			row:    copySlice(r),
		})
	}

	sort.Sort(rows)

	return &unsortedCsvTable{
		rows:     rows,
		len:      len(rows),
		key:      key,
		colLen:   len(cols),
		colMap:   colMap,
		colTypes: colTypes,
	}, nil
}

type unsortedCsvTable struct {
	rows csvRows
	len  int
	idx  int

	key []string

	colLen   int
	colMap   map[string]int
	colTypes map[string]string

	row *csvRow
}

func (t *unsortedCsvTable) Key() []string {
	return t.key
}

func (t *unsortedCsvTable) Cols() map[string]string {
	return t.colTypes
}

func (t *unsortedCsvTable) Row() Row {
	return t.row
}

func (t *unsortedCsvTable) Next() (bool, error) {
	// No more.
	if t.idx == t.len {
		return false, nil
	}

	row := t.rows[t.idx]

	if len(row.row) != t.colLen {
		return false, fmt.Errorf("expected %d columns, got %d", t.colLen, len(row.row))
	}

	t.row = row
	t.idx++

	return true, nil
}
