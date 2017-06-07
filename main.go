package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/lib/pq"
)

func main() {
	var (
		keyList  string
		diffRows bool

		db     string
		schema string
		table1 string
		table2 string

		db2     string
		schema2 string
	)

	flag.StringVar(&keyList, "key", "", "Required comma-separate list of columns.")

	flag.BoolVar(&diffRows, "diff", false, "Diff row values and output changes.")

	flag.StringVar(&db, "db", "", "Database 1 connection URL.")
	flag.StringVar(&schema, "schema", "", "Name of the first schema.")

	flag.StringVar(&table1, "table1", "", "Name of the first table.")
	flag.StringVar(&table2, "table2", "", "Name of the second table.")

	// Optional table 2 database and schema.
	flag.StringVar(&db2, "db2", "", "Database 2 connection URL. Defaults to db option.")
	flag.StringVar(&schema2, "schema2", "", "Name of the second schema. Default to schema option.")

	flag.Parse()

	key := strings.Split(keyList, ",")

	if db2 == "" {
		db2 = db
	}

	if schema2 == "" {
		schema2 = schema
	}

	t1 := &TableIterator{
		URL:    db,
		Schema: schema,
		Table:  table1,
		Key:    key,
	}

	t2 := &TableIterator{
		URL:    db2,
		Schema: schema2,
		Table:  table2,
		Key:    key,
	}

	diff, err := Diff(t1, t2, diffRows)
	if err != nil {
		log.Fatal(err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	if err := enc.Encode(diff); err != nil {
		log.Fatal(err)
	}
}

type TypeChange struct {
	Old string
	New string
}

type TableDiff struct {
	TotalRows   int64                    `json:"total_rows"`
	ColsAdded   []string                 `json:"columns_added"`
	ColsDropped []string                 `json:"columns_dropped"`
	TypeChanges map[string]*TypeChange   `json:"type_changes"`
	RowsAdded   int                      `json:"rows_added"`
	RowsDeleted int                      `json:"rows_deleted"`
	RowsChanged int                      `json:"rows_changed"`
	RowDiffs    []*RowDiff               `json:"row_diffs,omitempty"`
	NewRows     []map[string]interface{} `json:"new_rows,omitempty"`
	DeletedRows []map[string]interface{} `json:"deleted_rows,omitempty"`
}

type ValueChange struct {
	Old interface{} `json:"old"`
	New interface{} `json:"new"`
}

type RowDiff struct {
	Key     map[string]interface{}  `json:"key"`
	Changes map[string]*ValueChange `json:"changes"`
}

func Diff(t1, t2 *TableIterator, diffRows bool) (*TableDiff, error) {
	if len(t1.Key) == 0 || len(t2.Key) == 0 {
		return nil, errors.New("a key must be provided")
	}

	if len(t1.Key) != len(t2.Key) {
		return nil, errors.New("keys are different lengths")
	}

	if err := t1.Open(); err != nil {
		return nil, err
	}
	defer t1.Close()

	if err := t2.Open(); err != nil {
		return nil, err
	}
	defer t2.Close()

	diff := TableDiff{
		ColsAdded:   make([]string, 0),
		ColsDropped: make([]string, 0),
		TypeChanges: make(map[string]*TypeChange),
	}

	t1cols := t1.Cols()
	t2cols := t2.Cols()

	// For lookup.
	keyMap := make(map[string]struct{}, len(t1.Key))
	for _, c := range t1.Key {
		keyMap[c] = struct{}{}
	}

	// Columns to check when comparing rows.
	var cmpcols []string

	for c, ty1 := range t1cols {
		// Both exist check for type changes.
		if ty2, ok := t2cols[c]; ok {
			if ty1 != ty2 {
				diff.TypeChanges[c] = &TypeChange{
					Old: ty1,
					New: ty2,
				}
				continue
			}

			// Add non-key column for row comparison.
			if _, ok := keyMap[c]; !ok {
				cmpcols = append(cmpcols, c)
			}
			continue
		}

		// Column doesn't exist in t2, thus is was dropped.
		diff.ColsDropped = append(diff.ColsDropped, c)
	}

	// Check for new columns.
	for c := range t2cols {
		if _, ok := t1cols[c]; !ok {
			diff.ColsAdded = append(diff.ColsAdded, c)
		}
	}

	// Reset for simpler check below.
	if len(cmpcols) == 0 {
		cmpcols = nil
	}

	var (
		// Flags for whether to call next for the respective table.
		n1 = true
		n2 = true

		k1 = make([][]byte, len(t1.Key))
		k2 = make([][]byte, len(t2.Key))

		err error
		ok1 bool
		ok2 bool
	)

	// Single references.
	r1 := t1.Row()
	r2 := t2.Row()

	for {
		// Advance to rows.
		if n1 {
			n1 = false
			err, ok1 = t1.Next()
			if err != nil {
				return nil, err
			}

			// Set key.
			if ok1 {
				for i, c := range t1.Key {
					k1[i] = r1.Get(c)
				}
			}
		}

		if n2 {
			n2 = false
			err, ok2 = t2.Next()
			if err != nil {
				return nil, err
			}

			// Set key.
			if ok2 {
				for i, c := range t2.Key {
					k2[i] = r2.Get(c)
				}
			}
		}

		// Done.
		if !ok1 && !ok2 {
			break
		}

		diff.TotalRows++

		// No more rows in old table.
		if !ok1 {
			diff.RowsAdded++
			n2 = true
			if diffRows {
				diff.NewRows = append(diff.NewRows, r2.GetValues())
			}
			continue
		}

		// No more rows in new table.
		if !ok2 {
			diff.RowsDeleted++
			n1 = true

			if diffRows {
				rkey := make(map[string]interface{}, len(t1.Key))
				for _, kc := range t1.Key {
					rkey[kc] = r1.GetValue(kc)
				}
				diff.DeletedRows = append(diff.DeletedRows, rkey)
			}
			continue
		}

		// Check if keys match.
		p := compareRows(k1, k2)

		// Row seen in old table, but not new table, thus it has been deleted.
		if p == -1 {
			diff.RowsDeleted++
			n1 = true

			if diffRows {
				rkey := make(map[string]interface{}, len(t1.Key))
				for _, kc := range t1.Key {
					rkey[kc] = r1.GetValue(kc)
				}
				diff.DeletedRows = append(diff.DeletedRows, rkey)
			}
			continue
		}

		// Row seen in new table, but not old table, thus it has been added.
		if p == 1 {
			diff.RowsAdded++
			n2 = true
			if diffRows {
				diff.NewRows = append(diff.NewRows, r2.GetValues())
			}
			continue
		}

		if cmpcols != nil {
			var rd *RowDiff

			// Row keys match, compare column values.
			for _, c := range cmpcols {
				if !bytes.Equal(r1.Get(c), r2.Get(c)) {
					// Stop on first difference.
					if !diffRows {
						diff.RowsChanged++
						break
					}

					// Initialze row diff.
					if rd == nil {
						// Copy key.
						rkey := make(map[string]interface{}, len(t1.Key))
						for _, kc := range t1.Key {
							rkey[kc] = r1.GetValue(kc)
						}

						rd = &RowDiff{
							Key:     rkey,
							Changes: make(map[string]*ValueChange),
						}
					}

					rd.Changes[c] = &ValueChange{
						Old: r1.GetValue(c),
						New: r2.GetValue(c),
					}
				}
			}

			if rd != nil {
				diff.RowsChanged++
				diff.RowDiffs = append(diff.RowDiffs, rd)
			}
		}

		// Advance both.
		n1 = true
		n2 = true
	}

	return &diff, nil
}

func compareRows(r1, r2 [][]byte) int {
	for i, v1 := range r1 {
		if p := bytes.Compare(v1, r2[i]); p != 0 {
			return p
		}
	}
	return 0
}

type TableIterator struct {
	URL    string
	Schema string
	Table  string
	Key    []string

	db   *sql.DB
	rows *sql.Rows

	cols     []string
	colMap   map[string]int
	colTypes map[string]string

	bdest []interface{}
	bvals [][]byte

	rdest []interface{}
	rvals []interface{}
}

func (t *TableIterator) Open() error {
	db, err := sql.Open("postgres", t.URL)
	if err != nil {
		return err
	}
	t.db = db

	var table string
	if t.Schema != "" {
		table = fmt.Sprintf(
			"%s.%s",
			pq.QuoteIdentifier(t.Schema),
			pq.QuoteIdentifier(t.Table),
		)
	} else {
		table = pq.QuoteIdentifier(t.Table)
	}

	orderBy := make([]string, len(t.Key))
	for i, c := range t.Key {
		orderBy[i] = pq.QuoteIdentifier(c)
	}

	stmt := fmt.Sprintf("select * from %s order by %s", table, strings.Join(orderBy, ", "))

	rows, err := db.Query(stmt)
	if err != nil {
		return err
	}
	t.rows = rows

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	t.cols = cols

	// Create map of column name to index in the array.
	t.colMap = make(map[string]int, len(cols))
	t.colTypes = make(map[string]string, len(cols))

	for i, c := range cols {
		t.colMap[c] = i
		t.colTypes[c] = ""
	}

	// Bytes.
	t.bvals = make([][]byte, len(cols))
	t.bdest = make([]interface{}, len(cols))

	// Values.
	t.rvals = make([]interface{}, len(cols))
	t.rdest = make([]interface{}, len(cols))

	for i := range t.bdest {
		t.bdest[i] = &t.bvals[i]
		t.rdest[i] = &t.rvals[i]
	}

	return nil
}

func (t *TableIterator) Close() error {
	if err := t.rows.Close(); err != nil {
		return err
	}
	return t.db.Close()
}

func (t *TableIterator) Cols() map[string]string {
	return t.colTypes
}

func (t *TableIterator) Row() *Row {
	return &Row{
		colTypes: t.colTypes,
		colMap:   t.colMap,
		bvals:    t.bvals,
		bdest:    t.bdest,
		rvals:    t.rvals,
		rdest:    t.rdest,
	}
}

func (t *TableIterator) Next() (error, bool) {
	if !t.rows.Next() {
		return nil, false
	}

	// Scan as byte representations and real values.
	if err := t.rows.Scan(t.bdest...); err != nil {
		return err, false
	}

	if err := t.rows.Scan(t.rdest...); err != nil {
		return err, false
	}

	return nil, true
}

type Row struct {
	colTypes map[string]string
	colMap   map[string]int
	bdest    []interface{}
	bvals    [][]byte
	rdest    []interface{}
	rvals    []interface{}
}

// Get returns returns a column value as a byte array.
// This is used for comparision.
func (r *Row) Get(col string) []byte {
	i, ok := r.colMap[col]
	if !ok {
		return nil
	}

	return r.bvals[i]
}

// GetValue returns a column value in the native type.
func (r *Row) GetValue(col string) interface{} {
	i, ok := r.colMap[col]
	if !ok {
		return nil
	}

	if x, ok := r.rvals[i].([]byte); ok {
		return string(x)
	}

	return r.rvals[i]
}

func (r *Row) GetValues() map[string]interface{} {
	c := make(map[string]interface{}, len(r.rvals))
	for k, i := range r.colMap {
		if x, ok := r.rvals[i].([]byte); ok {
			c[k] = string(x)
		} else {
			c[k] = r.rvals[i]
		}
	}

	return c
}
