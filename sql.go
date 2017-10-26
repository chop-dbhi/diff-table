package difftable

import "database/sql"

func SQLTable(rows *sql.Rows, key []string) (Table, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create map of column name to index in the array.
	colMap := make(map[string]int, len(cols))
	colTypes := make(map[string]string, len(cols))

	for i, c := range cols {
		colMap[c] = i
		colTypes[c] = ""
	}

	// Bytes.
	bvals := make([][]byte, len(cols))
	bdest := make([]interface{}, len(cols))

	// Values.
	rvals := make([]interface{}, len(cols))
	rdest := make([]interface{}, len(cols))

	for i := range bdest {
		bdest[i] = &bvals[i]
		rdest[i] = &rvals[i]
	}

	return &sqlTable{
		rows:     rows,
		key:      key,
		cols:     cols,
		colMap:   colMap,
		colTypes: colTypes,
		bvals:    bvals,
		bdest:    bdest,
		rvals:    rvals,
		rdest:    rdest,
	}, nil
}

type sqlTable struct {
	rows *sql.Rows
	key  []string

	cols     []string
	colMap   map[string]int
	colTypes map[string]string

	bdest []interface{}
	bvals [][]byte

	rdest []interface{}
	rvals []interface{}
}

func (t *sqlTable) Key() []string {
	return t.key
}

func (t *sqlTable) Cols() map[string]string {
	return t.colTypes
}

func (t *sqlTable) Row() Row {
	return &sqlRow{
		colTypes: t.colTypes,
		colMap:   t.colMap,
		bvals:    t.bvals,
		bdest:    t.bdest,
		rvals:    t.rvals,
		rdest:    t.rdest,
	}
}

func (t *sqlTable) Next() (bool, error) {
	if !t.rows.Next() {
		return false, nil
	}

	// Scan as byte representations and real values.
	if err := t.rows.Scan(t.bdest...); err != nil {
		return false, err
	}

	if err := t.rows.Scan(t.rdest...); err != nil {
		return false, err
	}

	return true, nil
}

type sqlRow struct {
	colTypes map[string]string
	colMap   map[string]int
	bdest    []interface{}
	bvals    [][]byte
	rdest    []interface{}
	rvals    []interface{}
}

// Get returns returns a column value as a byte array.
// This is used for comparision.
func (r *sqlRow) Bytes(col string) []byte {
	i, ok := r.colMap[col]
	if !ok {
		return nil
	}

	return r.bvals[i]
}

// GetValue returns a column value in the native type.
func (r *sqlRow) Value(col string) interface{} {
	i, ok := r.colMap[col]
	if !ok {
		return nil
	}

	if x, ok := r.rvals[i].([]byte); ok {
		return string(x)
	}

	return r.rvals[i]
}