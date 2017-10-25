package difftable

import (
	"bytes"
	"errors"
)

// Table is an interface supporting iteration over rows.
type Table interface {
	Key() []string
	Cols() map[string]string
	Row() Row
	Next() (bool, error)
}

// Row is an interface representing a row being iterated and compared.
type Row interface {
	Bytes(col string) []byte
	Value(col string) interface{}
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

// TypeChange describes a column type change.
type TypeChange struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// ValueChange describes a value change.
type ValueChange struct {
	Old interface{} `json:"old"`
	New interface{} `json:"new"`
}

// RowDiff contains a row-level set of value changes and the key
// identifying the row.
type RowDiff struct {
	Key     map[string]interface{}  `json:"key"`
	Changes map[string]*ValueChange `json:"changes"`
}

// Diff takes two tables and diffs them. If diffRows is true, value-level changes
// will be reported as well.
func Diff(t1, t2 Table, diffRows bool) (*TableDiff, error) {
	key1 := t1.Key()
	key2 := t2.Key()

	if len(key1) == 0 || len(key2) == 0 {
		return nil, errors.New("a key must be provided")
	}

	if len(key1) != len(key2) {
		return nil, errors.New("keys are different lengths")
	}

	diff := TableDiff{
		ColsAdded:   make([]string, 0),
		ColsDropped: make([]string, 0),
		TypeChanges: make(map[string]*TypeChange),
	}

	cols1 := t1.Cols()
	cols2 := t2.Cols()

	// For lookup.
	keyMap := make(map[string]struct{}, len(key1))
	for _, c := range key1 {
		keyMap[c] = struct{}{}
	}

	// Columns to check when comparing rows.
	var cmpcols []string

	for c, ty1 := range cols1 {
		// Both exist check for type changes.
		if ty2, ok := cols2[c]; ok {
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
	for c := range cols2 {
		if _, ok := cols1[c]; !ok {
			diff.ColsAdded = append(diff.ColsAdded, c)
		}
	}

	var (
		// Flags for whether to call next for the respective table.
		n1 = true
		n2 = true

		// Key values as bytes for determine next in sequence.
		k1 = make([][]byte, len(key1))
		k2 = make([][]byte, len(key2))

		// Next call was ok.
		ok1 bool
		ok2 bool

		err error
	)

	// Single references.
	var r1, r2 Row

	for {
		// Advance to rows.
		if n1 {
			n1 = false
			ok1, err = t1.Next()
			if err != nil {
				return nil, err
			}

			r1 = t1.Row()

			// Set key.
			if ok1 {
				for i, c := range key1 {
					k1[i] = r1.Bytes(c)
				}
			}
		}

		if n2 {
			n2 = false
			ok2, err = t2.Next()
			if err != nil {
				return nil, err
			}

			r2 = t2.Row()

			// Set key.
			if ok2 {
				for i, c := range key2 {
					k2[i] = r2.Bytes(c)
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
				diff.NewRows = append(diff.NewRows, newValueMap(r2, cols2))
			}
			continue
		}

		// No more rows in new table.
		if !ok2 {
			diff.RowsDeleted++
			n1 = true

			if diffRows {
				diff.DeletedRows = append(diff.DeletedRows, newKeyMap(r1, key1))
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
				diff.DeletedRows = append(diff.DeletedRows, newKeyMap(r1, key1))
			}
			continue
		}

		// Row seen in new table, but not old table, thus it has been added.
		if p == 1 {
			diff.RowsAdded++
			n2 = true

			if diffRows {
				diff.NewRows = append(diff.NewRows, newValueMap(r2, cols2))
			}
			continue
		}

		if len(cmpcols) > 0 {
			var rd *RowDiff

			// Row keys match, compare column values.
			for _, c := range cmpcols {
				if !bytes.Equal(r1.Bytes(c), r2.Bytes(c)) {
					// Stop on first difference.
					if !diffRows {
						diff.RowsChanged++
						break
					}

					// Initialze row diff.
					if rd == nil {
						rd = &RowDiff{
							Key:     newKeyMap(r1, key1),
							Changes: make(map[string]*ValueChange),
						}
					}

					rd.Changes[c] = &ValueChange{
						Old: r1.Value(c),
						New: r2.Value(c),
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

func newValueMap(r Row, cols map[string]string) map[string]interface{} {
	c := make(map[string]interface{}, len(cols))
	for k := range cols {
		c[k] = r.Value(k)
	}

	return c
}

func newKeyMap(r Row, cols []string) map[string]interface{} {
	c := make(map[string]interface{}, len(cols))
	for _, k := range cols {
		c[k] = r.Value(k)
	}

	return c
}
