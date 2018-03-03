package difftable

import (
	"bytes"
	"errors"
	"fmt"
	"time"
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

const (
	EventColumnAdded   = "column-added"
	EventColumnChanged = "column-changed"
	EventColumnRemoved = "column-removed"
	EventRowAdded      = "row-added"
	EventRowChanged    = "row-changed"
	EventRowRemoved    = "row-removed"
	EventRowStored     = "row-stored"
)

type Event struct {
	Type    string                  `json:"type"`
	Time    int64                   `json:"time"`
	Offset  int64                   `json:"offset,omitempty"`
	Column  string                  `json:"column,omitempty"`
	OldType string                  `json:"old_type,omitempty"`
	NewType string                  `json:"new_type,omitempty"`
	Key     map[string]interface{}  `json:"key,omitempty"`
	Data    map[string]interface{}  `json:"data,omitempty"`
	Changes map[string]*ValueChange `json:"changes,omitempty"`
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

func Snapshot(t1 Table, h func(e *Event)) error {
	key1 := t1.Key()
	if len(key1) == 0 {
		return errors.New("a key must be provided")
	}

	ts := time.Now().Unix()
	cols1 := t1.Cols()
	k1 := make([][]byte, len(key1))

	var (
		r1     Row
		ok     bool
		err    error
		offset int64
	)

	for {
		// 1-based offset.
		offset++

		ok, err = t1.Next()

		// Error fetching row.
		if err != nil {
			return err
		}

		// Done.
		if !ok {
			break
		}

		// Reference row.
		r1 = t1.Row()

		// Set key.
		for i, c := range key1 {
			k1[i] = r1.Bytes(c)
		}

		// Emit event.
		h(&Event{
			Type:   EventRowStored,
			Time:   ts,
			Offset: offset,
			Key:    newKeyMap(r1, key1),
			Data:   newValueMap(r1, cols1),
		})
	}

	return nil
}

func DiffEvents(t1, t2 Table, h func(e *Event)) error {
	key1 := t1.Key()
	key2 := t2.Key()

	if len(key1) == 0 || len(key2) == 0 {
		return errors.New("a key must be provided")
	}

	if len(key1) != len(key2) {
		return errors.New("keys are different lengths")
	}

	ts := time.Now().Unix()

	cols1 := t1.Cols()
	cols2 := t2.Cols()

	// Validate both tables have the key columns.
	// Build lookups key columns.
	key1Map := make(map[string]struct{}, len(key2))
	for _, c := range key1 {
		if _, ok := cols1[c]; !ok {
			return fmt.Errorf("table 1 does not have key column `%s`", c)
		}
		key1Map[c] = struct{}{}
	}

	key2Map := make(map[string]struct{}, len(key2))
	for _, c := range key2 {
		if _, ok := cols2[c]; !ok {
			return fmt.Errorf("table 2 does not have key column `%s`", c)
		}
		key2Map[c] = struct{}{}
	}

	// Columns to check when comparing rows.
	var (
		cmpCols  []string
		dropCols []string
		newCols  []string
	)

	for c, ty1 := range cols1 {
		// Both exist check for type changes.
		if ty2, ok := cols2[c]; ok {
			// Not a shared key column. Mark for comparison.
			_, ok1 := key1Map[c]
			_, ok2 := key2Map[c]
			if !(ok1 && ok2) {
				cmpCols = append(cmpCols, c)
			}

			// Emit the type difference.
			if ty1 != ty2 {
				h(&Event{
					Type:    EventColumnChanged,
					Time:    ts,
					Column:  c,
					OldType: ty1,
					NewType: ty2,
				})
			}

			// Does not exist in new cols. Mark as dropped.
		} else {
			dropCols = append(dropCols, c)

			h(&Event{
				Type:   EventColumnRemoved,
				Time:   ts,
				Column: c,
			})

		}
	}

	// Check for new columns.
	for c := range cols2 {
		// New column.
		if _, ok := cols1[c]; !ok {
			newCols = append(newCols, c)

			h(&Event{
				Type:   EventColumnAdded,
				Time:   ts,
				Column: c,
			})
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
	var offset int64

	for {
		offset++

		// Advance to rows.
		if n1 {
			n1 = false
			ok1, err = t1.Next()
			if err != nil {
				return err
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
				return err
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

		// No more rows in old table.
		if !ok1 {
			n2 = true
			h(&Event{
				Type:   EventRowAdded,
				Time:   ts,
				Offset: offset,
				Key:    newKeyMap(r2, key2),
				Data:   newValueMap(r2, cols2),
			})

			continue
		}

		// No more rows in new table.
		if !ok2 {
			n1 = true

			h(&Event{
				Type:   EventRowRemoved,
				Time:   ts,
				Offset: offset,
				Key:    newKeyMap(r1, key1),
			})
			continue
		}

		// Check if keys match.
		p := compareRows(k1, k2)

		// Row seen in old table, but not new table, thus it has been deleted.
		if p == -1 {
			n1 = true

			h(&Event{
				Type:   EventRowRemoved,
				Time:   ts,
				Offset: offset,
				Key:    newKeyMap(r1, key1),
			})
			continue
		}

		// Row seen in new table, but not old table, thus it has been added.
		if p == 1 {
			n2 = true

			h(&Event{
				Type:   EventRowAdded,
				Time:   ts,
				Offset: offset,
				Key:    newKeyMap(r2, key2),
				Data:   newValueMap(r2, cols2),
			})
			continue
		}

		// Records have the same key. Compare the column-level values.
		changes := make(map[string]*ValueChange)

		for _, c := range cmpCols {
			if !bytes.Equal(r1.Bytes(c), r2.Bytes(c)) {
				changes[c] = &ValueChange{
					Old: r1.Value(c),
					New: r2.Value(c),
				}
			}
		}

		// Columns that have been dropped.
		for _, c := range dropCols {
			changes[c] = &ValueChange{
				Old: r1.Value(c),
				New: nil,
			}
		}

		// Columns that are new, just set the changes.
		for _, c := range newCols {
			changes[c] = &ValueChange{
				Old: nil,
				New: r2.Value(c),
			}
		}

		if len(changes) > 0 {
			h(&Event{
				Type:    EventRowChanged,
				Time:    ts,
				Offset:  offset,
				Key:     newKeyMap(r1, key1),
				Changes: changes,
			})
		}

		// Advance both.
		n1 = true
		n2 = true
	}

	return nil
}

// Diff takes two tables and diffs them. If diffRows is true, value-level changes
// will be reported as well.
func Diff(t1, t2 Table, diffRows bool) (*TableDiff, error) {
	// Initial empty values for proper JSON encoding..
	diff := TableDiff{
		ColsAdded:   make([]string, 0),
		ColsDropped: make([]string, 0),
		TypeChanges: make(map[string]*TypeChange),
		RowDiffs:    make([]*RowDiff, 0),
		NewRows:     make([]map[string]interface{}, 0),
		DeletedRows: make([]map[string]interface{}, 0),
	}

	err := DiffEvents(t1, t2, func(e *Event) {
		diff.TotalRows = e.Offset

		switch e.Type {
		case EventColumnAdded:
			diff.ColsAdded = append(diff.ColsAdded, e.Column)

		case EventColumnRemoved:
			diff.ColsDropped = append(diff.ColsDropped, e.Column)

		case EventColumnChanged:
			diff.TypeChanges[e.Column] = &TypeChange{
				Old: e.OldType,
				New: e.NewType,
			}

		case EventRowAdded:
			diff.RowsAdded++
			if diffRows {
				diff.NewRows = append(diff.NewRows, e.Data)
			}

		case EventRowRemoved:
			diff.RowsDeleted++
			if diffRows {
				diff.DeletedRows = append(diff.DeletedRows, e.Key)
			}

		case EventRowChanged:
			diff.RowsChanged++
			if diffRows {
				diff.RowDiffs = append(diff.RowDiffs, &RowDiff{
					Key:     e.Key,
					Changes: e.Changes,
				})
			}
		}
	})

	if err != nil {
		return nil, err
	}

	return &diff, nil
}
