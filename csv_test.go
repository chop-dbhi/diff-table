package difftable

import (
	"bytes"
	"encoding/json"
	"testing"
)

var (
	csvTable1 = `id,name,gender,color
1,John,Male,Blue
2,Pam,Female,Red
3,Sam,Female,Yellow
`

	csvTable2 = `id,name,gender,color,city
1,John,Male,Teal,Trenton
3,Sam,Female,Yellow,Philadelphia
4,Neal,Male,Black,Allentown
`

	csvTableDiff = &TableDiff{
		TotalRows:   4,
		ColsAdded:   []string{"city"},
		ColsDropped: []string{},
		TypeChanges: make(map[string]*TypeChange),
		RowsAdded:   1,
		RowsDeleted: 1,
		RowsChanged: 2,
		NewRows: []map[string]interface{}{
			{
				"city":   "Allentown",
				"color":  "Black",
				"gender": "Male",
				"id":     "4",
				"name":   "Neal",
			},
		},
		DeletedRows: []map[string]interface{}{
			{
				"id": "2",
			},
		},
		RowDiffs: []*RowDiff{
			{
				Key: map[string]interface{}{
					"id": "1",
				},
				Changes: map[string]*ValueChange{
					"city": &ValueChange{
						Old: nil,
						New: "Trenton",
					},
					"color": &ValueChange{
						Old: "Blue",
						New: "Teal",
					},
				},
			},
			{
				Key: map[string]interface{}{
					"id": "3",
				},
				Changes: map[string]*ValueChange{
					"city": &ValueChange{
						Old: nil,
						New: "Philadelphia",
					},
				},
			},
		},
	}

	csvDiffEvents = []*Event{
		{
			Type:   EventColumnAdded,
			Column: "city",
		},
		{
			Type:   EventRowChanged,
			Offset: 1,
			Key: map[string]interface{}{
				"id": "1",
			},
			Changes: map[string]*ValueChange{
				"city": &ValueChange{
					Old: nil,
					New: "Trenton",
				},
				"color": &ValueChange{
					Old: "Blue",
					New: "Teal",
				},
			},
		},
		{
			Type:   EventRowRemoved,
			Offset: 2,
			Key: map[string]interface{}{
				"id": "2",
			},
		},
		{
			Type:   EventRowChanged,
			Offset: 3,
			Key: map[string]interface{}{
				"id": "3",
			},
			Changes: map[string]*ValueChange{
				"city": &ValueChange{
					Old: nil,
					New: "Philadelphia",
				},
			},
		},
		{
			Type:   EventRowAdded,
			Offset: 4,
			Key: map[string]interface{}{
				"id": "4",
			},
			Data: map[string]interface{}{
				"city":   "Allentown",
				"color":  "Black",
				"gender": "Male",
				"id":     "4",
				"name":   "Neal",
			},
		},
	}
)

func jsonEqual(v1, v2 interface{}) (string, string, bool) {
	b1, _ := json.Marshal(v1)
	b2, _ := json.Marshal(v2)
	return string(b1), string(b2), bytes.Equal(b1, b2)
}

func TestCsvTable(t *testing.T) {
	r1 := bytes.NewBufferString(csvTable1)
	c1 := NewCSVReader(r1, ',')

	r2 := bytes.NewBufferString(csvTable2)
	c2 := NewCSVReader(r2, ',')

	key := []string{"id"}

	t1, err := CSVTable(c1, key)
	t2, err := CSVTable(c2, key)

	diff, err := Diff(t1, t2, true)
	if err != nil {
		t.Fatal(err)
	}

	if s1, s2, ok := jsonEqual(csvTableDiff, diff); !ok {
		t.Errorf("diff doesn't match. expected:\n%sgot:\n%s", s1, s2)
	}
}

func TestCsvTableEvents(t *testing.T) {
	r1 := bytes.NewBufferString(csvTable1)
	c1 := NewCSVReader(r1, ',')

	r2 := bytes.NewBufferString(csvTable2)
	c2 := NewCSVReader(r2, ',')

	key := []string{"id"}

	t1, err := CSVTable(c1, key)
	t2, err := CSVTable(c2, key)

	var events []*Event
	err = DiffEvents(t1, t2, func(e *Event) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatal(err)
	}

	if s1, s2, ok := jsonEqual(csvDiffEvents, events); !ok {
		t.Errorf("diff events don't match. expected:\n%sgot:\n%s", s1, s2)
	}
}

var (
	unsortedCsvTable1 = `id,name,gender,color
2,Pam,Female,Red
3,Sam,Female,Yellow
1,John,Male,Blue
`
	unsortedCsvTable2 = `id,name,gender,color,city
4,Neal,Male,Black,Allentown
1,John,Male,Teal,Trenton
3,Sam,Female,Yellow,Philadelphia
`
)

func TestUnsortedCsvTable(t *testing.T) {
	r1 := bytes.NewBufferString(unsortedCsvTable1)
	c1 := NewCSVReader(r1, ',')

	r2 := bytes.NewBufferString(unsortedCsvTable2)
	c2 := NewCSVReader(r2, ',')

	key := []string{"id"}

	t1, err := UnsortedCSVTable(c1, key)
	t2, err := UnsortedCSVTable(c2, key)

	diff, err := Diff(t1, t2, true)
	if err != nil {
		t.Fatal(err)
	}

	if s1, s2, ok := jsonEqual(csvTableDiff, diff); !ok {
		t.Errorf("diff doesn't match. expected:\n%sgot:\n%s", s1, s2)
	}
}

func TestUnsortedCsvTableEvents(t *testing.T) {
	r1 := bytes.NewBufferString(unsortedCsvTable1)
	c1 := NewCSVReader(r1, ',')

	r2 := bytes.NewBufferString(unsortedCsvTable2)
	c2 := NewCSVReader(r2, ',')

	key := []string{"id"}

	t1, err := UnsortedCSVTable(c1, key)
	t2, err := UnsortedCSVTable(c2, key)

	var events []*Event
	err = DiffEvents(t1, t2, func(e *Event) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatal(err)
	}

	if s1, s2, ok := jsonEqual(csvDiffEvents, events); !ok {
		t.Errorf("diff events don't match. expected:\n%sgot:\n%s", s1, s2)
	}
}
