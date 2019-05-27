package difftable

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/linkedin/goavro"
)

type avroRow struct {
	record map[string]interface{}
}

func (r *avroRow) Bytes(col string) []byte {
	val := r.record[col]
	return []byte(fmt.Sprint(val))
}

func (r *avroRow) Value(col string) interface{} {
	return r.record[col]
}

type avroTable struct {
	rdr    *goavro.OCFReader
	key    []string
	cols   map[string]string
	record map[string]interface{}
}

func (a *avroTable) Key() []string {
	return a.key
}

func (a *avroTable) Cols() map[string]string {
	return a.cols
}

func (a *avroTable) Row() Row {
	return &avroRow{
		record: a.record,
	}
}

func (a *avroTable) Next() (bool, error) {
	if !a.rdr.Scan() {
		return false, a.rdr.Err()
	}

	datum, err := a.rdr.Read()
	if err != nil {
		return false, err
	}

	a.record = datum.(map[string]interface{})

	return true, nil
}

func AvroTable(rdr *goavro.OCFReader, key []string, renames map[string]string) (Table, error) {
	for i, k := range key {
		if n, ok := renames[k]; ok {
			key[i] = n
		}
	}

	var m map[string]interface{}
	err := json.Unmarshal([]byte(rdr.Codec().Schema()), &m)
	if err != nil {
		return nil, err
	}

	if t, ok := m["type"].(string); !ok || t != "record" {
		return nil, fmt.Errorf("record schema required, got %s", t)
	}

	fields, ok := m["fields"].([]interface{})
	if !ok {
		return nil, errors.New("could not parse record fields")
	}

	cols := make(map[string]string)
	for _, x := range fields {
		f := x.(map[string]interface{})

		name, ok := f["name"].(string)
		if !ok {
			return nil, errors.New("invalid field name")
		}
		ftype, ok := f["type"].(string)
		if !ok {
			return nil, errors.New("invalid field type")
		}
		cols[name] = ftype
	}

	return &avroTable{
		rdr:  rdr,
		key:  key,
		cols: cols,
	}, nil
}
