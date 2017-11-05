package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	difftable "github.com/chop-dbhi/diff-table"
	"github.com/lib/pq"
)

func main() {
	var (
		keyList  string
		diffRows bool

		csv1      string
		csv1delim string
		csv1sort  bool

		csv2      string
		csv2delim string
		csv2sort  bool

		url1    string
		schema1 string
		table1  string

		url2    string
		schema2 string
		table2  string
	)

	flag.StringVar(&keyList, "key", "", "Required comma-separate list of columns.")
	flag.BoolVar(&diffRows, "diff", false, "Diff row values and output changes.")

	flag.StringVar(&csv1, "csv1", "", "Path to CSV file.")
	flag.StringVar(&csv1delim, "csv1.delim", ",", "CSV delimiter.")
	flag.BoolVar(&csv1sort, "csv1.sort", false, "CSV requires sorting.")

	flag.StringVar(&csv2, "csv2", "", "Path to CSV file.")
	flag.StringVar(&csv2delim, "csv2.delim", ",", "CSV delimiter.")
	flag.BoolVar(&csv2sort, "csv2.sort", false, "CSV requires sorting.")

	flag.StringVar(&url1, "db", "", "Database 1 connection URL.")
	flag.StringVar(&schema1, "schema", "", "Name of the first schema.")
	flag.StringVar(&table1, "table1", "", "Name of the first table.")

	flag.StringVar(&url2, "db2", "", "Database 2 connection URL. Defaults to db option.")
	flag.StringVar(&schema2, "schema2", "", "Name of the second schema. Default to schema option.")
	flag.StringVar(&table2, "table2", "", "Name of the second table.")

	flag.Parse()

	if keyList == "" {
		log.Print("key list required")
		return
	}

	key := strings.Split(keyList, ",")

	if url2 == "" {
		url2 = url1
	}

	if schema2 == "" {
		schema2 = schema1
	}

	var (
		t1, t2   difftable.Table
		db1, db2 *sql.DB
		err      error
	)

	if csv1 != "" && url1 != "" {
		log.Print("can't both a csv and db source defined")
		return
	}

	if csv2 != "" && url2 != "" {
		log.Print("can't both a csv and db target defined")
		return
	}

	if csv1 != "" {
		f1, err := os.Open(csv1)
		if err != nil {
			log.Printf("csv1 open: %s", err)
			return
		}
		defer f1.Close()

		cr1 := csv.NewReader(&uniReader{f1})

		cr1.Comma = rune(csv1delim[0])
		cr1.LazyQuotes = true
		cr1.TrimLeadingSpace = true
		cr1.ReuseRecord = true

		if csv1sort {
			t1, err = difftable.UnsortedCSVTable(cr1, key)
			if err != nil {
				log.Printf("csv1 table: %s", err)
				return
			}
		} else {
			t1, err = difftable.CSVTable(cr1, key)
			if err != nil {
				log.Printf("csv1 table: %s", err)
				return
			}
		}
	}

	if csv2 != "" {
		f2, err := os.Open(csv2)
		if err != nil {
			log.Printf("csv2 open: %s", err)
			return
		}
		defer f2.Close()

		cr2 := csv.NewReader(&uniReader{f2})

		cr2.Comma = rune(csv2delim[0])
		cr2.LazyQuotes = true
		cr2.TrimLeadingSpace = true
		cr2.ReuseRecord = true

		if csv2sort {
			t2, err = difftable.UnsortedCSVTable(cr2, key)
			if err != nil {
				log.Printf("csv2 table: %s", err)
				return
			}
		} else {
			t2, err = difftable.CSVTable(cr2, key)
			if err != nil {
				log.Printf("csv2 table: %s", err)
				return
			}
		}
	}

	if url1 != "" {
		// TODO: remove hard-coded postgres dependency
		db1, err = sql.Open("postgres", url1)
		if err != nil {
			log.Printf("db1 open: %s", err)
			return
		}
		defer db1.Close()
	}

	if db1 != nil {
		rows1, err := runQuery(db1, schema1, table1, key)
		if err != nil {
			log.Printf("db1 query: %s", err)
			return
		}
		defer rows1.Close()

		t1, err = difftable.SQLTable(rows1, key)
		if err != nil {
			log.Printf("db1 table: %s", err)
			return
		}
	}

	if url2 != "" {
		db2, err = sql.Open("postgres", url2)
		if err != nil {
			log.Printf("db2 open: %s", err)
			return
		}
		defer db2.Close()
	}

	if db2 != nil {
		rows2, err := runQuery(db2, schema2, table2, key)
		if err != nil {
			log.Printf("db2 query: %s", err)
			return
		}
		defer rows2.Close()

		t2, err = difftable.SQLTable(rows2, key)
		if err != nil {
			log.Printf("db2 table: %s", err)
			return
		}
	}

	diff, err := difftable.Diff(t1, t2, diffRows)
	if err != nil {
		log.Printf("diff: %s", err)
		return
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	if err := enc.Encode(diff); err != nil {
		log.Printf("json: %s", err)
		return
	}
}

func runQuery(db *sql.DB, schema, table string, key []string) (*sql.Rows, error) {
	var qtable string

	if schema != "" {
		qtable = fmt.Sprintf(
			"%s.%s",
			pq.QuoteIdentifier(schema),
			pq.QuoteIdentifier(table),
		)
	} else {
		qtable = pq.QuoteIdentifier(table)
	}

	orderBy := make([]string, len(key))
	for i, c := range key {
		orderBy[i] = pq.QuoteIdentifier(c)
	}

	stmt := fmt.Sprintf(`
		select *
		from %s
		order by %s
	`, qtable, strings.Join(orderBy, ", "))

	return db.Query(stmt)
}

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
