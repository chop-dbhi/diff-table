package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	difftable "github.com/chop-dbhi/diff-table"
	"github.com/lib/pq"
)

func main() {
	var (
		key1List string
		key2List string
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

		events bool
	)

	flag.StringVar(&key1List, "key", "", "Comma-separate list of columns in table 1.")
	flag.StringVar(&key2List, "key2", "", "Comma-separate list of columns in table 2. Default to key option.")
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

	flag.BoolVar(&events, "events", false, "Write an event stream to stdout.")

	flag.Parse()

	if key1List == "" {
		log.Fatal("key required")
		return
	}

	key1 := strings.Split(key1List, ",")
	var key2 []string

	if key2List == "" {
		key2 = key1
	} else {
		key2 = strings.Split(key2List, ",")
	}

	if len(key1) != len(key2) {
		log.Fatal("keys must be the same length")
	}

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
		log.Fatal("can't both a csv and db source defined")
		return
	}

	if csv2 != "" && url2 != "" {
		log.Fatal("can't both a csv and db target defined")
		return
	}

	if csv1 != "" {
		f1, err := os.Open(csv1)
		if err != nil {
			log.Printf("csv1 open: %s", err)
			return
		}
		defer f1.Close()

		cr1 := difftable.NewCSVReader(f1, rune(csv1delim[0]))

		if csv1sort {
			t1, err = difftable.UnsortedCSVTable(cr1, key1)
			if err != nil {
				log.Printf("csv1 table: %s", err)
				return
			}
		} else {
			t1, err = difftable.CSVTable(cr1, key1)
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

		cr2 := difftable.NewCSVReader(f2, rune(csv2delim[0]))

		if csv2sort {
			t2, err = difftable.UnsortedCSVTable(cr2, key2)
			if err != nil {
				log.Printf("csv2 table: %s", err)
				return
			}
		} else {
			t2, err = difftable.CSVTable(cr2, key2)
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
		rows1, err := runQuery(db1, schema1, table1, key1)
		if err != nil {
			db1.Close()
			log.Printf("db1 query: %s", err)
			return
		}
		defer rows1.Close()

		t1, err = difftable.SQLTable(rows1, key1)
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
		rows2, err := runQuery(db2, schema2, table2, key2)
		if err != nil {
			db2.Close()
			log.Printf("db2 query: %s", err)
			return
		}
		defer rows2.Close()

		t2, err = difftable.SQLTable(rows2, key2)
		if err != nil {
			log.Printf("db2 table: %s", err)
			return
		}
	}

	enc := json.NewEncoder(os.Stdout)

	if events {
		err := difftable.DiffEvents(t1, t2, func(e *difftable.Event) {
			enc.Encode(e)
		})
		if err != nil {
			log.Printf("diff stream: %s", err)
			return
		}
	} else {
		diff, err := difftable.Diff(t1, t2, diffRows)
		if err != nil {
			log.Printf("diff: %s", err)
			return
		}

		if err := enc.Encode(diff); err != nil {
			log.Printf("json: %s", err)
			return
		}
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
