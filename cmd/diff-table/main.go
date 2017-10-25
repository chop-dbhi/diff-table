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
		keyList  string
		diffRows bool

		url1    string
		schema1 string
		table1  string

		url2    string
		schema2 string
		table2  string
	)

	flag.StringVar(&keyList, "key", "", "Required comma-separate list of columns.")
	flag.BoolVar(&diffRows, "diff", false, "Diff row values and output changes.")

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

	// TODO: remove hard-coded postgres dependency
	db1, err := sql.Open("postgres", url1)
	if err != nil {
		log.Printf("db1 open: %s", err)
		return
	}
	defer db1.Close()

	db2, err := sql.Open("postgres", url2)
	if err != nil {
		log.Printf("db2 open: %s", err)
		return
	}
	defer db2.Close()

	rows1, err := runQuery(db1, schema1, table1, key)
	if err != nil {
		log.Printf("db1 query: %s", err)
		return
	}
	defer rows1.Close()

	rows2, err := runQuery(db2, schema2, table2, key)
	if err != nil {
		log.Printf("db2 query: %s", err)
		return
	}
	defer rows2.Close()

	t1, err := difftable.SQLTable(rows1, key)
	if err != nil {
		log.Printf("db1 table: %s", err)
		return
	}

	t2, err := difftable.SQLTable(rows2, key)
	if err != nil {
		log.Printf("db2 table: %s", err)
		return
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
