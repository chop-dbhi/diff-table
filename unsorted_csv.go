package difftable

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const (
	indexName = "results_key_index"
)

func newDb(cr *csv.Reader, table string, head, key []string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	cols := make([]string, len(head))

	for i, col := range head {
		cols[i] = fmt.Sprintf("`%s` TEXT", col)
	}

	keyCols := make([]string, len(key))
	for i, col := range key {
		keyCols[i] = fmt.Sprintf("`%s`", col)
	}

	stmts := []string{
		fmt.Sprintf(`CREATE TABLE %s (%s)`, table, strings.Join(cols, ",\n")),
		fmt.Sprintf(`CREATE INDEX %s ON %s (%s)`, indexName, table, strings.Join(keyCols, ",")),
	}

	for _, stmt := range stmts {
		if _, err = db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func insertStmt(table string, head []string) string {
	header := make([]string, len(head))
	for i, c := range head {
		header[i] = fmt.Sprintf("`%s`", c)
	}

	params := make([]string, len(head))

	for i, _ := range params {
		params[i] = "?"
	}

	return fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES (%s)
	`, table, strings.Join(header, ","), strings.Join(params, ","))
}

func CsvDB(cr *csv.Reader, table string, key []string) (*sql.DB, error) {
	head, err := cr.Read()
	if err != nil {
		return nil, err
	}

	db, err := newDb(cr, table, head, key)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return nil, err
	}
	defer tx.Rollback()

	sql := insertStmt(table, head)
	stmt, err := tx.Prepare(sql)
	if err != nil {
		db.Close()
		return nil, err
	}

	vals := make([]interface{}, len(head))

	for {
		row, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			stmt.Close()
			tx.Rollback()
			db.Close()
			return nil, err
		}

		for i, s := range row {
			vals[i] = s
		}

		if _, err := stmt.Exec(vals...); err != nil {
			stmt.Close()
			tx.Rollback()
			db.Close()
			return nil, err
		}
	}

	stmt.Close()

	if err := tx.Commit(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
