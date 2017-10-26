# diff-table

A tool to compare two tables of data. Currently the tool supported tables from Postgres and CSV files.

The primary use case is to compare the output of a query executed at different points in time. For example, in a batch ETL process that runs every night, you can compare the previous batch with the new batch.

## Install

Download a [release](https://github.com/chop-dbhi/diff-table/releases) or install from source:

```
go get -u github.com/chop-dbhi/diff-table/cmd/diff-table
```

## Usage

```
diff-table \
  -db postgres://localhost:5432/postgres \
  -table1 data_v1 \
  -table2 data_v2 \
  -key id
```

The output is a JSON encoded value which various information about the table differences. If `-diff` is supplied, `row_diffs`, `new_rows`, and `deleted_rows` are included as well. Note, this may significantly increase memory usage if the tables are vastly different.

```
{
  "total_rows": 2055856,
  "columns_added": [],
  "columns_dropped": [],
  "type_changes": {},
  "rows_added": 1,
  "rows_deleted": 0,
  "rows_changed": 1,
  "row_diffs": [
    {
      "key": {
        "id": 2009
      },
      "changes": {
        "val": {
          "old": 0.7576323747634888,
          "new": 1.323199987411499
        }
      }
    }
  ],
  "new_rows": [
    {
      "id": 2010,
      "val": 1.53921932383223
    }
  ]
}
```

## Examples

### Tables in the same database

```
diff-table \
  -db postgres://localhost:5432/postgres \
  -table1 data_v1 \
  -table2 data_v2 \
  -key id
```

### Tables from different servers/databases

```
diff-table \
  -db postgres://localhost:5432/postgres \
  -db2 postgres://localhost:5435/other \
  -table1 data_v1 \
  -table2 data_v2 \
  -key id
```

### CSV files

*Note: this assumes the CSV files are pre-sorted by the specified key columns.*

```
diff-table \
  -csv1 data_v1.csv \
  -csv2 data_v2.csv  \
  -key id
```

### Unsorted CSV files

```
diff-table \
  -csv1 data_v1.csv \
  -csv1.sort \
  -csv2 data_v2.csv  \
  -csv2.sort \
  -key id
```

### CSV file and database table (o.O)

*Note: this assumes the CSV file is pre-sorted by the specified key columns.*

```
diff-table \
  -csv1 data_v1.csv \
  -db2 postgres://localhost:5432/postgres \
  -table2 data_v2 \
  -key id
```
