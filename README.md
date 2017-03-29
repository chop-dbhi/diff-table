# diff-table

A tool to compare two tables of data. Currently the tool only supports executing queries against a Postgres database, but there are plans to add more SQL drivers and flat file support.

The primary use case is to compare the output of a query executed at different points in time. For example, in a batch ETL process that runs every night, you can compare the previous batch with the new batch.

## Install

Download a [release](https://github.com/chop-dbhi/diff-table/releases).

## Usage

This is a minimum example which shows the required options.

```
diff-table \
  -db postgres://localhost:5432/postgres \
  -table1 data_v1 \
  -table2 data_v2 \
  -key id
```

Here are the full set of options.

```
Usage of diff-table:
  -db string
    	Database 1 connection URL.
  -db2 string
    	Database 2 connection URL. Defaults to db option.
  -diff
    	Diff row values and output new rows and changes.
  -key string
    	Required comma-separate list of columns.
  -schema string
    	Name of the first schema.
  -schema2 string
    	Name of the second schema. Defaults to schema option.
  -table1 string
    	Name of the first table.
  -table2 string
    	Name of the second table.
```

## Exampl

```
diff-table \
  -db postgres://localhost:5432/postgres \
  -table1 data_v1 \
  -table2 data_v2 \
  -key id
```

The output is a JSON encoded value which various information about the table differences. If `-diff` is supplied, the `row_diffs` and `new_rows` are tracked and outputed as well. Note, this may significantly increase memory usage if the tables are vastly different.

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


