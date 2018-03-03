# diff-table

A tool to compare two tables of data. Currently the tool supported tables from Postgres and CSV files.

The primary use case is to compare the output of a query executed at different points in time. For example, in a batch ETL process that runs every night, you can compare the previous batch with the new batch.

## Install

Download a [release](https://github.com/chop-dbhi/diff-table/releases) or install from source:

```
go get -u github.com/chop-dbhi/diff-table/cmd/diff-table
```

## Usage

The minimum requirement is to specify two tables (CSV or relational) and specify the key columns. For example, using the provided example data in the repository, the command would look like this.

```
diff-table \
  -csv1 example/file1.csv \
  -csv2 example/file2.csv \
  -key id
```

The default output is a JSON encoded object with a summary of the differences, including column and row changes.

```json
{
  "total_rows": 4,
  "columns_added": [
    "city"
  ],
  "columns_dropped": [],
  "type_changes": {},
  "rows_added": 1,
  "rows_deleted": 1,
  "rows_changed": 1
}
```

Adding the `-diff` option in the command will result in row-level changes in the output.

```
diff-table \
  -csv1 example/file1.csv \
  -csv2 example/file2.csv \
  -key id \
  -diff
```

```json
{
  "total_rows": 4,
  "columns_added": [
    "city"
  ],
  "columns_dropped": [],
  "type_changes": {},
  "rows_added": 1,
  "rows_deleted": 1,
  "rows_changed": 2,
  "row_diffs": [
    {
      "key": {
        "id": "1"
      },
      "changes": {
        "city": {
          "old": null,
          "new": "Trenton"
        },
        "color": {
          "old": "Blue",
          "new": "Teal"
        }
      }
    },
    {
      "key": {
        "id": "3"
      },
      "changes": {
        "city": {
          "old": null,
          "new": "Philadelphia"
        }
      }
    }
  ],
  "new_rows": [
    {
      "city": "Allentown",
      "color": "Black",
      "gender": "Male",
      "id": "4",
      "name": "Neal"
    }
  ],
  "deleted_rows": [
    {
      "id": "2"
    }
  ]
}
```

### Events

The above type of output is convenient for summary usage, however in some use cases a set of events may be more useful. Using the `-events` option will result in the changes being streamed as they are discovered rather than aggregating everything up into a single output object.

```
diff-table \
  -csv1 example/file1.csv \
  -csv2 example/file2.csv \
  -key id \
  -events
```

The output is a newline-delimited set of JSON-encoded events. Column-based changes will always come first. The `type` field denotes the type of event which can be switched on during consumption. The base event structured looks as follows:

```js
{
  // Event name/type.
  "type": "row-added",

  // Unix epoch timestamp in seconds.
  "time": 1520114848,

  // Offset of the row in the batch, if applicable. This can also be used as an
  // ordering mechanism.
  "offset": 1,

  // Key of the row this event pertains to, if applicable.
  "key": {...},

  // Full snapshot of the row. This will be present row row-added events, but
  // can be optionally included for row-changed and row-removed events.
  "data": {...},

  // Set of changes found relative to the old row. Each key is the column
  // name and the value is an object with `old` and `new` keys with the
  // respective values.
  "changes": {...},

  // If a column -added, -changed, or -removed event, this is the name of the
  // column affected.
  "column": "city"

  // If a column-changed event, this is the old type.
  "old_type": "int32",

  // If a column-changed event, this is the new type.
  "new_type": "int64"
}
```

```json
{
  "type": "column-added",
  "column": "city"
}
{
  "type": "row-changed",
  "offset": 1,
  "key": {
    "id": "1"
  },
  "changes": {
    "city": {
      "old": null,
      "new": "Trenton"
    },
    "color": {
      "old": "Blue",
      "new": "Teal"
    }
  }
}
{
  "type": "row-removed",
  "offset": 2,
  "key": {
    "id": "2"
  }
}
{
  "type": "row-changed",
  "offset": 3,
  "key": {
    "id": "3"
  },
  "changes": {
    "city": {
      "old": null,
      "new": "Philadelphia"
    }
  }
}
{
  "type": "row-added",
  "offset": 4,
  "key": {
    "id": "4"
  },
  "data": {
    "city": "Allentown",
    "color": "Black",
    "gender": "Male",
    "id": "4",
    "name": "Neal"
  }
}
```

#### Snapshots

In addition to change events, snapshots are supported which scans a table and emits a `row-stored` event including the current state of each row. These events are structurally equivalent to `row-added` events which include the full `data`.

The use case for them is to initialize a stream from a stateful starting point, but can be used over time to make new starting points for consumers. This prevents consumers from needing to know what the starting state was when the stream was initiated as well as not needing to read from the very beginning of the stream in order to build up the current state. With snapshots, the consumer can start from the last known snapshot and consume from there.

To create a snapshot, use the `-snapshot` option with the newest table or CSV file specified.

```
diff-table \
  -csv1 example/file1.csv \
  -key id \
  -snapshot
```

## Examples

Below are examples of how tables can be specified including SQL-based tables and CSV files (with sorted or unsorted rows) and how columns can be renamed (not in the data source, just in the `diff-table` runtime) before the tables are compared.

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

### Tables with renamed columns

The `data_v1.foo` column will be renamed to `bar` (just in the `diff-table` runtime, not in the source) and compared to the `data_v2.bar` column. The same will happen for the `baz:buz` column rename.

```
diff-table \
  -db postgres://localhost:5432/postgres \
  -table1 data_v1 \
  -rename1 "foo:bar,baz:buz" \
  -table2 data_v2 \
  -key id
```
