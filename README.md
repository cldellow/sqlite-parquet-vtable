# sqlite-parquet-vtable

[![Build Status](https://travis-ci.org/cldellow/sqlite-parquet-vtable.svg?branch=master)](https://travis-ci.org/cldellow/sqlite-parquet-vtable)
[![codecov](https://codecov.io/gh/cldellow/sqlite-parquet-vtable/branch/master/graph/badge.svg)](https://codecov.io/gh/cldellow/sqlite-parquet-vtable)

A SQLite [virtual table](https://sqlite.org/vtab.html) extension to expose Parquet files as SQL tables. You may also find [csv2parquet](https://github.com/cldellow/csv2parquet/) useful.

This [blog post](https://cldellow.com/2018/06/22/sqlite-parquet-vtable.html) provides some context on why you might use this.

## Installing

### Download

You can fetch a version built for Ubuntu 16.04 at https://s3.amazonaws.com/cldellow/public/libparquet/libparquet.so.xz

### Building

```
./make-linux
```

The first run will git clone a bunch of libraries, patch them to be statically linkable and build them.

Subsequent builds will only build the parquet virtual table extension.

### Building (release)

Run `./make-linux-pgo` to build an instrumented binary, run tests to collect real-life usage samples, then build an optimized binary. PGO seems to give a 5-10% reduction in query times.

### Tests

Run:

```
tests/create-queries-from-templates
tests/test-all
```


## Use

```
$ sqlite/sqlite3
sqlite> .load build/linux/libparquet
sqlite> CREATE VIRTUAL TABLE demo USING parquet('parquet-generator/99-rows-1.parquet');
sqlite> SELECT * FROM demo;
...if all goes well, you'll see data here!...
```

Note: if you get an error like:

```
sqlite> .load build/linux/libparquet
Error: parquet/libparquet.so: wrong ELF class: ELFCLASS64
```

You have the 32-bit SQLite installed. To fix this, do:

```
sudo apt-get remove --purge sqlite3
sudo apt-get install sqlite3:amd64
```

## Supported features

### Row group filtering

Row group filtering is supported for strings and numerics so long as the SQLite
type matches the Parquet type.

e.g. if you have a column `foo` that is an INT32, this query will skip row groups whose
statistics prove that it does not contain relevant rows:

```
SELECT * FROM tbl WHERE foo = 123;
```

but this query will devolve to a table scan:

```
SELECT * FROM tbl WHERE foo = '123';
```

This is laziness on my part and could be fixed without too much effort.

### Row filtering

For common constraints, the row is checked to see if it satisfies the query's
constraints before returning control to SQLite's virtual machine. This minimizes
the number of allocations performed when many rows are filtered out by
the user's criteria.

### Memoized slices

Individual clauses are mapped to the row groups they match.

eg going on row group statistics, which store minimum and maximum values, a clause
like `WHERE city = 'Dawson Creek'` may match 80% of row groups.

In reality, it may only be present in one or two row groups.

This is recorded in a shadow table so future queries that contain that clause
can read only the necessary row groups.

### Types

These Parquet types are supported:

* INT96 timestamps (exposed as milliseconds since the epoch)
* INT8/INT16/INT32/INT64
* UTF8 strings
* BOOLEAN
* FLOAT
* DOUBLE
* Variable- and fixed-length byte arrays

These are not currently supported:

* UINT8/UINT16/UINT32/UINT64
* DECIMAL
