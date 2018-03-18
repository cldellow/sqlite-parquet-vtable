# sqlite-parquet-vtable

A SQLite [virtual table](https://sqlite.org/vtab.html) extension to expose Parquet files as SQL tables.

## Caveats

I'm not an experienced C/C++ programmer. This library is definitely not bombproof. It's good enough for my use case,
and may be good enough for yours, too.

* I don't use `sqlite3_malloc` and `sqlite3_free` for C++ objects
  * Maybe this doesn't matter, since portability isn't a goal
* The C -> C++ interop definitely leaks some C++ exceptions
  * Obvious cases like file not found and unsupported Parquet types are OK
  * Low memory conditions aren't handled gracefully.

## Building

1. Install [`parquet-cpp`](https://github.com/apache/parquet-cpp)
    1. Master appears to be broken for text row group stats; see https://github.com/cldellow/sqlite-parquet-vtable/issues/5 for which versions to use
2. Run `./build-sqlite` to fetch and build the SQLite dev bits
3. Run `./parquet/make` to build the module
    1. You will need to fixup the paths in this file to point at your local parquet-cpp folder.

## Tests

Run:

```
tests/create-queries-from-templates
tests/test-all
```

## Use

```
$ sqlite/sqlite3
sqlite> .load parquet/libparquet
sqlite> CREATE VIRTUAL TABLE demo USING parquet('parquet-generator/99-rows-1.parquet');
sqlite> SELECT * FROM demo;
...if all goes well, you'll see data here!...
```

Note: if you get an error like:

```
sqlite> .load parquet/libparquet
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
