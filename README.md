# sqlite-parquet-vtable

A SQLite [virtual table](https://sqlite.org/vtab.html) extension to expose Parquet files as SQL tables.

## Caveats

I'm not a professional C/C++ programmer. These are the caveats I'm aware of, but there are probably others:

* I don't use `sqlite3_malloc` and `sqlite3_free` for C++ objects
  * Maybe this doesn't matter, since portability isn't a goal
* The C (SQLite API implementation) -> C++ interop (to talk to parquet-cpp) probably leaks some C++ exceptions
  * Your process may crash due to my error. Sorry!
  * I handle the obvious cases like file not found and unsupported Parquet types but I suspect low memory conditions aren't handled gracefully

## Building

1. Install [`parquet-cpp`](https://github.com/apache/parquet-cpp)
2. Run `./build-sqlite` to fetch and build the SQLite dev bits
3. Run `./parquet/make` to build the module
  1. You will need to fixup the paths in this file to point at your local parquet-cpp folder.

## Use

```
$ sqlite/sqlite3
sqlite> .load parquet/libparquet
sqlite> create virtual table demo USING parquet('demo.parquet');
sqlite> select * from demo limit 1;
...if all goes well, you'll see data here!...
```

## Supported features

### Index

Only full table scans are supported.

### Types

These types are supported:

* INT96 timestamps (exposed as milliseconds since the epoch)
* INT8/INT16/INT32/INT64
* UTF8 strings
* BOOLEAN
* FLOAT
* DOUBLE

These are not supported:

* UINT8/UINT16/UINT32/UINT64
* Fixed length byte arrays, including JSON and BSON subtypes
* DECIMAL
