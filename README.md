# parquet-vtable

A SQLite [virtual table](https://sqlite.org/vtab.html) extension to expose Parquet files as SQL tables.

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
