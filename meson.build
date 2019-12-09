project('sqlite-parquet-vtable', 'c', 'cpp', default_options:['c_std=c11', 'cpp_std=c++11'])
parquet = dependency('parquet', version: '1.5.1')
sqlite = dependency('sqlite3', version: '>=3.21.0')
icu = dependency('icu-i18n')
shared_library('sqlite-parquet-vtable', 
	dependencies:[parquet, sqlite, icu],
	sources: ['src/parquet.cc', 'src/parquet_cursor.cc', 'src/parquet_filter.cc', 'src/parquet_table.cc']
)
