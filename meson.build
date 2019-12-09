project('sqlite-parquet-vtable', 'c', 'cpp', default_options:['c_std=c11', 'cpp_std=c++11'])
arrow = dependency('arrow', version:'0.15.1')
parquet = dependency('parquet', version: '1.5.1')
sqlite = dependency('sqlite3', version: '>=3.21.0')
icu = dependency('icu-i18n')

dependencies = [arrow, parquet, sqlite, icu]
sources = ['src/parquet.cc', 'src/parquet_cursor.cc', 'src/parquet_filter.cc', 'src/parquet_table.cc']
static_library('sqlite-parquet-vtable-static', 
	dependencies: dependencies,
	sources: sources 
)
shared_library('sqlite-parquet-vtable', 
	dependencies: dependencies,
	sources: sources 
)
