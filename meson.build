project('sqlite-parquet-vtable', 'c', 'cpp', default_options:['c_std=c11', 'cpp_std=c++11'])
arrow = dependency('arrow', version:'0.15.1')
parquet = dependency('parquet', version: '1.5.1')
sqlite = dependency('sqlite3', version: '>=3.7.17')
icu = dependency('icu-i18n', version: '>=50.2')

dependencies = [arrow, parquet, sqlite, icu]
sources = ['src/parquet.cc', 'src/parquet_cursor.cc', 'src/parquet_filter.cc', 'src/parquet_table.cc']
shared_library('parquet', 
	dependencies: dependencies,
	sources: sources 
)

test('Load the extension', find_program('tests/test-smoke'))
test('Try loading an unsupported file', find_program('tests/test-bad-create-table'))
test('Try loading a non-existent file', find_program('tests/test-non-existent'))

