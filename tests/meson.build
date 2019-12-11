
# For more test args see https://mesonbuild.com/Unit-tests.html#other-test-options
runner = find_program('runner.py')

test('Load the extension', runner, args: files('NOTHING', 'NOTHING')+['0'])
test('Try loading a non-existent file', runner, args: files('./segfault-safety/nonexistent-file.sql', 'NOTHING')+['1'])
test('Try loading an unspecified file', runner, args: files('./segfault-safety/unspecified-file.sql', 'NOTHING')+['1'])
test('Try loading an unsupported file', runner, args: files('NOTHING', 'NOTHING')+['1','unsupported'])
test('Load all supported test files', runner, args: files('./segfault-safety/load-valid-file/query.sql', './segfault-safety/load-valid-file/results.txt')+['0','no-nulls','nulls'])
