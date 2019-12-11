#!/usr/bin/python3
import argparse
import os
import subprocess

DESCRIPTION = '''sqlite-parquet-vtable test runner
Run a query against several data files with varying encodings,
and verify that the output matches an expected value
'''

COMMON_QUERY_LINES = [
        '.echo off',
        '.load ./libparquet',
]

TEST_ROOT = os.path.dirname(os.path.abspath(__file__))

def read(filename):
    '''Take a filename, read it into a variable -- critically, closing it after we're done'''
    with open(filename) as file:
        return file.read()

def run_and_verify(query, expected_results, expected_exit_code):
    '''Run a given query, compare the results and error code'''
    proc = subprocess.run(
        'sqlite3',
        stdout=subprocess.PIPE,
        input=query,
        encoding='UTF-8',
        check=False
    )
    assert proc.returncode == expected_exit_code, f'Expected exit code {expected_exit_code}, found {proc.returncode}'
    assert proc.stdout == expected_results, f'Expected stdout to be {expected_results}, it was actually {proc.stdout}'

def dispatch(query_file, results_file, expected_exit_code, datasets):
    '''Run a query against assorted formats of the datasets, verifying the result each time'''
    query = read(query_file)
    expected_results = read(results_file)
    # Some of the error test cases don't need multiple datasets
    if len(datasets) == 0:
        run_and_verify(query, expected_results, expected_exit_code)

    # For most tests, run against all members of a dataset
    for dataset in datasets:
        for file in os.listdir(TEST_ROOT+'/datasets/'+dataset):
            filepath = TEST_ROOT+'/datasets/'+dataset+'/'+file
            if not filepath.endswith('.parquet'):
                print(f'Ignoring {file} -- does not end in .parquet')
                continue
            vtable_statement = f'CREATE VIRTUAL TABLE dataset USING parquet(\'{filepath}\');'
            # Append test-specified query to common lines, insert \n between lines
            full_query = '\n'.join(COMMON_QUERY_LINES+[vtable_statement, query])
            run_and_verify(full_query, expected_results, expected_exit_code)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description=DESCRIPTION)
    PARSER.add_argument('query', help='the .sql file containing the query to run')
    PARSER.add_argument('results', help='the file containing the expected result of the query')
    PARSER.add_argument('exit_code', help='the exit code you expect to recieve')
    PARSER.add_argument('dataset', nargs='*', help='a dataset to run the query against')

    ARGS = PARSER.parse_args()

    # Verify that each query and result file exist
    assert os.path.isfile(ARGS.query), f'Expected to find query file at {ARGS.query}'
    assert os.path.isfile(ARGS.results), f'Expected to find results file at {ARGS.results}'
    # Verify that each dataset argument is a folder (ideally with .parquet files inside)
    for dataset_dir in ARGS.dataset:
        assert os.path.isdir(TEST_ROOT+'/datasets/'+dataset_dir), f'Expected dataset to be a directory'

    dispatch(
        query_file=ARGS.query,
        results_file=ARGS.results,
        expected_exit_code=int(ARGS.exit_code),
        datasets=ARGS.dataset
    )
