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


def read(filename):
    '''Take a filename, read it into a variable -- critically, closing it after we're done'''
    with open(filename) as file:
        return file.read()


def dispatch(query_file, results_file, expected_exit_code, datasets):
    '''Run a query against assorted formats of the datasets, verifying the result each time'''
    query = read(query_file)
    expected_results = read(results_file)
    for dataset in datasets:
        for file in os.listdir(dataset):
            if not file.endswith('.parquet'):
                print(f'Ignoring {file} -- does not end in .parquet')
                continue
            vtable_statement = f'CREATE VIRTUAL TABLE dataset USING parquet(\'{file}\');'
            # Append test-specified query to common lines, insert \n between lines
            full_query = '\n'.join(COMMON_QUERY_LINES+[vtable_statement, query])
            proc = subprocess.run(
                'sqlite3',
                stdout=subprocess.PIPE,
                input=full_query,
                encoding='UTF-8',
                check=False
            )
            assert proc.returncode == expected_exit_code
            assert proc.stdout == expected_results


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description=DESCRIPTION)
    PARSER.add_argument('query', metavar='query', help='the .sql file containing the query')
    PARSER.add_argument('results', help='the file containing .output format expected results')
    PARSER.add_argument('exit_code', help='the exit code you expect to recieve')
    PARSER.add_argument('dataset', nargs='+', help='a dataset to run the query against')

    ARGS = PARSER.parse_args()

    # Verify that each query and result file exist
    assert os.path.isfile(ARGS.query)
    assert os.path.isfile(ARGS.results)
    # Verify that each dataset argument is a folder (ideally with .parquet files inside)
    for dataset_dir in ARGS.dataset:
        assert os.path.isdir(dataset_dir)

    dispatch(
        query_file=ARGS.query,
        results_file=ARGS.results,
        expected_exit_code=ARGS.exit_code,
        datasets=ARGS.dataset
    )
