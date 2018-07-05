from datetime import datetime, timedelta, timezone
import re

import pyarrow as pa
import pyarrow.parquet as pq

def make_99_rows():
    '''Create 99 rows with unique values in each field, exercising all the main
       physical types.'''
    rows = []
    for i in range(99):
        ba_fixed = bytearray()
        ba_fixed.append(i)
        ba_variable = bytearray()
        for j in range(1 + i % 5):
            ba_variable.append(i)
        row = []
        # BOOLEAN, INT32, INT64, INT96, DOUBLE, BYTE_ARRAY, FLOAT
        row.append(i % 2 == 0) # BOOLEAN
        row.append(50 - i) # INT32/INT8
        row.append(100 * (50 - i)) # INT32/INT16
        row.append(1000 * 1000 * (50 - i)) # INT32/INT32
        row.append(1000 * 1000 * 1000 * (50 - i)) # INT64/INT64
        row.append(datetime(1985, 7, 20, tzinfo=timezone.utc) + timedelta(days=i)) # INT96
        row.append(99.0 / (i + 1)) # DOUBLE
        row.append(str(i)) # BYTE_ARRAY/UTF8
        row.append('{:03}'.format(i)), # BYTE_ARRAY/UTF8
        row.append(bytes(ba_variable)), # BYTE_ARRAY
        row.append(bytes(ba_fixed)) # FIXED_LENGTH_BYTE_ARRAY
        row.append(1.0 / (i + 1)) # FLOAT

        rows.append(row)
    return rows

def get_99_rows_types():
    '''The types for the columns in `make_99_rows`.'''
    return [
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.timestamp('ns'),
        pa.float64(),
        pa.string(),
        pa.string(),
        pa.binary(-1),
        pa.binary(1),
        pa.float32()
    ]

def name_of(type, i):
    name = '{}_{}'.format(type, i)
    name = name.replace('timestamp[ns]', 'ts')
    name = name.replace('fixed_size_binary[1]', 'binary')
    return name


def write_parquet(file_name, rows, types, row_group_size):
    '''Create two parquets with columns we support.'''
    # pivot to be column major, create arrow structures
    fields = []
    for i in range(len(types)):
        col = []
        col.append([row[i] for row in rows])
        fields.append(pa.chunked_array(col, type=types[i]))

    cols = [pa.Column.from_array(name_of(types[i], i), fields[i]) for i in range(len(fields))]
    table = pa.Table.from_arrays(cols)
    print('Writing {}'.format(file_name))
    pq.write_table(table,
                   file_name,
                   row_group_size=row_group_size,
                   use_deprecated_int96_timestamps=True)

def write_unsupported_parquets():
    # Taken from https://arrow.apache.org/docs/python/api.html
    unsupported = [
        pa.decimal128(10),
        pa.null(),
        pa.uint8(),
        pa.uint16(),
        # per https://issues.apache.org/jira/browse/ARROW-436, I think
        # Parquet v1.0 can't serialize UINT32
        #pa.uint32(),
        pa.uint64(),
        # pa.float16() <-- not supported by us, but also not by pyarrow
        # TODO: list_, struct, dict
    ]

    for type in unsupported:
        file_name = 'unsupported-{}.parquet'.format(type)
        file_name = re.sub(r'[^0-9a-z.-]', '-', file_name)
        file_name = re.sub(r'--*', '-', file_name)

        write_parquet(file_name, [], [type], row_group_size=1)

def write_csv(file_name, rows):
    r'''Write a TSV that can be imported to Postgres.

    Use "\N" for NULLs, tab literal for field separator.'''
    print('Writing {}'.format(file_name))
    with open(file_name, 'w') as f:
        for rowid, row in enumerate(rows):
            line = str(rowid + 1)
            for col in row:
                line += '\t'

                if col == True:
                    line += '1'
                elif col == False:
                    line += '0'
                elif col is None:
                    line += r'\N'
                elif isinstance(col, bytes):
                    # Here we cheat and serialize a string that matches the output of
                    # quote(binary_field) in SQLite
                    entry = r"X'"
                    for b in col:
                        entry += '%0.2X' % b

                    entry += "'"
                    line += entry
                elif isinstance(col, datetime):
                    line += str(1000 * int(col.timestamp()))
                else:
                    line += str(col)

            f.write(line + '\n')

def type_of(type):
    if type == pa.bool_():
        return 'BOOLEAN'
    elif type == pa.int8():
        return 'TINYINT'
    elif type == pa.int16():
        return 'SMALLINT'
    elif type == pa.int32():
        return 'INT'
    elif type == pa.int64() or type == pa.timestamp('ns'):
        return 'BIGINT'
    elif type == pa.float64():
        return 'DOUBLE'
    elif type == pa.string():
        return 'TEXT'
    elif type == pa.binary():
        return 'BLOB'
    elif type == pa.binary(1):
        return 'BLOB'
    elif type == pa.float32():
        return 'DOUBLE'
    else:
        raise ValueError('unknown type: {}'.format(type))

def write_sql(file_name, rows, types):
    table_name = file_name.replace('.sql', '').replace('-', '_')
    print('Writing {} [{}]'.format(file_name, table_name))

    with open(file_name, 'w') as f:
        f.write('BEGIN;')
        f.write('DROP TABLE IF EXISTS {};\n'.format(table_name))
        f.write('CREATE TABLE {} ('.format(table_name))
        for i, col in enumerate(types):
            if i > 0:
                f.write(', ');

            col_name = name_of(col, i)
            f.write('{} {}'.format(name_of(col, i), type_of(col)))
        f.write(');\n')

        for row in rows:
            f.write('INSERT INTO {} VALUES ('.format(table_name))
            line = ''
            for i, col in enumerate(row):
                if i > 0:
                    line += ', '

                if col == True:
                    line += '1'
                elif col == False:
                    line += '0'
                elif col is None:
                    line += 'NULL'
                elif isinstance(col, bytes):
                    entry = r"X'"
                    for b in col:
                        entry += '%0.2X' % b

                    entry += "'"
                    line += entry
                elif isinstance(col, datetime):
                    line += str(1000 * int(col.timestamp()))
                elif isinstance(col, str):
                    line += "'{}'".format(col)
                else:
                    line += str(col)

            f.write(line)
            f.write(');\n')
        f.write('COMMIT;\n')



def main():
    '''Entrypoint.'''
    rows = make_99_rows()
    types = get_99_rows_types()

    write_parquet('99-rows-1.parquet', rows, types, row_group_size=99)
    write_csv('no-nulls.csv', rows)
    write_parquet('99-rows-10.parquet', rows, types, row_group_size=10)
    write_parquet('99-rows-99.parquet', rows, types, row_group_size=1)
    write_sql('no-nulls.sql', rows, types)

    for i in range(len(rows)):
        for j in range(len(rows[i])):
            if (i >= 10 and i <= 19) or (i >= 20 and (i + j) % 2 == 0):
                rows[i][j] = None
    write_parquet('99-rows-nulls-99.parquet', rows, types,row_group_size=99)
    write_parquet('99-rows-nulls-10.parquet', rows, types,row_group_size=10)
    write_parquet('99-rows-nulls-1.parquet', rows, types,row_group_size=1)
    write_csv('nulls.csv', rows)
    write_sql('nulls.sql', rows, types)

    write_unsupported_parquets()

if __name__ == '__main__':
    main()
