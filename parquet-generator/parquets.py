from datetime import datetime, timedelta
import re

import pyarrow as pa
import pyarrow.parquet as pq

def make_100_rows():
    '''Create 100 rows with unique values in each field, exercising all the main
       physical types.'''
    rows = []
    for i in range(100):
        ba_fixed = bytearray()
        ba_fixed.append(i)
        ba_variable = bytearray()
        for j in range(i):
            ba_variable.append(j)
        row = []
        # BOOLEAN, INT32, INT64, INT96, DOUBLE, BYTE_ARRAY, FLOAT
        row.append(i % 2 == 0) # BOOLEAN
        row.append(50 - i) # INT32/INT8
        row.append(100 * (50 - i)) # INT32/INT16
        row.append(1000 * 1000 * (50 - i)) # INT32/INT32
        row.append(1000 * 1000 * 1000 * (50 - i)) # INT64/INT64
        row.append(datetime(1985, 7, 20) + timedelta(days=i)) # INT96
        row.append(100.0 / (i + 1)) # DOUBLE
        row.append(str(i)) # BYTE_ARRAY/UTF8
        row.append('{:03}'.format(i)), # BYTE_ARRAY/UTF8
        row.append(bytes(ba_variable)), # BYTE_ARRAY
        row.append(bytes(ba_fixed)) # FIXED_LENGTH_BYTE_ARRAY
# pyarrow does not support float yet :(
#        row.append(1.0 / (i + 1)) # FLOAT

        rows.append(row)
    return rows

def get_100_rows_types():
    '''The types for the columns in `make_100_rows`.'''
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
        pa.binary(1)
#        pa.float32()
    ]

def write_parquet(file_name, rows, types, row_group_size):
    '''Create two parquets with columns we support.'''
    # pivot to be column major, create arrow structures
    fields = []
    for i in range(len(types)):
        col = []
        col.append([row[i] for row in rows])
        fields.append(pa.chunked_array(col, type=types[i]))

    def name_of(i):
        name = '{}_{}'.format(types[i], i)
        name = name.replace('timestamp[ns]', 'ts')
        name = name.replace('fixed_size_binary[1]', 'binary')
        return name

    cols = [pa.Column.from_array(name_of(i), fields[i]) for i in range(len(fields))]
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

def main():
    '''Entrypoint.'''
    rows = make_100_rows()
    types = get_100_rows_types()

    write_parquet('100-rows-1.parquet', rows, types, row_group_size=100)
    write_parquet('100-rows-10.parquet', rows, types, row_group_size=10)

    for i in range(len(rows)):
        for j in range(len(rows[i])):
            if (i >= 10 and i <= 19) or (i >= 20 and (i + j) % 2 == 0):
                rows[i][j] = None
    write_parquet('100-rows-nulls.parquet', rows, types,row_group_size=10)

    write_unsupported_parquets()

if __name__ == '__main__':
    main()
