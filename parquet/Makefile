PARQUET_CPP=~/src/parquet-cpp

CC = g++
CFLAGS = -I ../sqlite -O3 -std=c++11 -Wall -fPIC -g
PARQUET_LIB = $(PARQUET_CPP)/build/release/libparquet.a
THRIFT_LIB = $(PARQUET_CPP)/thrift_ep/src/thrift_ep-install/lib/libthrift.a
LZ4_LIB = /tmp/parquet-deps/lz4/lib/liblz4.a
ARROW_LIB = $(PARQUET_CPP)/arrow_ep-prefix/src/arrow_ep-build/release/libarrow.a
SNAPPY_LIB = /tmp/parquet-deps/snappy/build/libsnappy.a
ZSTD_LIB = /tmp/parquet-deps/zstd/lib/libzstd.a
BOOST_LIB = /usr/lib/x86_64-linux-gnu/libboost_regex.so
BOOST_SYSTEM_LIB = /usr/lib/x86_64-linux-gnu/libboost_system.so
BOOST_FILESYSTEM_LIB = /usr/lib/x86_64-linux-gnu/libboost_filesystem.so
BROTLI_COMMON_LIB = /tmp/parquet-deps/brotli/out/libbrotlicommon-static.a
BROTLI_ENC_LIB = /tmp/parquet-deps/brotli/out/libbrotlienc-static.a
BROTLI_DEC_LIB = /tmp/parquet-deps/brotli/out/libbrotlidec-static.a

LDFLAGS = -O3 \
	  -Wl,--whole-archive $(PARQUET_LIB) $(LZ4_LIB) $(ZSTD_LIB) $(THRIFT_LIB) $(SNAPPY_LIB) $(ARROW_LIB) \
	      $(BROTLI_ENC_LIB) $(BROTLI_COMMON_LIB) $(BROTLI_DEC_LIB) \
	  -Wl,--no-whole-archive -lz -lcrypto -lssl $(BOOST_LIB) $(BOOST_SYSTEM_LIB) $(BOOST_FILESYSTEM_LIB)
OBJ = parquet.o parquet_filter.o parquet_table.o parquet_cursor.o

libparquet.so: $(OBJ)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

parquet_filter.o: parquet_filter.cc parquet_filter.h
	$(CC) -c -o $@ $< $(CFLAGS)

parquet_cursor.o: parquet_cursor.cc parquet_cursor.h parquet_table.h parquet_filter.h
	$(CC) -c -o $@ $< $(CFLAGS)

parquet_table.o: parquet_table.cc parquet_table.h
	$(CC) -c -o $@ $< $(CFLAGS)

parquet.o: parquet.cc parquet_cursor.h parquet_table.h parquet_filter.h
	$(CC) -c -o $@ $< $(CFLAGS)

.PHONY: clean

clean:
	rm -f *.o *.so
