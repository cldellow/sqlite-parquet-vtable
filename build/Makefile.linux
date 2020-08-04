HERE:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
ROOT:=$(HERE)/../..
VTABLE:=$(ROOT)/parquet
SQLITE:=$(ROOT)/sqlite

# Directories
ARROW=$(HERE)/arrow
ARROW_RELEASE=$(ARROW)/cpp/release
BOOST_ROOT=$(ARROW_RELEASE)/boost_ep-prefix/src/boost_ep
BOOST=$(BOOST_ROOT)/stage/lib
BROTLI=$(ARROW_RELEASE)/brotli_ep/src/brotli_ep-install/lib/x86_64-linux-gnu
ICU=$(HERE)/icu
LZ4=$(ARROW_RELEASE)/lz4_ep-prefix/src/lz4_ep/lib
PARQUET_CPP=$(HERE)/parquet-cpp
SNAPPY=$(ARROW_RELEASE)/snappy_ep/src/snappy_ep-install/lib
ZLIB=$(ARROW_RELEASE)/zlib_ep/src/zlib_ep-install/lib
ZSTD=$(ARROW_RELEASE)/zstd_ep-prefix/src/zstd_ep/lib

# Libraries
# profile_gen, profile_build for PGO
APACHE_BUILD=release

ARROW_LIB = $(ARROW_RELEASE)/$(APACHE_BUILD)/libarrow.a
BOOST_FILESYSTEM_LIB = $(BOOST)/libboost_filesystem.a
BOOST_REGEX_LIB = $(BOOST)/libboost_regex.a
BOOST_SYSTEM_LIB = $(BOOST)/libboost_system.a
BROTLI_COMMON_LIB = $(BROTLI)/libbrotlicommon.a
BROTLI_DEC_LIB = $(BROTLI)/libbrotlidec.a
BROTLI_ENC_LIB = $(BROTLI)/libbrotlienc.a
ICU_I18N_LIB=$(ICU)/source/lib/libicui18n.a
ICU_UC_LIB=$(ICU)/source/lib/libicuuc.a
ICU_DATA_LIB=$(ICU)/source/lib/libicudata.a
LZ4_LIB = $(LZ4)/liblz4.a
PARQUET_CPP_LIB = $(PARQUET_CPP)/build/$(APACHE_BUILD)/libparquet.a
SNAPPY_LIB = $(SNAPPY)/libsnappy.a
THRIFT_LIB = $(PARQUET_CPP)/thrift_ep/src/thrift_ep-install/lib/libthrift.a
ZLIB_LIB = $(ZLIB)/libz.a
ZSTD_LIB = $(ZSTD)/libzstd.a

# Flags
CC = gcc
CXX = g++
OPTIMIZATIONS = -O3
CPUS:=$(shell nproc)
CFLAGS = -I $(SQLITE) -I $(PARQUET_CPP)/src -I $(ARROW)/cpp/src $(OPTIMIZATIONS) -std=c++11 -Wall -fPIC -g

ALL_LIBS = $(PARQUET_CPP_LIB) $(LZ4_LIB) $(ZSTD_LIB) $(THRIFT_LIB) $(SNAPPY_LIB) $(ARROW_LIB) \
	  $(ICU_I18N_LIB) $(ICU_UC_LIB) $(ICU_DATA_LIB) \
	  $(BROTLI_ENC_LIB) $(BROTLI_COMMON_LIB) $(BROTLI_DEC_LIB) $(BOOST_REGEX_LIB) $(BOOST_SYSTEM_LIB) $(BOOST_FILESYSTEM_LIB)

LDFLAGS = $(OPTIMIZATIONS) \
	  -Wl,--whole-archive $(ALL_LIBS) \
	  -Wl,--no-whole-archive -lz -lcrypto -lssl
OBJ = parquet.o parquet_filter.o parquet_table.o parquet_cursor.o
LIBS = $(ARROW_LIB) $(PARQUET_CPP_LIB) $(ICU_I18N_LIB)

PROF =

libparquet.so: $(LIBS) $(OBJ)
	$(CXX) $(PROF) -shared -o $@ $(OBJ) $(LDFLAGS)

parquet_filter.o: $(VTABLE)/parquet_filter.cc $(VTABLE)/parquet_filter.h $(ARROW) $(PARQUET_CPP)
	$(CXX) $(PROF) -c -o $@ $< $(CFLAGS)

parquet_cursor.o: $(VTABLE)/parquet_cursor.cc $(VTABLE)/parquet_cursor.h $(VTABLE)/parquet_table.h $(VTABLE)/parquet_filter.h $(ARROW) $(PARQUET_CPP)
	$(CXX) $(PROF) -c -o $@ $< $(CFLAGS)

parquet_table.o: $(VTABLE)/parquet_table.cc $(VTABLE)/parquet_table.h $(ARROW) $(PARQUET_CPP)
	$(CXX) $(PROF) -c -o $@ $< $(CFLAGS)

parquet.o: $(VTABLE)/parquet.cc $(VTABLE)/parquet_cursor.h $(VTABLE)/parquet_table.h $(VTABLE)/parquet_filter.h $(ARROW) $(PARQUET_CPP)
	$(CXX) $(PROF) -c -o $@ $< $(CFLAGS)

$(ARROW):
	rm -rf $(ARROW)
	git clone https://github.com/apache/arrow.git $(ARROW)
	cd $(ARROW) && git checkout apache-arrow-0.9.0
	mkdir $(ARROW)/cpp/release
	cd $(ARROW)/cpp/release && cmake -DCMAKE_BUILD_TYPE=$(APACHE_BUILD) -DARROW_BOOST_VENDORED=ON -DARROW_BOOST_USE_SHARED=OFF -DPARQUET_BUILD_SHARED=OFF ..
	touch -d @0 $(ARROW)

$(ARROW_LIB): $(ARROW)
	cd $(ARROW)/cpp/release && make -j$(CPUS)

# This is pretty gross. I'm sure someone who knows what they're doing could do this more cleanly.
$(ICU_I18N_LIB):
	rm -rf $(ICU)
	mkdir $(ICU)
	cd $(ICU) && wget https://github.com/unicode-org/icu/releases/download/release-$(ICU_VERSION)/icu4c-$(ICU_VERSION_U)-src.tgz
	cd $(ICU) && tar xf icu4c-$(ICU_VERSION_U)-src.tgz --strip-components=1
	cd $(ICU)/source && ./configure --enable-static
	cd $(ICU)/source && make -j$(CPUS) LIBCFLAGS='-fPIC' LIBCXXFLAGS='-fPIC'

$(PARQUET_CPP):
	rm -rf $(PARQUET_CPP)
	git clone https://github.com/apache/parquet-cpp.git $(PARQUET_CPP)
	cd $(PARQUET_CPP) && git checkout apache-parquet-cpp-1.4.0
	cd $(PARQUET_CPP) && BOOST_ROOT=$(BOOST_ROOT) BOOST_STATIC_REGEX_LIBRARY=$(BOOST_REGEX_LIB) SNAPPY_STATIC_LIB=$(SNAPPY_LIB) BROTLI_STATIC_LIB_ENC=$(BROTLI_ENC_LIB) BROTLI_STATIC_LIB_DEC=$(BROTLI_DEC_LIB) BROTLI_STATIC_LIB_COMMON=$(BROTLI_COMMON_LIB) ZLIB_STATIC_LIB=$(ZLIB_LIB) LZ4_STATIC_LIB=$(LZ4_LIB) ZSTD_STATIC_LIB=$(ZSTD_LIB) cmake -DCMAKE_BUILD_TYPE=$(APACHE_BUILD) -DPARQUET_MINIMAL_DEPENDENCY=ON -DPARQUET_ARROW_LINKAGE=static -DPARQUET_BOOST_USE_SHARED=OFF -DPARQUET_BUILD_SHARED=OFF .
	touch -d @0 $(PARQUET_CPP)

$(PARQUET_CPP_LIB): $(PARQUET_CPP) $(ARROW_LIB)
	cd $(PARQUET_CPP) && make -j$(CPUS)

.PHONY: clean arrow icu parquet publish_libs

clean:
	rm -f *.o *.so

distclean:
	rm -rf $(SQLITE) $(HERE)


arrow: $(ARROW_LIB)

icu: $(ICU_I18N_LIB)

parquet: $(PARQUET_CPP_LIB)

publish_libs:
	tar -cJf libs.tar.xz $(ALL_LIBS) $(SQLITE)/sqlite3
	s3cmd put libs.tar.xz s3://cldellow/public/libparquet/$$(lsb_release -s -r)/libs.tar.xz
