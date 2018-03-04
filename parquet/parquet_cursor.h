#ifndef PARQUET_CURSOR_H
#define PARQUET_CURSOR_H

#include "parquet_table.h"
#include "parquet/api/reader.h"

class ParquetCursor {

  ParquetTable* table;
  std::unique_ptr<parquet::ParquetFileReader> reader;
  std::unique_ptr<parquet::RowGroupMetaData> rowGroupMetadata;
  std::shared_ptr<parquet::RowGroupReader> rowGroup;
  std::vector<std::shared_ptr<parquet::Scanner>> scanners;
  std::vector<parquet::Type::type> types;
  std::vector<parquet::LogicalType::type> logicalTypes;

  std::vector<int> colRows;
  std::vector<bool> colNulls;
  std::vector<uintptr_t> colIntValues;
  std::vector<double> colDoubleValues;
  std::vector<parquet::ByteArray> colByteArrayValues;

  int rowId;
  int rowGroupId;
  int numRows;
  int numRowGroups;
  int rowsLeftInRowGroup;

  void nextRowGroup();

public:
  ParquetCursor(ParquetTable* table);
  int getRowId();
  void next();
  bool eof();

  void ensureColumn(int col);
  bool isNull(int col);
  parquet::Type::type getPhysicalType(int col);
  parquet::LogicalType::type getLogicalType(int col);

  int getInt32(int col);
  long getInt64(int col);
  double getDouble(int col);
  parquet::ByteArray* getByteArray(int col);
};

#endif

