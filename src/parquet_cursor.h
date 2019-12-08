#ifndef PARQUET_CURSOR_H
#define PARQUET_CURSOR_H

#include "parquet/api/reader.h"
#include "parquet_filter.h"
#include "parquet_table.h"

class ParquetCursor {

  ParquetTable *table;
  std::unique_ptr<parquet::ParquetFileReader> reader;
  std::unique_ptr<parquet::RowGroupMetaData> rowGroupMetadata;
  std::shared_ptr<parquet::RowGroupReader> rowGroup;
  std::vector<std::shared_ptr<parquet::Scanner>> scanners;
  std::vector<parquet::Type::type> types;
  std::vector<parquet::ConvertedType::type> logicalTypes;

  std::vector<int> colRows;
  std::vector<bool> colNulls;
  std::vector<int64_t> colIntValues;
  std::vector<double> colDoubleValues;
  std::vector<parquet::ByteArray> colByteArrayValues;

  int rowId;
  int rowGroupId;
  int rowGroupStartRowId;
  int rowGroupSize;
  int numRows;
  int numRowGroups;
  int rowsLeftInRowGroup;

  bool nextRowGroup();

  std::vector<Constraint> constraints;

  bool currentRowSatisfiesFilter();
  bool currentRowGroupSatisfiesFilter();
  bool currentRowGroupSatisfiesRowIdFilter(Constraint &constraint);
  bool currentRowGroupSatisfiesTextFilter(
      Constraint &constraint, std::shared_ptr<parquet::Statistics> stats);
  bool currentRowGroupSatisfiesBlobFilter(
      Constraint &constraint, std::shared_ptr<parquet::Statistics> stats);
  bool currentRowGroupSatisfiesIntegerFilter(
      Constraint &constraint, std::shared_ptr<parquet::Statistics> stats);
  bool currentRowGroupSatisfiesDoubleFilter(
      Constraint &constraint, std::shared_ptr<parquet::Statistics> stats);

  bool currentRowSatisfiesTextFilter(Constraint &constraint);
  bool currentRowSatisfiesIntegerFilter(Constraint &constraint);
  bool currentRowSatisfiesDoubleFilter(Constraint &constraint);

public:
  ParquetCursor(ParquetTable *table);
  int getRowId();
  void next();
  void close();
  void reset(std::vector<Constraint> constraints);
  bool eof();

  void ensureColumn(int col);
  bool isNull(int col);
  unsigned int getNumRowGroups() const;
  unsigned int getNumConstraints() const;
  const Constraint &getConstraint(unsigned int i) const;
  parquet::Type::type getPhysicalType(int col);
  parquet::ConvertedType::type getLogicalType(int col);
  ParquetTable *getTable() const;

  int getInt32(int col);
  long getInt64(int col);
  double getDouble(int col);
  parquet::ByteArray *getByteArray(int col);
};

#endif
