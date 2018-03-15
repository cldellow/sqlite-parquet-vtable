#ifndef PARQUET_TABLE_H
#define PARQUET_TABLE_H

#include <vector>
#include <string>
#include "parquet/api/reader.h"

class ParquetTable {
  std::vector<std::string> columnNames;
  std::shared_ptr<parquet::FileMetaData> metadata;


public:
  ParquetTable(std::string file);
  std::string CreateStatement();
  std::string file;
  std::string columnName(int idx);
  std::shared_ptr<parquet::FileMetaData> getMetadata();
};

#endif
