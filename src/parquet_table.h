#ifndef PARQUET_TABLE_H
#define PARQUET_TABLE_H

#include <vector>
#include <string>
#include "parquet/api/reader.h"

class ParquetTable {
  std::string file;
  std::string tableName;
  std::vector<std::string> columnNames;
  std::shared_ptr<parquet::FileMetaData> metadata;


public:
  ParquetTable(std::string file, std::string tableName);
  std::string CreateStatement();
  std::string columnName(int idx);
  unsigned int getNumColumns();
  std::shared_ptr<parquet::FileMetaData> getMetadata();
  const std::string& getFile();
  const std::string& getTableName();
};

#endif
