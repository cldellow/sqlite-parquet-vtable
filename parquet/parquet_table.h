#ifndef PARQUET_TABLE_H
#define PARQUET_TABLE_H

#include <vector>
#include <string>

class ParquetTable {
  std::vector<std::string> columnNames;

public:
  ParquetTable(std::string file);
  std::string CreateStatement();
  std::string file;
  std::string columnName(int idx);
};

#endif
