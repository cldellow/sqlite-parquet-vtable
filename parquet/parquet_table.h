#ifndef PARQUET_TABLE_H
#define PARQUET_TABLE_H

#include <string>

class ParquetTable {
public:
  ParquetTable(std::string file);
  std::string CreateStatement();
  std::string file;

};

#endif
