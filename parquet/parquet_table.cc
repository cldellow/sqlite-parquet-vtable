#include "parquet_table.h"

#include "parquet/api/reader.h"

ParquetTable::ParquetTable(std::string file, std::string tableName): file(file), tableName(tableName) {
  std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(file.data());
  metadata = reader->metadata();
}

std::string ParquetTable::columnName(int i) {
  if(i == -1)
    return "rowid";
  return columnNames[i];
}

unsigned int ParquetTable::getNumColumns() {
  return columnNames.size();
}


std::string ParquetTable::CreateStatement() {
  std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(
      file.data(),
      true,
      parquet::default_reader_properties(),
      metadata);
  std::string text("CREATE TABLE x(");
  auto schema = reader->metadata()->schema();

  for(auto i = 0; i < schema->num_columns(); i++) {
    auto _col = schema->GetColumnRoot(i);
    columnNames.push_back(_col->name());
  }

  for(auto i = 0; i < schema->num_columns(); i++) {
    auto _col = schema->GetColumnRoot(i);

    if(!_col->is_primitive()) {
      std::ostringstream ss;
      ss << __FILE__ << ":" << __LINE__ << ": column " << i << " has non-primitive type";
      throw std::invalid_argument(ss.str());
    }

    if(_col->is_repeated()) {
      std::ostringstream ss;
      ss << __FILE__ << ":" << __LINE__ << ": column " << i << " has non-scalar type";
      throw std::invalid_argument(ss.str());
    }

    parquet::schema::PrimitiveNode* col = (parquet::schema::PrimitiveNode*)_col;

    if(i > 0)
      text += ", ";

    text += "\"";
    // Horrifically inefficient, but easy to understand.
    std::string colName = col->name();
    for(char& c : colName) {
      if(c == '"')
        text += "\"\"";
      else
        text += c;
    }
    text += "\"";

    std::string type;

    parquet::Type::type physical = col->physical_type();
    parquet::LogicalType::type logical = col->logical_type();
    // Be explicit about which types we understand so we don't mislead someone
    // whose unsigned ints start getting interpreted as signed. (We could
    // support this for UINT_8/16/32 -- and for UINT_64 we could throw if
    // the high bit was set.)
    if(logical == parquet::LogicalType::NONE ||
        logical == parquet::LogicalType::UTF8 ||
        logical == parquet::LogicalType::DATE ||
        logical == parquet::LogicalType::TIME_MILLIS ||
        logical == parquet::LogicalType::TIMESTAMP_MILLIS ||
        logical == parquet::LogicalType::TIME_MICROS ||
        logical == parquet::LogicalType::TIMESTAMP_MICROS ||
        logical == parquet::LogicalType::INT_8 ||
        logical == parquet::LogicalType::INT_16 ||
        logical == parquet::LogicalType::INT_32 ||
        logical == parquet::LogicalType::INT_64) {
      switch(physical) {
        case parquet::Type::BOOLEAN:
          type = "TINYINT";
          break;
        case parquet::Type::INT32:
          if(logical == parquet::LogicalType::NONE ||
              logical == parquet::LogicalType::INT_32) {
            type = "INT";
          } else if(logical == parquet::LogicalType::INT_8) {
            type = "TINYINT";
          } else if(logical == parquet::LogicalType::INT_16) {
            type = "SMALLINT";
          }
          break;
        case parquet::Type::INT96:
          // INT96 is used for nanosecond precision on timestamps; we truncate
          // to millisecond precision.
        case parquet::Type::INT64:
          type = "BIGINT";
          break;
        case parquet::Type::FLOAT:
          type = "REAL";
          break;
        case parquet::Type::DOUBLE:
          type = "DOUBLE";
          break;
        case parquet::Type::BYTE_ARRAY:
          if(logical == parquet::LogicalType::UTF8) {
            type = "TEXT";
          } else {
            type = "BLOB";
          }
          break;
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
          type = "BLOB";
          break;
        default:
          break;
      }
    }

    if(type.empty()) {
      std::ostringstream ss;
      ss << __FILE__ << ":" << __LINE__ << ": column " << i << " has unsupported type: " <<
        parquet::TypeToString(physical) << "/" << parquet::LogicalTypeToString(logical);

      throw std::invalid_argument(ss.str());
    }

#ifdef DEBUG
    printf("col %d[name=%s, p=%d:%s, l=%d:%s] is %s\n",
        i,
        col->name().data(),
        col->physical_type(),
        parquet::TypeToString(col->physical_type()).data(),
        col->logical_type(),
        parquet::LogicalTypeToString(col->logical_type()).data(),
        type.data());
#endif

    text += " ";
    text += type;
  }
  text +=");";
  return text;
}

std::shared_ptr<parquet::FileMetaData> ParquetTable::getMetadata() { return metadata; }

const std::string& ParquetTable::getFile() { return file; }
const std::string& ParquetTable::getTableName() { return tableName; }
