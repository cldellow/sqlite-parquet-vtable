#include "parquet_table.h"

#include "parquet/api/reader.h"

ParquetTable::ParquetTable(std::string file) {
  this->file = file;
}

std::string ParquetTable::CreateStatement() {
  std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(file.data());
  // TODO: parse columns from file
  std::string text("CREATE TABLE x(");
  auto schema = reader->metadata()->schema();
  printf("num cols: %d\n", schema->num_columns());
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

    text += col->name();

    std::string type;
    switch(col->physical_type()) {
      case parquet::Type::BOOLEAN:
        type = "TINYINT";
        break;
      case parquet::Type::INT32:
        if(col->logical_type() == parquet::LogicalType::NONE) {
          type = "INT";
        } else if(col->logical_type() == parquet::LogicalType::INT_8) {
          type = "TINYINT";
        } else if(col->logical_type() == parquet::LogicalType::INT_16) {
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
        if(col->logical_type() == parquet::LogicalType::UTF8) {
          type = "TEXT";
        }
        break;
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      default:
        break;
    }

    if(type.empty()) {
      std::ostringstream ss;
      ss << __FILE__ << ":" << __LINE__ << ": column " << i << " has unsupported type: " <<
        parquet::TypeToString(col->physical_type()) << "/" << parquet::LogicalTypeToString(col->logical_type());

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
