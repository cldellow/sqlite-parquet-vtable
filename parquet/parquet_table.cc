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
      throw std::invalid_argument("parquet file has non-primitive column");
    }

    if(_col->is_repeated()) {
      throw std::invalid_argument("parquet file has non-scalar column");
    }

    parquet::schema::PrimitiveNode* col = (parquet::schema::PrimitiveNode*)_col;

    printf("col %d[p=%d:%s, l=%d:%s] is %s\n",
        i,
        col->physical_type(),
        parquet::TypeToString(col->physical_type()).data(),
        col->logical_type(),
        parquet::LogicalTypeToString(col->logical_type()).data(),
        col->name().data());

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
      case parquet::Type::INT96:
      default:
        break;
    }

    if(type.empty()) {
      throw std::invalid_argument("unsupported type");
    }
    printf("...%s\n", type.data());
    text += " ";
    text += type;
  }
  text +=");";
  return text;
}
