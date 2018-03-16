#include "parquet_filter.h"

Constraint::Constraint(
  int column,
  ConstraintOperator op,
  ValueType type,
  int64_t intValue,
  double doubleValue,
  std::vector<unsigned char> blobValue
) {
  this->column = column;
  this->op = op;
  this->type = type;
  this->intValue = intValue;
  this->doubleValue = doubleValue;
  this->blobValue = blobValue;

  if(type == Text)
    stringValue = std::string((char*)&blobValue[0], blobValue.size());
}
