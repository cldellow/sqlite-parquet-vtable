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

int Constraint::getColumn() {
  return column;
}

ConstraintOperator Constraint::getOperator() {
  return op;
}

ValueType Constraint::getType() {
  return type;
}

int64_t Constraint::getInt() {
  return intValue;
}

double Constraint::getDouble() {
  return doubleValue;
}

const std::vector<unsigned char>& Constraint::getBytes() {
  return blobValue;
}

const std::string& Constraint::getString() {
  return stringValue;
}
