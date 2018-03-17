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

  if(type == Text) {
    stringValue = std::string((char*)&blobValue[0], blobValue.size());

    if(op == Like) {
      // This permits more rowgroups than is strictly needed
      // since it assumes an implicit wildcard. But it's
      // simple to implement, so we'll go with it.
      likeStringValue = stringValue;
      size_t idx = likeStringValue.find_first_of("%");
      if(idx != std::string::npos) {
        likeStringValue = likeStringValue.substr(0, idx);
      }
      idx = likeStringValue.find_first_of("_");
      if(idx != std::string::npos) {
        likeStringValue = likeStringValue.substr(0, idx);
      }
    }
  }
}
