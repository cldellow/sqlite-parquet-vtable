#include "parquet_filter.h"

Constraint::Constraint(
  RowGroupBitmap bitmap,
  int column,
  std::string columnName,
  ConstraintOperator op,
  ValueType type,
  int64_t intValue,
  double doubleValue,
  std::vector<unsigned char> blobValue
): bitmap(bitmap),
   column(column),
   columnName(columnName),
   op(op),
   type(type),
   intValue(intValue),
   doubleValue(doubleValue),
   blobValue(blobValue),
   hadRows(false) {
     RowGroupBitmap bm = bitmap;
     this->bitmap = bm;

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

std::string Constraint::describe() const {
  std::string rv;
  rv.append(columnName);
  rv.append(" ");
  switch(op) {
    case Equal:
      rv.append("=");
      break;
    case GreaterThan:
      rv.append(">");
      break;
    case LessThanOrEqual:
      rv.append("<=");
      break;
    case LessThan:
      rv.append("<");
      break;
    case GreaterThanOrEqual:
      rv.append(">=");
      break;
    case Like:
      rv.append("LIKE");
      break;
    case Glob:
      rv.append("GLOB");
      break;
    case NotEqual:
      rv.append("<>");
      break;
    case IsNot:
      rv.append("IS NOT");
      break;
    case IsNotNull:
      rv.append("IS NOT NULL");
      break;
    case IsNull:
      rv.append("IS NULL");
      break;
    case Is:
      rv.append("IS");
      break;
  }
  rv.append(" ");

  switch(type) {
    case Null:
      rv.append("NULL");
      break;
    case Integer:
      rv.append(std::to_string(intValue));
      break;
    case Double:
      rv.append(std::to_string(doubleValue));
      break;
    case Blob:
      break;
    case Text:
      rv.append(stringValue);
      break;
  }
  return rv;
}
