#ifndef PARQUET_FILTER_H
#define PARQUET_FILTER_H

#include <vector>
#include <string>
#include <cstdint>

enum ConstraintOperator {
  Equal,
  GreaterThan,
  LessThanOrEqual,
  LessThan,
  GreaterThanOrEqual,
  Match,
  Like,
  Glob,
  Regexp,
  NotEqual,
  IsNot,
  IsNotNull,
  IsNull,
  Is
};

enum ValueType {
  Null,
  Integer,
  Double,
  Blob,
  Text
};

class Constraint {
  int column; // underlying column in the query
  ConstraintOperator op;
  ValueType type;

  int64_t intValue;
  double doubleValue;
  std::vector<unsigned char> blobValue;
  // Only set when blobValue is set
  std::string stringValue;

public:
  // Kind of a messy constructor function, but it's just for internal use, so whatever.
  Constraint(
    int column,
    ConstraintOperator op,
    ValueType type,
    int64_t intValue,
    double doubleValue,
    std::vector<unsigned char> blobValue
  );

  int getColumn();
  ConstraintOperator getOperator();
  ValueType getType();
  int64_t getInt();
  double getDouble();
  const std::vector<unsigned char>& getBytes();
  const std::string& getString();
};

#endif
