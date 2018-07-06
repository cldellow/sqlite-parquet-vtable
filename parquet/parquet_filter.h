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
  Like,
  Glob,
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

class RowGroupBitmap {
  void setBit(std::vector<unsigned char>& membership, unsigned int rowGroup, bool isSet) {
    int byte = rowGroup / 8;
    int offset = rowGroup % 8;
    unsigned char c = membership[byte];
    c &= ~(1UL << offset);
    if(isSet) {
      c |= 1UL << offset;
    }
    membership[byte] = c;
  }
// Compares estimated rowGroupFilter results against observed results
// when we explored the row group. This lets us cache 
public:
  RowGroupBitmap(unsigned int totalRowGroups) {
    // Initialize everything to assume that all row groups match.
    // As we discover otherwise, we'll update that assumption.
    for(unsigned int i = 0; i < (totalRowGroups + 7) / 8; i++) {
      estimatedMembership.push_back(0xFF);
      actualMembership.push_back(0xFF);
    }
  }

  RowGroupBitmap(
      std::vector<unsigned char> estimatedMembership,
      std::vector<unsigned char> actualMembership) :
    estimatedMembership(estimatedMembership),
    actualMembership(actualMembership) {
  }

  std::vector<unsigned char> estimatedMembership;
  std::vector<unsigned char> actualMembership;
  // Pass false only if definitely does not have rows
  void setEstimatedMembership(unsigned int rowGroup, bool hasRows) {
    setBit(estimatedMembership, rowGroup, hasRows);
  }

  // Pass false only after exhausting all rows
  void setActualMembership(unsigned int rowGroup, bool hadRows) {
    setBit(actualMembership, rowGroup, hadRows);
  }

  bool getActualMembership(unsigned int rowGroup) {
    int byte = rowGroup / 8;
    int offset = rowGroup % 8;

    return (actualMembership[byte] >> offset) & 1U;
  }
};

class Constraint {
public:
  // Kind of a messy constructor function, but it's just for internal use, so whatever.
  Constraint(
    RowGroupBitmap bitmap,
    int column,
    std::string columnName,
    ConstraintOperator op,
    ValueType type,
    int64_t intValue,
    double doubleValue,
    std::vector<unsigned char> blobValue
  );

  RowGroupBitmap bitmap;
  int column; // underlying column in the query
  std::string columnName;
  ConstraintOperator op;
  ValueType type;

  int64_t intValue;
  double doubleValue;
  std::vector<unsigned char> blobValue;
  // Only set when blobValue is set
  std::string stringValue;

  // Only set when stringValue is set and op == Like
  std::string likeStringValue;

  // A unique identifier for this constraint, e.g.
  // col0 = 'Dawson Creek'
  std::string describe() const;

  // This is a temp field used while evaluating if a rowgroup had rows
  // that matched this constraint.
  int rowGroupId;
  bool hadRows;
};

#endif
