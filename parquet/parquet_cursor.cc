#include "parquet_cursor.h"

ParquetCursor::ParquetCursor(ParquetTable* table): table(table) {
  reader = NULL;
  reset(std::vector<Constraint>());
}

bool ParquetCursor::currentRowGroupSatisfiesRowIdFilter(Constraint& constraint) {
  if(constraint.type != Integer)
    return true;

  int64_t target = constraint.intValue;
  switch(constraint.op) {
    case IsNull:
      return false;
    case Is:
    case Equal:
      return target >= rowId && target < rowId + rowGroupSize;
    case GreaterThan:
      // rowId > target
      return rowId + rowGroupSize > target;
    case GreaterThanOrEqual:
      // rowId >= target
      return rowId + rowGroupSize >= rowId;
    case LessThan:
      return target > rowId;
    case LessThanOrEqual:
      return target >= rowId;
    default:
      return true;
  }
}

bool ParquetCursor::currentRowGroupSatisfiesBlobFilter(Constraint& constraint, std::shared_ptr<parquet::RowGroupStatistics> _stats) {
  if(!_stats->HasMinMax()) {
    return true;
  }

  if(constraint.type != Blob) {
    return true;
  }

  const unsigned char* minPtr = NULL;
  const unsigned char* maxPtr = NULL;
  size_t minLen = 0;
  size_t maxLen = 0;

  parquet::Type::type pqType = types[constraint.column];

  if(pqType == parquet::Type::BYTE_ARRAY) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::BYTE_ARRAY>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::BYTE_ARRAY>>*)_stats.get();

    minPtr = stats->min().ptr;
    minLen = stats->min().len;
    maxPtr = stats->max().ptr;
    maxLen = stats->max().len;
  } else if(pqType == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    // It seems like parquet-cpp doesn't actually produce stats for FLBA yet, so
    // rather than have untested code here, we'll just short circuit.
    //
    // Once I can get my hands on such a file, it should be easy to add support.
    return true;
  } else {
    // Should be impossible to get here
    std::ostringstream ss;
    ss << __FILE__ << ":" << __LINE__ << ": currentRowGroupSatisfiesBlobFilter called on unsupported type: " <<
      parquet::TypeToString(pqType);
    throw std::invalid_argument(ss.str());
  }

  const std::vector<unsigned char>& blob = constraint.blobValue;

  switch(constraint.op) {
    case Is:
    case Equal:
    {
      bool minEqual = blob.size() == minLen && memcmp(&blob[0], minPtr, minLen) == 0;
      bool maxEqual = blob.size() == maxLen && memcmp(&blob[0], maxPtr, maxLen) == 0;

      bool blobGteMinBlob = std::lexicographical_compare(
          minPtr,
          minPtr + minLen,
          &blob[0],
          &blob[0] + blob.size());

      bool blobLtMaxBlob = std::lexicographical_compare(
          &blob[0],
          &blob[0] + blob.size(),
          maxPtr,
          maxPtr + maxLen);


      return (minEqual || blobGteMinBlob) && (maxEqual || blobLtMaxBlob);
    }
    case GreaterThanOrEqual:
    {
      bool maxEqual = blob.size() == maxLen && memcmp(&blob[0], maxPtr, maxLen) == 0;

      return maxEqual || std::lexicographical_compare(
          &blob[0],
          &blob[0] + blob.size(),
          maxPtr,
          maxPtr + maxLen);
    }
    case GreaterThan:
      return std::lexicographical_compare(
          &blob[0],
          &blob[0] + blob.size(),
          maxPtr,
          maxPtr + maxLen);
    case LessThan:
      return std::lexicographical_compare(
          minPtr,
          minPtr + minLen,
          &blob[0],
          &blob[0] + blob.size());
    case LessThanOrEqual:
    {
      bool minEqual = blob.size() == minLen && memcmp(&blob[0], minPtr, minLen) == 0;
      return minEqual || std::lexicographical_compare(
          minPtr,
          minPtr + minLen,
          &blob[0],
          &blob[0] + blob.size());
    }
    case NotEqual:
    {
      // If min == max == blob, we can skip this.
      bool blobMaxEqual = blob.size() == maxLen && memcmp(&blob[0], maxPtr, maxLen) == 0;
      bool minMaxEqual = minLen == maxLen && memcmp(minPtr, maxPtr, minLen) == 0;
      return !(blobMaxEqual && minMaxEqual);
    }
    case IsNot:
    default:
      return true;
  }
}

bool ParquetCursor::currentRowGroupSatisfiesTextFilter(Constraint& constraint, std::shared_ptr<parquet::RowGroupStatistics> _stats) {
  parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::BYTE_ARRAY>>* stats =
    (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::BYTE_ARRAY>>*)_stats.get();

  if(!stats->HasMinMax()) {
    return true;
  }

  if(constraint.type != Text) {
    return true;
  }

  const std::string& str = constraint.stringValue;
  const parquet::ByteArray& min = stats->min();
  const parquet::ByteArray& max = stats->max();
  std::string minStr((const char*)min.ptr, min.len);
  std::string maxStr((const char*)max.ptr, max.len);
//  printf("min=%s [%d], max=%s [%d], target=%s\n", minStr.data(), min.len, maxStr.data(), max.len, str.data());

  switch(constraint.op) {
    case Is:
    case Equal:
      return str >= minStr && str <= maxStr;
    case GreaterThanOrEqual:
      return maxStr >= str;
    case GreaterThan:
      return maxStr > str;
    case LessThan:
      return minStr < str;
    case LessThanOrEqual:
      return minStr <= str;
    case NotEqual:
      // If min == max == str, we can skip this.
      return !(minStr == maxStr && str == minStr);
    case Like:
    {
      const std::string& likeStringValue = constraint.likeStringValue;
      std::string truncatedMin = minStr.substr(0, likeStringValue.size());
      std::string truncatedMax = maxStr.substr(0, likeStringValue.size());
      return likeStringValue.empty() || (likeStringValue >= truncatedMin && likeStringValue <= truncatedMax);
    }
    case IsNot:
    default:
      return true;
  }
}

int64_t int96toMsSinceEpoch(const parquet::Int96& rv) {
  __int128 ns = rv.value[0] + ((unsigned long)rv.value[1] << 32);
  __int128 julianDay = rv.value[2];
  __int128 nsSinceEpoch = (julianDay - 2440588);
  nsSinceEpoch *= 86400;
  nsSinceEpoch *= 1000 * 1000 * 1000;
  nsSinceEpoch += ns;
  nsSinceEpoch /= 1000000;
  return nsSinceEpoch;
}

bool ParquetCursor::currentRowGroupSatisfiesIntegerFilter(Constraint& constraint, std::shared_ptr<parquet::RowGroupStatistics> _stats) {
  if(!_stats->HasMinMax()) {
    return true;
  }

  if(constraint.type != Integer) {
    return true;
  }

  int column = constraint.column;

  int64_t min = std::numeric_limits<int64_t>::min();
  int64_t max = std::numeric_limits<int64_t>::max();
  parquet::Type::type pqType = types[column];

  if(pqType == parquet::Type::INT32) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::INT32>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::INT32>>*)_stats.get();

    min = stats->min();
    max = stats->max();
  } else if(pqType == parquet::Type::INT64) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::INT64>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::INT64>>*)_stats.get();

    min = stats->min();
    max = stats->max();
  } else if(pqType == parquet::Type::INT96) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::INT96>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::INT96>>*)_stats.get();

    min = int96toMsSinceEpoch(stats->min());
    max = int96toMsSinceEpoch(stats->max());

  } else if(pqType == parquet::Type::BOOLEAN) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::BOOLEAN>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::BOOLEAN>>*)_stats.get();

    min = stats->min();
    max = stats->max();

  } else {
    // Should be impossible to get here as we should have forbidden this at
    // CREATE time -- maybe file changed underneath us?
    std::ostringstream ss;
    ss << __FILE__ << ":" << __LINE__ << ": currentRowGroupSatisfiesIntegerFilter called on unsupported type: " <<
      parquet::TypeToString(pqType);
    throw std::invalid_argument(ss.str());
  }

  const int64_t value = constraint.intValue;
//  printf("min=%s [%d], max=%s [%d], target=%s\n", minStr.data(), min.len, maxStr.data(), max.len, str.data());

  switch(constraint.op) {
    case Is:
    case Equal:
      return value >= min && value <= max;
    case GreaterThanOrEqual:
      return max >= value;
    case GreaterThan:
      return max > value;
    case LessThan:
      return min < value;
    case LessThanOrEqual:
      return min <= value;
    case NotEqual:
      // If min == max == str, we can skip this.
      return !(min == max && value == min);
    case Like:
    case IsNot:
    default:
      return true;
  }

  return true;
}

bool ParquetCursor::currentRowGroupSatisfiesDoubleFilter(Constraint& constraint, std::shared_ptr<parquet::RowGroupStatistics> _stats) {
  if(!_stats->HasMinMax()) {
    return true;
  }

  if(constraint.type != Double) {
    return true;
  }

  int column = constraint.column;

  double min = std::numeric_limits<double>::min();
  double max = std::numeric_limits<double>::max();
  parquet::Type::type pqType = types[column];

  if(pqType == parquet::Type::DOUBLE) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::DOUBLE>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::DOUBLE>>*)_stats.get();

    min = stats->min();
    max = stats->max();
  } else if(pqType == parquet::Type::FLOAT) {
    parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::FLOAT>>* stats =
      (parquet::TypedRowGroupStatistics<parquet::DataType<parquet::Type::FLOAT>>*)_stats.get();

    min = stats->min();
    max = stats->max();
  } else {
    // Should be impossible to get here as we should have forbidden this at
    // CREATE time -- maybe file changed underneath us?
    std::ostringstream ss;
    ss << __FILE__ << ":" << __LINE__ << ": currentRowGroupSatisfiesIntegerFilter called on unsupported type: " <<
      parquet::TypeToString(pqType);
    throw std::invalid_argument(ss.str());
  }

  const double value = constraint.doubleValue;
//  printf("min=%s [%d], max=%s [%d], target=%s\n", minStr.data(), min.len, maxStr.data(), max.len, str.data());

  switch(constraint.op) {
    case Is:
    case Equal:
      return value >= min && value <= max;
    case GreaterThanOrEqual:
      return max >= value;
    case GreaterThan:
      return max > value;
    case LessThan:
      return min < value;
    case LessThanOrEqual:
      return min <= value;
    case NotEqual:
      // If min == max == str, we can skip this.
      return !(min == max && value == min);
    case Like:
    case IsNot:
    default:
      return true;
  }

  return true;

}

bool ParquetCursor::currentRowSatisfiesTextFilter(Constraint& constraint) {
  if(constraint.type != Text) {
    return true;
  }

  parquet::ByteArray* ba = getByteArray(constraint.column);

  switch(constraint.op) {
    case Is:
    case Equal:
    {
      const std::vector<unsigned char>& blob = constraint.blobValue;

      if(blob.size() != ba->len)
        return false;

      return 0 == memcmp(&blob[0], ba->ptr, ba->len);
    }
    case NotEqual:
    {
      const std::vector<unsigned char>& blob = constraint.blobValue;

      if(blob.size() != ba->len)
        return true;

      return 0 != memcmp(&blob[0], ba->ptr, ba->len);
    }
    case GreaterThan:
    {
      const std::vector<unsigned char>& blob = constraint.blobValue;

      return std::lexicographical_compare(
          &blob[0],
          &blob[0] + blob.size(),
          ba->ptr,
          ba->ptr + ba->len);
    }
    case GreaterThanOrEqual:
    {
      const std::vector<unsigned char>& blob = constraint.blobValue;

      bool equal = blob.size() == ba->len && 0 == memcmp(&blob[0], ba->ptr, ba->len);

      return equal || std::lexicographical_compare(
          &blob[0],
          &blob[0] + blob.size(),
          ba->ptr,
          ba->ptr + ba->len);
    }
    case LessThan:
    {
      const std::vector<unsigned char>& blob = constraint.blobValue;

      return std::lexicographical_compare(
          ba->ptr,
          ba->ptr + ba->len,
          &blob[0],
          &blob[0] + blob.size());
    }
    case LessThanOrEqual:
    {
      const std::vector<unsigned char>& blob = constraint.blobValue;

      bool equal = blob.size() == ba->len && 0 == memcmp(&blob[0], ba->ptr, ba->len);

      return equal || std::lexicographical_compare(
          ba->ptr,
          ba->ptr + ba->len,
          &blob[0],
          &blob[0] + blob.size());
    }
    case Like:
    {
      const std::string& likeStringValue = constraint.likeStringValue;
      if(likeStringValue.size() > ba->len)
        return false;

      size_t len = ba->len;
      if(likeStringValue.size() < len)
        len = likeStringValue.size();
      return 0 == memcmp(&likeStringValue[0], ba->ptr, len);
    }
    case IsNot:
    default:
      return true;
  }
}

bool ParquetCursor::currentRowSatisfiesIntegerFilter(Constraint& constraint) {
  if(constraint.type != Integer) {
    return true;
  }

  int column = constraint.column;

  // CONSIDER: should we just store int64s everywhere?
  int64_t value = 0;

  if(column == -1) {
    value = rowId;
  } else {
    parquet::Type::type pqType = types[column];

    if(pqType == parquet::Type::INT32 || pqType == parquet::Type::BOOLEAN) {
      value = getInt32(column);
    } else if(pqType == parquet::Type::INT64 || pqType == parquet::Type::INT96) {
      value = getInt64(column);
    } else {
      // Should be impossible to get here
      std::ostringstream ss;
      ss << __FILE__ << ":" << __LINE__ << ": currentRowSatisfiesIntegerFilter called on unsupported type: " <<
        parquet::TypeToString(pqType);
      throw std::invalid_argument(ss.str());
    }
  }

  int64_t constraintValue = constraint.intValue;

  switch(constraint.op) {
    case Is:
    case Equal:
      return constraintValue == value;
    case NotEqual:
      return constraintValue != value;
    case GreaterThan:
      return value > constraintValue;
    case GreaterThanOrEqual:
      return value >= constraintValue;
    case LessThan:
      return value < constraintValue;
    case LessThanOrEqual:
      return value <= constraintValue;
    case Like:
    case IsNot:
    default:
      return true;
  }

  return true;
}

bool ParquetCursor::currentRowSatisfiesDoubleFilter(Constraint& constraint) {
  if(constraint.type != Double) {
    return true;
  }

  int column = constraint.column;
  double value = getDouble(column);
  double constraintValue = constraint.doubleValue;

  switch(constraint.op) {
    case Is:
    case Equal:
      return constraintValue == value;
    case NotEqual:
      return constraintValue != value;
    case GreaterThan:
      return value > constraintValue;
    case GreaterThanOrEqual:
      return value >= constraintValue;
    case LessThan:
      return value < constraintValue;
    case LessThanOrEqual:
      return value <= constraintValue;
    case Like:
    case IsNot:
    default:
      return true;
  }

  return true;
}


// Return true if it is _possible_ that the current
// rowgroup satisfies the constraints. Only return false
// if it definitely does not.
//
// This avoids opening rowgroups that can't return useful
// data, which provides substantial performance benefits.
bool ParquetCursor::currentRowGroupSatisfiesFilter() {
  for(unsigned int i = 0; i < constraints.size(); i++) {
    int column = constraints[i].column;
    int op = constraints[i].op;
    bool rv = true;

    if(column == -1) {
      rv = currentRowGroupSatisfiesRowIdFilter(constraints[i]);
    } else {
      std::unique_ptr<parquet::ColumnChunkMetaData> md = rowGroupMetadata->ColumnChunk(column);
      if(md->is_stats_set()) {
        std::shared_ptr<parquet::RowGroupStatistics> stats = md->statistics();

        // SQLite is much looser with types than you might expect if you
        // come from a Postgres background. The constraint '30.0' (that is,
        // a string containing a floating point number) should be treated
        // as equal to a field containing an integer 30.
        //
        // This means that even if the parquet physical type is integer,
        // the constraint type may be a string, so dispatch to the filter
        // fn based on the Parquet type.

        if(op == IsNull) {
          rv = stats->null_count() > 0;
        } else if(op == IsNotNull) {
          rv = stats->num_values() > 0;
        } else {
          parquet::Type::type pqType = types[column];

          if(pqType == parquet::Type::BYTE_ARRAY && logicalTypes[column] == parquet::LogicalType::UTF8) {
            rv = currentRowGroupSatisfiesTextFilter(constraints[i], stats);
          } else if(pqType == parquet::Type::BYTE_ARRAY) {
            rv = currentRowGroupSatisfiesBlobFilter(constraints[i], stats);
          } else if(pqType == parquet::Type::INT32 ||
                    pqType == parquet::Type::INT64 ||
                    pqType == parquet::Type::INT96 ||
                    pqType == parquet::Type::BOOLEAN) {
            rv = currentRowGroupSatisfiesIntegerFilter(constraints[i], stats);
          } else if(pqType == parquet::Type::FLOAT || pqType == parquet::Type::DOUBLE) {
            rv = currentRowGroupSatisfiesDoubleFilter(constraints[i], stats);
          }
        }
      }
    }

    // and it with the existing actual, which may have come from a previous run
    rv = rv && constraints[i].bitmap.getActualMembership(rowGroupId);
    if(!rv) {
      constraints[i].bitmap.setEstimatedMembership(rowGroupId, rv);
      constraints[i].bitmap.setActualMembership(rowGroupId, rv);
      return rv;
    }
  }

//  printf("rowGroup %d %s\n", rowGroupId, overallRv ? "may satisfy" : "does not satisfy");
  return true;
}


bool ParquetCursor::nextRowGroup() {
start:
  // Ensure that rowId points at the start of this rowGroup (eg, in the case where
  // we skipped an entire row group).
  rowId = rowGroupStartRowId + rowGroupSize;

  if((rowGroupId + 1) >= numRowGroups) {
    return false;
  }

  while(table->getNumColumns() >= scanners.size()) {
    scanners.push_back(std::shared_ptr<parquet::Scanner>());
    // If it doesn't exist, it's the rowId as of the last nextRowGroup call
    colRows.push_back(rowGroupStartRowId);
    colNulls.push_back(false);
    colIntValues.push_back(0);
    colDoubleValues.push_back(0);
    colByteArrayValues.push_back(parquet::ByteArray());
  }


  rowGroupStartRowId = rowId;
  rowGroupId++;
  rowGroupMetadata = reader->metadata()->RowGroup(rowGroupId);
  rowGroupSize = rowsLeftInRowGroup = rowGroupMetadata->num_rows();
  rowGroup = reader->RowGroup(rowGroupId);
  for(unsigned int i = 0; i < scanners.size(); i++)
    scanners[i] = NULL;

  while(types.size() < (unsigned int)rowGroupMetadata->num_columns()) {
    types.push_back(rowGroupMetadata->schema()->Column(0)->physical_type());
  }

  while(logicalTypes.size() < (unsigned int)rowGroupMetadata->num_columns()) {
    logicalTypes.push_back(rowGroupMetadata->schema()->Column(0)->logical_type());
  }

  for(unsigned int i = 0; i < (unsigned int)rowGroupMetadata->num_columns(); i++) {
    types[i] = rowGroupMetadata->schema()->Column(i)->physical_type();
    logicalTypes[i] = rowGroupMetadata->schema()->Column(i)->logical_type();
  }

  for(unsigned int i = 0; i < colRows.size(); i++) {
    colRows[i] = rowId;
  }

  // Increment rowId so currentRowGroupSatisfiesRowIdFilter can access it;
  // it'll get decremented by our caller
  rowId++;

  // We're going to scan this row group; reset the expectation of discovering
  // a row
  for(unsigned int i = 0; i < constraints.size(); i++) {
    if(rowGroupId > 0 && constraints[i].rowGroupId == rowGroupId - 1) {
      constraints[i].bitmap.setActualMembership(rowGroupId - 1, constraints[i].hadRows);
    }
    constraints[i].hadRows = false;
  }

  if(!currentRowGroupSatisfiesFilter())
    goto start;

  for(unsigned int i = 0; i < constraints.size(); i++) {
    constraints[i].rowGroupId = rowGroupId;
  }
  return true;
}

// Return true if it is _possible_ that the current
// row satisfies the constraints. Only return false
// if it definitely does not.
//
// This avoids pointless transitions between the SQLite VM
// and the extension, which can add up on a dataset of tens
// of millions of rows.
bool ParquetCursor::currentRowSatisfiesFilter() {
  bool overallRv = true;
  for(unsigned int i = 0; i < constraints.size(); i++) {
    bool rv = true;
    int column = constraints[i].column;
    ensureColumn(column);
    int op = constraints[i].op;

    if(op == IsNull) {
      rv = isNull(column);
    } else if(op == IsNotNull) {
      rv = !isNull(column);
    } else {

      if(logicalTypes[column] == parquet::LogicalType::UTF8) {
        rv = currentRowSatisfiesTextFilter(constraints[i]);
      } else {
        parquet::Type::type pqType = types[column];
        if(pqType == parquet::Type::INT32 ||
           pqType == parquet::Type::INT64 ||
           pqType == parquet::Type::INT96 ||
           pqType == parquet::Type::BOOLEAN) {
          rv = currentRowSatisfiesIntegerFilter(constraints[i]);
        } else if(pqType == parquet::Type::FLOAT || pqType == parquet::Type::DOUBLE) {
          rv = currentRowSatisfiesDoubleFilter(constraints[i]);
        }
      }
    }

    // it defaults to false; so only set it if true
    // ideally we'd short-circuit if we'd already set this group as visited
    if(rv) {
      constraints[i].hadRows = true;
    }
    overallRv = overallRv && rv;
  }
  return overallRv;
}

void ParquetCursor::next() {
  // Returns true if we've crossed a row group boundary
start:
  if(rowsLeftInRowGroup == 0) {
    if(!nextRowGroup()) {
      // put rowId over the edge so eof returns true
      rowId = numRows + 1;
      return;
    } else {
      // After a successful nextRowGroup, rowId is pointing at the current row. Make it
      // point before so the rest of the logic works out.
      rowId--;
    }
  }

  rowsLeftInRowGroup--;
  rowId++;
  if(constraints.size() > 0 && !currentRowSatisfiesFilter())
    goto start;
}

int ParquetCursor::getRowId() {
  return rowId;
}

bool ParquetCursor::eof() {
  return rowId > numRows;
}

void ParquetCursor::ensureColumn(int col) {
  // -1 signals rowid, which is trivially available
  if(col == -1)
    return;

  // need to ensure a scanner exists (and skip the # of rows in the rowgroup)
  if(scanners[col].get() == NULL) {
    std::shared_ptr<parquet::ColumnReader> colReader = rowGroup->Column(col);
    scanners[col] = parquet::Scanner::Make(colReader);
  }

  // Actually fetch a value, stash data in colRows, colNulls, colValues
  if(colRows[col] != rowId) {
    // We may need to skip some records, eg, a query like
    // SELECT a WHERE b = 10
    // may have read b, but skipped a until b matches the predicate.
    bool wasNull = false;
    while(colRows[col] + 1 < rowId) {
      switch(types[col]) {
        case parquet::Type::INT32:
        {
          parquet::Int32Scanner* s = (parquet::Int32Scanner*)scanners[col].get();
          int rv = 0;
          s->NextValue(&rv, &wasNull);
          break;
        }
        case parquet::Type::FLOAT:
        {
          parquet::FloatScanner* s = (parquet::FloatScanner*)scanners[col].get();
          float rv = 0;
          s->NextValue(&rv, &wasNull);
          break;
        }
        case parquet::Type::DOUBLE:
        {
          parquet::DoubleScanner* s = (parquet::DoubleScanner*)scanners[col].get();
          double rv = 0;
          s->NextValue(&rv, &wasNull);
          break;
        }
        case parquet::Type::BYTE_ARRAY:
        {
          parquet::ByteArrayScanner* s = (parquet::ByteArrayScanner*)scanners[col].get();
          parquet::ByteArray ba;
          s->NextValue(&ba, &wasNull);
          break;
        }
        case parquet::Type::INT96:
        {
          parquet::Int96Scanner* s = (parquet::Int96Scanner*)scanners[col].get();
          parquet::Int96 rv;
          s->NextValue(&rv, &wasNull);
          break;
        }
        case parquet::Type::INT64:
        {
          parquet::Int64Scanner* s = (parquet::Int64Scanner*)scanners[col].get();
          long rv = 0;
          s->NextValue(&rv, &wasNull);
          break;
        }
        case parquet::Type::BOOLEAN:
        {
          parquet::BoolScanner* s = (parquet::BoolScanner*)scanners[col].get();
          bool rv = false;
          s->NextValue(&rv, &wasNull);
          break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        {
          parquet::FixedLenByteArrayScanner* s = (parquet::FixedLenByteArrayScanner*)scanners[col].get();
          parquet::FixedLenByteArray flba;
          s->NextValue(&flba, &wasNull);
          break;
        }
        default:
          // Should be impossible to get here as we should have forbidden this at
          // CREATE time -- maybe file changed underneath us?
          std::ostringstream ss;
          ss << __FILE__ << ":" << __LINE__ << ": column " << col << " has unsupported type: " <<
            parquet::TypeToString(types[col]);
          throw std::invalid_argument(ss.str());
        break;

      }
      colRows[col]++;
    }

    colRows[col] = rowId;
    wasNull = false;

    bool hadValue = false;
    switch(types[col]) {
      case parquet::Type::INT32:
      {
        parquet::Int32Scanner* s = (parquet::Int32Scanner*)scanners[col].get();
        int rv = 0;
        hadValue = s->NextValue(&rv, &wasNull);
        colIntValues[col] = rv;
        break;
      }
      case parquet::Type::FLOAT:
      {
        parquet::FloatScanner* s = (parquet::FloatScanner*)scanners[col].get();
        float rv = 0;
        hadValue = s->NextValue(&rv, &wasNull);
        colDoubleValues[col] = rv;
        break;
      }
      case parquet::Type::DOUBLE:
      {
        parquet::DoubleScanner* s = (parquet::DoubleScanner*)scanners[col].get();
        double rv = 0;
        hadValue = s->NextValue(&rv, &wasNull);
        colDoubleValues[col] = rv;
        break;
      }
      case parquet::Type::BYTE_ARRAY:
      {
        parquet::ByteArrayScanner* s = (parquet::ByteArrayScanner*)scanners[col].get();
        hadValue = s->NextValue(&colByteArrayValues[col], &wasNull);
        break;
      }
      case parquet::Type::INT96:
      {
        // INT96 tracks a date with nanosecond precision, convert to ms since epoch.
        // ...see https://github.com/apache/parquet-format/pull/49 for more
        //
        // First 8 bytes: nanoseconds into the day
        // Last 4 bytes: Julian day
        // To get nanoseconds since the epoch:
        // (julian_day - 2440588) * (86400 * 1000 * 1000 * 1000) + nanoseconds
        parquet::Int96Scanner* s = (parquet::Int96Scanner*)scanners[col].get();
        parquet::Int96 rv {0, 0, 0};
        hadValue = s->NextValue(&rv, &wasNull);
        colIntValues[col] = int96toMsSinceEpoch(rv);
        break;
      }
      case parquet::Type::INT64:
      {
        parquet::Int64Scanner* s = (parquet::Int64Scanner*)scanners[col].get();
        long rv = 0;
        hadValue = s->NextValue(&rv, &wasNull);
        colIntValues[col] = rv;
        break;
      }

      case parquet::Type::BOOLEAN:
      {
        parquet::BoolScanner* s = (parquet::BoolScanner*)scanners[col].get();
        bool rv = false;
        hadValue = s->NextValue(&rv, &wasNull);
        colIntValues[col] = rv ? 1 : 0;
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      {
        parquet::FixedLenByteArrayScanner* s = (parquet::FixedLenByteArrayScanner*)scanners[col].get();
        parquet::FixedLenByteArray flba;
        hadValue = s->NextValue(&flba, &wasNull);
        colByteArrayValues[col].ptr = flba.ptr;
        // TODO: cache this
        colByteArrayValues[col].len = rowGroupMetadata->schema()->Column(col)->type_length();
        break;
      }
      default:
        // Should be impossible to get here as we should have forbidden this at
        // CREATE time -- maybe file changed underneath us?
        std::ostringstream ss;
        ss << __FILE__ << ":" << __LINE__ << ": column " << col << " has unsupported type: " <<
          parquet::TypeToString(types[col]);
        throw std::invalid_argument(ss.str());
      break;
    }

    if(!hadValue)
      throw std::invalid_argument("unexpectedly lacking a next value");

    colNulls[col] = wasNull;
  }
}

bool ParquetCursor::isNull(int col) {
  // -1 is rowid, which is trivially non null
  if(col == -1)
    return false;

  return colNulls[col];
}

int ParquetCursor::getInt32(int col) {
  return colIntValues[col];
}

long ParquetCursor::getInt64(int col) {
  return colIntValues[col];
}

double ParquetCursor::getDouble(int col) {
  return colDoubleValues[col];
}

parquet::ByteArray* ParquetCursor::getByteArray(int col) {
  return &colByteArrayValues[col];
}

parquet::Type::type ParquetCursor::getPhysicalType(int col) {
  return types[col];
}

parquet::LogicalType::type ParquetCursor::getLogicalType(int col) {
  return logicalTypes[col];
}

void ParquetCursor::close() {
  if(reader != NULL) {
    reader->Close();
  }
}

void ParquetCursor::reset(std::vector<Constraint> constraints) {
  close();
  this->constraints = constraints;
  rowId = 0;
  // TODO: consider having a long lived handle in ParquetTable that can be borrowed
  // without incurring the cost of opening the file from scratch twice
  reader = parquet::ParquetFileReader::OpenFile(
      table->getFile().data(),
      true,
      parquet::default_reader_properties(),
      table->getMetadata());

  rowGroupId = -1;
  rowGroupSize = 0;
  rowGroupStartRowId = 0;
  // TODO: handle the case where rowgroups have disjoint schemas?
  // TODO: or at least, fail fast if detected
  rowsLeftInRowGroup = 0;

  numRows = reader->metadata()->num_rows();
  numRowGroups = reader->metadata()->num_row_groups();
}

ParquetTable* ParquetCursor::getTable() const { return table; }

unsigned int ParquetCursor::getNumRowGroups() const { return numRowGroups; }
unsigned int ParquetCursor::getNumConstraints() const { return constraints.size(); }
const Constraint& ParquetCursor::getConstraint(unsigned int i) const { return constraints[i]; }


