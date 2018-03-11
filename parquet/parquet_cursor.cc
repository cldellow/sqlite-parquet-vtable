#include "parquet_cursor.h"

ParquetCursor::ParquetCursor(ParquetTable* table) {
  this->table = table;
  reader = NULL;
  reset(std::vector<Constraint>());
}

// Return true if it is _possible_ that the current
// rowgroup satisfies the constraints. Only return false
// if it definitely does not.
//
// This avoids opening rowgroups that can't return useful
// data, which provides substantial performance benefits.
bool ParquetCursor::currentRowGroupSatisfiesFilter() {
  for(unsigned int i = 0; i < constraints.size(); i++) {
    int column = constraints[i].getColumn();
    int op = constraints[i].getOperator();
    bool rv = true;

    if(column == -1) {
      if(op == IsNull) {
        return false;
      }
    } else {
  //    printf("column = %d\n", column);
  //    std::unique_ptr<parquet::ColumnChunkMetaData> md = rowGroupMetadata->ColumnChunk(column);

      if(op == IsNull) {
      } else if(op == IsNotNull) {
      }
    }

    if(!rv)
      return false;
  }

  return true;
}


bool ParquetCursor::nextRowGroup() {
start:
  if((rowGroupId + 1) >= numRowGroups)
    return false;

  rowGroupStartRowId = rowId;
  rowGroupId++;
  rowGroupMetadata = reader->metadata()->RowGroup(rowGroupId);
  rowsLeftInRowGroup = rowGroupMetadata->num_rows();
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

  if(!currentRowGroupSatisfiesFilter())
    goto start;

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
  for(unsigned int i = 0; i < constraints.size(); i++) {
    bool rv = true;
    int column = constraints[i].getColumn();
    ensureColumn(column);
    int op = constraints[i].getOperator();

    if(op == IsNull) {
      rv = isNull(column);
    } else if(op == IsNotNull) {
      rv = !isNull(column);
    }

    if(!rv)
      return false;
  }
  return true;
}

void ParquetCursor::next() {
start:
  if(rowsLeftInRowGroup == 0) {
    if(!nextRowGroup()) {
      // put rowId over the edge so eof returns true
      rowId++;
      return;
    }
  }

  rowsLeftInRowGroup--;
  rowId++;
  if(!currentRowSatisfiesFilter())
    goto start;
}

int ParquetCursor::getRowId() {
  return rowId;
}

bool ParquetCursor::eof() {
  return rowId >= numRows;
}

void ParquetCursor::ensureColumn(int col) {
  // -1 signals rowid, which is trivially available
  if(col == -1)
    return;

  // need to ensure a scanner exists (and skip the # of rows in the rowgroup)
  while((unsigned int)col >= scanners.size()) {
    scanners.push_back(std::shared_ptr<parquet::Scanner>());
    // If it doesn't exist, it's the rowId as of the last nextRowGroup call
    colRows.push_back(rowGroupStartRowId);
    colNulls.push_back(false);
    colIntValues.push_back(0);
    colDoubleValues.push_back(0);
    colByteArrayValues.push_back(parquet::ByteArray());
  }

  if(scanners[col].get() == NULL) {
    std::shared_ptr<parquet::ColumnReader> colReader = rowGroup->Column(col);
    scanners[col] = parquet::Scanner::Make(colReader);
    // TODO: potentially skip rows if rowsLeftInRowGroup != rowGroupMetadata->num_rows()
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

    switch(types[col]) {
      case parquet::Type::INT32:
      {
        parquet::Int32Scanner* s = (parquet::Int32Scanner*)scanners[col].get();
        int rv = 0;
        if(s->NextValue(&rv, &wasNull)) {
          colIntValues[col] = rv;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
        break;
      }
      case parquet::Type::FLOAT:
      {
        parquet::FloatScanner* s = (parquet::FloatScanner*)scanners[col].get();
        float rv = 0;
        if(s->NextValue(&rv, &wasNull)) {
          colDoubleValues[col] = rv;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
        break;
      }
      case parquet::Type::DOUBLE:
      {
        parquet::DoubleScanner* s = (parquet::DoubleScanner*)scanners[col].get();
        double rv = 0;
        if(s->NextValue(&rv, &wasNull)) {
          colDoubleValues[col] = rv;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
        break;
      }
      case parquet::Type::BYTE_ARRAY:
      {
        parquet::ByteArrayScanner* s = (parquet::ByteArrayScanner*)scanners[col].get();
        if(!s->NextValue(&colByteArrayValues[col], &wasNull)) {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
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
        parquet::Int96 rv;
        rv.value[0] = 0;
        rv.value[1] = 0;
        rv.value[2] = 0;
        if(s->NextValue(&rv, &wasNull)) {
          __int128 ns = rv.value[0] + ((unsigned long)rv.value[1] << 32);
          __int128 julianDay = rv.value[2];
          __int128 nsSinceEpoch = (julianDay - 2440588);
          nsSinceEpoch *= 86400;
          nsSinceEpoch *= 1000 * 1000 * 1000;
          nsSinceEpoch += ns;
          nsSinceEpoch /= 1000000;

          colIntValues[col] = nsSinceEpoch;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
        break;
      }
      case parquet::Type::INT64:
      {
        parquet::Int64Scanner* s = (parquet::Int64Scanner*)scanners[col].get();
        long rv = 0;
        if(s->NextValue(&rv, &wasNull)) {
          colIntValues[col] = rv;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
        break;
      }

      case parquet::Type::BOOLEAN:
      {
        parquet::BoolScanner* s = (parquet::BoolScanner*)scanners[col].get();
        bool rv = false;
        if(s->NextValue(&rv, &wasNull)) {
          colIntValues[col] = rv ? 1 : 0;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      {
        parquet::FixedLenByteArrayScanner* s = (parquet::FixedLenByteArrayScanner*)scanners[col].get();
        parquet::FixedLenByteArray flba;
        if(s->NextValue(&flba, &wasNull)) {
          colByteArrayValues[col].ptr = flba.ptr;
          // TODO: cache this
          colByteArrayValues[col].len = rowGroupMetadata->schema()->Column(col)->type_length();
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
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
  rowId = -1;
  // TODO: consider having a long lived handle in ParquetTable that can be borrowed
  // without incurring the cost of opening the file from scratch twice
  reader = parquet::ParquetFileReader::OpenFile(table->file.data());

  rowGroupId = -1;
  // TODO: handle the case where rowgroups have disjoint schemas?
  // TODO: or at least, fail fast if detected
  rowsLeftInRowGroup = 0;

  numRows = reader->metadata()->num_rows();
  numRowGroups = reader->metadata()->num_row_groups();
}

ParquetTable* ParquetCursor::getTable() { return table; }
