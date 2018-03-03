#include "parquet_cursor.h"

ParquetCursor::ParquetCursor(ParquetTable* table) {
  this->table = table;
  this->rowId = -1;
  // TODO: consider having a long lived handle in ParquetTable that can be borrowed
  // without incurring the cost of opening the file from scratch twice
  this->reader = parquet::ParquetFileReader::OpenFile(this->table->file.data());

  this->rowGroupId = -1;
  // TODO: handle the case where rowgroups have disjoint schemas?
  // TODO: or at least, fail fast if detected
  this->rowsLeftInRowGroup = 0;

  this->numRows = reader->metadata()->num_rows();
  this->numRowGroups = reader->metadata()->num_row_groups();
}

void ParquetCursor::nextRowGroup() {
  // TODO: skip row groups that cannot satisfy the constraints
  if(this->rowGroupId >= this->numRowGroups)
    return;

  rowGroupId++;
  rowGroupMetadata = this->reader->metadata()->RowGroup(0);
  rowsLeftInRowGroup = rowGroupMetadata->num_rows();
  rowGroup = reader->RowGroup(rowGroupId);
  for(unsigned int i = 0; i < scanners.size(); i++)
    scanners[i] = NULL;

  while(types.size() < (unsigned int)rowGroupMetadata->num_columns()) {
    types.push_back(rowGroupMetadata->schema()->Column(0)->physical_type());
  }

  for(unsigned int i = 0; i < (unsigned int)rowGroupMetadata->num_columns(); i++) {
    types[i] = rowGroupMetadata->schema()->Column(i)->physical_type();
  }
}

void ParquetCursor::next() {
  if(rowsLeftInRowGroup == 0)
    nextRowGroup();
  rowsLeftInRowGroup--;
  rowId++;
}

int ParquetCursor::getRowId() {
  return rowId;
}

bool ParquetCursor::eof() {
  return rowId >= numRows;
}

void ParquetCursor::ensureColumn(int col) {
  // need to ensure a scanner exists (and skip the # of rows in the rowgroup)
  while((unsigned int)col >= scanners.size()) {
    scanners.push_back(std::shared_ptr<parquet::Scanner>());
    colRows.push_back(-1);
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
    colRows[col] = rowId;
    bool wasNull = false;

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
      }
      break;
      case parquet::Type::DOUBLE:
      {
        parquet::DoubleScanner* s = (parquet::DoubleScanner*)scanners[col].get();
        double rv = 0;
        if(s->NextValue(&rv, &wasNull)) {
          colDoubleValues[col] = rv;
        } else {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
      }
      break;
      case parquet::Type::BYTE_ARRAY:
      {
        parquet::ByteArrayScanner* s = (parquet::ByteArrayScanner*)scanners[col].get();
        if(!s->NextValue(&colByteArrayValues[col], &wasNull)) {
          throw std::invalid_argument("unexpectedly lacking a next value");
        }
      }
      break;

      case parquet::Type::BOOLEAN:
      case parquet::Type::INT64:
      case parquet::Type::FLOAT:
      case parquet::Type::INT96:
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      default:
        throw std::invalid_argument("cannot handle");
      break;
    }

    colNulls[col] = wasNull;
  }
}

bool ParquetCursor::isNull(int col) {
  return colNulls[col];
}

int ParquetCursor::getInt(int col) {
  return colIntValues[col];
}

double ParquetCursor::getDouble(int col) {
  return colDoubleValues[col];
}

parquet::ByteArray* ParquetCursor::getByteArray(int col) {
  return &colByteArrayValues[col];
}



parquet::Type::type ParquetCursor::getPhysicalType(int col) {
//  return rowGroupMetadata->schema()->Column(col)->physical_type();
  return types[col];
}
