/*
* This file contains the implementation of an SQLite virtual table for
* reading Parquet files.
*
* Usage:
*
*    .load ./parquet
*    CREATE VIRTUAL TABLE demo USING parquet(FILENAME);
*    SELECT * FROM demo;
*
*/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>

#include <memory>

#include "parquet_table.h"
#include "parquet_cursor.h"
#include "parquet_filter.h"

/* Forward references to the various virtual table methods implemented
 * in this file. */
static int parquetCreate(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int parquetConnect(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int parquetBestIndex(sqlite3_vtab*,sqlite3_index_info*);
static int parquetDisconnect(sqlite3_vtab*);
static int parquetOpen(sqlite3_vtab*, sqlite3_vtab_cursor**);
static int parquetClose(sqlite3_vtab_cursor*);
static int parquetFilter(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv);
static int parquetNext(sqlite3_vtab_cursor*);
static int parquetEof(sqlite3_vtab_cursor*);
static int parquetColumn(sqlite3_vtab_cursor*,sqlite3_context*,int);
static int parquetRowid(sqlite3_vtab_cursor*,sqlite3_int64*);

/* An instance of the Parquet virtual table */
typedef struct sqlite3_vtab_parquet {
  sqlite3_vtab base;              /* Base class.  Must be first */
  ParquetTable* table;
} sqlite3_vtab_parquet;


/* A cursor for the Parquet virtual table */
typedef struct sqlite3_vtab_cursor_parquet {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  ParquetCursor* cursor;
} sqlite3_vtab_cursor_parquet;

/*
** This method is the destructor fo a sqlite3_vtab_parquet object.
*/
static int parquetDisconnect(sqlite3_vtab *pVtab){
  sqlite3_vtab_parquet *p = (sqlite3_vtab_parquet*)pVtab;
  delete p->table;
  sqlite3_free(p);
  return SQLITE_OK;
}

static int parquetConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  if(argc != 4 || strlen(argv[3]) < 2) {
    *pzErr = sqlite3_mprintf("must provide exactly one argument, the path to a parquet file");
    return SQLITE_ERROR;
  }

  // Remove the delimiting single quotes
  std::string fname = argv[3];
  fname = fname.substr(1, fname.length() - 2);
  std::unique_ptr<ParquetTable> table(new ParquetTable(fname));

  std::unique_ptr<sqlite3_vtab_parquet, void(*)(void*)> vtab(
      (sqlite3_vtab_parquet*)sqlite3_malloc(sizeof(sqlite3_vtab_parquet)),
      sqlite3_free);
  memset(vtab.get(), 0, sizeof(*vtab.get()));

  try {
    std::string create = table->CreateStatement();
    int rc = sqlite3_declare_vtab(db, create.data());
    if(rc)
      return rc;

  } catch (const std::exception& e) {
    *pzErr = sqlite3_mprintf(e.what());
    return SQLITE_ERROR;
  }

  vtab->table = table.release();
  *ppVtab = (sqlite3_vtab*)vtab.release();
  return SQLITE_OK;
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int parquetCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
 return parquetConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a sqlite3_vtab_cursor_parquet.
*/
static int parquetClose(sqlite3_vtab_cursor *cur){
  sqlite3_vtab_cursor_parquet* p = (sqlite3_vtab_cursor_parquet*)cur;
  p->cursor->close();
  delete p->cursor;
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new sqlite3_vtab_parquet cursor object.
*/
static int parquetOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  std::unique_ptr<sqlite3_vtab_cursor_parquet, void(*)(void*)> cursor(
      (sqlite3_vtab_cursor_parquet*)sqlite3_malloc(sizeof(sqlite3_vtab_cursor_parquet)),
      sqlite3_free);
  memset(cursor.get(), 0, sizeof(*cursor.get()));

  sqlite3_vtab_parquet* pParquet = (sqlite3_vtab_parquet*)p;
  cursor->cursor = new ParquetCursor(pParquet->table);

  *ppCursor = (sqlite3_vtab_cursor*)cursor.release();
  return SQLITE_OK;
}

const char* opName(int op) {
  switch(op) {
    case SQLITE_INDEX_CONSTRAINT_EQ:
      return "=";
    case SQLITE_INDEX_CONSTRAINT_GT:
      return ">";
    case SQLITE_INDEX_CONSTRAINT_LE:
      return "<=";
    case SQLITE_INDEX_CONSTRAINT_LT:
      return "<";
    case SQLITE_INDEX_CONSTRAINT_GE:
      return ">=";
    case SQLITE_INDEX_CONSTRAINT_MATCH:
      return "match";
    case SQLITE_INDEX_CONSTRAINT_LIKE:
      return "LIKE";
    case SQLITE_INDEX_CONSTRAINT_GLOB:
      return "GLOB";
    case SQLITE_INDEX_CONSTRAINT_REGEXP:
      return "REGEXP";
    case SQLITE_INDEX_CONSTRAINT_NE:
      return "!=";
    case SQLITE_INDEX_CONSTRAINT_ISNOT:
      return "IS NOT";
    case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
      return "IS NOT NULL";
    case SQLITE_INDEX_CONSTRAINT_ISNULL:
      return "IS NULL";
    case SQLITE_INDEX_CONSTRAINT_IS:
      return "IS";
    default:
      return "unknown";
  }
}

/*
** Advance a sqlite3_vtab_cursor_parquet to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int parquetNext(sqlite3_vtab_cursor *cur){
  ParquetCursor* cursor = ((sqlite3_vtab_cursor_parquet*)cur)->cursor;
  cursor->next();
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the sqlite3_vtab_cursor_parquet
** is currently pointing.
*/
static int parquetColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int col                       /* Which column to return */
){
  ParquetCursor *cursor = ((sqlite3_vtab_cursor_parquet*)cur)->cursor;
  cursor->ensureColumn(col);

  if(cursor->isNull(col)) {
    sqlite3_result_null(ctx);
  } else {
    switch(cursor->getPhysicalType(col)) {
      case parquet::Type::BOOLEAN:
      case parquet::Type::INT32:
      {
        int rv = cursor->getInt32(col);
        sqlite3_result_int(ctx, rv);
        break;
      }
      case parquet::Type::FLOAT:
      case parquet::Type::DOUBLE:
      {
        double rv = cursor->getDouble(col);
        sqlite3_result_double(ctx, rv);
        break;
      }
      case parquet::Type::BYTE_ARRAY:
      {
        parquet::ByteArray* rv = cursor->getByteArray(col);
        if(cursor->getLogicalType(col) == parquet::LogicalType::UTF8) {
          sqlite3_result_text(ctx, (const char*)rv->ptr, rv->len, SQLITE_TRANSIENT);
        } else {
          sqlite3_result_blob(ctx, (void*)rv->ptr, rv->len, SQLITE_TRANSIENT);
        }
        break;
      }
      case parquet::Type::INT96:
        // This type exists to store timestamps in nanoseconds due to legacy
        // reasons. We just interpret it as a timestamp in milliseconds.
      case parquet::Type::INT64:
      {
        long rv = cursor->getInt64(col);
        sqlite3_result_int64(ctx, rv);
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      {
        parquet::ByteArray* rv = cursor->getByteArray(col);
        sqlite3_result_blob(ctx, (void*)rv->ptr, rv->len, SQLITE_TRANSIENT);
        break;
      }
      default:
        // Should be impossible to get here as we should have forbidden this at
        // CREATE time -- maybe file changed underneath us?
        std::ostringstream ss;
        ss << __FILE__ << ":" << __LINE__ << ": column " << col << " has unsupported type: " <<
          parquet::TypeToString(cursor->getPhysicalType(col));

        throw std::invalid_argument(ss.str());
        break;
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int parquetRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  ParquetCursor *cursor = ((sqlite3_vtab_cursor_parquet*)cur)->cursor;
  *pRowid = cursor->getRowId(); 
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int parquetEof(sqlite3_vtab_cursor *cur){
  ParquetCursor* cursor = ((sqlite3_vtab_cursor_parquet*)cur)->cursor;
  if(cursor->eof())
    return 1;
  return 0;
}

void debugConstraints(sqlite3_index_info *pIdxInfo, ParquetTable *table, int argc, sqlite3_value** argv) {
  printf("debugConstraints, argc=%d\n", argc);
  int j = 0;
  for(int i = 0; i < pIdxInfo->nConstraint; i++) {
    std::string valueStr = "?";
    if(argv != NULL && pIdxInfo->aConstraint[i].usable) {
      int type = sqlite3_value_type(argv[j]);
      switch(type) {
        case SQLITE_INTEGER:
        {
          sqlite3_int64 rv = sqlite3_value_int64(argv[j]);
          std::ostringstream ss;
          ss << rv;
          valueStr = ss.str();
          break;
        }
        case SQLITE_FLOAT:
        {
          double rv = sqlite3_value_double(argv[j]);
          std::ostringstream ss;
          ss << rv;
          valueStr = ss.str();
          break;
        }
        case SQLITE_TEXT:
        {
          const unsigned char* rv = sqlite3_value_text(argv[j]);
          std::ostringstream ss;
          ss << "'" << rv << "'";
          valueStr = ss.str();
          break;
        }
        case SQLITE_BLOB:
        {
          int sizeBytes = sqlite3_value_bytes(argv[j]);
          std::ostringstream ss;
          ss << "'..." << sizeBytes << "-byte blob...'";
          valueStr = ss.str();
          break;
        }
        case SQLITE_NULL:
        {
          valueStr = "NULL";
          break;
        }
      }
      j++;
    }
    printf("  constraint %d: col %s %s %s, usable %d\n",
        i,
        table->columnName(pIdxInfo->aConstraint[i].iColumn).data(),
        opName(pIdxInfo->aConstraint[i].op),
        valueStr.data(),
        pIdxInfo->aConstraint[i].usable);
  }
}

ConstraintOperator constraintOperatorFromSqlite(int op) {
  switch(op) {
    case SQLITE_INDEX_CONSTRAINT_EQ:
      return Equal;
    case SQLITE_INDEX_CONSTRAINT_GT:
      return GreaterThan;
    case SQLITE_INDEX_CONSTRAINT_LE:
      return LessThanOrEqual;
    case SQLITE_INDEX_CONSTRAINT_LT:
      return LessThan;
    case SQLITE_INDEX_CONSTRAINT_GE:
      return GreaterThanOrEqual;
    case SQLITE_INDEX_CONSTRAINT_MATCH:
      return Match;
    case SQLITE_INDEX_CONSTRAINT_LIKE:
      return Like;
    case SQLITE_INDEX_CONSTRAINT_GLOB:
      return Glob;
    case SQLITE_INDEX_CONSTRAINT_REGEXP:
      return Regexp;
    case SQLITE_INDEX_CONSTRAINT_NE:
      return NotEqual;
    case SQLITE_INDEX_CONSTRAINT_ISNOT:
      return IsNot;
    case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
      return IsNotNull;
    case SQLITE_INDEX_CONSTRAINT_ISNULL:
      return IsNull;
    case SQLITE_INDEX_CONSTRAINT_IS:
      return Is;
  }

  std::ostringstream ss;
  ss << __FILE__ << ":" << __LINE__ << ": operator " << op << " is unsupported";
  throw std::invalid_argument(ss.str());
}

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int parquetFilter(
  sqlite3_vtab_cursor *cur,
  int idxNum,
  const char *idxStr,
  int argc,
  sqlite3_value **argv
){
  ParquetCursor* cursor = ((sqlite3_vtab_cursor_parquet*)cur)->cursor;
  printf("xFilter: idxNum=%d, idxStr=%lu, argc=%d\n", idxNum, (long unsigned int)idxStr, argc);
  sqlite3_index_info* indexInfo = (sqlite3_index_info*)idxStr;
  debugConstraints(indexInfo, cursor->getTable(), argc, argv);
  std::vector<Constraint> constraints;
  int j = 0;
  for(int i = 0; i < indexInfo->nConstraint; i++) {
    if(!indexInfo->aConstraint[i].usable) {
      continue;
    }

    ValueType type = Null;
    int64_t intValue = 0;
    double doubleValue = 0;
    std::vector<unsigned char> blobValue;
    int sqliteType = sqlite3_value_type(argv[j]);

    if(sqliteType == SQLITE_INTEGER) {
      type = Integer;
      intValue = sqlite3_value_int64(argv[j]);
    } else if(sqliteType == SQLITE_FLOAT) {
      type = Double;
      doubleValue = sqlite3_value_double(argv[j]);
    } else if(sqliteType == SQLITE_TEXT) {
      type = Text;
      int len = sqlite3_value_bytes(argv[j]);
      const unsigned char* ptr = sqlite3_value_text(argv[j]);
      for(int k = 0; k < len; k++) {
        blobValue.push_back(ptr[k]);
      }
    } else if(sqliteType == SQLITE_BLOB) {
      type = Blob;
      int len = sqlite3_value_bytes(argv[j]);
      const unsigned char* ptr = (const unsigned char*)sqlite3_value_blob(argv[j]);
      for(int k = 0; k < len; k++) {
        blobValue.push_back(ptr[k]);
      }
    } else if(sqliteType == SQLITE_NULL) {
      type = Null;
    }

    Constraint constraint(
      indexInfo->aConstraint[i].iColumn,
      constraintOperatorFromSqlite(indexInfo->aConstraint[i].op),
      type,
      intValue,
      doubleValue,
      blobValue);
    constraints.push_back(constraint);
    j++;
  }
  cursor->reset(constraints);
  return parquetNext(cur);
}

/*
* We'll always indicate to SQLite that we prefer it to use an index so that it will
* pass additional context to xFilter, which we may or may not use.
*
* We copy the sqlite3_index_info structure, as is, into idxStr for later use.
*/
static int parquetBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  ParquetTable* table = ((sqlite3_vtab_parquet*)tab)->table;

  printf("xBestIndex: nConstraint=%d, nOrderBy=%d\n", pIdxInfo->nConstraint, pIdxInfo->nOrderBy);
  debugConstraints(pIdxInfo, table, 0, NULL);

  if(pIdxInfo->nConstraint == 0) {
    pIdxInfo->estimatedCost = 1000000000000;
    pIdxInfo->idxNum = 0;
  } else {
    pIdxInfo->estimatedCost = 1;
    pIdxInfo->idxNum = 1;
    int j = 0;
    for(int i = 0; i < pIdxInfo->nConstraint; i++) {
      if(pIdxInfo->aConstraint[i].usable) {
        j++;
        pIdxInfo->aConstraintUsage[i].argvIndex = j;
      }
    }

    // TODO: consider setting this when querying by rowid? Unclear if that's implied.
    // pIdxInfo->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
  }
  printf("idx %d has cost %f\n", pIdxInfo->idxNum, pIdxInfo->estimatedCost);

  size_t dupeSize = sizeof(sqlite3_index_info) +
    //pIdxInfo->nConstraint * sizeof(sqlite3_index_constraint) +
    pIdxInfo->nConstraint * sizeof(sqlite3_index_info::sqlite3_index_constraint) +
    pIdxInfo->nOrderBy * sizeof(sqlite3_index_info::sqlite3_index_orderby) +
    pIdxInfo->nConstraint * sizeof(sqlite3_index_info::sqlite3_index_constraint_usage);
  sqlite3_index_info* dupe = (sqlite3_index_info*)sqlite3_malloc(dupeSize);
  pIdxInfo->idxStr = (char*)dupe;
  pIdxInfo->needToFreeIdxStr = 1;

  // TODO: populate argvIndex.
  memset(dupe, 0, dupeSize);
  memcpy(dupe, pIdxInfo, sizeof(sqlite3_index_info));

  dupe->aConstraint = (sqlite3_index_info::sqlite3_index_constraint*)((char*)dupe + sizeof(sqlite3_index_info));
  dupe->aOrderBy = (sqlite3_index_info::sqlite3_index_orderby*)((char*)dupe +
      sizeof(sqlite3_index_info) +
      pIdxInfo->nConstraint * sizeof(sqlite3_index_info::sqlite3_index_constraint));
  dupe->aConstraintUsage = (sqlite3_index_info::sqlite3_index_constraint_usage*)((char*)dupe +
      sizeof(sqlite3_index_info) +
      pIdxInfo->nConstraint * sizeof(sqlite3_index_info::sqlite3_index_constraint) +
      pIdxInfo->nOrderBy * sizeof(sqlite3_index_info::sqlite3_index_orderby));


  for(int i = 0; i < pIdxInfo->nConstraint; i++) {
    dupe->aConstraint[i].iColumn = pIdxInfo->aConstraint[i].iColumn;
    dupe->aConstraint[i].op = pIdxInfo->aConstraint[i].op;
    dupe->aConstraint[i].usable = pIdxInfo->aConstraint[i].usable;
    dupe->aConstraint[i].iTermOffset = pIdxInfo->aConstraint[i].iTermOffset;

    dupe->aConstraintUsage[i].argvIndex = pIdxInfo->aConstraintUsage[i].argvIndex;
    dupe->aConstraintUsage[i].omit = pIdxInfo->aConstraintUsage[i].omit;
  }

  for(int i = 0; i < pIdxInfo->nOrderBy; i++) {
    dupe->aOrderBy[i].iColumn = pIdxInfo->aOrderBy[i].iColumn;
    dupe->aOrderBy[i].desc = pIdxInfo->aOrderBy[i].desc;
  }

  return SQLITE_OK;
}


static sqlite3_module ParquetModule = {
  0,                       /* iVersion */
  parquetCreate,            /* xCreate */
  parquetConnect,           /* xConnect */
  parquetBestIndex,         /* xBestIndex */
  parquetDisconnect,        /* xDisconnect */
  parquetDisconnect,        /* xDestroy */
  parquetOpen,              /* xOpen - open a cursor */
  parquetClose,             /* xClose - close a cursor */
  parquetFilter,            /* xFilter - configure scan constraints */
  parquetNext,              /* xNext - advance a cursor */
  parquetEof,               /* xEof - check for end of scan */
  parquetColumn,            /* xColumn - read data */
  parquetRowid,             /* xRowid - read data */
  0,                       /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};

/* 
* This routine is called when the extension is loaded.  The new
* Parquet virtual table module is registered with the calling database
* connection.
*/
extern "C" {
  int sqlite3_parquet_init(
    sqlite3 *db, 
    char **pzErrMsg, 
    const sqlite3_api_routines *pApi
  ){
    int rc;
    SQLITE_EXTENSION_INIT2(pApi);
    rc = sqlite3_create_module(db, "parquet", &ParquetModule, 0);
    return rc;
  }
}
