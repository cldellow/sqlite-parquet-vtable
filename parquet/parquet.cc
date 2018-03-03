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

#include "parquet/api/reader.h"

char const *EMPTY_STRING = "";

void gogo() {
  printf("ok");
  try {
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile("/home/cldellow/src/csv2parquet/12m.parquet.snappy");
    printf("%d\n", reader->metadata()->size());
    printf("%ld\n", reader->metadata()->num_rows());
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
  }
}

/* Max size of the error message in a CsvReader */
#define PARQUET_MXERR 200

/* Size of the CsvReader input buffer */
#define PARQUET_INBUFSZ 1024

/* A context object used when read a Parquet file. */
typedef struct CsvReader CsvReader;
struct CsvReader {
  FILE *in;              /* Read the Parquet text from this input stream */
  char *z;               /* Accumulated text for a field */
  int n;                 /* Number of bytes in z */
  int nAlloc;            /* Space allocated for z[] */
  int nLine;             /* Current line number */
  int bNotFirst;         /* True if prior text has been seen */
  int cTerm;             /* Character that terminated the most recent field */
  size_t iIn;            /* Next unread character in the input buffer */
  size_t nIn;            /* Number of characters in the input buffer */
  char *zIn;             /* The input buffer */
  char zErr[PARQUET_MXERR];  /* Error message */
};

/* Initialize a CsvReader object */
static void csv_reader_init(CsvReader *p){
  p->in = 0;
  p->z = 0;
  p->n = 0;
  p->nAlloc = 0;
  p->nLine = 0;
  p->bNotFirst = 0;
  p->nIn = 0;
  p->zIn = 0;
  p->zErr[0] = 0;
}

/* Close and reset a CsvReader object */
static void csv_reader_reset(CsvReader *p){
  if( p->in ){
    fclose(p->in);
    sqlite3_free(p->zIn);
  }
  sqlite3_free(p->z);
  csv_reader_init(p);
}

/* Report an error on a CsvReader */
static void csv_errmsg(CsvReader *p, const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(PARQUET_MXERR, p->zErr, zFormat, ap);
  va_end(ap);
}

/* Open the file associated with a CsvReader
** Return the number of errors.
*/
static int csv_reader_open(
  CsvReader *p,               /* The reader to open */
  const char *zFilename,      /* Read from this filename */
  const char *zData           /*  ... or use this data */
){
  if( zFilename ){
    p->zIn = (char*)sqlite3_malloc( PARQUET_INBUFSZ );
    if( p->zIn==0 ){
      csv_errmsg(p, "out of memory");
      return 1;
    }
    p->in = fopen(zFilename, "rb");
    if( p->in==0 ){
      csv_reader_reset(p);
      csv_errmsg(p, "cannot open '%s' for reading", zFilename);
      return 1;
    }
  }else{
    assert( p->in==0 );
    p->zIn = (char*)zData;
    p->nIn = strlen(zData);
  }
  return 0;
}

/* The input buffer has overflowed.  Refill the input buffer, then
** return the next character
*/
static int csv_getc_refill(CsvReader *p){
  size_t got;

  assert( p->iIn>=p->nIn );  /* Only called on an empty input buffer */
  assert( p->in!=0 );        /* Only called if reading froma file */

  got = fread(p->zIn, 1, PARQUET_INBUFSZ, p->in);
  if( got==0 ) return EOF;
  p->nIn = got;
  p->iIn = 1;
  return p->zIn[0];
}

/* Return the next character of input.  Return EOF at end of input. */
static int csv_getc(CsvReader *p){
  if( p->iIn >= p->nIn ){
    if( p->in!=0 ) return csv_getc_refill(p);
    return EOF;
  }
  return ((unsigned char*)p->zIn)[p->iIn++];
}

/* Increase the size of p->z and append character c to the end. 
** Return 0 on success and non-zero if there is an OOM error */
static int csv_resize_and_append(CsvReader *p, char c){
  char *zNew;
  int nNew = p->nAlloc*2 + 100;
  zNew = (char*)sqlite3_realloc64(p->z, nNew);
  if( zNew ){
    p->z = zNew;
    p->nAlloc = nNew;
    p->z[p->n++] = c;
    return 0;
  }else{
    csv_errmsg(p, "out of memory");
    return 1;
  }
}

/* Append a single character to the CsvReader.z[] array.
** Return 0 on success and non-zero if there is an OOM error */
static int csv_append(CsvReader *p, char c){
  if( p->n>=p->nAlloc-1 ) return csv_resize_and_append(p, c);
  p->z[p->n++] = c;
  return 0;
}

/* Read a single field of Parquet text.  Compatible with rfc4180 and extended
** with the option of having a separator other than ",".
**
**   +  Input comes from p->in.
**   +  Store results in p->z of length p->n.  Space to hold p->z comes
**      from sqlite3_malloc64().
**   +  Keep track of the line number in p->nLine.
**   +  Store the character that terminates the field in p->cTerm.  Store
**      EOF on end-of-file.
**
** Return "" at EOF.  Return 0 on an OOM error.
*/
static const char *csv_read_one_field(CsvReader *p){
  int c;
  p->n = 0;
  c = csv_getc(p);
  if( c==EOF ){
    p->cTerm = EOF;
    return EMPTY_STRING;
  }
  if( c=='"' ){
    int pc, ppc;
    int startLine = p->nLine;
    pc = ppc = 0;
    while( 1 ){
      c = csv_getc(p);
      if( c<='"' || pc=='"' ){
        if( c=='\n' ) p->nLine++;
        if( c=='"' ){
          if( pc=='"' ){
            pc = 0;
            continue;
          }
        }
        if( (c==',' && pc=='"')
         || (c=='\n' && pc=='"')
         || (c=='\n' && pc=='\r' && ppc=='"')
         || (c==EOF && pc=='"')
        ){
          do{ p->n--; }while( p->z[p->n]!='"' );
          p->cTerm = (char)c;
          break;
        }
        if( pc=='"' && c!='\r' ){
          csv_errmsg(p, "line %d: unescaped %c character", p->nLine, '"');
          break;
        }
        if( c==EOF ){
          csv_errmsg(p, "line %d: unterminated %c-quoted field\n",
                     startLine, '"');
          p->cTerm = (char)c;
          break;
        }
      }
      if( csv_append(p, (char)c) ) return 0;
      ppc = pc;
      pc = c;
    }
  }else{
    /* If this is the first field being parsed and it begins with the
    ** UTF-8 BOM  (0xEF BB BF) then skip the BOM */
    if( (c&0xff)==0xef && p->bNotFirst==0 ){
      csv_append(p, (char)c);
      c = csv_getc(p);
      if( (c&0xff)==0xbb ){
        csv_append(p, (char)c);
        c = csv_getc(p);
        if( (c&0xff)==0xbf ){
          p->bNotFirst = 1;
          p->n = 0;
          return csv_read_one_field(p);
        }
      }
    }
    while( c>',' || (c!=EOF && c!=',' && c!='\n') ){
      if( csv_append(p, (char)c) ) return 0;
      c = csv_getc(p);
    }
    if( c=='\n' ){
      p->nLine++;
      if( p->n>0 && p->z[p->n-1]=='\r' ) p->n--;
    }
    p->cTerm = (char)c;
  }
  if( p->z ) p->z[p->n] = 0;
  p->bNotFirst = 1;
  return p->z;
}


/* Forward references to the various virtual table methods implemented
** in this file. */
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
typedef struct ParquetTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  char *zFilename;                /* Name of the Parquet file */
  char *zData;                    /* Raw Parquet data in lieu of zFilename */
  long iStart;                    /* Offset to start of data in zFilename */
  unsigned int nCol;                       /* Number of columns in the Parquet file */
  unsigned int tstFlags;          /* Bit values used for testing */
} ParquetTable;


/* A cursor for the Parquet virtual table */
typedef struct ParquetCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  CsvReader rdr;                  /* The CsvReader object */
  char **azVal;                   /* Value of the current row */
  int *aLen;                      /* Length of each entry */
  sqlite3_int64 iRowid;           /* The current rowid.  Negative for EOF */
} ParquetCursor;

/* Transfer error message text from a reader into a ParquetTable */
static void csv_xfer_error(ParquetTable *pTab, CsvReader *pRdr){
  sqlite3_free(pTab->base.zErrMsg);
  pTab->base.zErrMsg = sqlite3_mprintf("%s", pRdr->zErr);
}

/*
** This method is the destructor fo a ParquetTable object.
*/
static int parquetDisconnect(sqlite3_vtab *pVtab){
  ParquetTable *p = (ParquetTable*)pVtab;
  sqlite3_free(p->zFilename);
  sqlite3_free(p->zData);
  sqlite3_free(p);
  return SQLITE_OK;
}

/* Skip leading whitespace.  Return a pointer to the first non-whitespace
** character, or to the zero terminator if the string has only whitespace */
static const char *csv_skip_whitespace(const char *z){
  while( isspace((unsigned char)z[0]) ) z++;
  return z;
}

/* Remove trailing whitespace from the end of string z[] */
static void csv_trim_whitespace(char *z){
  size_t n = strlen(z);
  while( n>0 && isspace((unsigned char)z[n]) ) n--;
  z[n] = 0;
}

/* Dequote the string */
static void csv_dequote(char *z){
  int j;
  char cQuote = z[0];
  size_t i, n;

  if( cQuote!='\'' && cQuote!='"' ) return;
  n = strlen(z);
  if( n<2 || z[n-1]!=z[0] ) return;
  for(i=1, j=0; i<n-1; i++){
    if( z[i]==cQuote && z[i+1]==cQuote ) i++;
    z[j++] = z[i];
  }
  z[j] = 0;
}

/* Check to see if the string is of the form:  "TAG = VALUE" with optional
** whitespace before and around tokens.  If it is, return a pointer to the
** first character of VALUE.  If it is not, return NULL.
*/
static const char *csv_parameter(const char *zTag, int nTag, const char *z){
  z = csv_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = csv_skip_whitespace(z+nTag);
  if( z[0]!='=' ) return 0;
  return csv_skip_whitespace(z+1);
}

/* Decode a parameter that requires a dequoted string.
**
** Return 1 if the parameter is seen, or 0 if not.  1 is returned
** even if there is an error.  If an error occurs, then an error message
** is left in p->zErr.  If there are no errors, p->zErr[0]==0.
*/
static int csv_string_parameter(
  CsvReader *p,            /* Leave the error message here, if there is one */
  const char *zParam,      /* Parameter we are checking for */
  const char *zArg,        /* Raw text of the virtual table argment */
  char **pzVal             /* Write the dequoted string value here */
){
  const char *zValue;
  zValue = csv_parameter(zParam,(int)strlen(zParam),zArg);
  if( zValue==0 ) return 0;
  p->zErr[0] = 0;
  if( *pzVal ){
    csv_errmsg(p, "more than one '%s' parameter", zParam);
    return 1;
  }
  *pzVal = sqlite3_mprintf("%s", zValue);
  if( *pzVal==0 ){
    csv_errmsg(p, "out of memory");
    return 1;
  }
  csv_trim_whitespace(*pzVal);
  csv_dequote(*pzVal);
  return 1;
}


/* Return 0 if the argument is false and 1 if it is true.  Return -1 if
** we cannot really tell.
*/
static int csv_boolean(const char *z){
  if( sqlite3_stricmp("yes",z)==0
   || sqlite3_stricmp("on",z)==0
   || sqlite3_stricmp("true",z)==0
   || (z[0]=='1' && z[1]==0)
  ){
    return 1;
  }
  if( sqlite3_stricmp("no",z)==0
   || sqlite3_stricmp("off",z)==0
   || sqlite3_stricmp("false",z)==0
   || (z[0]=='0' && z[1]==0)
  ){
    return 0;
  }
  return -1;
}


/*
** Parameters:
**    filename=FILENAME          Name of file containing Parquet content
**    data=TEXT                  Direct Parquet content.
**    schema=SCHEMA              Alternative Parquet schema.
**    header=YES|NO              First row of Parquet defines the names of
**                               columns if "yes".  Default "no".
**    columns=N                  Assume the Parquet file contains N columns.
**
**
** If schema= is omitted, then the columns are named "c0", "c1", "c2",
** and so forth.  If columns=N is omitted, then the file is opened and
** the number of columns in the first row is counted to determine the
** column count.  If header=YES, then the first row is skipped.
*/
static int parquetConnect(
  sqlite3 *db,
  void *pAux,
  int argcOrig, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  unsigned int argc = argcOrig;
  ParquetTable *pNew = 0;        /* The ParquetTable object to construct */
  int bHeader = -1;          /* header= flags.  -1 means not seen yet */
  int rc = SQLITE_OK;        /* Result code from this routine */
  unsigned int i, j;                  /* Loop counters */
  int nCol = -99;            /* Value of the columns= parameter */
  CsvReader sRdr;            /* A Parquet file reader used to store an error
                             ** message and/or to count the number of columns */
  static const char *azParam[] = {
     "filename", "data", "schema", 
  };
  char *azPValue[3];         /* Parameter values */
# define PARQUET_FILENAME (azPValue[0])
# define PARQUET_DATA     (azPValue[1])
# define PARQUET_SCHEMA   (azPValue[2])


  assert( sizeof(azPValue)==sizeof(azParam) );
  memset(&sRdr, 0, sizeof(sRdr));
  memset(azPValue, 0, sizeof(azPValue));
  for(i=3; i<argc; i++){
    const char *z = argv[i];
    const char *zValue;
    for(j=0; j<sizeof(azParam)/sizeof(azParam[0]); j++){
      if( csv_string_parameter(&sRdr, azParam[j], z, &azPValue[j]) ) break;
    }
    if( j<sizeof(azParam)/sizeof(azParam[0]) ){
      if( sRdr.zErr[0] ) goto parquet_connect_error;
    }else
    if( (zValue = csv_parameter("header",6,z))!=0 ){
      int x;
      if( bHeader>=0 ){
        csv_errmsg(&sRdr, "more than one 'header' parameter");
        goto parquet_connect_error;
      }
      x = csv_boolean(zValue);
      if( x==1 ){
        bHeader = 1;
      }else if( x==0 ){
        bHeader = 0;
      }else{
        csv_errmsg(&sRdr, "unrecognized argument to 'header': %s", zValue);
        goto parquet_connect_error;
      }
    }else
    if( (zValue = csv_parameter("columns",7,z))!=0 ){
      if( nCol>0 ){
        csv_errmsg(&sRdr, "more than one 'columns' parameter");
        goto parquet_connect_error;
      }
      nCol = atoi(zValue);
      if( nCol<=0 ){
        csv_errmsg(&sRdr, "must have at least one column");
        goto parquet_connect_error;
      }
    }else
    {
      csv_errmsg(&sRdr, "unrecognized parameter '%s'", z);
      goto parquet_connect_error;
    }
  }
  if( (PARQUET_FILENAME==0)==(PARQUET_DATA==0) ){
    csv_errmsg(&sRdr, "must either filename= or data= but not both");
    goto parquet_connect_error;
  }
  if( nCol<=0 && csv_reader_open(&sRdr, PARQUET_FILENAME, PARQUET_DATA) ){
    goto parquet_connect_error;
  }
  pNew = (ParquetTable*)sqlite3_malloc( sizeof(*pNew) );
  *ppVtab = (sqlite3_vtab*)pNew;
  if( pNew==0 ) goto parquet_connect_oom;
  memset(pNew, 0, sizeof(*pNew));
  if( nCol>0 ){
    pNew->nCol = nCol;
  }else{
    do{
      const char *z = csv_read_one_field(&sRdr);
      if( z==0 ) goto parquet_connect_oom;
      pNew->nCol++;
    }while( sRdr.cTerm==',' );
  }
  pNew->zFilename = PARQUET_FILENAME;  PARQUET_FILENAME = 0;
  pNew->zData = PARQUET_DATA;          PARQUET_DATA = 0;
  pNew->iStart = bHeader==1 ? ftell(sRdr.in) : 0;
  csv_reader_reset(&sRdr);
  if( PARQUET_SCHEMA==0 ){
    const char *zSep = EMPTY_STRING;
    PARQUET_SCHEMA = sqlite3_mprintf("CREATE TABLE x(");
    if( PARQUET_SCHEMA==0 ) goto parquet_connect_oom;
    for(i=0; i<pNew->nCol; i++){
      PARQUET_SCHEMA = sqlite3_mprintf("%z%sc%d TEXT",PARQUET_SCHEMA, zSep, i);
      zSep = ",";
    }
    PARQUET_SCHEMA = sqlite3_mprintf("%z);", PARQUET_SCHEMA);
  }
  rc = sqlite3_declare_vtab(db, PARQUET_SCHEMA);
  if( rc ) goto parquet_connect_error;
  for(i=0; i<sizeof(azPValue)/sizeof(azPValue[0]); i++){
    sqlite3_free(azPValue[i]);
  }
  return SQLITE_OK;

parquet_connect_oom:
  rc = SQLITE_NOMEM;
  csv_errmsg(&sRdr, "out of memory");

parquet_connect_error:
  if( pNew ) parquetDisconnect(&pNew->base);
  for(i=0; i<sizeof(azPValue)/sizeof(azPValue[0]); i++){
    sqlite3_free(azPValue[i]);
  }
  if( sRdr.zErr[0] ){
    sqlite3_free(*pzErr);
    *pzErr = sqlite3_mprintf("%s", sRdr.zErr);
  }
  csv_reader_reset(&sRdr);
  if( rc==SQLITE_OK ) rc = SQLITE_ERROR;
  return rc;
}

/*
** Reset the current row content held by a ParquetCursor.
*/
static void parquetCursorRowReset(ParquetCursor *pCur){
  ParquetTable *pTab = (ParquetTable*)pCur->base.pVtab;
  unsigned int i;
  for(i=0; i<pTab->nCol; i++){
    sqlite3_free(pCur->azVal[i]);
    pCur->azVal[i] = 0;
    pCur->aLen[i] = 0;
  }
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
** Destructor for a ParquetCursor.
*/
static int parquetClose(sqlite3_vtab_cursor *cur){
  ParquetCursor *pCur = (ParquetCursor*)cur;
  parquetCursorRowReset(pCur);
  csv_reader_reset(&pCur->rdr);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new ParquetTable cursor object.
*/
static int parquetOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  ParquetTable *pTab = (ParquetTable*)p;
  ParquetCursor *pCur;
  size_t nByte;
  nByte = sizeof(*pCur) + (sizeof(char*)+sizeof(int))*pTab->nCol;
  pCur = (ParquetCursor*)sqlite3_malloc64( nByte );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, nByte);
  pCur->azVal = (char**)&pCur[1];
  pCur->aLen = (int*)&pCur->azVal[pTab->nCol];
  *ppCursor = &pCur->base;
  if( csv_reader_open(&pCur->rdr, pTab->zFilename, pTab->zData) ){
    csv_xfer_error(pTab, &pCur->rdr);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}


/*
** Advance a ParquetCursor to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int parquetNext(sqlite3_vtab_cursor *cur){
  ParquetCursor *pCur = (ParquetCursor*)cur;
  ParquetTable *pTab = (ParquetTable*)cur->pVtab;
  unsigned int i = 0;
  const char *z;
  do{
    z = csv_read_one_field(&pCur->rdr);
    if( z==0 ){
      csv_xfer_error(pTab, &pCur->rdr);
      break;
    }
    if( i<pTab->nCol ){
      if( pCur->aLen[i] < pCur->rdr.n+1 ){
        char *zNew = (char*)sqlite3_realloc64(pCur->azVal[i], pCur->rdr.n+1);
        if( zNew==0 ){
          csv_errmsg(&pCur->rdr, "out of memory");
          csv_xfer_error(pTab, &pCur->rdr);
          break;
        }
        pCur->azVal[i] = zNew;
        pCur->aLen[i] = pCur->rdr.n+1;
      }
      memcpy(pCur->azVal[i], z, pCur->rdr.n+1);
      i++;
    }
  }while( pCur->rdr.cTerm==',' );
  if( z==0 || (pCur->rdr.cTerm==EOF && i<pTab->nCol) ){
    pCur->iRowid = -1;
  }else{
    pCur->iRowid++;
    while( i<pTab->nCol ){
      sqlite3_free(pCur->azVal[i]);
      pCur->azVal[i] = 0;
      pCur->aLen[i] = 0;
      i++;
    }
  }
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the ParquetCursor
** is currently pointing.
*/
static int parquetColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int iOrig                       /* Which column to return */
){
  unsigned int i = iOrig;
  ParquetCursor *pCur = (ParquetCursor*)cur;
  ParquetTable *pTab = (ParquetTable*)cur->pVtab;
  if( i>=0 && i<pTab->nCol && pCur->azVal[i]!=0 ){
    sqlite3_result_text(ctx, pCur->azVal[i], -1, SQLITE_STATIC);
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int parquetRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  ParquetCursor *pCur = (ParquetCursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int parquetEof(sqlite3_vtab_cursor *cur){
  ParquetCursor *pCur = (ParquetCursor*)cur;
  return pCur->iRowid<0;
}

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int parquetFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  ParquetCursor *pCur = (ParquetCursor*)pVtabCursor;
  ParquetTable *pTab = (ParquetTable*)pVtabCursor->pVtab;
  pCur->iRowid = 0;
  if( pCur->rdr.in==0 ){
    assert( pCur->rdr.zIn==pTab->zData );
    assert( pTab->iStart>=0 );
    assert( (size_t)pTab->iStart<=pCur->rdr.nIn );
    pCur->rdr.iIn = pTab->iStart;
  }else{
    fseek(pCur->rdr.in, pTab->iStart, SEEK_SET);
    pCur->rdr.iIn = 0;
    pCur->rdr.nIn = 0;
  }
  return parquetNext(pVtabCursor);
}

/*
* Only a forward full table scan is supported.  xBestIndex is mostly
* a no-op.
*/
static int parquetBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = 1000000;
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
