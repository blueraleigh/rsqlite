#include <R.h>
#include <Rinternals.h>
#include "include/sqlite3.h"


/**********************************************************************
*
** Private functions. Functions below are not directly interacted with
** by the R user.
**
**********************************************************************
*/


struct connection;
struct connection {
    sqlite3* db;            // sqlite3 database handle
    int bufsize;            // size of result buffer
    SEXP name;              // name of the database
};


struct connection* connection_init(const char* dbname);
struct connection* connection_init(const char* dbname)
{
    struct connection* db = Calloc(1, struct connection);
    int rc = sqlite3_open_v2(dbname, &(db->db), SQLITE_OPEN_READONLY, NULL);

    if (rc) {
        const char* errmsg = sqlite3_errmsg(db->db);
        sqlite3_close_v2(db->db);
        Free(db);
        error(errmsg);
    }

    db->bufsize = 5000;   // initialize buffer to hold a limit of 5000 rows

    // requires special compilation flags to sqlite to work
    sqlite3_db_config(db->db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL);
    sqlite3_enable_load_extension(db->db, 1);
    return db;
}


void connection_free(struct connection* db);
void connection_free(struct connection* db)
{
    if (db == NULL)
        return;
    sqlite3_close_v2(db->db);
    Free(db);
    return;
}


void connection_close(SEXP rdb);
void connection_close(SEXP rdb)
{
    if (TYPEOF(rdb) == NILSXP)
        return;
    struct connection* db = (struct connection*)R_ExternalPtrAddr(rdb);
    sqlite3_close_v2(db->db);
    db->db = NULL;
    return;
}


void connection_destroy(SEXP rdb);
void connection_destroy(SEXP rdb)
{
    if (TYPEOF(rdb) == NILSXP)
        return;
    struct connection* db = (struct connection*)R_ExternalPtrAddr(rdb);
    connection_free(db);
    R_ClearExternalPtr(rdb);
    return;
}


// callback to invoke for processing a row of query results
SEXP process_row(sqlite3_stmt* stmt);
SEXP process_row(sqlite3_stmt* stmt)
{
    int ncol = sqlite3_column_count(stmt);
    SEXP row = PROTECT(allocVector(VECSXP, ncol));
    SEXP names = PROTECT(allocVector(STRSXP, ncol));
    SEXP field;
    for (int i = 0; i < ncol; ++i) {
        switch(sqlite3_column_type(stmt, i)) {
            case SQLITE_INTEGER:
                field = PROTECT(allocVector(INTSXP, 1));
                INTEGER(field)[0] = sqlite3_column_int(stmt, i);
                break;
            case SQLITE_FLOAT:
                field = PROTECT(allocVector(REALSXP, 1));
                REAL(field)[0] = sqlite3_column_double(stmt, i);
                break;
            case SQLITE_TEXT:
                field = PROTECT(allocVector(STRSXP, 1));
                SET_STRING_ELT(field, 0, mkChar(
                    (const char*)sqlite3_column_text(stmt, i)));
                break;
            case SQLITE_NULL:
            case SQLITE_BLOB:
            default:
                field = PROTECT(allocVector(NILSXP, 1));
                break;
        }
        SET_VECTOR_ELT(row, i, field);
        SET_STRING_ELT(names, i, mkChar(sqlite3_column_name(stmt, i)));
        UNPROTECT(1);
    }
    setAttrib(row, R_NamesSymbol, names);
    UNPROTECT(2);
    return row;
}


/**********************************************************************
**
** Public interface functions. Functions below are called by the user
** from the R side
**
**********************************************************************
*/


SEXP rsqlite_connect(SEXP dbname, SEXP bufsize);
SEXP rsqlite_connect(SEXP dbname, SEXP bufsize)
{
    struct connection* db = connection_init(CHAR(STRING_ELT(dbname, 0)));

    if (TYPEOF(bufsize) == INTSXP)
        db->bufsize = INTEGER(bufsize)[0];
    db->name = dbname;
    SEXP rdb = PROTECT(R_MakeExternalPtr(db, db->name, R_NilValue));
    R_RegisterCFinalizerEx(rdb, &connection_destroy, 1);
    UNPROTECT(1);
    return rdb;
}


SEXP rsqlite_disconnect(SEXP rdb);
SEXP rsqlite_disconnect(SEXP rdb)
{
    connection_close(rdb);
    return R_NilValue;
}


SEXP rsqlite_dbinfo(SEXP rdb);
SEXP rsqlite_dbinfo(SEXP rdb)
{
    struct connection* db = (struct connection*)R_ExternalPtrAddr(rdb);
    if (db->db == NULL)
        error("The database connection is closed.");
    SEXP dbinfo = PROTECT(allocVector(VECSXP, 2));
    SEXP bufsize = PROTECT(allocVector(INTSXP, 1));
    INTEGER(bufsize)[0] = db->bufsize;
    SET_VECTOR_ELT(dbinfo, 0, db->name);
    SET_VECTOR_ELT(dbinfo, 1, bufsize);
    SEXP names = PROTECT(allocVector(STRSXP, 2));
    SET_STRING_ELT(names, 0, mkChar("database_file"));
    SET_STRING_ELT(names, 1, mkChar("buffer_size"));
    setAttrib(dbinfo, R_NamesSymbol, names);
    UNPROTECT(3);
    return dbinfo;
}


SEXP rsqlite_dblimit(SEXP rdb, SEXP limit);
SEXP rsqlite_dblimit(SEXP rdb, SEXP limit)
{
    struct connection* db = (struct connection*)R_ExternalPtrAddr(rdb);
    if (db->db == NULL)
        error("The database connection is closed.");
    db->bufsize = INTEGER(limit)[0];
    return R_NilValue;
}


SEXP rsqlite_eval(SEXP rdb, SEXP sql);
SEXP rsqlite_eval(SEXP rdb, SEXP sql)
{
    const char* zSql = CHAR(STRING_ELT(sql, 0));  // sql statement to process
    sqlite3_stmt* pStmt;     // prepared sql statement

    struct connection* db = (struct connection*)R_ExternalPtrAddr(rdb);
    if (db->db == NULL)
        error("The database connection is closed.");
    sqlite3_prepare_v2(db->db, zSql, -1, &pStmt, NULL);

    int nrow = 0;
    int count = 0;
    int rc = SQLITE_ROW;

    // https://stackoverflow.com/a/8797232 for method
    SEXP buf;
    SEXP root = PROTECT(list1(buf = allocVector(VECSXP, db->bufsize)));
    SEXP tail = root;

    while (rc == SQLITE_ROW) {
        rc = sqlite3_step(pStmt);
        if (rc == SQLITE_ROW) {
            SET_VECTOR_ELT(buf, count++, process_row(pStmt));
            ++nrow;
            if (count == db->bufsize) {
                tail = SETCDR(tail, list1(buf = allocVector(VECSXP, db->bufsize)));
                count = 0;
            }
        } else if (rc != SQLITE_DONE) {
            sqlite3_finalize(pStmt);
            UNPROTECT(1);
            error(sqlite3_errmsg(db->db));
        }
    }
    sqlite3_finalize(pStmt);

    SEXP res = PROTECT(allocVector(VECSXP, nrow));
    int end = 0;
    while (root != R_NilValue) {
        int size = (CDR(root) == R_NilValue) ? count : db->bufsize;
        for (int i = 0; i < size; ++i) {
            SET_VECTOR_ELT(res, end++, VECTOR_ELT(CAR(root), i));
        }
        root = CDR(root);
    }

    UNPROTECT(2);
    return res;
}
