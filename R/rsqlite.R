##' @title Connect to a SQLite database file
##' @aliases db.open
##' @description Creates a connection with a SQLite database file.
##' @param dbfile The file path to the database.
##' @param bufsize The maximum number of results to read before triggering
##'   a memory allocation. The default is 5000.
##' @return An external pointer that can be used as a database handle
##'   for the other functions in the package.
db.connect = function(dbfile, bufsize=NULL) {
    stopifnot(class(dbfile) == "character")
    if (!is.null(bufsize)) {
        stopifnot(class(bufsize) == "integer" || class(bufsize) == "numeric")
        bufsize = as.integer(bufsize)
    }
    db = .Call(rsqlite_connect, dbfile, bufsize)
    class(db) = "db"
    return (db)
}


db.open = function(dbfile, bufsize=NULL) {
    db.connect(dbfile, bufsize)
}


##' @title Database handle test
##' @return A logical indicating whether the
##' object inherits the \code{db} class
is.db = function(db) {
    return (inherits(db, "db"))
}


##' @title Disconnect from a SQLite database file
##' @aliases db.close
##' @description Closes the connection with a SQLite database file.
##' @param db The database connection handle returned by \code{db.connect}.
db.disconnect = function(db) {
    stopifnot(is.db(db))
    invisible(.Call(rsqlite_disconnect, db))
}


db.close = function(db) {
    stopifnot(is.db(db))
    db.disconnect(db)
}


##' @title Evaluate a SQL statement
##' @description Sends a SQL statement to the database and evaluates it.
##' @param db The database connection handle returned by \code{db.connect}.
##' @param sql The SQL text to evaluate.
##' @return A matrix representing the result of the evaluation.
##' @note The matrix is non-standard in the sense that it is a container for
##'     lists (i.e. subsets of the matrix return lists).
db.eval = function(db, sql) {
    stopifnot(is.db(db))
    sql = enc2utf8(sql)
    res = .Call(rsqlite_eval, db, sql)
    return (do.call(rbind, res))
}


##' @title Database connection metadata
##' @description Return the metadata association with a database connection handle.
##' @param db The database connection handle returned by \code{db.connect}.
##' @return A list. Each item in the list is a token of metadata associated with the
##'    database connection pointer.
db.info = function(db) {
    stopifnot(is.db(db))
    .Call(rsqlite_dbinfo, db)
}


##' @title Set the result limit
##' @description Alters the size on the result buffer.
##' @param db The database connection handle returned by \code{db.connect}.
##' @param limit The new buffer size to use.
db.limit = function(db, limit) {
    stopifnot(is.db(db))
    stopifnot(!is.null(limit))
    stopifnot(class(limit) == "integer" || class(limit) == "numeric")
    invisible(.Call(rsqlite_dblimit, db, as.integer(limit)))
}


##' @title Database tables
##' @description Return the names of tables in the database.
##' @param db The database connection handle returned by \code{db.connect}.
##' @return A vector of table names.
list.tables = function(db) {
    stopifnot(is.db(db))
    sql = enc2utf8("SELECT tbl_name FROM sqlite_master WHERE type=\"table\"")
    res = .Call(rsqlite_eval, db, sql)
    return (sapply(res, "[[", "tbl_name"))
}


##' @title Table fields
##' @description Return the fields associated with a table.
##' @param db The database connection handle returned by \code{db.connect}.
##' @param name The name of a database table.
##' @return A vector of field names.
list.fields = function(db, name) {
    stopifnot(is.db(db))
    sql = enc2utf8(sprintf("PRAGMA table_info(%s)", name))
    res = .Call(rsqlite_eval, db, sql)
    return (sapply(res, "[[", "name"))
}
