# rsqlite

Provides a read-only interface to execute SELECT statements on a [SQLite](https://sqlite.org) 
database file from within an R session. Not to be confused with [RSQLite](https://github.com/r-dbi/RSQLite). 
That package is a bit more complicated than this one due to its greater functionality and the considerable 
boilerplate code that goes along with the [R-DBI](https://www.r-dbi.org/) project. By contrast, this package 
just sticks to the simple API functions provided by the SQLite library.
