/*
 *   CuttDB - a fast key-value storage engine
 *
 *
 *   http://code.google.com/p/cuttdb/
 *   
 *   Copyright (c) 2012, Siyuan Fu.  All rights reserved.
 *   Use and distribution licensed under the BSD license. 
 *   See the LICENSE file for full text
 *
 *   Author: Siyuan Fu <fusiyuan2010@gmail.com>
 *
 */


#ifndef _CUTTDB_H_
#define _CUTTDB_H_
#include <stdint.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct CDB CDB;
typedef void (*CDB_ERRCALLBACK)(void *, int, const char *, int);
typedef bool (*CDB_ITERCALLBACK)(void *, const char *, int, const char *, int, uint32_t, uint64_t);

/* performance statistical information of an database instance */
typedef struct {
    /* number of records in db */
    uint64_t rnum;
    /* number of records in cache */
    uint64_t rcnum;
    /* number of pages in db */
    uint64_t pnum;
    /* number of pages in cache */
    uint64_t pcnum;
    /* cache hit of record cache */
    uint64_t rchit;
    /* cache miss of record cache */
    uint64_t rcmiss;
    /* cache hit of page cache */
    uint64_t pchit;
    /* cache miss of page cache */
    uint64_t pcmiss;
    /* average disk read latency */
    uint32_t rlatcy;
    /* average disk write latency */
    uint32_t wlatcy;
} CDBSTAT;

/* options to open a database*/
enum {
    /* create an database if not exist */
    CDB_CREAT = 0x1,
    /* clean the database if already exist */
    CDB_TRUNC = 0x2,
    /* fill the cache when start up */
    CDB_PAGEWARMUP = 0x4,
};

/* error codes */
enum {
    CDB_SUCCESS = 0,
    CDB_NOTFOUND,
    CDB_EXIST,
    CDB_DIRNOEXIST,
    CDB_OPENERR,
    CDB_PIDEXIST,
    CDB_DATAERRDAT,
    CDB_DATAERRIDX,
    CDB_WRITEERR,
    CDB_READERR,
    CDB_NOFID,
    CDB_INTERNALERR,
    CDB_DATAERRMETA,
    CDB_MEMDBNOCACHE,
};

/* record insertion options */
enum {
    CDB_OVERWRITE = 0,
    CDB_INSERTIFEXIST = 0x1,
    CDB_INSERTIFNOEXIST = 0x2,
    CDB_INSERTCACHE = 0x8,
};

/* if database path is CDB_MEMDB, records are never written to disk, they stay in cache only */
#define CDB_MEMDB ":memory:"

/*
 WARNING: 

 the library has auxiliary thread, which means do fork() after open a database will cause
 unpredictable situation.
*/

/* create an cuttdb object, which should be freed by cdb_destory() */
CDB *cdb_new();

/* cdb_option() must be called before cdb_open()

 the second parameter 'hsize' indicates the size of main hash table, which can't be
 modified after the database be created. To get better performance, it is suggest to
 set the 'hsize' to 10% - 1% of the total number of records. The default value 1 million
 should be proper for about 100 million records. Too large or small of the value would
 lead to drop in speed or waste of memory

 the third parameter 'rcacheMB' indicates the size limit of record cache (measured by 
 MegaBytes), every record in cache would have about 40 bytes overhead. 

 the fourth parameter 'pcacheMB' indicates the size limit of index page cache (measured 
 by MegaBytes). If a record is not in record cache, it will be read by only 1 disk seek
 with enough page cache, or it have to make an extra disk seek to load the page. 
 cuttdb will use about {10 * number of records} bytes to cache all index pages, which 
 ensures fastest 'set' operation.

 the default parameter is (_db, 1000000, 128, 1024)

 return 0 if success, or -1 at failure. */
int cdb_option(CDB *db, int hsize, int rcacheMB, int pcacheMB);

/* Enable bloomfilter, size should be the estimated number of records in database 
 must be called before cdb_open(),
 The value is 100000 at minimum. Memory cost of bloomfilter is size/8 bytes */
void cdb_option_bloomfilter(CDB *db, uint64_t size);

/* this is an advanced parameter. It is the size for cuttdb making a read from disk.
 CuttDB do not know the record size even if the index is in memory,
 so at least a read with default size will performed while in cdb_get().
 The value is recommended to be larger than the size of most records in database,
 unless the records are mostly larger than tens of KB.
 If the value is much larger than recommended, it will be a waste of computing. 
 The value can only be 65536 at maximum, 1024 at minimum */
void cdb_option_areadsize(CDB *db, uint32_t size);

/* open an database, 'file' should be an existing directory, or CDB_MEMDB for temporary store,
   'mode' should be combination of CDB_CREAT / CDB_TRUNC / CDB_PAGEWARMUP 
   CDB_PAGEWARMUP means to warm up page cache while opening 
   If there is a file called 'force_recovery' in the data directory, even if it might be made by 'touch force_recovery',
   a force recovery will happen to rebuild the index (be aware that some deleted records would reappear after this)
 */
int cdb_open(CDB *db, const char *file, int mode);


/* simplified cdb_set2, insert a record with CDB_OVERWRITE and never expire */
int cdb_set(CDB *db, const char *key, int ksize, const char *val, int vsize);

/* set a record by 'key' and 'value', 
   opt could be bit combination of CDB_INSERTCACHE and one in {CDB_INSERTIFEXIST, CDB_INSERTNOEXIST,
   CDB_OVERWRITE}
   expire is the time for the record be valid, measured by second. 0 means never expire.
   return 0 if success, or -1 at failure. */
int cdb_set2(CDB *db, const char *key, int ksize, const char *val, int vsize, int opt, int expire);


/* get an record by 'key', the value will be allocated and passed out by 'val', its size is
   'vsize'.  return 0 if success, or -1 at failure. */
int cdb_get(CDB *db, const char *key, int ksize, void **val, int *vsize);


/* the val got by cdb_get should be freed by this for safety.
   If there is more than one memory allocator */
void cdb_free_val(void **val);


/* delete an record by 'key'. However ,the space of the record would not be recycled. 
   'vsize'.  return 0 if success, or -1 at failure. */
int cdb_del(CDB *db, const char *key, int ksize);


/* create a new iterator begins at given operation id */
void *cdb_iterate_new(CDB *db, uint64_t oid);

/* iterate through the database with a callback, the function would stop if callback returned false
   The callback should accept key, ksize, value, vsize, expire time, oid
   Returns the number of records have been visited */
uint64_t cdb_iterate(CDB *db, CDB_ITERCALLBACK itcb, void *arg, void *iter);

/* destroy the iterator */
void cdb_iterate_destroy(CDB *db, void *iter);

/* get the current statistic information of db. 'stat' should be the struct already allocated.
   if 'stat' is NULL, the statistic will be reset to zero. */
void cdb_stat(CDB *db, CDBSTAT *stat);


/* close the database. IT MUST BE CALLED BEFORE PROGRAM EXITS TO ENSURE DATA COMPLETION */
int cdb_close(CDB *db);


/* close the database if it opened, and free the object */
int cdb_destroy(CDB *db);


/* get last error number in current thread */
int cdb_errno(CDB *db);


/* get the description of an error number */
const char *cdb_errmsg(int ecode);


/* set callback when error happened, 'cdb_deferrorcb' is optional, which shows the error to stderr */
void cdb_seterrcb(CDB *db, CDB_ERRCALLBACK errcb, void *arg);

/* a possible error callback */
void cdb_deferrorcb(void *arg, int errno, const char *file, int line);

#if defined(__cplusplus)
}
#endif

#endif
