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


#ifndef _CDB_CORE_H_
#define _CDB_CORE_H_
#include "cuttdb.h"
#include "cdb_types.h"
#include "cdb_hashtable.h"
#include "cdb_bloomfilter.h"
#include "cdb_lock.h"
#include "cdb_vio.h"
#include "cdb_bgtask.h"
#include <stdint.h>
#include <stdbool.h>


enum {
    CDB_PAGEDELETEOFF = 0,
    CDB_PAGEINSERTOFF = 1,
};

/* the DB object */
struct CDB
{
    /* size limit for record cache */
    uint64_t rclimit;
    /* size limit for index page cache */
    uint64_t pclimit;
    /* size of bloom filter */
    uint64_t bfsize;
    /* record number in db */
    uint64_t rnum;
    /* always increment operation id */
    uint64_t oid;
    /* recovery point oid */
    uint64_t roid;
    /* hash table size */
    uint32_t hsize;
    /* last timestamp of no dirty page state */
    uint32_t ndpltime;
    /* currently the database opened or not */
    bool opened;
    /* the size for a disk seek&read, should not greater than SBUFSIZE */
    uint32_t areadsize;

    /* record cache */
    CDBHASHTABLE *rcache;
    /* (clean) index page cache */
    CDBHASHTABLE *pcache;
    /* dirty index page cache */
    CDBHASHTABLE *dpcache;
    /* Bloom Filter */
    CDBBLOOMFILTER *bf;

    /* lock for rcache */
    CDBLOCK *rclock;
    /* lock for pcache */
    CDBLOCK *pclock;
    /* lock for dpcache */
    CDBLOCK *dpclock;
    /* lock for hash table operation, split to MLOCKNUM groups */
    CDBLOCK *mlock[MLOCKNUM];
    /* lock for statistic */
    CDBLOCK *stlock;
    /* lock for operation id */
    CDBLOCK *oidlock;
    /* lock for bloom filter */
    CDBLOCK *bflock;
    /* background tasks in another thread */
    CDBBGTASK *bgtask;

    /* main hash table, contains 'hsize' elements */
    FOFF *mtable;
    /* disk i/o layer object */
    CDBVIO *vio;

    /* callback function when error occurs */
    CDB_ERRCALLBACK errcb;
    /* argument for callback function */
    void *errcbarg;
    /* key to get error code in current thread */
    void *errkey;

    /* statistics below, this fields have no lock protection */
    /* record cache hit/miss */
    uint64_t rchit;
    uint64_t rcmiss;
    /* page cache hit/miss */
    uint64_t pchit;
    uint64_t pcmiss;
    /* cumulative disk read time */
    uint64_t rtime;
    /* number of disk read operation */
    uint64_t rcount;
    /* cumulative disk write time */
    uint64_t wtime;
    /* number of disk write operation */
    uint64_t wcount;
};


bool cdb_checkoff(CDB *db, uint64_t hash, FOFF off, int locked);
int cdb_getoff(CDB *db, uint64_t hash, FOFF **offs, int locked);
int cdb_replaceoff(CDB *db, uint64_t hash, FOFF off, FOFF noff, int locked);
int cdb_updatepage(CDB *db, uint64_t hash, FOFF off, int opt, int locked);
void cdb_flushalldpage(CDB *db);
uint64_t cdb_genoid(CDB *db);

#endif

