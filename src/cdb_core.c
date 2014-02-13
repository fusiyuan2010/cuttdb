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


#include "cuttdb.h"
#include "cdb_crc64.h"
#include "cdb_types.h"
#include "cdb_hashtable.h"
#include "cdb_bloomfilter.h"
#include "cdb_lock.h"
#include "cdb_bgtask.h"
#include "cdb_errno.h"
#include "cdb_vio.h"
#include "cdb_core.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

static void _cdb_pageout(CDB *db);
static void _cdb_defparam(CDB *db);
static void _cdb_recout(CDB *db);
static uint32_t _pagehash(const void *key, int len);
static void _cdb_flushdpagetask(void *arg);
static void _cdb_timerreset(struct timespec *ts);
static uint32_t _cdb_timermicrosec(struct timespec *ts);
static void _cdb_pagewarmup(CDB *db, bool loadbf);


/* it isn't necessary to rehash bid in hash table cache */
static uint32_t _pagehash(const void *key, int len)
{
    return *(uint32_t*)key;
}


/* used to get the duration of a procedure */
static void _cdb_timerreset(struct timespec *ts)
{
    clock_gettime(CLOCK_MONOTONIC, ts);
}


static uint32_t _cdb_timermicrosec(struct timespec *ts)
{
    struct timespec ts2;
    uint32_t diff;
    clock_gettime(CLOCK_MONOTONIC, &ts2);
    diff = (ts2.tv_sec - ts->tv_sec) * 1000000;
    diff += ts2.tv_nsec / 1000;
    diff -= ts->tv_nsec / 1000;
    return diff;
}


/* reset the parameters */
static void _cdb_defparam(CDB *db)
{
    db->rnum = 0;
    db->bfsize = 0;
    db->rclimit = 128 * MB;
    db->pclimit = 1024 * MB;
    db->hsize = 1000000; 
    db->rcache = db->pcache = db->dpcache = NULL;
    db->bf = NULL;
    db->opened = false;
    db->vio = NULL;
    db->mtable = NULL;
    db->oid = 0;
    db->roid = 0;
    db->errcbarg = NULL;
    db->errcb = NULL;
    db->areadsize = 4 * KB;
    return;
}


/* flush all dirty pages */
void cdb_flushalldpage(CDB *db)
{
    if (db->dpcache) {
        while (db->dpcache->num) {
            CDBHTITEM *item = cdb_ht_poptail(db->dpcache);    
            uint32_t bid = *(uint32_t*)cdb_ht_itemkey(db->dpcache, item);
            FOFF off;
            db->vio->wpage(db->vio, (CDBPAGE*)cdb_ht_itemval(db->dpcache, item), &off);
            db->mtable[bid] = off;
            free(item);
        } 

        db->roid = db->oid; 
        db->vio->cleanpoint(db->vio);
    }
}


/* flush oldest dirty index page to disk, it runs in another thread and triggered by timer */
static void _cdb_flushdpagetask(void *arg)
{
    CDB *db = (CDB *)arg;
    CDBHTITEM *item;
    CDBPAGE *page;
    time_t now = time(NULL);
    bool cleandcache = false;
    uint32_t bid;

    if (!db->dpcache)
        /* no dirty page cache */
        return;

    /* if there isn't too much dirty page and some time passed since last clean,
     write out all dirty pages to make a recovery point(oid) */
    if (db->dpcache->num < 1024 && now > db->ndpltime + 120)
        cleandcache = true;
        
    while(db->dpcache->num) {
        FOFF off;
        cdb_lock_lock(db->dpclock);
        item = cdb_ht_gettail(db->dpcache);
        /* no item in dpcache after lock */
        if (item == NULL) {
            cdb_lock_unlock(db->dpclock);
            return;
        }
        page = (CDBPAGE *)cdb_ht_itemval(db->dpcache, item);
        /* bid = page->bid; also OK */
        bid = *(uint32_t*)cdb_ht_itemkey(db->dpcache, item);
        /* been dirty for too long? */
        if (now > page->mtime + DPAGETIMEOUT || cleandcache) {
            if (cdb_lock_trylock(db->mlock[page->bid % MLOCKNUM])) {
                /* avoid dead lock, since dpclock is holding */
                cdb_lock_unlock(db->dpclock);
                return;
            }
            /* remove it from dpcache */
            cdb_ht_poptail(db->dpcache);
            cdb_lock_unlock(db->dpclock);

            /* write to disk */
            struct timespec ts;
            _cdb_timerreset(&ts);
            db->vio->wpage(db->vio, page, &off);
            db->wcount++;
            db->wtime += _cdb_timermicrosec(&ts);
            db->mtable[bid] = off;

            /* move the clean page into pcache */
            cdb_lock_lock(db->pclock);
            cdb_ht_insert(db->pcache, item);
            cdb_lock_unlock(db->pclock);
            cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);
        } else {
            /* tail in dpcache isn't expired */
            cdb_lock_unlock(db->dpclock);
            return;
        }
    }

    if (db->dpcache->num == 0 && cleandcache)
        db->ndpltime = now;

    if (cleandcache) {
        /* clean succeed if goes here, remember the recovery point */
        /* it's not necessary to lock */
        db->roid = db->oid; 
        db->vio->cleanpoint(db->vio);
    }
}


/* fill the index page cache, and set the bloomfilter if necessary */
static void _cdb_pagewarmup(CDB *db, bool loadbf)
{
    char sbuf[SBUFSIZE];
    void *it = db->vio->pageitfirst(db->vio, 0);

    if (it == NULL)
        return;

    for(;;) {
        CDBPAGE *page = (CDBPAGE *)sbuf;
        if (db->vio->pageitnext(db->vio, &page, it) < 0)
            break;

        /* the page is the newest one because its offset matches the one in main table */
        if (OFFEQ(page->ooff, db->mtable[page->bid])) {
            if (loadbf) {
                /* iterate key hashes in page, set to the filter */
                cdb_lock_lock(db->bflock);
                for(uint32_t i = 0; i < page->num; i++) {
                    uint64_t hash = (page->bid << 24) | (page->items[i].hash.i2 << 8)
                        | (page->items[i].hash.i1);
                    /* bloom filter use the combined record hash as key */
                    cdb_bf_set(db->bf, &hash, SI8);
                }
                cdb_lock_unlock(db->bflock);
            }

            /* set the page to pcache if it doesn't exceed the limit size */
            if (db->pcache && db->pcache->size < db->pclimit) {
                cdb_lock_lock(db->pclock);
                cdb_ht_insert2(db->pcache, &page->bid, SI4, page, MPAGESIZE(page));
                cdb_lock_unlock(db->pclock);
            }
        }
        /* the page may not be still in stack */
        if (page != (CDBPAGE *)sbuf)
            free(page);

        if (!loadbf && (db->pcache && db->pcache->size > db->pclimit))
            break;
    }

    db->vio->pageitdestroy(db->vio, it);
}


/* generate an incremental global operation id */
uint64_t cdb_genoid(CDB *db)
{
    uint64_t oid;
    cdb_lock_lock(db->oidlock);
    oid = db->oid++;
    cdb_lock_unlock(db->oidlock);
    return oid;
}


/* get a new record iterator */
void *cdb_iterate_new(CDB *db, uint64_t oid)
{
    return db->vio->recitfirst(db->vio, oid);
}



/* iterate the database by callback */
uint64_t cdb_iterate(CDB *db, CDB_ITERCALLBACK itcb, void *arg, void *iter)
{
    char sbuf[SBUFSIZE];
    uint64_t cnt = 0;

    if (iter == NULL)
        return cnt;
    for(;;) {
        /* the rec is a copy from file, may in stack or allocated in heap */
        CDBREC *rec = (CDBREC *)sbuf;
        bool ret = true;
        if (db->vio->recitnext(db->vio, &rec, iter) < 0) 
            break;
        
        if (cdb_checkoff(db, CDBHASH64(rec->key, rec->ksize), rec->ooff, CDB_NOTLOCKED)) {
            ret = itcb(arg, rec->key, rec->ksize, rec->val, rec->vsize, rec->expire, rec->oid);
            cnt++;
        }
        if (rec != (CDBREC *)sbuf)
            free(rec);
        if (!ret) 
            break;
    }
    return cnt;
}



/* destroy the iterator */
void cdb_iterate_destroy(CDB *db, void *iter)
{
    db->vio->recitdestroy(db->vio, iter);
}


/* difficult to implement */
/*
static void _cdb_rcachewarmup(CDB *db)
{
}
*/


CDB *cdb_new()
{
    CDB *db;
    db = (CDB *)malloc(sizeof(CDB));
    /* I assume all operation in this layer is 'fast', so no mutex used here */
    for(int i = 0; i < MLOCKNUM; i++) 
        db->mlock[i] = cdb_lock_new(CDB_LOCKSPIN);
    db->dpclock = cdb_lock_new(CDB_LOCKSPIN);
    db->pclock = cdb_lock_new(CDB_LOCKSPIN);
    db->rclock = cdb_lock_new(CDB_LOCKSPIN);
    db->stlock = cdb_lock_new(CDB_LOCKSPIN);
    db->oidlock = cdb_lock_new(CDB_LOCKSPIN);
    db->bflock = cdb_lock_new(CDB_LOCKSPIN);
    db->bgtask = cdb_bgtask_new();
    /* every thread should has its own errno */
    db->errkey = (pthread_key_t *)malloc(sizeof(pthread_key_t));
    pthread_key_create(db->errkey, NULL);
    /* set default parameter */
    _cdb_defparam(db);
    return db;
}


int cdb_option(CDB *db, int bnum, int rcacheMB, int pcacheMB)
{
    /* too small bnum is not allowed */
    db->hsize = bnum > 4096? bnum : 4096;

    if (rcacheMB >= 0)
        db->rclimit = (uint64_t)rcacheMB * MB;
    if (pcacheMB >= 0)
        db->pclimit = (uint64_t)pcacheMB * MB;
    return 0;
}


void cdb_option_bloomfilter(CDB *db, uint64_t size)
{
    db->bfsize = size;
}

void cdb_option_areadsize(CDB *db, uint32_t size)
{
    db->areadsize = size;
    if (db->areadsize < 1 * KB)
        db->areadsize = 1 * KB;

    if (db->areadsize > SBUFSIZE - (sizeof(CDBREC) - RECHSIZE)) 
        db->areadsize = SBUFSIZE - (sizeof(CDBREC) - RECHSIZE);
}

int cdb_open(CDB *db, const char *file_name, int mode)
{
    /* if will become into a hash table when file_name == CDB_MEMDB */
    int memdb = (strcmp(file_name, CDB_MEMDB) == 0);

    if (db->rclimit)
        /* record cache is enabled */
        db->rcache = cdb_ht_new(true, NULL);
    else if (memdb) {
        /* record cache is disabled, but in MEMDB mode */
        cdb_seterrno(db, CDB_MEMDBNOCACHE, __FILE__, __LINE__);
        goto ERRRET;
    }

    if (db->pclimit && !memdb) {
        /* page cache enabled. page cache is meaningless under MEMDB  mode */
        db->dpcache = cdb_ht_new(true, _pagehash);
        db->pcache = cdb_ht_new(true, _pagehash);
    }


    if (!memdb) {
        if (db->bfsize) {
            /* bloom filter enabled */
            db->bf = cdb_bf_new(db->bfsize, db->bfsize);
        }
        /* now only one storage format is supported */
        db->vio = cdb_vio_new(CDBVIOAPND2);
        db->vio->db = db;
        if (db->vio->open(db->vio, file_name, mode) < 0)
            goto ERRRET;
        if (db->vio->rhead(db->vio) < 0) {
            db->mtable = (FOFF*)malloc(sizeof(FOFF) * db->hsize);
            memset(db->mtable, 0, sizeof(FOFF) * db->hsize);
        }
        /* dirty index page would be swap to disk by timer control */
        cdb_bgtask_add(db->bgtask, _cdb_flushdpagetask, db, 1);
        db->ndpltime = time(NULL);
        /* start background task thread */
        cdb_bgtask_start(db->bgtask);
    } else {
        /* no persistent storage under MEMDB mode */
        db->vio = NULL;
        db->bgtask = NULL;
        db->mtable = NULL;
    }

    if (db->bf || ((mode & CDB_PAGEWARMUP) && db->pcache)) {
        /* fill the bloom filter if it is enabled, and fill the page cache */
        _cdb_pagewarmup(db, !!db->bf);
    }

    /* reset the statistic info */
    cdb_stat(db, NULL);
    db->opened = true;
    return 0;

ERRRET:
    if (db->rcache)
        cdb_ht_destroy(db->rcache);
    if (db->pcache)
        cdb_ht_destroy(db->pcache);
    if (db->dpcache)
        cdb_ht_destroy(db->dpcache);
    if (db->bf)
        cdb_bf_destroy(db->bf);
    cdb_bgtask_stop(db->bgtask);
    _cdb_defparam(db);
    return -1;
}


/* check if the page cache size exceed the limit. clean oldest page if necessary */
static void _cdb_pageout(CDB *db)
{
    while (PCOVERFLOW(db)) {
        if (db->pcache->num) {
            /* clean page cache is prior */
            cdb_lock_lock(db->pclock);
            cdb_ht_removetail(db->pcache);
            cdb_lock_unlock(db->pclock);
        } else if (db->dpcache->num) {
            CDBHTITEM *item;
            uint32_t bid;
            FOFF off;
            cdb_lock_lock(db->dpclock);
            item = cdb_ht_gettail(db->dpcache);    
            if (item == NULL) {
                cdb_lock_unlock(db->dpclock);
                break;
            }

            bid = *(uint32_t*)cdb_ht_itemkey(db->dpcache, item);
            /* must lock the main table inside the dpclock protection */
            if (cdb_lock_trylock(db->mlock[bid % MLOCKNUM]) < 0) {
                /* avoid dead lock since dpclock is holding */
                cdb_lock_unlock(db->dpclock);
                /* do nothing this time */
                break;
            }
            cdb_ht_poptail(db->dpcache);
            cdb_lock_unlock(db->dpclock);

            /* write out dirty page */
            struct timespec ts;
            _cdb_timerreset(&ts);
            db->vio->wpage(db->vio, (CDBPAGE*)cdb_ht_itemval(db->dpcache, item), &off);
            db->wcount++;
            db->wtime += _cdb_timermicrosec(&ts);
            db->mtable[bid] = off;
            cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);
            free(item);
        }
    }
}


/* check if the record cache size exceed the limit. clean oldest record if necessary */
static void _cdb_recout(CDB *db)
{
    while (RCOVERFLOW(db)) {
        cdb_lock_lock(db->rclock);
        if (db->rcache->num)
            cdb_ht_removetail(db->rcache);
        cdb_lock_unlock(db->rclock);
    }
}


/* get all offsets from index(page) by key, even if only one of them at most is valid.
 Others are due to the hash collision */
int cdb_getoff(CDB *db, uint64_t hash, FOFF **offs, int locked) 
{
    char sbuf[SBUFSIZE];
    CDBPAGE *page = NULL;
    int rnum;
    bool incache = true;
    uint32_t bid = (hash >> 24) % db->hsize;
    PHASH phash;

    phash.i1 = hash & 0xff;
    phash.i2 = (hash >> 8) & 0xffff;

    if (db->bf) {
        uint64_t bfkey = (bid << 24) | (hash & 0xffffff);
        /* check the key-hash in bloom filter? return now if not exist */
        cdb_lock_lock(db->bflock);
        if (!cdb_bf_exist(db->bf, &bfkey, SI8)) {
            cdb_lock_unlock(db->bflock);
            return 0;
        }
        cdb_lock_unlock(db->bflock);
    }

    if (locked == CDB_NOTLOCKED) cdb_lock_lock(db->mlock[bid % MLOCKNUM]);
    /* page exists in clean page cache? */
    if (db->pcache) {
        cdb_lock_lock(db->pclock);
        page = cdb_ht_get2(db->pcache, &bid, SI4, true);
        cdb_lock_unlock(db->pclock);
    }

    /* not in pcache, exists in dirty page cache? */
    if (page == NULL && db->dpcache) {
        cdb_lock_lock(db->dpclock);
        page = cdb_ht_get2(db->dpcache, &bid, SI4, true);
        cdb_lock_unlock(db->dpclock);
    }

    if (page == NULL) {
        /* not in dpcache either, read from disk */
        incache = false;
        db->pcmiss++;
        /* page stays in stack by default */
        page = (CDBPAGE *)sbuf;
        if (OFFNOTNULL(db->mtable[bid])) {
            /* page offset not null in main table */
            int ret;
            struct timespec ts;
            _cdb_timerreset(&ts);
            ret = db->vio->rpage(db->vio, &page, db->mtable[bid]);
            db->rcount++;
            db->rtime += _cdb_timermicrosec(&ts);

            /* read page error, return */
            if (ret < 0) {
                if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);
                if (page != (CDBPAGE *)sbuf)
                    free(page);
                return -1;
            }
        } else {
            /* no page in this bucket */
            page->cap = page->num = 0;
            page->osize = 0;
            OFFZERO(page->ooff);
        }
    } else {
        db->pchit++;
    }

    rnum = 0;
    for(uint32_t i = 0; i < page->num; i++) {
        /* compare every hash in the page */
        if (PHASHEQ(page->items[i].hash, phash)) {
            (*offs)[rnum] = page->items[i].off;
            /* result offset list stays in stack by default. Allocate one in heap if 
            it exceeds the limit */
            if (++rnum == SFOFFNUM) {
                /* very little possibility goes here */
                FOFF *tmp = (FOFF*)malloc((page->num - i + SFOFFNUM + 1) * sizeof(FOFF));
                memcpy(tmp, *offs, SFOFFNUM * sizeof(FOFF));
                *offs = tmp;
            } 
        }
    }

    if (!incache) {
        /* set into clean page cache if not exists before */
        if (db->pcache) {
            cdb_lock_lock(db->pclock);
            cdb_ht_insert2(db->pcache, &bid, SI4, page, MPAGESIZE(page));
            cdb_lock_unlock(db->pclock);
        }
        /* if page now points to heap memory, free it */
        if (page != (CDBPAGE *)sbuf) {
            free(page);
        }
    }
    if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);

    /* check page cache overflow */
    if (PCOVERFLOW(db))
        _cdb_pageout(db);

    return rnum;
}


/* replace a specified record's offset, may be used at disk space recycling 
 off indicates its previous offset, noff is the new offset. return negative if not found */
int cdb_replaceoff(CDB *db, uint64_t hash, FOFF off, FOFF noff, int locked)
{
    char sbuf[SBUFSIZE];
    CDBPAGE *page = NULL;
    CDBHTITEM *pitem = NULL;
    bool indpcache = false;
    uint32_t bid = (hash >> 24) % db->hsize;
    PHASH phash;
    bool found = false;

    phash.i1 = hash & 0xff;
    phash.i2 = (hash >> 8) & 0xffff;

    if (locked == CDB_NOTLOCKED) cdb_lock_lock(db->mlock[bid % MLOCKNUM]);
    if (db->pcache) {
        /* in clean page cache, since it would be modified, it should be deleted from pcache */
        cdb_lock_lock(db->pclock);
        pitem = cdb_ht_del(db->pcache, &bid, SI4);
        cdb_lock_unlock(db->pclock);
        if (pitem)
            page = (CDBPAGE *)cdb_ht_itemval(db->pcache, pitem);
    }
    if (page == NULL && db->dpcache) {
        /* not in pcache, but in dirty page cache */
        cdb_lock_lock(db->dpclock);
        page = cdb_ht_get2(db->dpcache, &bid, SI4, true);
        cdb_lock_unlock(db->dpclock);
        if (page)
            indpcache = true;
    }
    if (page == NULL) {
        /* not exists either, read from disk */
        db->pcmiss++;
        page = (CDBPAGE *)sbuf;
        if (OFFNOTNULL(db->mtable[bid])) {
            int ret;
            struct timespec ts;
            _cdb_timerreset(&ts);
            ret = db->vio->rpage(db->vio, &page, db->mtable[bid]);
            db->rcount++;
            db->rtime += _cdb_timermicrosec(&ts);
            
            if (ret < 0) {
                if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);
                if (page != (CDBPAGE *)sbuf)
                    free(page);
                return -1;
            }
        } else {
            /* nullified the empty page */
            page->cap = page->num = 0;
            page->osize = 0;
            OFFZERO(page->ooff);
        }
    } else {
        db->pchit++;
    }

    /* check and modify */
    for(uint32_t i = 0; i < page->num; i++) {
        if (PHASHEQ(page->items[i].hash, phash)
            && OFFEQ(page->items[i].off, off)) {
                page->items[i].off = noff;
                found = true;
                break;
        }
    }

    if (db->dpcache && !indpcache) {
        /* if page already dirty in cache, need not do anything */
        /* dirty page cache is enabled but not exists before */
        if (pitem) {
            /* pitem not NULL indicates it belongs to pcache */
            if (found) {
                /* modified page */
                cdb_lock_lock(db->dpclock);
                cdb_ht_insert(db->dpcache, pitem);
                cdb_lock_unlock(db->dpclock);
            } else {
                /* got from pcache, but not modified */
                cdb_lock_lock(db->pclock);
                cdb_ht_insert(db->pcache, pitem);
                cdb_lock_unlock(db->pclock);
            }
            /* page belongs to memory in 'cache', must not free */
        } else if (page != NULL) {
            /* page read from disk, but not in cache */
            cdb_lock_lock(db->dpclock);
            cdb_ht_insert2(db->dpcache, &bid, SI4, page, MPAGESIZE(page));
            cdb_lock_unlock(db->dpclock);
            /* the 'page' won't be use anymore */
            if (page != (CDBPAGE *)sbuf) 
                free(page);
        }
    } else if (!db->dpcache){
        /* no page cache. Write out dirty page immediately */
        FOFF poff;
        struct timespec ts;
        _cdb_timerreset(&ts);
        db->vio->wpage(db->vio, page, &poff);
        db->wcount++;
        db->wtime += _cdb_timermicrosec(&ts);

        db->mtable[bid] = poff;
        if (page != (CDBPAGE *)sbuf) 
                free(page);
    }
    if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);

    /* check page cache overflow */
    if (PCOVERFLOW(db))
        _cdb_pageout(db);

    return 0;
}


/* insert/delete a key-offset pair from index page */
int cdb_updatepage(CDB *db, uint64_t hash, FOFF off, int opt, int locked)
{
    char sbuf[SBUFSIZE], sbuf2[SBUFSIZE];
    CDBPAGE *page = NULL, *npage = NULL;
    CDBHTITEM *pitem = NULL, *nitem = NULL;
    CDBHASHTABLE *tmpcache = NULL;
    CDBLOCK *tmpclock = NULL;
    int npsize = 0;
    uint32_t bid = (hash >> 24) % db->hsize;
    PHASH phash;

    phash.i1 = hash & 0xff;
    phash.i2 = (hash >> 8) & 0xffff;

    if (locked == CDB_NOTLOCKED) cdb_lock_lock(db->mlock[bid % MLOCKNUM]);
    /* firstly, try move the page out of the cache if possible, 
    it assumes that the page would be modified(pair exists) */
    if (db->pcache) {
        /* try clean page cache */
        cdb_lock_lock(db->pclock);
        pitem = cdb_ht_del(db->pcache, &bid, SI4);
        cdb_lock_unlock(db->pclock);
        if (pitem) {
            page = (CDBPAGE *)cdb_ht_itemval(db->pcache, pitem);
            tmpcache = db->pcache;
            tmpclock = db->pclock;
        }
    }
    if (page == NULL && db->dpcache) {
        /* try dirty page cache */
        cdb_lock_lock(db->dpclock);
        pitem = cdb_ht_del(db->dpcache, &bid, SI4);
        cdb_lock_unlock(db->dpclock);
        if (pitem) {
            page = (CDBPAGE *)cdb_ht_itemval(db->dpcache, pitem);
            tmpcache = db->dpcache;
            tmpclock = db->dpclock;
        }
    }

    if (page == NULL) {
        db->pcmiss++;
        page = (CDBPAGE *)sbuf;
        /* doesn't exist in cache, read from disk */
        if (OFFNOTNULL(db->mtable[bid])) {
            int ret;
            struct timespec ts;
            _cdb_timerreset(&ts);
            ret = db->vio->rpage(db->vio, &page, db->mtable[bid]);
            db->rcount++;
            db->rtime += _cdb_timermicrosec(&ts);

            if (ret < 0) {
                if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);
                if (page != (CDBPAGE *)sbuf)
                    free(page);
                return -1;
            }
        } else {
            page->cap = 0;
            page->num = 0;
            page->osize = 0;
            OFFZERO(page->ooff);
        }
    } else {
        db->pchit++;
    }

    npsize = MPAGESIZE(page);

    if (opt == CDB_PAGEDELETEOFF)
    ;//    npsize = MPAGESIZE(page) - sizeof(PITEM);
    /* do not malloc new page on deletion */

    else if (opt == CDB_PAGEINSERTOFF && page->cap == page->num) {
    /* get a new page, from dirty page cache if possible */
        npsize = MPAGESIZE(page) + CDB_PAGEINCR * sizeof(PITEM);
        if (db->dpcache) {
            nitem = cdb_ht_newitem(db->dpcache, SI4, npsize);
            *(uint32_t*)cdb_ht_itemkey(db->dpcache, nitem) = bid;
            npage = (CDBPAGE *)cdb_ht_itemval(db->dpcache, nitem);
        } else {
            /* no dpcache, use stack if size fits */
            if (npsize > SBUFSIZE) 
                npage = (CDBPAGE *)malloc(npsize);
            else
                npage = (CDBPAGE *)sbuf2;
        }

        /* initialize the new page */
    
        npage->bid = bid;
        npage->oid = cdb_genoid(db);
        npage->osize = page->osize;
        npage->ooff = page->ooff;
        npage->mtime = time(NULL);
        npage->cap = page->cap + CDB_PAGEINCR;
        npage->num = page->num;
        memcpy(npage->items, page->items, page->num * sizeof(PITEM)); 
        /* old page got from cache */
        if (pitem)
            free(pitem);
        /* old page read from disk, if in stack? */
        else if (page != (CDBPAGE *)sbuf)
            free(page);

        page = npage;
        pitem = nitem;
    }

    uint32_t onum = page->num;

    if (opt == CDB_PAGEDELETEOFF) {
        bool found = false;
        for(uint32_t i = 0; i < page->num; i++) {
            if (!found) {
                if (PHASHEQ(page->items[i].hash, phash)
                    && OFFEQ(page->items[i].off, off))
                {
                    found = true;
                    /* records num is consistant with index */
                    cdb_lock_lock(db->stlock);
                    db->rnum--;
                    cdb_lock_unlock(db->stlock);
                }
            }
            if (found && i + 1 < page->num)
                page->items[i] = page->items[i+1];
        }
        if (found)
            page->num--;
    } else if (opt == CDB_PAGEINSERTOFF) {
        bool found = false;
        /* check already exist? */
        for(uint32_t i = 0; i < page->num; i++) {
            if (PHASHEQ(page->items[i].hash, phash)
                && OFFEQ(page->items[i].off, off)) {
                /* avoid exceptional deduplicated item */
                found = true;
                break;
            }
        }

        /* append to the tail */
        if (!found) {
            page->items[page->num].hash = phash;
            page->items[page->num].off = off;
            page->num++;
            /* records num is consistant with index */
            cdb_lock_lock(db->stlock);
            db->rnum++;
            cdb_lock_unlock(db->stlock);
            if (db->bf) {
                uint64_t bfkey = (((hash >> 24) % db->hsize) << 24) | (hash & 0xffffff);
                cdb_lock_lock(db->bflock);
                cdb_bf_set(db->bf, &bfkey, SI8);
                cdb_lock_unlock(db->bflock);
            }
        }
    }

    if (page->num == onum) {
        /* nothing done */
        if (pitem) {
            /* insert the item back to the cache where it belongs */
            cdb_lock_lock(tmpclock);
            cdb_ht_insert(tmpcache, pitem);
            cdb_lock_unlock(tmpclock);
        } else {
            if (page != (CDBPAGE *)sbuf2
                    && page != (CDBPAGE *)sbuf)
                free(page);
        }
        if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);
        return -1;
    } else {
        if (pitem) {
            cdb_lock_lock(db->dpclock);
            cdb_ht_insert(db->dpcache, pitem);
            cdb_lock_unlock(db->dpclock);
        } else {
            struct timespec ts;
            _cdb_timerreset(&ts);
            db->vio->wpage(db->vio, page, &off);
            db->wcount++;
            db->wtime += _cdb_timermicrosec(&ts);

            db->mtable[bid] = off;
            if (page != (CDBPAGE *)sbuf2
                    && page != (CDBPAGE *)sbuf)
                free(page);
        }
    }

    if (locked == CDB_NOTLOCKED) cdb_lock_unlock(db->mlock[bid % MLOCKNUM]);

    /* check page cache overflow */
    if (PCOVERFLOW(db))
        _cdb_pageout(db);

    return 0;
}


/* check if an record with specified key-offset exists in index */
bool cdb_checkoff(CDB *db, uint64_t hash, FOFF off, int locked)
{
    FOFF soffs[SFOFFNUM];
    FOFF *soff = (FOFF *)soffs;
    int dupnum;
    int ret = false;

    /* get all possible offsets */
    dupnum = cdb_getoff(db, hash, &soff, locked);
    for(int i = 0; i < dupnum; i++) {
        if (OFFEQ(soff[i], off)) {
            ret = true;
            break;
        }
    }

    if (soff != (FOFF *)soffs) {
        free(soff);
    }

    return ret;
}


/* wrapper and simplified of set operation */
int cdb_set(CDB *db, const char *key, int ksize, const char *val, int vsize)
{
    return cdb_set2(db, key, ksize, val, vsize, CDB_OVERWRITE, 0);
}


int cdb_set2(CDB *db, const char *key, int ksize, const char *val, int vsize, int opt, int expire)
{
    CDBREC rec;
    FOFF ooff, noff;
    uint32_t now = time(NULL);
    uint64_t hash;
    uint32_t lockid;
    bool expired = false;
 
    if (db->vio == NULL) {
        /* if it is a memdb, just operate on the record cache and return */
        cdb_lock_lock(db->rclock);
        cdb_ht_insert2(db->rcache, key, ksize, val, vsize);
        cdb_lock_unlock(db->rclock);
        if (RCOVERFLOW(db))
            _cdb_recout(db);
        return 0;
    }

    hash = CDBHASH64(key, ksize);
    lockid = (hash >> 24) % db->hsize % MLOCKNUM;
    OFFZERO(rec.ooff);
    OFFZERO(ooff);
    rec.osize = 0;
    rec.key = (char*)key;
    rec.val = (char*)val;
    rec.ksize = ksize;
    rec.vsize = vsize;
    rec.oid = cdb_genoid(db);
    rec.expire = expire? now + expire : 0;
        
    cdb_lock_lock(db->mlock[lockid]);
    if (db->rcache) {
        /* if record already exists, get its old meta info */
        int item_vsize;
        char *cval;
        uint32_t old_expire = 0;
        cdb_lock_lock(db->rclock);
        cval = cdb_ht_get(db->rcache, key, ksize, &item_vsize, false);
        if (cval) {
            /* record already exists */
            ooff = rec.ooff = *(FOFF*)cval;
            rec.osize = item_vsize - SFOFF - SI4;
            old_expire = *(uint32_t*)(cval + SFOFF); 
        }
        cdb_lock_unlock(db->rclock);
        if (old_expire && old_expire <= now)
            /* once exist but expired? */
            expired = true;
    }
    
    if (OFFNULL(ooff)) {
        FOFF soffs[SFOFFNUM];
        FOFF *soff = soffs;
        char sbuf[SBUFSIZE];
        CDBREC *rrec = (CDBREC*)sbuf;
        
        int retnum;
        if ((retnum = cdb_getoff(db, hash, &soff, CDB_LOCKED)) < 0) {
            cdb_lock_unlock(db->mlock[lockid]);
            return -1;
        }
            
        for(int i = 0; i < retnum; i++) {
            /* check for duplicate records/older version*/
            int cret;
            if (rrec != (CDBREC*)sbuf) {
                free(rrec);
                rrec = (CDBREC*)sbuf;
            }
            
            struct timespec ts;
            _cdb_timerreset(&ts);
            cret = db->vio->rrec(db->vio, &rrec, soff[i], false);
            db->rcount++;
            db->rtime += _cdb_timermicrosec(&ts);
            
            if (cret < 0)
                continue;
                
            if (ksize == rrec->ksize && memcmp(rrec->key, key, ksize) == 0) {
                /* got its old meta info */
                rec.osize = rrec->osize;
                rec.ooff = rrec->ooff;
                ooff = rec.ooff;
                if (rrec->expire <= now)
                    expired = true;
                break;
            }
        }
        if (soff != soffs)
            free(soff);
        if (rrec != (CDBREC*)sbuf) 
            free(rrec);
    }
    
    if (OFFNOTNULL(ooff) && !expired) {
        /* record already exists*/
        if (opt & CDB_INSERTIFNOEXIST) {
            cdb_lock_unlock(db->mlock[lockid]);
            cdb_seterrno(db, CDB_EXIST, __FILE__, __LINE__);
            return -2;
        }
    } else {
        if (opt & CDB_INSERTIFEXIST) {
            cdb_lock_unlock(db->mlock[lockid]);
            cdb_seterrno(db, CDB_NOTFOUND, __FILE__, __LINE__);
            return -3;
        }
    }
    
    struct timespec ts;
    _cdb_timerreset(&ts);
    if (db->vio->wrec(db->vio, &rec, &noff) < 0) {
        cdb_lock_unlock(db->mlock[lockid]);
        return -1;
    }
    db->wcount++;
    db->wtime += _cdb_timermicrosec(&ts);
    
    if (OFFNOTNULL(ooff)) {
        cdb_replaceoff(db, hash, ooff, noff, CDB_LOCKED);
    } else {
        cdb_updatepage(db, hash, noff, CDB_PAGEINSERTOFF, CDB_LOCKED);
    }
    
    if (db->rcache) {
        if ((opt & CDB_INSERTCACHE) == CDB_INSERTCACHE) {
            char *cval;
            CDBHTITEM *item = cdb_ht_newitem(db->rcache, ksize, vsize + SI4 + SFOFF);
            memcpy(cdb_ht_itemkey(db->rcache, item), key, ksize);
            cval = cdb_ht_itemval(db->rcache, item);
            memcpy(cval + SI4 + SFOFF, val, vsize);
            *(FOFF*)(cval) = rec.ooff;
            *(uint32_t*)(cval + SFOFF) = rec.expire;
            cdb_lock_lock(db->rclock);
            cdb_ht_insert(db->rcache, item);
            cdb_lock_unlock(db->rclock);
        }
    } 
    cdb_lock_unlock(db->mlock[lockid]);
    
    if (RCOVERFLOW(db))
        _cdb_recout(db);

    cdb_seterrno(db, CDB_SUCCESS, __FILE__, __LINE__);
    return 0;
}



int cdb_get(CDB *db, const char *key, int ksize, void **val, int *vsize)
{
    char sbuf[SBUFSIZE];
    CDBREC *rec = (CDBREC *)sbuf;
    FOFF soffs[SFOFFNUM];
    FOFF *offs;
    int dupnum, ret = -3;
    uint64_t hash;
    uint32_t now = time(NULL);
    uint32_t lockid;

    *vsize = 0;
    *val = NULL;
    if (db->rcache) {
        char *cval;
        cdb_lock_lock(db->rclock);
        cval = cdb_ht_get(db->rcache, key, ksize, vsize, true);
        if (cval) {
            db->rchit++;
            if (db->vio) {
                (*vsize) -= SI4 + SFOFF;
                if (*(uint32_t*)(cval + SFOFF)
                    && *(uint32_t*)(cval + SFOFF) <= now) {
                    cdb_lock_unlock(db->rclock);
                    /* not found no not report error now */
                    //cdb_seterrno(db, CDB_NOTFOUND, __FILE__, __LINE__);
                    return -3;
                }
                cval = (void*)(cval + SI4 + SFOFF);
            }
            *val = malloc(*vsize);
            memcpy(*val, cval, *vsize);
            cdb_lock_unlock(db->rclock);
            return 0;
        } else {
            db->rcmiss++;
            if (db->vio == NULL) {
                cdb_lock_unlock(db->rclock);
                return -3;
            }
        }
        cdb_lock_unlock(db->rclock);
    }

    offs = soffs;
    hash = CDBHASH64(key, ksize);
    lockid = (hash >> 24) % db->hsize % MLOCKNUM;
    cdb_lock_lock(db->mlock[lockid]);
    dupnum = cdb_getoff(db, hash, &offs, CDB_LOCKED);
    if (dupnum < 0) {
        cdb_lock_unlock(db->mlock[lockid]);
        return -1;
    }

    for(int i = 0; i < dupnum; i++) {
        int cret;
        if (rec != (CDBREC*)sbuf) {
            free(rec);
            rec = (CDBREC*)sbuf;
        }

        struct timespec ts;
        _cdb_timerreset(&ts);
        cret = db->vio->rrec(db->vio, &rec, offs[i], true);
        db->rcount++;
        db->rtime += _cdb_timermicrosec(&ts);

        if (cret < 0)
            continue;

        if (ksize == rec->ksize && memcmp(rec->key, key, ksize) == 0) {
            if (rec->expire && rec->expire <= now) {
                break;
            }
            *vsize = rec->vsize;
            *val = malloc(*vsize);
            memcpy(*val, rec->val, *vsize);
            ret = 0;
            break;
        } 
    }

    if (ret == 0 && db->rcache) {
        char *cval;
        CDBHTITEM *item = cdb_ht_newitem(db->rcache, ksize, *vsize + SI4 + SFOFF);
        memcpy(cdb_ht_itemkey(db->rcache, item), key, ksize);
        cval = cdb_ht_itemval(db->rcache, item);
        memcpy(cval + SI4 + SFOFF, *val, *vsize);
        *(FOFF*)(cval) = rec->ooff;
        *(uint32_t*)(cval + SFOFF) = rec->expire;
        cdb_lock_lock(db->rclock);
        cdb_ht_insert(db->rcache, item);
        cdb_lock_unlock(db->rclock);
    }
    cdb_lock_unlock(db->mlock[lockid]);
    
    if (RCOVERFLOW(db))
        _cdb_recout(db);
            
    if (offs != soffs)
        free(offs);
        
    if (rec != (CDBREC*)sbuf) 
        free(rec);

    if (ret < 0)
        cdb_seterrno(db, CDB_NOTFOUND, __FILE__, __LINE__);
    else {
        db->rcmiss++;
        cdb_seterrno(db, CDB_SUCCESS, __FILE__, __LINE__);
    }
    return ret;
}


void cdb_free_val(void **val)
{
    if (*val) 
        free(*val);
    *val = NULL;
}


int cdb_del(CDB *db, const char *key, int ksize)
{
    FOFF ooff;
    CDBREC rec;
    uint32_t lockid;
    uint64_t hash;
    
    OFFZERO(rec.ooff);
    OFFZERO(ooff);
    rec.osize = 0;
    rec.key = (char*)key;
    rec.ksize = ksize;
    rec.val = NULL;
    rec.vsize = 0;
    
    if (db->vio == NULL) {
        /* if it is a memdb, just operate on the record cache and return */
        cdb_lock_lock(db->rclock);
        cdb_ht_del2(db->rcache, key, ksize);
        cdb_lock_unlock(db->rclock);
        if (RCOVERFLOW(db))
            _cdb_recout(db);
        return 0;
    }
    
    hash = CDBHASH64(key, ksize);
    lockid = (hash >> 24) % db->hsize % MLOCKNUM;
    cdb_lock_lock(db->mlock[lockid]);
    if (db->rcache) {
        /* if record already exists, get its old meta info */
        CDBHTITEM *item;
        cdb_lock_lock(db->rclock);
        item = cdb_ht_del(db->rcache, key, ksize);
        cdb_lock_unlock(db->rclock);
        if (item) {
            char *cval = cdb_ht_itemval(db->rcache, item);
            ooff = rec.ooff = *(FOFF*)cval;
            rec.osize = item->vsize - SFOFF - SI4;
            rec.expire = *(uint32_t*)(cval + SFOFF);
            free(item);
        }
    }
    
    if (OFFNULL(ooff)) {
        FOFF soffs[SFOFFNUM];
        FOFF *soff = soffs;
        char sbuf[SBUFSIZE];
        CDBREC *rrec = (CDBREC*)sbuf;
        
        int retnum;
        if ((retnum = cdb_getoff(db, hash, &soff, CDB_LOCKED)) < 0) {
            cdb_lock_unlock(db->mlock[lockid]);
            return -1;
        }
            
        for(int i = 0; i < retnum; i++) {
            /* check for duplicate records/older version*/
            int cret;
            if (rrec != (CDBREC*)sbuf) {
                free(rrec);
                rrec = (CDBREC*)sbuf;
            }
            
            struct timespec ts;
            _cdb_timerreset(&ts);
            cret = db->vio->rrec(db->vio, &rrec, soff[i], false);
            db->rcount++;
            db->rtime += _cdb_timermicrosec(&ts);
            
            if (cret < 0)
                continue;
                
            if (ksize == rrec->ksize && memcmp(rrec->key, key, ksize) == 0) {
                /* got its old meta info */
                rec.osize = rrec->osize;
                rec.ooff = rrec->ooff;
                ooff = rec.ooff;
                break;
            }
        }
        if (soff != soffs)
            free(soff);
        if (rrec != (CDBREC*)sbuf) 
            free(rrec);
    }
    
    if (OFFNOTNULL(ooff)) {
        cdb_updatepage(db, hash, ooff, CDB_PAGEDELETEOFF, CDB_LOCKED);
        cdb_lock_unlock(db->mlock[lockid]);
        
        struct timespec ts;
        _cdb_timerreset(&ts);
        if (db->vio->drec(db->vio, &rec, ooff) < 0)
            ; // return -1;  succeed or not doesn't matter
        db->wcount++;
        db->wtime += _cdb_timermicrosec(&ts);
        cdb_seterrno(db, CDB_SUCCESS, __FILE__, __LINE__);
        return 0;
    } else {
        cdb_lock_unlock(db->mlock[lockid]);
        cdb_seterrno(db, CDB_NOTFOUND, __FILE__, __LINE__);
        return -3;
    }
}


void cdb_stat(CDB *db, CDBSTAT *stat)
{
    if (stat == NULL) {
        db->rchit = db->rcmiss = 0;
        db->pchit = db->pcmiss = 0;
        db->rcount = db->rtime = 0;
        db->wcount = db->wtime = 0;
    } else {
        stat->rnum = db->rnum;
        stat->rcnum = db->rcache? db->rcache->num : 0;
        stat->pnum = db->hsize;
        stat->pcnum = (db->pcache? db->pcache->num : 0) 
            + (db->dpcache? db->dpcache->num : 0);
        stat->rchit = db->rchit;
        stat->rcmiss = db->rcmiss;
        stat->pchit = db->pchit;
        stat->pcmiss = db->pcmiss;
        stat->rlatcy = db->rcount ? db->rtime / db->rcount : 0;
        stat->wlatcy = db->wcount ? db->wtime / db->wcount : 0;
    }
}


int cdb_close(CDB *db)
{
    if (!db->opened)
        return -1;

    if (db->bgtask)
        cdb_bgtask_stop(db->bgtask);
    if (db->rcache)
        cdb_ht_destroy(db->rcache);
    if (db->pcache)
        cdb_ht_destroy(db->pcache);
    if (db->dpcache) {
        cdb_flushalldpage(db);
        cdb_ht_destroy(db->dpcache);
    }

    if (db->vio) {
        db->vio->whead(db->vio);
        db->vio->close(db->vio);
        cdb_vio_destroy(db->vio);
    }
    if (db->mtable)
        free(db->mtable);
    db->opened = false;
    _cdb_defparam(db);
    return 0;
}


void cdb_deferrorcb(void *arg, int errno, const char *file, int line)
{
    fprintf(stderr, "DBERR: [%s:%d] %d - %s\n", file, line, errno, cdb_errmsg(errno));
}


int cdb_destroy(CDB *db)
{
    if (db->opened)
        cdb_close(db);
    for(int i = 0; i < MLOCKNUM; i++)
        cdb_lock_destory(db->mlock[i]);
    cdb_lock_destory(db->dpclock);
    cdb_lock_destory(db->pclock);
    cdb_lock_destory(db->rclock);
    cdb_lock_destory(db->stlock);
    cdb_lock_destory(db->oidlock);
    cdb_lock_destory(db->bflock);
    cdb_bgtask_destroy(db->bgtask);
    pthread_key_delete(*(pthread_key_t*)db->errkey);
    free(db->errkey);
    free(db);
    return 0;
}



