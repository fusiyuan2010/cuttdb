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


#ifndef _CDB_TYPES_H_
#define _CDB_TYPES_H_
#include <stdint.h>

#define KB 1024
#define MB 1048576
#define CDBMIN(a, b) ((a)<(b)?(a):(b))
#define CDBMAX(a, b) ((a)>(b)?(a):(b))

#define SI8 8
#define SI4 4
/* space reserved in stack for i/o, avoid some malloc/free */
#define SBUFSIZE (64 * KB)

/* a default disk read size for index page, 3KB is enough(a page with 300 items) */
#define PAGEAREADSIZE (3 * KB)

/* reserved in stack for matched items in a hash index page */
#define SFOFFNUM 8

/* a valid virtual offset */
#define OFFNOTNULL(o) (((o).i4)||((o).i2))
/* a null virtual offset */
#define OFFNULL(o) (((o).i4==0)&&((o).i2==0))
/* nullify an offset  */
#define OFFZERO(o) do{(o).i4=0;(o).i2=0;}while(0)
/* offset is equal ? */
#define OFFEQ(a,b) (((a).i4==(b).i4)&&((a).i2==(b).i2))
/* hash in page is equal ? */
#define PHASHEQ(a,b) (((a).i2==(b).i2)&&((a).i1==(b).i1))
/* page size increment */
#define CDB_PAGEINCR 4


/* if page cache size exceeds the limit */
#define PCOVERFLOW(db) ((db)->dpcache && (db)->dpcache->size + (db)->pcache->size > (db)->pclimit)
/* if record cache size exceeds the limit */
#define RCOVERFLOW(db) ((db)->rcache && (db)->rcache->size > (db)->rclimit)

/* timeout for a dirty index page stays since last modify */
#define DPAGETIMEOUT 40
/* operation on main table are isolated by these locks */
#define MLOCKNUM 256

#define CDBHASH64(a, b) cdb_crc64(a, b) 

/* all virtual offsets are 48-bits */
typedef struct FOFF
{
    uint32_t i4;
    uint16_t i2;
} __attribute__((packed)) FOFF;



#define SFOFF (sizeof(FOFF))


/* all hash value in index page are 24-bits 
    range 0..16M guarantee very low collision 
    with less than a hundred records in a page */
typedef struct PHASH
{
    uint16_t i2;
    uint8_t i1;
} __attribute__((packed)) PHASH;


/* an item in index page contains a hash and an offset */
typedef struct PITEM
{
    FOFF off;
    PHASH hash;
} __attribute__((packed)) PITEM;


/* data record */
typedef struct CDBREC{
    /* where the data come from */
    FOFF ooff;
    uint32_t osize;

    /* access convenient*/
    void *key;
    void *val;

    /* disk store starts at following field */
    uint32_t magic;
    uint32_t ksize;
    uint32_t vsize;
    uint32_t expire;
    uint64_t oid;
    char buf[0];
} __attribute__((packed)) CDBREC;

/* real size of a record header when stored on disk */
#define RECHSIZE (SI4 * 4 + SI8)
/* real size of a record when stored on disk */
#define RECSIZE(r) (RECHSIZE + (r)->ksize + (r)->vsize)


/* index page */
typedef struct CDBPAGE{
    FOFF ooff;
    uint32_t osize;
    uint32_t cap;

    union {
        /* what it be on disk */
        uint32_t magic;
        /* what it be in memory */
        uint32_t mtime;
    };
    /* which bucket it belongs to */
    uint32_t bid;
    uint32_t num;
    uint64_t oid;
    PITEM items[0];
} __attribute__((packed)) CDBPAGE;

/* real size of a page header when stored on disk */
#define PAGEHSIZE (SI4 * 3 + SI8)
/* real size of a page when stored on disk */
#define PAGESIZE(p) (PAGEHSIZE + sizeof(PITEM) * (p)->num)
/* in-memory size of an record structure */
#define MPAGESIZE(p) (sizeof(CDBPAGE) + sizeof(PITEM) * (p)->cap)

#endif

