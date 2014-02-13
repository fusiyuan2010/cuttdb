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


#ifndef _CDB_HASHTABLE_H_
#define _CDB_HASHTABLE_H_
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef uint32_t (*CDBHASHFUNC)(const void *, int);

/* default 1<<8 level-1 buckets, which makes the table expanding more smoothly */
#define CDBHTBNUMPOW 8


typedef struct CDBHTITEM
{
    int ksize;
    int vsize;
    uint32_t hash;
    /* next element with the same hash */
    struct CDBHTITEM *hnext;
    /* if LRU is true, the first several bytes are two pointers of prev/next element */
    struct CDBHTITEM *lruptr[0];
    char buf[0];
} __attribute__((packed)) CDBHTITEM;


typedef struct {
    /* array for items */
    CDBHTITEM **items;
    /* number of allocated slots in the bucket */
    uint32_t bnum;
    /* number of items exist in the bucket */
    uint32_t rnum;
} CDBHTBUCKET;


typedef struct CDBHASHTABLE {
    /* is in LRU mode? */
    bool lru;
    /* user specified hash function */
    CDBHASHFUNC hash;
    /* fixed number for level-1 buckets */
    CDBHTBUCKET buckets[1<<CDBHTBNUMPOW];
    /* memory usage */
    uint64_t size;
    /* number of items */
    uint64_t num;
    /* in LRU mode, the newest item */
    CDBHTITEM *head;
    /* in LRU mode, the oldest item */
    CDBHTITEM *tail;
} CDBHASHTABLE;


/* get the pointer of key in current item */
/* #define cdb_ht_itemkey(ht, item) (item->buf + ht->lru * 2 * sizeof(void*)) */
void *cdb_ht_itemkey(CDBHASHTABLE *ht, CDBHTITEM *item);

/* get the pointer of value in current item */
/* #define cdb_ht_itemval(ht, item) (item->buf + ht->lru * 2 * sizeof(void*) + item->ksize) */
void *cdb_ht_itemval(CDBHASHTABLE *ht, CDBHTITEM *item);

/* create an hashtable, it can be a simple hashtable or with LeastRecentUse
   The LRU mode needs extra two pointer space for every element
   hash function can by specified by user */
CDBHASHTABLE *cdb_ht_new(bool lru, CDBHASHFUNC hashfunc);

/* clean and free the hastable */
void cdb_ht_destroy(CDBHASHTABLE *ht);

/* allocate a new item with specified size, but do not insert it into table */
CDBHTITEM *cdb_ht_newitem(CDBHASHTABLE *ht, int ksize, int vsize);

/* insert an item which already exists into table */
void cdb_ht_insert(CDBHASHTABLE *ht, CDBHTITEM *item);

/* allocate and insert an item into table by key and value, return the pointer of value in table */
void *cdb_ht_insert2(CDBHASHTABLE *ht, const void *key, int ksize, const void *val, int vsize);

/* get the value of an item and its size in table, move the item to front if mtf == true */
void *cdb_ht_get(CDBHASHTABLE *ht, const void *key, int ksize, int *vsize, bool mtf);

/* get the value of an item, assume the size is known, move the item to front if mtf == true */
void *cdb_ht_get2(CDBHASHTABLE *ht, const void *key, int ksize, bool mtf);

/* get the pointer of an item, it hasn't been copied */
CDBHTITEM *cdb_ht_get3(CDBHASHTABLE *ht, const void *key, int ksize, bool mtf);

/* check if an item with the key exists */
bool cdb_ht_exist(CDBHASHTABLE *ht, const void *key, int ksize);

/* delete and free an item from table by its key */
int cdb_ht_del2(CDBHASHTABLE *ht, const void *key, int ksize);

/* return and delete an item from table, the item should be freed by user */
CDBHTITEM *cdb_ht_del(CDBHASHTABLE *ht, const void *key, int ksize);

/* delete and free the last item in table */
void cdb_ht_removetail(CDBHASHTABLE *ht);

/* return last item in table, do not delete nor free */
CDBHTITEM *cdb_ht_gettail(CDBHASHTABLE *ht);

/* return last item in table, delete but should be freed by user */
CDBHTITEM *cdb_ht_poptail(CDBHASHTABLE *ht);

/* clean and free all elements in the table*/
void cdb_ht_clean(CDBHASHTABLE *ht);

/* iterate the table by get the front one firstly */
CDBHTITEM *cdb_ht_iterbegin(CDBHASHTABLE *ht);

/* get the next item of current element */
CDBHTITEM *cdb_ht_iternext(CDBHASHTABLE *ht, CDBHTITEM *cur);

#if defined(__cplusplus)
}
#endif

#endif

