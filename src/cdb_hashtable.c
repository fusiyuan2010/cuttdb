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


#include "cdb_hashtable.h"
#include <stdlib.h>
#include <string.h>

/*
#define LRUPREV(i) (*(CDBHTITEM**)&((i)->buf[0]))
#define LRUNEXT(i) (*(CDBHTITEM**)&((i)->buf[sizeof(void*)]))
*/

#define LRUPREV(i) ((i)->lruptr[0])
#define LRUNEXT(i) ((i)->lruptr[1])

static uint32_t MurmurHash1( const void * key, int len)
{
    const unsigned int m = 0xc6a4a793;
    const int r = 16;
    unsigned int h = 0x19900917 ^ (len * m);
    const unsigned char * data = (const unsigned char *)key;

    while(len >= 4)
    {
        unsigned int k = *(unsigned int *)data;
        h += k; h *= m; h ^= h >> 16;
        data += 4; len -= 4;
    }

    switch(len)
    {
    case 3:
        h += data[2] << 16;
    case 2:
        h += data[1] << 8;
    case 1:
        h += data[0];
        h *= m;
        h ^= h >> r;
    };

    h *= m; h ^= h >> 10;
    h *= m; h ^= h >> 17;
    return h;
} 

void *cdb_ht_itemkey(CDBHASHTABLE *ht, CDBHTITEM *item)
{
    return (void *)(item->buf + ht->lru * 2 * sizeof(void*));
}

void *cdb_ht_itemval(CDBHASHTABLE *ht, CDBHTITEM *item)
{
    return (void *)(item->buf + ht->lru * 2 * sizeof(void*) + item->ksize);
}

CDBHASHTABLE *cdb_ht_new(bool lru, CDBHASHFUNC hashfunc)
{
    CDBHASHTABLE *ht;

    ht = (CDBHASHTABLE*)malloc(sizeof(CDBHASHTABLE));
    ht->hash = NULL;
    ht->lru = lru;
    ht->num = ht->size = 0;
    ht->tail = ht->head = NULL;
    for(uint32_t i = 0; i < (1<<CDBHTBNUMPOW); i++) {
        CDBHTBUCKET *bucket = &(ht->buckets[i]);
        bucket->bnum = 2;
        uint32_t lsize = sizeof(CDBHTITEM *) * bucket->bnum;
        bucket->rnum = 0;
        bucket->items = (CDBHTITEM **)malloc(lsize);
        ht->size += lsize;
        memset(bucket->items, 0, lsize);
    }
    ht->hash = hashfunc;
    if (ht->hash == NULL)
        ht->hash = MurmurHash1;

    ht->size += sizeof(CDBHASHTABLE);

    return ht;
}

CDBHTITEM *cdb_ht_newitem(CDBHASHTABLE *ht, int ksize, int vsize)
{
    CDBHTITEM *item;
    int hsize;

    if (ht->lru)
        hsize = sizeof(CDBHTITEM) + 2 * sizeof(void*);
    else
        hsize = sizeof(CDBHTITEM);

    item = (CDBHTITEM*)malloc(hsize + ksize + vsize);
    item->ksize = ksize;
    item->vsize = vsize;
    if (ht->lru) {
        LRUPREV(item) = NULL;
        LRUNEXT(item) = NULL;
    }
    return item;
}




void cdb_ht_insert(CDBHASHTABLE *ht, CDBHTITEM *item)
{
    uint32_t bid, hid;
    CDBHTBUCKET *bucket;

    item->hash = ht->hash(cdb_ht_itemkey(ht, item), item->ksize);
    bid = item->hash & ((1<<CDBHTBNUMPOW)-1);
    bucket = &(ht->buckets[bid]);
    hid = (item->hash >> CDBHTBNUMPOW) & (bucket->bnum-1);

    if (bucket->rnum > bucket->bnum * 2) {
        CDBHTITEM **ilist;
        uint32_t exp = 2;
        if (bucket->bnum < 512) 
            exp = 4;
        int listsize = (bucket->bnum * exp) * sizeof(CDBHTITEM*);
        ilist = (CDBHTITEM**)malloc(listsize);
        memset(ilist, 0, listsize);
        for(uint32_t i = 0; i < bucket->bnum; i++) {
            CDBHTITEM *curitem = bucket->items[i];
            while(curitem != NULL) {
                CDBHTITEM *nextitem = curitem->hnext;
                uint32_t hid = (curitem->hash>>CDBHTBNUMPOW)
                    & (bucket->bnum * exp - 1);
                curitem->hnext = ilist[hid];
                ilist[hid] = curitem;
                curitem = nextitem;
            }
        }
        free(bucket->items);
        bucket->items = ilist;
        ht->size += listsize - bucket->bnum * sizeof(CDBHTITEM *);
        bucket->bnum *= exp;
        hid = (item->hash >> CDBHTBNUMPOW) & (bucket->bnum - 1);
    }

    {
        CDBHTITEM *curitem = bucket->items[hid];
        CDBHTITEM *preitem = NULL;
        while(curitem != NULL) {
            if (curitem->hash == item->hash
                && curitem->ksize == item->ksize
                && memcmp(cdb_ht_itemkey(ht, curitem),
                cdb_ht_itemkey(ht, item) ,curitem->ksize) == 0) {
                    CDBHTITEM *tmp;
                    if (ht->lru) {
                        if (LRUPREV(curitem))
                            LRUNEXT(LRUPREV(curitem)) = LRUNEXT(curitem);
                        if (LRUNEXT(curitem))
                            LRUPREV(LRUNEXT(curitem)) = LRUPREV(curitem);
                        if (ht->head == curitem)
                            ht->head = LRUNEXT(curitem);
                        if (ht->tail == curitem) 
                            ht->tail = LRUPREV(curitem);
                    }
                    if (preitem)
                        preitem->hnext = curitem->hnext;
                    else
                        bucket->items[hid] = curitem->hnext;
                    tmp = curitem->hnext;
                    ht->size -= sizeof(CDBHTITEM) + curitem->ksize + curitem->vsize
                        + (ht->lru > 0) * sizeof(CDBHTITEM*) * 2;
                    ht->num--;
                    bucket->rnum--;
                    free(curitem);
                    curitem = tmp;
                    break;
            }
            preitem = curitem;
            curitem = curitem->hnext;
        }
    }

    item->hnext = bucket->items[hid];
    bucket->items[hid] = item;

    if (ht->lru) {
        if (ht->head) LRUPREV(ht->head) = item;
        LRUPREV(item) = NULL;
        LRUNEXT(item) = ht->head;
        ht->head = item;
        if (ht->tail == NULL)
            ht->tail = item;
    }

    bucket->rnum++;
    ht->num++;
    ht->size += sizeof(CDBHTITEM) + item->ksize + item->vsize
        + ht->lru * sizeof(CDBHTITEM*) * 2;
}


void *cdb_ht_insert2(CDBHASHTABLE *ht, const void *key, int ksize, const void *val, int vsize)
{
    CDBHTITEM *item;

    item = cdb_ht_newitem(ht, ksize, vsize);
    memcpy(cdb_ht_itemkey(ht, item), key, ksize);
    memcpy(cdb_ht_itemval(ht, item), val, vsize);
    cdb_ht_insert(ht, item);
    return cdb_ht_itemval(ht, item);
}

void *cdb_ht_get(CDBHASHTABLE *ht, const void *key, int ksize, int *vsize, bool mtf)
{
    CDBHTITEM *res;

    res = cdb_ht_get3(ht, key, ksize, mtf);
    if (res) {
        *vsize = res->vsize;
        return cdb_ht_itemval(ht, res);
    } else { 
        *vsize = 0;
        return NULL;
    }
}


void *cdb_ht_get2(CDBHASHTABLE *ht, const void *key, int ksize, bool mtf)
{
    CDBHTITEM *res;

    res = cdb_ht_get3(ht, key, ksize, mtf);
    if (res)
        return cdb_ht_itemval(ht, res);
    else
        return NULL;
}


CDBHTITEM *cdb_ht_get3(CDBHASHTABLE *ht, const void *key, int ksize, bool mtf)
{
    uint32_t hash, bid, hid;
    CDBHTBUCKET *bucket;
    CDBHTITEM *curitem;

    hash = ht->hash(key, ksize);
    bid = hash & ((1<<CDBHTBNUMPOW)-1);
    bucket = &(ht->buckets[bid]);
    hid = (hash >> CDBHTBNUMPOW) & (bucket->bnum - 1);

    curitem = bucket->items[hid];
    while (curitem != NULL) {
        if (curitem->hash == hash
            && curitem->ksize == ksize
            && memcmp(cdb_ht_itemkey(ht, curitem), key , ksize) == 0) {
                if (ht->lru && mtf && ht->head != curitem) {
                    if (LRUPREV(curitem))
                        LRUNEXT(LRUPREV(curitem)) = LRUNEXT(curitem);
                    if (LRUNEXT(curitem))
                        LRUPREV(LRUNEXT(curitem)) = LRUPREV(curitem);             
                    if (ht->tail == curitem) 
                        ht->tail = LRUPREV(curitem);

                    LRUNEXT(curitem) = ht->head;
                    LRUPREV(ht->head) = curitem;
                    ht->head = curitem;
                    LRUPREV(curitem) = NULL;
                }
                return curitem;
        }
        curitem = curitem->hnext;
    }
    return NULL;
}


bool cdb_ht_exist(CDBHASHTABLE *ht, const void *key, int ksize)
{
    int vsize;
    return (cdb_ht_get(ht, key, ksize, &vsize, false) != NULL);
}


int cdb_ht_del2(CDBHASHTABLE *ht, const void *key, int ksize)
{
    CDBHTITEM *res = NULL;
    res = cdb_ht_del(ht, key, ksize);
    if (res) {
        free(res);
        return 0;
    }
    return -1;
}


CDBHTITEM *cdb_ht_del(CDBHASHTABLE *ht, const void *key, int ksize)
{
    uint32_t hash, bid, hid;
    CDBHTBUCKET *bucket;
    CDBHTITEM *curitem, *preitem;
    CDBHTITEM *res = NULL;

    hash = ht->hash(key, ksize);
    bid = hash & ((1<<CDBHTBNUMPOW)-1);
    bucket = &(ht->buckets[bid]);
    hid = (hash >> CDBHTBNUMPOW) & (bucket->bnum - 1);

    curitem = bucket->items[hid];
    preitem = NULL;
    while(curitem != NULL) {
        if (curitem->hash == hash
            && curitem->ksize == ksize
            && memcmp(cdb_ht_itemkey(ht, curitem),
            key, ksize) == 0) {
            if (ht->lru) {
                if (LRUPREV(curitem))
                    LRUNEXT(LRUPREV(curitem)) = LRUNEXT(curitem);
                if (LRUNEXT(curitem))
                    LRUPREV(LRUNEXT(curitem)) = LRUPREV(curitem);
                if (ht->head == curitem)
                    ht->head = LRUNEXT(curitem);
                if (ht->tail == curitem) 
                    ht->tail = LRUPREV(curitem);
            }
            if (preitem)
                preitem->hnext = curitem->hnext;
            else
                bucket->items[hid] = curitem->hnext;
            ht->size -= sizeof(CDBHTITEM) + curitem->ksize + curitem->vsize
                + (ht->lru > 0) * sizeof(CDBHTITEM*) * 2;
            ht->num--;
            bucket->rnum--;
            res = curitem;
            curitem = curitem->hnext;
            break;
        }
        preitem = curitem;
        curitem = curitem->hnext;
    }

    return res;
}


void cdb_ht_removetail(CDBHASHTABLE *ht)
{
    CDBHTITEM *item;

    item = cdb_ht_poptail(ht);
    if (item)
        free(item);
    return;
}


CDBHTITEM *cdb_ht_gettail(CDBHASHTABLE *ht)
{
    return ht->tail;
}


CDBHTITEM *cdb_ht_poptail(CDBHASHTABLE *ht)
{
    CDBHTITEM *item = ht->tail, *curitem, *preitem;;
    CDBHTBUCKET *bucket;
    uint32_t bid, hid;

    if (!(ht->lru) || item == NULL)
        return NULL;

    bid = item->hash & ((1<<CDBHTBNUMPOW)-1);
    bucket = &(ht->buckets[bid]);
    hid = (item->hash >> CDBHTBNUMPOW) & (bucket->bnum - 1);

    curitem = bucket->items[hid];
    preitem = NULL;
    while (curitem != NULL) {
        if (curitem->hash == item->hash
            && curitem->ksize == item->ksize
            && memcmp(cdb_ht_itemkey(ht, curitem),
            cdb_ht_itemkey(ht, item), item->ksize) == 0) {
                if (preitem) {
                    preitem->hnext = curitem->hnext;
                } else {
                    bucket->items[hid] = curitem->hnext;
                }
                break;   
        }
        preitem = curitem;
        curitem = curitem->hnext;
    }

    if (LRUPREV(item))
        LRUNEXT(LRUPREV(item)) = NULL;
    if (ht->head == item)
        ht->head = NULL;
    ht->tail = LRUPREV(item);
    bucket->rnum--;
    ht->num--;
    ht->size -= sizeof(CDBHTITEM) + item->ksize + item->vsize
        + sizeof(CDBHTITEM*) * 2;
    return item;
}

void cdb_ht_clean(CDBHASHTABLE *ht)
{
    for(uint32_t i = 0; i < (1<<CDBHTBNUMPOW); i++) {
        CDBHTBUCKET *bucket = &(ht->buckets[i]);
        for(uint32_t j = 0; j < bucket->bnum; j++) {
            CDBHTITEM *curitem = bucket->items[j];
            while(curitem != NULL) {
                CDBHTITEM *tmp = curitem->hnext;
                free(curitem);
                curitem = tmp;
            }
            bucket->items[j] = NULL;
        }
        bucket->rnum = 0;
    }
    ht->num = 0;
}


void cdb_ht_destroy(CDBHASHTABLE *ht)
{
    if (ht->lru) {
        CDBHTITEM *curitem = ht->head;
        while(curitem) {
            CDBHTITEM *nextitem = LRUNEXT(curitem);
            free(curitem);
            curitem = nextitem;
        }
    }

    for(uint32_t i = 0; i < (1<<CDBHTBNUMPOW); i++) {
        CDBHTBUCKET *bucket = &(ht->buckets[i]);

        for(uint32_t j = 0; j < bucket->bnum && (!ht->lru); j++) {
            CDBHTITEM *curitem = bucket->items[j];
            while(curitem != NULL) {
                CDBHTITEM *tmp = curitem->hnext;
                free(curitem);
                curitem = tmp;
            }
        }
        free(bucket->items);
    }
    free(ht);
}


CDBHTITEM *cdb_ht_iterbegin(CDBHASHTABLE *ht)
{
    for(uint32_t i = 0; i < (1<<CDBHTBNUMPOW); i++) {
        CDBHTBUCKET *bucket = &(ht->buckets[i]);
        if (!bucket->rnum)
            continue;
        for(uint32_t j = 0; j < bucket->bnum; j++)
            if (bucket->items[j])
                return bucket->items[j];
    }

    return NULL;
}


CDBHTITEM *cdb_ht_iternext(CDBHASHTABLE *ht, CDBHTITEM *cur)
{
    if (cur == NULL)
        return NULL;

    if (cur->hnext)
        return cur->hnext;

    uint32_t bid = cur->hash & ((1<<CDBHTBNUMPOW)-1);
    CDBHTBUCKET *bucket = &(ht->buckets[bid]);
    uint32_t hid = (cur->hash >> CDBHTBNUMPOW) & (bucket->bnum - 1);

    for(uint32_t i = hid + 1; i < bucket->bnum; i++) {
        if (bucket->items[i])
            return bucket->items[i];
    }

    for(uint32_t i = bid + 1; i < (1<<CDBHTBNUMPOW); i++) {
        CDBHTBUCKET *bucket = &(ht->buckets[i]);
        if (!bucket->rnum)
            continue;
        for(int j = 0; j < bucket->bnum; j++)
            if (bucket->items[j])
                return bucket->items[j];
    }

    return NULL;
}


#ifdef _UT_
#include <stdio.h>
#include <time.h>
int main(int argc, char *argv[])
{
    CDBHASHTABLE *ht;
    long k, v;
    ht = cdb_ht_new(true, NULL);
    for(int i = 0; i < 1000; i++) {
        k = i;
        v = i * 1000;
        cdb_ht_insert2(ht, &k, sizeof(long), &v, sizeof(long));
    }

    srand(time(NULL));

    for(int i = 0; i < 1000; i++) {
        long *v, k = rand() % 1000;
        int vsize;
        v = (long*)cdb_ht_get(ht, &k, sizeof(long), &vsize, true);
        printf("get: %ld -> %ld (%d)\n", k, *v, vsize);
    }

    printf("total size: %d  num: %d\n", ht->size, ht->num);

    CDBHTITEM *item;
    item = cdb_ht_poptail(ht);
    printf("tail:  %ld - %ld\n", *(long*)cdb_ht_itemkey(ht, item), *(long*)cdb_ht_itemval(ht, item));
    free(item);
    item = cdb_ht_poptail(ht);
    printf("tail:  %ld - %ld\n", *(long*)cdb_ht_itemkey(ht, item), *(long*)cdb_ht_itemval(ht, item));
    free(item);
}
#endif
