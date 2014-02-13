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


#include "cdb_bloomfilter.h"
#include <stdlib.h>
#include <string.h>

#define CDBBFHASHNUM 16
#define CDBBFSPLITPOW 6

static uint64_t BFSEEDS[CDBBFHASHNUM] = {217636919,290182597,386910137,515880193,
                                        687840301,917120411,1222827239,1610612741,
                                        3300450239,3300450259,3300450281,3300450289,
                                        3221225473ul,4294967291ul,163227661,122420729,};

struct CDBBLOOMFILTER
{
    uint8_t *bitmap[1<<CDBBFSPLITPOW];
    uint64_t rnum;
    uint64_t size;
    int hnum;
    int ratio;
};


CDBBLOOMFILTER *cdb_bf_new(uint64_t rnum, uint64_t size)
{
    CDBBLOOMFILTER *bf = (CDBBLOOMFILTER *)malloc(sizeof(CDBBLOOMFILTER));
    bf->rnum = 0;
    bf->size = size;
    /* number of hash should be 0.7 * ratio */
    bf->hnum = size * 8 * 7 / (rnum * 10);
    /* number of hash is limit in [1, 16] */
    if (bf->hnum > CDBBFHASHNUM)
        bf->hnum = CDBBFHASHNUM;
    if (bf->hnum == 0)
        bf->hnum = 1;
    /* avoid malloc too much memory once */
    for(int i = 0; i < (1 << CDBBFSPLITPOW); i++) {
        bf->bitmap[i] = (uint8_t*)malloc(size >> CDBBFSPLITPOW);
        memset(bf->bitmap[i], 0, size >> CDBBFSPLITPOW);
    }
    return bf;
}


void cdb_bf_set(CDBBLOOMFILTER *bf, void *key, int ksize)
{
    uint8_t *src = (uint8_t *)key, *end = src + ksize;
    uint64_t hval[CDBBFHASHNUM] = {0};

    for(;src < end; src++) 
        for(int i = 0; i < bf->hnum; i++) 
            hval[i] = hval[i] * BFSEEDS[i] + *src;

    for(int i = 0; i < bf->hnum; i++) {
        uint64_t p = (hval[i] >> CDBBFSPLITPOW) % ((bf->size >> CDBBFSPLITPOW) << 3);
        uint8_t *bitmap = bf->bitmap[hval[i] & ((1<<CDBBFSPLITPOW) - 1)];
        bitmap[p >> 3] |= (1 << (p & 0x07));
    }

    bf->rnum++;
}


bool cdb_bf_exist(CDBBLOOMFILTER *bf, void *key, int ksize)
{
    uint8_t *src = (uint8_t *)key, *end = src + ksize;
    uint64_t hval[CDBBFHASHNUM] = {0};
    int exist = 0;

    for(;src < end; src++) 
        for(int i = 0; i < bf->hnum; i++) 
            hval[i] = hval[i] * BFSEEDS[i] + *src;

    for(int i = 0; i < bf->hnum; i++) {
        uint64_t p = (hval[i] >> CDBBFSPLITPOW) % ((bf->size >> CDBBFSPLITPOW) << 3);
        uint8_t *bitmap = bf->bitmap[hval[i] & ((1<<CDBBFSPLITPOW) - 1)];
        if (bitmap[p >> 3] & (1 << (p & 0x07)))
            exist++;
        else 
            break;
    }

    return (exist == bf->hnum);
}

void cdb_bf_clean(CDBBLOOMFILTER *bf)
{
    for(int i = 0; i < (1 << CDBBFSPLITPOW); i++) 
        memset(bf->bitmap[i], 0, bf->size >> CDBBFSPLITPOW);

    bf->rnum = 0;
}


void cdb_bf_destroy(CDBBLOOMFILTER *bf)
{
    for(int i = 0; i < (1 << CDBBFSPLITPOW); i++) 
        free(bf->bitmap[i]);
    free(bf);
}


#ifdef _UT_CDBBF_
#include <stdio.h>
#include <stdlib.h>
#include "cdb_bloomfilter.h"

int main(int argc, char *argv[])
{
    int size = 1048576;
    int rnum = 1048576;
    if (argc > 1)
        rnum = atoi(argv[1]);
    if (argc > 2)
        size = atoi(argv[2]);

    CDBBLOOMFILTER *bf = cdb_bf_new(rnum, size);
    for(int i = 0; i < rnum; i++) {
        int j = 2 * i;
        cdb_bf_set(bf, &j, 4);
    }

    int exist = 0;
    for(int i = 0; i < rnum; i++) {
        int j = 2 * i;
        if (cdb_bf_exist(bf, &j, 4))
            exist++;
    }
    printf("right positive: %.2f%%%%\n", (float)exist/(float)rnum*10000);

    exist = 0;
    for(int i = 0; i < rnum * 2; i++) {
        int j = 2 * i + 1;
        if (cdb_bf_exist(bf, &j, 4))
            exist++;
    }

    printf("false positive: %.2f%%%%  %d/%d\n", (float)exist/(float)rnum*5000, exist, rnum * 2);
    printf("element num: %d\n", bf->rnum);
    cdb_bf_destroy(bf);
    return 0;
}
#endif

