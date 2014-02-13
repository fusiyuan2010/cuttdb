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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>


bool itcb(void *arg, const char *key, int ksize, const char *val, int vsize, uint32_t expire, uint64_t oid)
{
#define SBUFSIZE 4096
    char buf[SBUFSIZE];
    char *kvbuf = buf;
    if (ksize + vsize + 2 > SBUFSIZE)
        kvbuf = (char*)malloc(ksize + vsize + 2);
    memcpy(kvbuf, key, ksize);
    kvbuf[ksize] = '\t';
    memcpy(kvbuf + ksize + 1, val, vsize);
    kvbuf[ksize + vsize + 1] = '\0';
    printf("%s\t%u\n", kvbuf, expire);
    if (kvbuf != buf)
        free(kvbuf);
    return true;
}

int main(int argc, char *argv[])
{
    /* 1TB */
    int cache_limit = 1048576;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s dbpath [cachelimit(MB)].... \n", argv[0]);
        return -1;
    }
    if (argc > 2) {
        cache_limit = atoi(argv[2]);
    }
    
    CDB *db = cdb_new();
    cdb_option(db, 0, 0, cache_limit);
    if (cdb_open(db, argv[1], CDB_PAGEWARMUP) < 0) {
        fprintf(stderr, "Database open error, unable to recovery\n");
        return -1;
    }
    void *it = cdb_iterate_new(db, 0);
    cdb_iterate(db, itcb, NULL, it);
    cdb_iterate_destroy(db, it);
    cdb_destroy(db);
}





