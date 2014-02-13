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


/*
Bloom Filter is currently not used in cuttdb
*/
#ifndef _CDB_BLOOMFILTER_H_
#define _CDB_BLOOMFILTER_H_
#include <stdbool.h>
#include <stdint.h>

typedef struct CDBBLOOMFILTER CDBBLOOMFILTER;

#define CDBBFRATIO 8

CDBBLOOMFILTER *cdb_bf_new(uint64_t rnum, uint64_t size);
void cdb_bf_set(CDBBLOOMFILTER *bf, void *key, int ksize);
bool cdb_bf_exist(CDBBLOOMFILTER *bf, void *key, int ksize);
void cdb_bf_clean(CDBBLOOMFILTER *bf);
void cdb_bf_destroy(CDBBLOOMFILTER *bf);

#endif
