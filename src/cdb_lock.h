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


#ifndef _CDB_LOCK_H_
#define _CDB_LOCK_H_


enum {
    /* spinlock */
    CDB_LOCKSPIN,
    /* mutex, which may cause OS context switch, mainly used in where Disk IO happens */
    CDB_LOCKMUTEX,
};

/* may be used to indicated whether the area is protected */
enum {
    CDB_LOCKED,
    CDB_NOTLOCKED,
};

typedef struct CDBLOCK
{
    int ltype;
    char lock[0];
} CDBLOCK;


CDBLOCK *cdb_lock_new(int ltype);
void cdb_lock_lock(CDBLOCK *lock);
void cdb_lock_unlock(CDBLOCK *lock);
void cdb_lock_destory(CDBLOCK *lock);
int cdb_lock_trylock(CDBLOCK *lock);



#endif

