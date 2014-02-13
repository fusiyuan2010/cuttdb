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


#include "cdb_lock.h"
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>


CDBLOCK *cdb_lock_new(int ltype)
{
    CDBLOCK *lock = NULL;
    if (ltype == CDB_LOCKSPIN) {
        lock = (CDBLOCK *)malloc(sizeof(CDBLOCK) + sizeof(pthread_spinlock_t));
        pthread_spin_init((pthread_spinlock_t*)&lock->lock, PTHREAD_PROCESS_PRIVATE);
    } else if (ltype == CDB_LOCKMUTEX) {
        lock = (CDBLOCK *)malloc(sizeof(CDBLOCK) + sizeof(pthread_mutex_t));
        pthread_mutex_init((pthread_mutex_t*)&lock->lock, NULL);
    }
    lock->ltype = ltype;

    return lock;
}


void cdb_lock_lock(CDBLOCK *lock)
{
    if (lock->ltype == CDB_LOCKSPIN)
        pthread_spin_lock((pthread_spinlock_t*)&lock->lock);
    else if (lock->ltype == CDB_LOCKMUTEX)
        pthread_mutex_lock((pthread_mutex_t*)&lock->lock);
}


void cdb_lock_unlock(CDBLOCK *lock)
{
    if (lock->ltype == CDB_LOCKSPIN)
        pthread_spin_unlock((pthread_spinlock_t*)&lock->lock);
    else if (lock->ltype == CDB_LOCKMUTEX)
        pthread_mutex_unlock((pthread_mutex_t*)&lock->lock);
}


void cdb_lock_destory(CDBLOCK *lock)
{
    if (lock->ltype == CDB_LOCKSPIN)
        pthread_spin_destroy((pthread_spinlock_t*)&lock->lock);
    else if (lock->ltype == CDB_LOCKMUTEX)
        pthread_mutex_destroy((pthread_mutex_t*)&lock->lock);

    free(lock);
}


int cdb_lock_trylock(CDBLOCK *lock)
{
    if (lock->ltype == CDB_LOCKSPIN)
        return pthread_spin_trylock((pthread_spinlock_t*)&lock->lock);
    else if (lock->ltype == CDB_LOCKMUTEX)
        return pthread_mutex_trylock((pthread_mutex_t*)&lock->lock);
    return 0;
}

