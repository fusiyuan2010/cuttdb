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


#ifndef _CDB_BGTASK_H_
#define _CDB_BGTASK_H_
#include <time.h>
#include <pthread.h>


/* 16 tasks at most in a task thread */
#define MAXTASKNUM 16

typedef void (*TASKFUNC)(void *);

/* struct for timer task */
typedef struct {
    /* task function */
    TASKFUNC func;
    /* task argument */
    void *arg;
    /* task run interval(seconds) */
    int intval;
    /* time of last run */
    time_t ltime;
} TASK;

/* struct for a background task manager */
typedef struct CDBBGTASK
{
    TASK tasks[MAXTASKNUM];
    /* number of tasks */
    int tnum;
    /* is running? */
    int run;
    pthread_t tid;
    /* for wait the thread exit */
    pthread_mutex_t smutex;
    pthread_cond_t scond;
} CDBBGTASK;



CDBBGTASK *cdb_bgtask_new();
int cdb_bgtask_add(CDBBGTASK *task, TASKFUNC func, void *arg, int intval);
void cdb_bgtask_start(CDBBGTASK *bt);
void cdb_bgtask_stop(CDBBGTASK *task);
void cdb_bgtask_destroy(CDBBGTASK *task);


#endif
