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


#include "cdb_bgtask.h"
#include <stdlib.h>
#include <sys/signal.h>


/* where thread begins */
static void *_cdb_bgtask_func(void *arg);


CDBBGTASK *cdb_bgtask_new()
{
    CDBBGTASK *bt = (CDBBGTASK *)malloc(sizeof(CDBBGTASK));

    bt->tnum = 0;
    bt->run = 0;
    bt->tid = 0;
    pthread_cond_init(&bt->scond, NULL);
    pthread_mutex_init(&bt->smutex, NULL);
    return bt;
}


/* add a task into task list, must called before the thread run */
int cdb_bgtask_add(CDBBGTASK *bt, TASKFUNC func, void *arg, int intval)
{
    TASK *task = &bt->tasks[bt->tnum];

    if (bt->tid || bt->tnum > MAXTASKNUM)
        return -1;

    task->arg = arg;
    task->func = func;
    task->intval = intval;
    task->ltime = time(NULL);
    bt->tnum++;
    return 0;
}


static void *_cdb_bgtask_func(void *arg)
{
    CDBBGTASK *bt = (CDBBGTASK *)arg;

    /* block all signals coming into current thread */
    sigset_t smask;
    sigfillset(&smask);
    pthread_sigmask(SIG_BLOCK, &smask, NULL);

    /* loop */
    while(bt->run) {
        time_t now = time(NULL);
        struct timespec timeout;

        /* check should run some tasks every 1 second */
        timeout.tv_sec = now + 1;
        timeout.tv_nsec = 0;

        /* iterate and run the tasks */
        for(int i = 0; i < bt->tnum; i++) {
            TASK *task = &bt->tasks[i];
            if (now >= task->ltime + task->intval) {
                task->func(task->arg);
                task->ltime = now;
            }
        }
        pthread_cond_timedwait(&bt->scond, &bt->smutex, &timeout);
    }

    return NULL;
}


/* create a thread for tasks */
void cdb_bgtask_start(CDBBGTASK *bt)
{
    if (bt->run)
        return;

    bt->run = 1;
    pthread_create(&bt->tid, NULL, _cdb_bgtask_func, bt);
    return;
}


/* wait for the task thread exits */
void cdb_bgtask_stop(CDBBGTASK *bt)
{
    if (bt->run) {
        void **ret = NULL;
        bt->run = 0;
        pthread_cond_signal(&bt->scond);
        pthread_join(bt->tid, ret);
    }

    bt->tnum = 0;
}


void cdb_bgtask_destroy(CDBBGTASK *bt)
{
    cdb_bgtask_stop(bt);
    pthread_cond_destroy(&bt->scond);
    pthread_mutex_destroy(&bt->smutex);
    free(bt);
}




