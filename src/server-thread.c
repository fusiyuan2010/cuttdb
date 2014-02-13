/*
 *   CuttDB - a fast key-value storage engine
 *
 *
 *   http://code.google.com/p/cuttdb/
 *   
 *   The server&network part of CuttDB is based on Beansdb:
 *
 *   http://beansdb.googlecode.com
 *
 *   Beansdb is most based on Memcachedb and Memcached:
 *
 *   http://memcachedb.org/
 *   http://danga.com/memcached/
 *
 *   Copyright (c) 2012, Siyuan Fu.  All rights reserved.
 *   Use and distribution licensed under the BSD license. 
 *   See the LICENSE file for full text
 *
 *   Author: Siyuan Fu <fusiyuan2010@gmail.com>
 *
 */

#include "cuttdb-server.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include <pthread.h>

typedef struct EventLoop {
//    int   maxfd;
    conn* conns[AE_SETSIZE];
    int   fired[AE_SETSIZE];
    int   nready;
    void *apidata;
} EventLoop;

/* Lock for connection freelist */
static pthread_mutex_t conn_lock;

/* Lock for item buffer freelist */
static pthread_mutex_t ibuffer_lock;

static EventLoop loop;
static pthread_mutex_t leader;

/*
 * Pulls a conn structure from the freelist, if one is available.
 */
conn *mt_conn_from_freelist() {
    conn *c;
    pthread_mutex_lock(&conn_lock);
    c = do_conn_from_freelist();
    pthread_mutex_unlock(&conn_lock);
    return c;
}

/*
 * Adds a conn structure to the freelist.
 *
 * Returns 0 on success, 1 if the structure couldn't be added.
 */
bool mt_conn_add_to_freelist(conn *c) {
    bool result;

    pthread_mutex_lock(&conn_lock);
    result = do_conn_add_to_freelist(c);
    pthread_mutex_unlock(&conn_lock);

    return result;
}


/******************************* GLOBAL STATS ******************************/

void mt_stats_lock() {
}

void mt_stats_unlock() {
}

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EPOLL
#include "ae_epoll.c"
#else
    #ifdef HAVE_KQUEUE
    #include "ae_kqueue.c"
    #else
    #include "ae_select.c"
    #endif
#endif

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of event handler threads to spawn
 */
void thread_init(int nthreads) {
    pthread_mutex_init(&ibuffer_lock, NULL);
    pthread_mutex_init(&conn_lock, NULL);
    pthread_mutex_init(&leader, NULL);
    
    memset(&loop, 0, sizeof(loop));
    if (aeApiCreate(&loop) == -1) {
        exit(1);
    }
}

int add_event(int fd, int mask, conn *c)
{
    if (fd >= AE_SETSIZE) {
        fprintf(stderr, "fd is too large: %d\n", fd);
        return AE_ERR;
    }
    assert(loop.conns[fd] == NULL);
    loop.conns[fd] = c;
    if (aeApiAddEvent(&loop, fd, mask) == -1){
        loop.conns[fd] = NULL;
        return AE_ERR;
    }
//    if (fd > loop.maxfd)
//        loop.maxfd = fd;
    return AE_OK;
}

int update_event(int fd, int mask, conn *c)
{
    loop.conns[fd] = c;
    if (aeApiUpdateEvent(&loop, fd, mask) == -1){
        loop.conns[fd] = NULL;
        return AE_ERR;
    }
    return AE_OK;
}

int delete_event(int fd)
{
    if (fd >= AE_SETSIZE) return -1;
    if (loop.conns[fd] == NULL) return 0;
    if (aeApiDelEvent(&loop, fd) == -1)
        return -1;
    loop.conns[fd] = NULL;
    return 0;
}

static void *worker_main(void *arg) {
    pthread_setcanceltype (PTHREAD_CANCEL_ASYNCHRONOUS, 0);
    
    struct timeval tv = {1, 0};
    while (!daemon_quit) {
        pthread_mutex_lock(&leader);

AGAIN:
        while(loop.nready == 0 && daemon_quit == 0)
            loop.nready = aeApiPoll(&loop, &tv);
        if (daemon_quit) {
            pthread_mutex_unlock(&leader);
            break;
        }
       
        loop.nready --;
        int fd = loop.fired[loop.nready];
        conn *c = loop.conns[fd];
        if (c == NULL){
            fprintf(stderr, "Bug: conn %d should not be NULL\n", fd);
            close(fd);
            goto AGAIN;
        }
        loop.conns[fd] = NULL; 
        pthread_mutex_unlock(&leader);
        
        drive_machine(c);
        if (c->ev_flags > 0) {
            update_event(fd, c->ev_flags, c);
        }
    }
    return NULL; 
}

void loop_run(int nthread)
{
    int i, ret;
    pthread_attr_t  attr;
    pthread_attr_init(&attr);
    pthread_t* tids = malloc(sizeof(pthread_t) * nthread);
    
    for (i=0; i<nthread - 1; i++) {
        if ((ret = pthread_create(tids + i, &attr, worker_main, NULL)) != 0) {
            fprintf(stderr, "Can't create thread: %s\n",
                    strerror(ret));
            exit(1);
        }
    }
    
    worker_main(NULL);
    
    // wait workers to stop
    for (i=0; i<nthread - 1; i++) {
        (void) pthread_join(tids[i], NULL);
    }
    free(tids);
}

