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


#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "cuttdb.h"


CDB *db;

enum {
    SETOP,
    GETOP,
    DELOP,
};

#if 1
static int prob_table1[8] = {SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, DELOP, GETOP};
static int prob_table2[8] = {SETOP, SETOP, SETOP, SETOP, SETOP, DELOP, DELOP, GETOP};
static int prob_table3[8] = {SETOP, SETOP, SETOP, DELOP, DELOP, DELOP, DELOP, GETOP};
#else
static int prob_table1[8] = {SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, GETOP};
static int prob_table2[8] = {SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, GETOP};
static int prob_table3[8] = {SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, SETOP, GETOP};
#endif
int *optable = NULL;


long get_rand() 
{
    return (long)rand() * RAND_MAX + rand();
}


void *test_thread(void *arg)
{
    char key[64];
    char value[128];
    void *v;
    int knum = *(int*)arg;
    while(1) {
        int krand = get_rand() % knum;
        int ksize = snprintf(key, 64, "%ld%ld%ld", krand, krand, krand);
        int vsize = snprintf(value, 128, "%ld%ld%ld%ld%d%ld%ld%ld%ld",
                krand, krand, krand, krand, krand, krand, krand, krand);
        int op = optable[rand() & 0x07];
        int expire = 600 + 20 * (rand() % 1000);
        switch(op) {
            case SETOP:
                if (cdb_set2(db, key, ksize, value, vsize, CDB_OVERWRITE | CDB_INSERTCACHE, expire) < 0)
                    printf("ERROR! %s:%d\n", __FILE__, __LINE__);
                break;
            case GETOP:
                if (cdb_get(db, key, ksize, &v, &vsize) == -1)
                    printf("ERROR! %s:%d\n", __FILE__, __LINE__);
                if (v)
                    cdb_free_val(&v);
                break;
            case DELOP:
                if (cdb_del(db, key, ksize) == -1)
                    printf("ERROR! %s:%d\n", __FILE__, __LINE__);
                break;
            default:
                break;
        }
    }
}



int main(int argc, char *argv[])
{
    int thread_num = 2;
    int record_num = 10000000;
    char *db_path = NULL;
    printf("Usage: %s db_path [record_num] [thread_num]\n", argv[0]);
    if (argc >= 2)
        db_path = argv[1];
    else
        return -1;

    if (argc >= 3)
        record_num = atoi(argv[2]);
    if (argc >= 4)
        thread_num = atoi(argv[3]);

    record_num = record_num < 100? 100: record_num;
    thread_num = thread_num < 1? 1: thread_num;
    srand(time(NULL));

    db = cdb_new();
    cdb_option(db, record_num / 100, 0, 1024000);
    if (cdb_open(db, db_path, CDB_CREAT | CDB_TRUNC) < 0) {
        printf("DB Open err\n");
        return -1;
    }


    optable = prob_table1;
    pthread_t threads[thread_num];
    for(int i = 0; i < thread_num; i++) {
        pthread_create(&threads[i], NULL, test_thread, &record_num);
    }

    int clear_interval = 0;
    while(1) {
        CDBSTAT st;
        cdb_stat(db, &st);
        printf("rnum: %lu, rcnum: %lu, pnum: %lu, pcnum %lu, rlatcy: %u  wlatcy: %u"
                " rh/m: %lu/%lu ph/m: %lu/%lu\n",
                st.rnum, st.rcnum, st.pnum, st.pcnum, st.rlatcy, st.wlatcy,
                st.rchit, st.rcmiss, st.pchit, st.pcmiss);
        if (++clear_interval % 20 == 0)
            cdb_stat(db, NULL);

        if (st.rnum > 0.7 * record_num)
            optable = prob_table2;
        if (st.rnum > 0.9 * record_num)
            optable = prob_table3;

        if (st.rnum < 0.8 * record_num)
            optable = prob_table2;

        if (st.rnum < 0.6 * record_num)
            optable = prob_table1;
        fflush(stdout);
        sleep(1);
    }
    
    return 0;
}



