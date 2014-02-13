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
#include <stdint.h>
#include <time.h>

int main(int argc, char *argv[])
{
    CDB *db = cdb_new();
    if (argc < 2) {
        fprintf(stderr, "Usage: %s db_path [hsize = 2000000]\n", argv[0]);
        return 0;
    }
        
    /* 1TB memory limit(unlimited) */
    cdb_option(db, argc >= 3? atoi(argv[2]):2000000 , 0, 1048576);
    cdb_seterrcb(db, cdb_deferrorcb, NULL);
    if (cdb_open(db, argv[1], CDB_CREAT | CDB_PAGEWARMUP) < 0) {
        return -1;
    }
    char *buf = NULL;
    long count = 0;

    size_t size, size2;
    while((size = getline(&buf, &size2, stdin)) != -1) {
        /* remove the delimiter*/
        buf[--size] = '\0';
        int klen = -1;
        int vlen = -1;
        uint32_t expire = 0;
        int parsenum = 0;
        for(int i = 0; i < size; i++) {
            if (buf[i] == '\t') {
                if (klen == -1)
                    klen = i;
                else {
                    vlen = i - klen - 1;
                    parsenum = 1;
                }
            } else if (buf[i] >= '0' && buf[i] <= '9' && parsenum) {
                expire = expire * 10 + buf[i] - '0';
            }
        }

        if (klen > 0 && vlen > 0) {
            cdb_set2(db, buf, klen, buf + klen + 1, vlen,
                    CDB_OVERWRITE, expire > 0? expire - time(NULL): 0);
            count++;
        }
        free(buf);
        buf = NULL;
    }
    cdb_destroy(db);
    fprintf(stderr, "imported %ld records\n", count);
    return 0;
}


