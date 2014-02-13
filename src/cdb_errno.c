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
#include "cdb_errno.h"
#include "cdb_types.h"
#include "cdb_core.h"
#include <pthread.h>


int cdb_errno(CDB *db)
{
    return (long)pthread_getspecific(*(pthread_key_t*)db->errkey);
}

const char *cdb_errmsg(int ecode)
{
    switch(ecode) {
        case CDB_SUCCESS:
            return "Success";
        case CDB_NOTFOUND:
            return "Key Not Found";
        case CDB_EXIST:
            return "Item Already Exists";
        case CDB_DIRNOEXIST:
            return "Path Open Failed";
        case CDB_OPENERR:
            return "File Open Failed";
        case CDB_PIDEXIST:
            return "Opened By Another Process";
        case CDB_DATAERRDAT:
            return "Data File Content Error";
        case CDB_DATAERRIDX:
            return "Index File Content Error";
        case CDB_WRITEERR:
            return "Write To File Error";
        case CDB_READERR:
            return "Read From File Error";
        case CDB_NOFID:
            return "Internal File Lost";
        case CDB_INTERNALERR:
            return "Internal Error";
        case CDB_DATAERRMETA:
            return "File Header Error";
        case CDB_MEMDBNOCACHE:
            return "MemDB Mode With Zero Record Cache Size";
        default:
            return "Error For Errno";
    }
}


void cdb_seterrcb(CDB *db, CDB_ERRCALLBACK errcb, void *arg)
{
    db->errcb = errcb;
    db->errcbarg = arg;
}


void cdb_seterrno(CDB *db, int ecode, const char *source, int line)
{
    pthread_setspecific(*(pthread_key_t*)db->errkey, (void*)(long)ecode);
    if (ecode != CDB_SUCCESS && db->errcb) {
        db->errcb(db->errcbarg, ecode, source, line);
    }
}
