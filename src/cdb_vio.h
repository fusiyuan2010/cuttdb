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


#ifndef _CDB_VIO_H_
#define _CDB_VIO_H_
#include "cdb_types.h"
#include "cuttdb.h"
#include <stdint.h>
#include <stdbool.h>

enum {
    /* obsoleted */
    CDBVIOAPPEND,
    /* append only format storage */
    CDBVIOAPND2,
};

typedef struct CDBVIO CDBVIO;

/* write a record, returns virtual offset at 3rd parameter */
typedef int (*VIOWRITEREC)(CDBVIO*, CDBREC*, FOFF*);
/* delete a record, pass in the current offset at 3rd parameter */
typedef int (*VIODELETEREC)(CDBVIO*, CDBREC*, FOFF);
/* read a record, 2nd parameter default points to stack buffer, if its real size
greater than the stack buffer size, it will be changed to points to a space in heap, 
the last parameter decides whether read the whole record or just read key for comparsion */
typedef int (*VIOREADREC)(CDBVIO*, CDBREC**, FOFF, bool);
/* close the storage */
typedef int (*VIOCLOSE)(CDBVIO*);
/* open the storage, pass in the storage path and open mode */
typedef int (*VIOOPEN)(CDBVIO*, const char*, int);
/* write an index page, return its virtual offset at 3rd parameter */
typedef int (*VIOWRITEPAGE)(CDBVIO*, CDBPAGE *, FOFF*);
/* read an index page, 2nd parameter default points to stack buffer, if its real size
greater than the stack buffer size, it will be changed to points to a space in heap */
typedef int (*VIOREADPAGE)(CDBVIO*, CDBPAGE **, FOFF);
/* make the storage do an sync operation */
typedef int (*VIOSYNC)(CDBVIO*);
/* write db header, which contains main-index */
typedef int (*VIOWRITEHEAD)(CDBVIO*);
/* read db header, which contains main-index */
typedef int (*VIOREADHEAD)(CDBVIO*);
/* tell that no dirty page exists */
typedef void (*VIOCLEANPOINT)(CDBVIO*);
/* get the record/page iterator at oid */
typedef void* (*VIOITFIRST)(CDBVIO *, uint64_t oid);
/* get the next index page by iterator */
typedef int (*VIOPAGEITNEXT)(CDBVIO *, CDBPAGE **, void *);
/* get the next record by iterator */
typedef int (*VIORECITNEXT)(CDBVIO *, CDBREC **, void *);
/* destroy and free the iterator */
typedef void (*VIOITDESTROY)(CDBVIO *, void *);

struct CDBVIO 
{
    VIOOPEN open;
    VIOCLOSE close;

    VIOWRITEREC wrec;
    VIODELETEREC drec;
    VIOREADREC rrec;

    VIOWRITEPAGE wpage;
    VIOREADPAGE rpage;

    VIOSYNC sync;
    VIOWRITEHEAD whead;
    VIOREADHEAD rhead;
    
    VIOCLEANPOINT cleanpoint;

    VIOITFIRST pageitfirst;
    VIOPAGEITNEXT pageitnext;
    VIOITDESTROY pageitdestroy;

    VIOITFIRST recitfirst;
    VIORECITNEXT recitnext;
    VIOITDESTROY recitdestroy;

    CDB *db;
    void *iometa;
};


CDBVIO *cdb_vio_new(int type);
int cdb_vio_destroy(CDBVIO *vio);


#endif
