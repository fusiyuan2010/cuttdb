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


#include "cdb_vio.h"
#include "cdb_types.h"
#include "vio_apnd2.h"
#include "stdlib.h"


CDBVIO *cdb_vio_new(int type)
{
    CDBVIO *res;
    res = (CDBVIO *)malloc(sizeof(CDBVIO));
    switch(type) {
        case CDBVIOAPND2:
            vio_apnd2_init(res);
            break;
        default:
            vio_apnd2_init(res);
            break;
    }
    return res;
}

int cdb_vio_destroy(CDBVIO *vio)
{
    free(vio);
    return 0;
}

