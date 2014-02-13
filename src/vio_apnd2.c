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


#include "vio_apnd2.h"
#include "cdb_hashtable.h"
#include "cdb_bgtask.h"
#include "cdb_lock.h"
#include "cuttdb.h"
#include "cdb_core.h"
#include "cdb_errno.h"
#include "cdb_types.h"
#include "cdb_crc64.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

/* record magic bytes */
#define RECMAGIC 0x19871022
/* obsoleted, but appeared in some code */
#define DELRECMAGIC 0x19871023
#define PAGEMAGIC 0x19890604

/* data buffered before pwrite to disk */
#define IOBUFSIZE (2 * MB)
/* structure of deletion buffer differs from the others, buffered DELBUFMAX records at most */
#define DELBUFMAX 10000

/* index(page) file size limit */
#define FIDXMAXSIZE (16 * MB)
/* data file size limit */
#define FDATMAXSIZE (128 * MB)
/* all meta information are regulated to fix size */
#define FILEMETASIZE 64
/* the file opened simultaneously limit, managed by LRU */
#define MAXFD 16384
#define MAX_PATH_LEN 255

#define FILEMAGICHEADER "CuTtDbFiLePaRtIaL"
#define FILEMAGICLEN (strlen(FILEMAGICHEADER))
/* page or data records are stored at aligned offset */
#define ALIGNBYTES 16

/* virtual offset(48bits) transform into real offset(fid,offset) */
#define VOFF2ROFF(off, fid, roff) do{fid = (off).i4 >> 8; \
    roff = ((off).i4 & 0xff) << 16; roff = (roff | (off).i2) * ALIGNBYTES;}while(0)

/* real offset transform into virtual offset */
#define ROFF2VOFF(fid, roff, off) do{(off).i4 = fid << 8; \
    (off).i4 |= (roff / ALIGNBYTES) >> 16; (off).i2 = (roff / ALIGNBYTES) & 0xffff;} while(0)

/* align to a integer offset */
#define OFFALIGNED(off) ((((off)-1) | (ALIGNBYTES - 1)) + 1)

/* used in fd LRU-cached, distinguish index or data files' fd */
#define VFIDIDX(fid) (fid * 2)
#define VFIDDAT(fid) (fid * 2 + 1)

/* how often write out buffered data */
#define FLUSHTIMEOUT 5
/* how often to check if index file needs space recycle */
#define RCYLEPAGEINTERVAL 60
/* how often to check if data file needs space recycle */
#define RCYLEDATAINTERVAL 120
/* data file space recycle check interval factor (seconds per data file/128MB)*/
#define DATARCYLECHECKFACTOR 1800


/* three type of file */
enum {
    /* random value */
    VIOAPND2_INDEX = 0x97,
    VIOAPND2_DATA = 0x98,
    VIOAPND2_DELLOG = 0x99,
};


/* where the record comes from when calling writerec */
enum {
    VIOAPND2_RECEXTERNAL = 0,
    VIOAPND2_RECINTERNAL = 1,
};


/* a file is writing or full? */
enum {
    VIOAPND2_WRITING = 0,
    VIOAPND2_FULL = 1,
};

/* signature in the header file, indicates it's open or be safety closed */
enum {
    /* any number doens't matter */
    VIOAPND2_SIGOPEN = 2,
    VIOAPND2_SIGCLOSED = 3,
};


/* buffer for IO */
typedef struct {
    uint32_t limit;
    uint32_t off;
    uint32_t pos;
    uint32_t fid;
    uint64_t oid;
    int fd;
    char buf[IOBUFSIZE];
} VIOAPND2IOBUF;


/* file information for every file */
typedef struct VIOAPND2FINFO {
    /* fid */
    uint32_t fid;
    /* first oid */
    uint64_t oidf;
    /* last oid */
    uint64_t oidl;
    
    /* next file */
    struct VIOAPND2FINFO *fnext;
    /* prev file */
    struct VIOAPND2FINFO *fprev;

    uint32_t fsize;
    /* junk space */
    uint32_t rcyled;
    /* nearest expire time */
    uint32_t nexpire;
    /* last time for recycle check */
    uint32_t lcktime;
    /* index page file or data file? */
    uint8_t ftype;
    /* writing or full? */
    uint8_t fstatus;
    /* ref count, avoid unlink failure */
    uint32_t ref;
    /* whether unlink the file after dereference */
    bool unlink;
} VIOAPND2FINFO;


typedef struct {
    /* a new db? */
    bool create;
    /* fd number limit */
    int maxfds;
    /* opened files' fds cache */
    CDBHASHTABLE *fdcache;

    /* number of data file */
    uint32_t dfnum;
    /* number of index file */
    uint32_t ifnum;

    /* Buffers */
    VIOAPND2IOBUF dbuf;
    VIOAPND2IOBUF ibuf;
    FOFF delbuf[DELBUFMAX];
    int delbufpos;

    /* db path */
    char *filepath;


    /* file information of index files */
    CDBHASHTABLE *idxmeta;
    VIOAPND2FINFO *idxfhead;
    VIOAPND2FINFO *idxftail;
    /* file information of data files */
    CDBHASHTABLE *datmeta;
    VIOAPND2FINFO *datfhead;
    VIOAPND2FINFO *datftail;

    /* fd for db header */
    int hfd;
    /* fd for files meta header */
    int mfd;
    /* fd for deletion log */
    int dfd;

    /* lock for all I/O operation */
    CDBLOCK *lock;

    int idxitfid;
    uint32_t idxitoff;
    char *idxmmap;

} VIOAPND2;


/* iterator for index/data */
typedef struct {
    /* current open fd */
    int fd;
    /* current offset in file*/
    uint32_t off;
    /* current operation id */
    uint64_t oid;
    /* current file size*/
    uint64_t fsize;
    /* mapped of file */
    char *mmap;
    /* reference of filemeta struct */
    VIOAPND2FINFO *finfo;
} VIOAPND2ITOR;


static int _vio_apnd2_open(CDBVIO *vio, const char *filepath, int flags);
static int _vio_apnd2_checkpid(CDBVIO *vio);
static int _vio_apnd2_write(CDBVIO *vio, int fd, void *buf, uint32_t size, bool aligned);
static int _vio_apnd2_read(CDBVIO *vio, int fd, void *buf, uint32_t size, uint64_t off);
static int _vio_apnd2_readmeta(CDBVIO *vio, bool overwrite);
static int _vio_apnd2_writemeta(CDBVIO *vio);
static int _vio_apnd2_close(CDBVIO *vio);
static int _vio_apnd2_writerec(CDBVIO *vio, CDBREC *rec, FOFF *off, int ptrtype);
static int _vio_apnd2_writerecexternal(CDBVIO *vio, CDBREC *rec, FOFF *off);
static int _vio_apnd2_writerecinternal(CDBVIO *vio, CDBREC *rec, FOFF *off);
static int _vio_apnd2_deleterec(CDBVIO *vio, CDBREC *rec, FOFF off);
static int _vio_apnd2_readrec(CDBVIO *vio, CDBREC** rec, FOFF off, bool readval);
static int _vio_apnd2_writepage(CDBVIO *vio, CDBPAGE *page, FOFF *off);
static int _vio_apnd2_readpage(CDBVIO *vio, CDBPAGE **page, FOFF off);
static int _vio_apnd2_sync(CDBVIO *vio);
static int _vio_apnd2_writehead2(CDBVIO *vio);
static int _vio_apnd2_writehead(CDBVIO *vio, bool wtable);
static int _vio_apnd2_readhead2(CDBVIO *vio);
static int _vio_apnd2_readhead(CDBVIO *vio, bool rtable);
static int _vio_apnd2_writefmeta(CDBVIO *vio, int fd, VIOAPND2FINFO *finfo);
static int _vio_apnd2_readfmeta(CDBVIO *vio, int fd, VIOAPND2FINFO *finfo);
static int _vio_apnd2_flushbuf(CDBVIO *vio, int dtype);
static void _vio_apnd2_flushtask(void *arg);
static void _vio_apnd2_rcyledataspacetask(void *arg);
static void _vio_apnd2_fixcachepageooff(CDB *db, uint32_t bit, FOFF off);
static void _vio_apnd2_rcylepagespacetask(void *arg);
static int _vio_apnd2_shiftnew(CDBVIO *vio, int dtype);
static int _vio_apnd2_recovery(CDBVIO *vio, bool force);
static void _vio_apnd2_unlink(CDBVIO *vio, VIOAPND2FINFO *finfo, int dtype);
static VIOAPND2FINFO* _vio_apnd2_fileiternext(CDBVIO *vio, int dtype, uint64_t oid);
static int _vio_apnd2_iterfirst(CDBVIO *vio, VIOAPND2ITOR *it, int dtype, int64_t oid);
static int _vio_apnd2_iterfree(CDBVIO *vio, int dtype, VIOAPND2ITOR *it);
static int _vio_apnd2_pageiternext(CDBVIO *vio, CDBPAGE **page, void *iter);
static int _vio_apnd2_reciternext(CDBVIO *vio, CDBREC **rec, void *iter);
static void* _vio_apnd2_reciterfirst(CDBVIO *vio, uint64_t oid);
static void* _vio_apnd2_pageiterfirst(CDBVIO *vio, uint64_t oid);
static void _vio_apnd2_reciterdestory(CDBVIO *vio, void *iter);
static void _vio_apnd2_pageiterdestory(CDBVIO *vio, void *iter);
static void _vio_apnd2_cleanpoint(CDBVIO *vio);
static int _vio_apnd2_cmpfuncsreorder(const void *p1, const void *p2);
static int _vio_apnd2_checkopensig(CDBVIO *vio);
static int _vio_apnd2_setopensig(CDBVIO *vio, int sig);
static int _vio_apnd2_rcyledatafile(CDBVIO *vio, VIOAPND2FINFO *finfo, bool rcyle);


/* hook the io methods */
void vio_apnd2_init(CDBVIO *vio)
{
    vio->close = _vio_apnd2_close;
    vio->open = _vio_apnd2_open;
    vio->rpage = _vio_apnd2_readpage;
    vio->wpage = _vio_apnd2_writepage;
    vio->rrec = _vio_apnd2_readrec;
    vio->drec = _vio_apnd2_deleterec;
    vio->wrec = _vio_apnd2_writerecexternal;
    vio->sync = _vio_apnd2_sync;
    vio->rhead = _vio_apnd2_readhead2;
    vio->whead = _vio_apnd2_writehead2;
    vio->cleanpoint = _vio_apnd2_cleanpoint;
    vio->pageitfirst = _vio_apnd2_pageiterfirst;
    vio->pageitnext = _vio_apnd2_pageiternext;
    vio->pageitdestroy = _vio_apnd2_pageiterdestory;
    vio->recitfirst = _vio_apnd2_reciterfirst;
    vio->recitnext = _vio_apnd2_reciternext;
    vio->recitdestroy = _vio_apnd2_reciterdestory;
}

/* the hash table used in VIOAPND2 need not rehash, just use the key id is OK */
static uint32_t _directhash(const void *key, int size) 
{
    return *(uint32_t*)key;
}


/* allocate a new VIOAPND2 object, called when open db */
static void _vio_apnd2_new(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)malloc(sizeof(VIOAPND2));

    myio->dfnum = myio->ifnum = 0;

    myio->dbuf.fid = 0;
    myio->dbuf.pos = 0;
    myio->dbuf.off = 0;
    myio->dbuf.oid = 0;
    memset(myio->dbuf.buf, 0, IOBUFSIZE);
    myio->idxfhead = NULL;
    myio->idxftail = NULL;

    myio->ibuf.fid = 0;
    myio->ibuf.pos = 0;
    myio->ibuf.off = 0;
    myio->ibuf.oid = 0;
    memset(myio->ibuf.buf, 0, IOBUFSIZE);
    myio->datfhead = NULL;
    myio->datftail = NULL;
    
    myio->delbufpos = 0;

    myio->ifnum = 0;
    myio->dfnum = 0;
    
    myio->mfd = -1;
    myio->hfd = -1;
    myio->dfd = -1;

    myio->fdcache = cdb_ht_new(true, _directhash);
    /* the following two are look-up table, need not LRU */
    myio->idxmeta = cdb_ht_new(false, _directhash);
    myio->datmeta = cdb_ht_new(false, _directhash);

    myio->lock = cdb_lock_new(CDB_LOCKMUTEX);

    myio->create = true;
    myio->maxfds = MAXFD;
    myio->filepath = NULL;

    vio->iometa = myio;
}


/* free a VIOAPND2 object, called when close db */
static void _vio_apnd2_destroy(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    cdb_ht_destroy(myio->fdcache);
    cdb_ht_destroy(myio->idxmeta);
    cdb_ht_destroy(myio->datmeta);
    cdb_lock_destory(myio->lock);
    if (myio->filepath)
        free(myio->filepath);
    free(myio);
    vio->iometa = NULL;
}

/* check if another process has already open the current db */
static int _vio_apnd2_checkpid(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    struct stat st;
    char filename[MAX_PATH_LEN] = {0};
    char syspidpath[MAX_PATH_LEN] = {0};
    snprintf(filename, MAX_PATH_LEN, "%s/pid.cdb", myio->filepath);

    if (stat(filename, &st) == 0) {
        /* pid file exist */
        FILE *f = fopen(filename, "rt");
        int pid = -1;
        if (f == NULL) {
            cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
            return -1;
        }

        int ret = fscanf(f, "%d", &pid);
        fclose(f);
        if (ret != 1) {
            cdb_seterrno(vio->db, CDB_PIDEXIST, __FILE__, __LINE__);
            return -1;
        }

        /* check if the process still alive */
        snprintf(syspidpath, MAX_PATH_LEN, "/proc/%d", pid);
        if (stat(syspidpath, &st) == 0) {
            cdb_seterrno(vio->db, CDB_PIDEXIST, __FILE__, __LINE__);
            return -1;
        }
    } 

    /* pid file non-exist or obsoleted */
    FILE *f = fopen(filename, "wt");
    if (f == NULL) {
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        return -1;
    }
    fprintf(f, "%d\n", getpid());
    fclose(f);
    return 0;
}

/* open an db by path and mode */
static int _vio_apnd2_open(CDBVIO *vio, const char *filepath, int flags)
{
    int rflags = O_RDWR;
    char filename[MAX_PATH_LEN] = {0};
    int fsize;
    int sigstatus;
    VIOAPND2 *myio;

    _vio_apnd2_new(vio);
    myio = (VIOAPND2 *)vio->iometa;
    myio->filepath = strdup(filepath);

    if (flags & CDB_CREAT)
        rflags |= O_CREAT;
    if (flags & CDB_TRUNC)
        rflags |= O_TRUNC;

    if (_vio_apnd2_checkpid(vio) < 0) {
        goto ERRRET;
    }

    snprintf(filename, MAX_PATH_LEN, "%s/mainindex.cdb", myio->filepath);
    myio->hfd = open(filename, rflags, 0644);
    if (myio->hfd < 0 && errno == ENOENT && (rflags & O_CREAT)) {
        /* try to create, but path not exists */
        cdb_seterrno(vio->db, CDB_DIRNOEXIST, __FILE__, __LINE__);
        goto ERRRET;
    } else if (myio->hfd < 0) {
        /* other open error */
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        goto ERRRET;
    }
    
    fsize = lseek(myio->hfd, 0, SEEK_END);
    if (fsize) {
        myio->create = false;
        sigstatus = _vio_apnd2_checkopensig(vio);
        if (sigstatus < 0) {
            /* main table read error */
            cdb_seterrno(vio->db, CDB_READERR, __FILE__, __LINE__);
            goto ERRRET;
        }
    } else {
        sigstatus = VIOAPND2_SIGCLOSED;
    }
    
    /* */
    struct stat st;
    snprintf(filename, MAX_PATH_LEN, "%s/force_recovery", myio->filepath);
    if (stat(filename, &st) == 0) {
        /* special file exist, force recovery to fix the database */
        _vio_apnd2_recovery(vio, true);
        unlink(filename);
    }  else if (sigstatus == VIOAPND2_SIGOPEN) {
        /* didn't properly closed last time */
        _vio_apnd2_recovery(vio, false);
    } else if (sigstatus != VIOAPND2_SIGCLOSED) {
        cdb_seterrno(vio->db, CDB_DATAERRMETA, __FILE__, __LINE__);
        goto ERRRET;
    }

    if (_vio_apnd2_setopensig(vio, VIOAPND2_SIGOPEN) < 0) {
        cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
        goto ERRRET;
    }

    snprintf(filename, MAX_PATH_LEN, "%s/mainmeta.cdb", myio->filepath);
    myio->mfd = open(filename, rflags, 0644);
    if (myio->mfd < 0) {
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        goto ERRRET;
    }
    
    fsize = lseek(myio->mfd, 0, SEEK_END);
    if (fsize) {
        /* exist database */
        _vio_apnd2_readmeta(vio, false);

        /* open current data file and index file for buffer */
        snprintf(filename, MAX_PATH_LEN, "%s/idx%08d.cdb", myio->filepath, myio->ibuf.fid);
        myio->ibuf.fd = open(filename, rflags, 0644);
        myio->ibuf.limit = CDBMIN(IOBUFSIZE, FIDXMAXSIZE - myio->ibuf.off);
        myio->ibuf.pos = 0;

        snprintf(filename, MAX_PATH_LEN, "%s/dat%08d.cdb", myio->filepath, myio->dbuf.fid);
        myio->dbuf.fd = open(filename, rflags, 0644);
        myio->dbuf.limit = CDBMIN(IOBUFSIZE, FDATMAXSIZE - myio->dbuf.off);
        myio->dbuf.pos = 0;
    } else {
        /* new database */
        myio->create = true;
        /* remember the bnum */
        _vio_apnd2_writehead(vio, false);
        _vio_apnd2_shiftnew(vio, VIOAPND2_INDEX);
        _vio_apnd2_shiftnew(vio, VIOAPND2_DATA);
    }
    
    snprintf(filename, MAX_PATH_LEN, "%s/dellog.cdb", myio->filepath);
    myio->dfd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (myio->dfd < 0) {
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        goto ERRRET;
    }

    /* set background tasks, flush buffer and recycle space */
    cdb_bgtask_add(vio->db->bgtask, _vio_apnd2_flushtask, vio, FLUSHTIMEOUT);
    cdb_bgtask_add(vio->db->bgtask, _vio_apnd2_rcylepagespacetask, vio, RCYLEPAGEINTERVAL);
    cdb_bgtask_add(vio->db->bgtask, _vio_apnd2_rcyledataspacetask, vio, RCYLEDATAINTERVAL);
    return 0;

ERRRET:
    if (myio->mfd > 0)
        close(myio->mfd);
    if (myio->hfd > 0)
        close(myio->hfd);
    if (myio->dfd > 0)
        close(myio->dfd);
    _vio_apnd2_destroy(vio);
    return -1;
}


/* task for flush buffer */
static void _vio_apnd2_flushtask(void *arg)
{
    CDBVIO *vio = (CDBVIO *)arg;
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    cdb_lock_lock(myio->lock);
    _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
    _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
    _vio_apnd2_flushbuf(vio, VIOAPND2_DELLOG);
    cdb_lock_unlock(myio->lock);
}


/* read information for db files, 'overwrite' indicates recovery */
static int _vio_apnd2_readmeta(CDBVIO *vio, bool overwrite)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    char buf[FILEMETASIZE];
    char *hbuf;
    int hbufsize;
    int pos = 0;

    if (pread(myio->mfd, buf, FILEMETASIZE, 0) != FILEMETASIZE) {
        if (overwrite)
            return 0;
        cdb_seterrno(vio->db, CDB_READERR, __FILE__, __LINE__);
        return -1;
    }

    if (memcmp(buf, FILEMAGICHEADER, FILEMAGICLEN) != 0) {
        cdb_seterrno(vio->db, CDB_DATAERRMETA, __FILE__, __LINE__);
        return -1;
    }

    pos += FILEMAGICLEN;
    cdb_lock_lock(myio->lock);
    if (!overwrite)
        myio->ibuf.off = *(uint32_t*)(buf + pos); 
    pos += SI4;
    myio->ibuf.limit = *(uint32_t*)(buf + pos); 
    pos += SI4;
    if (!overwrite)
        myio->dbuf.off = *(uint32_t*)(buf + pos); 
    pos += SI4;
    myio->dbuf.limit = *(uint32_t*)(buf + pos); 
    pos += SI4;
    if (!overwrite)
        myio->ifnum = *(uint32_t*)(buf + pos); 
    pos += SI4;
    if (!overwrite)
        myio->dfnum = *(uint32_t*)(buf + pos); 
    pos += SI4;
    if (!overwrite)
        myio->ibuf.fid = *(uint32_t*)(buf + pos); 
    pos += SI4;
    if (!overwrite)
        myio->dbuf.fid = *(uint32_t*)(buf + pos); 
    pos += SI4;

    hbufsize = (SI4 + SI4 + SI4 + SI8 + SI8 + 1 + 1) * myio->ifnum;
    hbufsize += (SI4 + SI4 + SI4 + SI4 + SI8 + SI8 + 1 + 1) * myio->dfnum;
    hbuf = (char*)malloc(hbufsize);
    pos = 0;

    if (pread(myio->mfd, hbuf, hbufsize, FILEMETASIZE) != hbufsize) {
        cdb_lock_unlock(myio->lock);
        free(hbuf);
        if (overwrite)
            return 0;
        cdb_seterrno(vio->db, CDB_READERR, __FILE__, __LINE__);
        return -1;
    }

    for(int i = 0; i < myio->ifnum; i++) {
        VIOAPND2FINFO finfo, *finfo2;
        finfo.fid = *(uint32_t*)(hbuf + pos); 
        pos += SI4;
        finfo.fsize = *(uint32_t*)(hbuf + pos); 
        pos += SI4;
        finfo.rcyled = *(uint32_t*)(hbuf + pos); 
        pos += SI4;;
        finfo.oidf = *(uint64_t*)(hbuf + pos); 
        pos += SI8;
        finfo.oidl = *(uint64_t*)(hbuf + pos); 
        pos += SI8;
        finfo.fstatus = *(uint8_t*)(hbuf + pos); 
        pos += 1;
        finfo.ftype = *(uint8_t*)(hbuf + pos); 
        pos += 1;
        finfo.ref = 0;
        finfo.unlink = false;
        if (overwrite) {
            /* in recovery mode only fix 'recycled size' */
            /* But do nothing with index files */
            continue;
        }
        finfo2 = (VIOAPND2FINFO *)cdb_ht_insert2(myio->idxmeta, &finfo.fid, SI4, &finfo, sizeof(finfo));
        if (myio->idxfhead) {
            finfo2->fprev = myio->idxftail;
            myio->idxftail->fnext = finfo2;
            finfo2->fnext = NULL;
            myio->idxftail = finfo2;
        } else {
            myio->idxfhead = myio->idxftail = finfo2;
            finfo2->fprev = finfo2->fnext = NULL;
        }
    }

    for(int i = 0; i < myio->dfnum; i++) {
        VIOAPND2FINFO finfo, *finfo2;
        finfo.fid = *(uint32_t*)(hbuf + pos); 
        pos += SI4;
        finfo.fsize = *(uint32_t*)(hbuf + pos); 
        pos += SI4;
        finfo.rcyled = *(uint32_t*)(hbuf + pos); 
        pos += SI4;
        finfo.nexpire = *(uint32_t*)(hbuf + pos); 
        pos += SI4;
        finfo.oidf = *(uint64_t*)(hbuf + pos); 
        pos += SI8;
        finfo.oidl = *(uint64_t*)(hbuf + pos); 
        pos += SI8;
        finfo.fstatus = *(uint8_t*)(hbuf + pos); 
        pos += 1;
        finfo.ftype = *(uint8_t*)(hbuf + pos); 
        pos += 1;
        finfo.ref = 0;
        finfo.unlink = false;
        finfo.lcktime = time(NULL);
        if (overwrite) {
            /* in recovery mode only fix 'recycled size' */
            finfo2 = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &finfo.fid, SI4, false);
            if (finfo2) {
                finfo2->rcyled = finfo.rcyled;
                finfo2->nexpire = finfo.nexpire;
            }
            continue;
        }
        finfo2 = (VIOAPND2FINFO *)cdb_ht_insert2(myio->datmeta, &finfo.fid, SI4, &finfo, sizeof(finfo));
        if (myio->datfhead) {
            finfo2->fprev = myio->datftail;
            myio->datftail->fnext = finfo2;
            finfo2->fnext = NULL;
            myio->datftail = finfo2;
        } else {
            myio->datfhead = myio->datftail = finfo2;
            finfo2->fprev = finfo2->fnext = NULL;
        }
    }
    cdb_lock_unlock(myio->lock);
    free(hbuf);

    return 0;
}


/* flush i/o buffer */
static int _vio_apnd2_flushbuf(CDBVIO *vio, int dtype)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    VIOAPND2FINFO *finfo;
    VIOAPND2IOBUF *iobuf;
    CDBHASHTABLE *ht;
    uint32_t *fid;
    uint32_t fsizemax;

    /* link to the proper operation object */
    if (dtype == VIOAPND2_INDEX) {
        iobuf = &myio->ibuf;
        ht = myio->idxmeta;
        fsizemax = FIDXMAXSIZE;
    } else if (dtype == VIOAPND2_DATA) {
        iobuf = &myio->dbuf;
        ht = myio->datmeta;
        fsizemax = FDATMAXSIZE;
    } else if (dtype == VIOAPND2_DELLOG) {
        /* buffer for deletion is special */
        if (myio->delbufpos == 0)
            return 0;
        if (write(myio->dfd, myio->delbuf, sizeof(FOFF) * myio->delbufpos)
                != sizeof(FOFF) * myio->delbufpos) {
            cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
            return -1;
        }
        myio->delbufpos = 0;
        return 0;
    } else {
        cdb_seterrno(vio->db, CDB_INTERNALERR, __FILE__, __LINE__);
        return -1;
    }
    fid = &iobuf->fid;

    /* get information from table */
    finfo = (VIOAPND2FINFO *)cdb_ht_get2(ht, fid, SI4, false);
    if (finfo == NULL) {
        cdb_seterrno(vio->db, CDB_INTERNALERR, __FILE__, __LINE__);
        return -1;
    }

    /* write out if buffered */
    if (iobuf->pos > 0) {
        if (pwrite(iobuf->fd, iobuf->buf, iobuf->pos, iobuf->off) != iobuf->pos) {
            /* to avoid compile warning */
            if (ftruncate(iobuf->fd, iobuf->off) < 0) ;
            cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
            return -1;
        }
    }

    /* mark the operation id */
    finfo->oidl = iobuf->oid;

    /* reset the buffer information */
    iobuf->pos = 0;
    iobuf->off = lseek(iobuf->fd, 0, SEEK_END);
    /* fix file size info whenever possible */
    finfo->fsize = iobuf->off;
    iobuf->off = OFFALIGNED(iobuf->off);

    /* current writing file nearly full? open a new one */
    if (iobuf->off > fsizemax - 16 * KB) {
        finfo->fstatus = VIOAPND2_FULL;
        _vio_apnd2_writefmeta(vio, iobuf->fd, finfo);
        close(iobuf->fd);
        _vio_apnd2_shiftnew(vio, dtype);
    } else
        iobuf->limit = CDBMIN(IOBUFSIZE, fsizemax - iobuf->off) ;

    return 0;
}

/* create a new file for buffer and writing */
static int _vio_apnd2_shiftnew(CDBVIO *vio, int dtype)
{
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    VIOAPND2IOBUF *iobuf;
    CDBHASHTABLE *ht;
    uint32_t *fnum;
    uint32_t tryiter, curfid;
    char filename[MAX_PATH_LEN];
    char ipfx[] = "idx";
    char dpfx[] = "dat";
    char *pfx;

    /* link to proper object by dtype */
    if (dtype == VIOAPND2_INDEX) {
        iobuf = &myio->ibuf;
        ht = myio->idxmeta;
        fnum = &myio->ifnum;
        pfx = ipfx;
    } else if (dtype == VIOAPND2_DATA) {
        iobuf = &myio->dbuf;
        ht = myio->datmeta;
        fnum = &myio->dfnum;
        pfx = dpfx;
    } else {
        cdb_seterrno(vio->db, CDB_INTERNALERR, __FILE__, __LINE__);
        return -1;
    }

    curfid = iobuf->fid;

    /* reset invalid buffer, prevent for misuse */
    iobuf->fd = -1;
    iobuf->fid = 0xffffff;
    iobuf->limit = iobuf->pos = iobuf->off = 0xffffffff;

    /* find a valid fid, try 16M times at most */
    tryiter = 0;
    while(cdb_ht_exist(ht, &curfid, SI4)) {
        curfid++;
        tryiter++;
        if (tryiter == 0xffffff) {
            cdb_seterrno(vio->db, CDB_NOFID, __FILE__, __LINE__);
            return -1;
        }
        if (curfid == 0xffffff) 
            curfid = 0;
    }

    /* open new file */
    snprintf(filename, MAX_PATH_LEN, "%s/%s%08d.cdb", myio->filepath, pfx, curfid);
    iobuf->fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (iobuf->fd < 0) {
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        return -1;
    }
    iobuf->limit = IOBUFSIZE;
    iobuf->fid = curfid;
    iobuf->off = FILEMETASIZE;
    iobuf->pos = 0;

    /* set meta information for new file */
    VIOAPND2FINFO finfo, *finfo2;
    finfo.fsize = lseek(iobuf->fd, 0, SEEK_END);
    finfo.oidf = iobuf->oid;
    finfo.oidl = iobuf->oid;
    finfo.rcyled = 0;
    finfo.lcktime = time(NULL);
    finfo.fstatus = VIOAPND2_WRITING;
    finfo.ftype = dtype;
    finfo.fid = curfid;
    finfo.unlink = false;
    finfo.nexpire = 0xffffffff;
    finfo.ref = 0;
    /* meta information also be written to disk immediately */
    if (_vio_apnd2_writefmeta(vio, iobuf->fd, &finfo) < 0) {
        close(iobuf->fd);
        iobuf->fd = -1;
        iobuf->fid = 0xffffff;
        iobuf->limit = iobuf->pos = iobuf->off = 0xffffffff;
        return -1;
    }
    (*fnum)++;
    finfo2 = cdb_ht_insert2(ht, &curfid, SI4, &finfo, sizeof(VIOAPND2FINFO));
    if (dtype == VIOAPND2_INDEX) {
        if (myio->idxfhead) {
            finfo2->fprev = myio->idxftail;
            myio->idxftail->fnext = finfo2;
            finfo2->fnext = NULL;
            myio->idxftail = finfo2;
        } else {
            myio->idxfhead = myio->idxftail = finfo2;
            finfo2->fprev = finfo2->fnext = NULL;
        }
    } else if (dtype == VIOAPND2_DATA) {
        if (myio->datfhead) {
            finfo2->fprev = myio->datftail;
            myio->datftail->fnext = finfo2;
            finfo2->fnext = NULL;
            myio->datftail = finfo2;
        } else {
            myio->datfhead = myio->datftail = finfo2;
            finfo2->fprev = finfo2->fnext = NULL;
        }
    }

    return 0;
}


/* write a single file's meta information */
static int _vio_apnd2_writefmeta(CDBVIO *vio, int fd, VIOAPND2FINFO *finfo)
{
    char buf[FILEMETASIZE];
    int pos = 0;

    memset(buf, 'X', FILEMETASIZE);
    memcpy(buf, FILEMAGICHEADER, FILEMAGICLEN);
    pos += FILEMAGICLEN;
    *(uint64_t*)(buf + pos) = finfo->oidf;
    pos += SI8;
    *(uint64_t*)(buf + pos) = finfo->oidl;
    pos += SI8;
    *(uint32_t*)(buf + pos) = finfo->fsize;
    pos += SI4;
    *(uint32_t*)(buf + pos) = finfo->fid;
    pos += SI4;
    *(uint8_t*)(buf + pos) = finfo->fstatus;
    pos++;
    *(uint8_t*)(buf + pos) = finfo->ftype;
    pos++;

    if (pwrite(fd, buf, FILEMETASIZE, 0) != FILEMETASIZE) {
        cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
        return -1;
    }
    return 0;
}

/* read a single file's meta information */
static int _vio_apnd2_readfmeta(CDBVIO *vio, int fd, VIOAPND2FINFO *finfo)
{
    char buf[FILEMETASIZE];
    int pos = 0;

    memset(buf, 'X', FILEMETASIZE);
    if (pread(fd, buf, FILEMETASIZE, 0) != FILEMETASIZE) {
        cdb_seterrno(vio->db, CDB_READERR, __FILE__, __LINE__);
        return -1;
    }

    if (memcmp(buf, FILEMAGICHEADER, FILEMAGICLEN)) {
        cdb_seterrno(vio->db, CDB_DATAERRMETA, __FILE__, __LINE__);
        return -1;
    }

    pos += FILEMAGICLEN;
    finfo->oidf = *(uint64_t*)(buf + pos);
    pos += SI8;
    finfo->oidl = *(uint64_t*)(buf + pos);
    pos += SI8;
    finfo->fsize = *(uint32_t*)(buf + pos);
    pos += SI4;
    finfo->fid = *(uint32_t*)(buf + pos);
    pos += SI4;
    finfo->fstatus = *(uint8_t*)(buf + pos);
    pos++;
    finfo->ftype  = *(uint8_t*)(buf + pos);
    pos++;
    return 0;
}


/* write to disk directly instead of using buffer(Only Appends) */
static int _vio_apnd2_write(CDBVIO *vio, int fd, void *buf, uint32_t size, bool aligned)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    uint32_t off;

    if (size == 0)
        return 0;

    off = lseek(fd, 0, SEEK_END);
    if (aligned)
        off = OFFALIGNED(off);
    if (pwrite(fd, buf, size, off) != size) {
        /* to avoid compile warning */
        if (ftruncate(myio->ibuf.fd, off) < 0) ;
        cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
        return -1;
    }

    return size;
}


/* read from disk; if data has not been written, read from buffer */
static int _vio_apnd2_read(CDBVIO *vio, int fd, void *buf, uint32_t size, uint64_t off)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    int ret;

    /* in buffer? */
    if (fd == myio->dbuf.fd && off >= myio->dbuf.off) {
        uint64_t boff = off - myio->dbuf.off;
        ret = CDBMIN(size, myio->dbuf.pos - boff);
        memcpy(buf, myio->dbuf.buf + boff, ret);
    } else if (fd == myio->ibuf.fd && off >= myio->ibuf.off) {
        uint64_t boff = off - myio->ibuf.off;
        ret = CDBMIN(size, myio->ibuf.pos - boff);
        memcpy(buf, myio->ibuf.buf + boff, ret);
    } else {
        /* not in buffer */
        ret = pread(fd, buf, size, off);
        if (ret < 0) {
            cdb_seterrno(vio->db, CDB_READERR, __FILE__, __LINE__);
            return -1;
        }
    }
    return ret;
}


/* write all files meta information into a file */
static int _vio_apnd2_writemeta(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    char buf[FILEMETASIZE];
    char *hbuf;
    int hbufsize;
    int pos = 0;

    memset(buf, 'X', FILEMETASIZE);
    memcpy(buf, FILEMAGICHEADER, FILEMAGICLEN);
    pos += FILEMAGICLEN;
    cdb_lock_lock(myio->lock);
    *(uint32_t*)(buf + pos) = myio->ibuf.off;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->ibuf.limit;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->dbuf.off;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->dbuf.limit;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->ifnum;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->dfnum;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->ibuf.fid;
    pos += SI4;
    *(uint32_t*)(buf + pos) = myio->dbuf.fid;
    pos += SI4;

    hbufsize = (SI4 + SI4 + SI4 + SI8 + SI8 + 1 + 1) * myio->ifnum;
    hbufsize += (SI4 + SI4 + SI4 + SI4 + SI8 + SI8 + 1 + 1) * myio->dfnum;
    hbuf = (char*)malloc(hbufsize);
    memset(hbuf, 'X', hbufsize);
    pos = 0;
    /* iterate all the index files order by oid */
    VIOAPND2FINFO *finfo = myio->idxfhead;
    while(finfo != NULL) {
        *(uint32_t*)(hbuf + pos) = finfo->fid;
        pos += 4;
        *(uint32_t*)(hbuf + pos) = finfo->fsize;
        pos += 4;
        *(uint32_t*)(hbuf + pos) = finfo->rcyled;
        pos += 4;
        *(uint64_t*)(hbuf + pos) = finfo->oidf;
        pos += 8;
        *(uint64_t*)(hbuf + pos) = finfo->oidl;
        pos += 8;
        *(uint8_t*)(hbuf + pos) = finfo->fstatus;
        pos += 1;
        *(uint8_t*)(hbuf + pos) = finfo->ftype;
        pos += 1;
        finfo = finfo->fnext;
    }

    /* iterate all the data files order by oid */
    finfo = myio->datfhead;
    while(finfo != NULL) {
        *(uint32_t*)(hbuf + pos) = finfo->fid;
        pos += 4;
        *(uint32_t*)(hbuf + pos) = finfo->fsize;
        pos += 4;
        *(uint32_t*)(hbuf + pos) = finfo->rcyled;
        pos += 4;
        *(uint32_t*)(hbuf + pos) = finfo->nexpire;
        pos += 4;
        *(uint64_t*)(hbuf + pos) = finfo->oidf;
        pos += 8;
        *(uint64_t*)(hbuf + pos) = finfo->oidl;
        pos += 8;
        *(uint8_t*)(hbuf + pos) = finfo->fstatus;
        pos += 1;
        *(uint8_t*)(hbuf + pos) = finfo->ftype;
        pos += 1;
        finfo = finfo->fnext;
    }
    cdb_lock_unlock(myio->lock);

    if (pwrite(myio->mfd, buf, FILEMETASIZE, 0) != FILEMETASIZE) {
        cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
        free(hbuf);
        return -1;
    }

    if (pwrite(myio->mfd, hbuf, hbufsize, FILEMETASIZE) != hbufsize) {
        cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
        free(hbuf);
        return -1;
    }
    free(hbuf);

    return 0;
}


/* close db */
static int _vio_apnd2_close(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDBHTITEM *item;
    char filename[MAX_PATH_LEN] = {0};
    VIOAPND2FINFO *finfo;

    /* flush buffer */
    _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
    finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->idxmeta, &myio->ibuf.fid, SI4, false);
    if (finfo)
        _vio_apnd2_writefmeta(vio, myio->ibuf.fd, finfo);
    _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
    finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &myio->dbuf.fid, SI4, false);
    if (finfo)
        _vio_apnd2_writefmeta(vio, myio->dbuf.fd, finfo);

    /* iterate and close the fd cache */
    item = cdb_ht_iterbegin(myio->fdcache);
    while(item != NULL) {
        close(*(int*)cdb_ht_itemval(myio->fdcache, item));
        item = cdb_ht_iternext(myio->fdcache, item);
    }

    if (myio->dbuf.fd > 0)
        close(myio->dbuf.fd);
    if (myio->ibuf.fd > 0)
        close(myio->ibuf.fd);

    /* rewrite the metafile */
    _vio_apnd2_writemeta(vio);
    /* close all open files */
    snprintf(filename, MAX_PATH_LEN, "%s/pid.cdb", myio->filepath);
    unlink(filename);
    /* dellog only be useful for recovery of database unsafety close */
    snprintf(filename, MAX_PATH_LEN, "%s/dellog.cdb", myio->filepath);
    unlink(filename);
    _vio_apnd2_setopensig(vio, VIOAPND2_SIGCLOSED);
    if (myio->hfd > 0)
        close(myio->hfd);
    if (myio->mfd > 0)
        close(myio->mfd);
    if (myio->dfd > 0)
        close(myio->dfd);
    _vio_apnd2_destroy(vio);
    return 0;
}


/* open a file, and remember its fd. The function runs under lock protection */
static int _vio_apnd2_loadfd(CDBVIO *vio, uint32_t fid, int dtype)
{
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    int fd;
    char filename[MAX_PATH_LEN];
    char ipfx[] = "idx";
    char dpfx[] = "dat";
    char *pfx;
    uint32_t vfid;

    if (dtype == VIOAPND2_INDEX) {
        pfx = ipfx;
        vfid = VFIDIDX(fid);
    } else if (dtype == VIOAPND2_DATA) {
        pfx = dpfx;
        vfid = VFIDDAT(fid);
    } else {
        cdb_seterrno(vio->db, CDB_INTERNALERR, __FILE__, __LINE__);
        return -1;
    }

    snprintf(filename, MAX_PATH_LEN, "%s/%s%08d.cdb", myio->filepath, pfx, fid);
    fd = open(filename, O_RDONLY, 0644);
    if (fd < 0) {
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        return -1;
    }

    /* cache the fd, close the oldest file not touched */
    cdb_ht_insert2(myio->fdcache, &vfid, SI4, &fd, sizeof(int));
    while(myio->fdcache->num > myio->maxfds) {
        CDBHTITEM *item = cdb_ht_poptail(myio->fdcache);
        close(*(int*)cdb_ht_itemval(myio->fdcache, item));
        free(item);
    }

    return fd;
}

/* read a index page */
static int _vio_apnd2_readpage(CDBVIO *vio, CDBPAGE **page, FOFF off)
{
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    int ret, fd;
    uint32_t psize;
    uint32_t fid, roff;
    uint32_t fixbufsize = SBUFSIZE - (sizeof(CDBPAGE) - PAGEHSIZE);
    uint32_t areadsize = PAGEAREADSIZE; //vio->db->areadsize;

    VOFF2ROFF(off, fid, roff);
    /* avoid dirty memory */
    (*page)->magic = 0;

    cdb_lock_lock(myio->lock);
    if (fid == myio->ibuf.fid)
        /* read from current writing file? */
        fd = myio->ibuf.fd;
    else {
        /* old index file */
        int vfid, *fdret;
        vfid = VFIDIDX(fid);
        /* in cache? */
        fdret = cdb_ht_get2(myio->fdcache, &vfid, sizeof(vfid), true);
        if (fdret == NULL) {
            fd = _vio_apnd2_loadfd(vio, fid, VIOAPND2_INDEX);
            if (fd < 0) {
                cdb_lock_unlock(myio->lock);
                return -1;
            }
        } else 
            fd = *fdret;
    }

    /* NOTICE: the data on disk actually starts at 'magic' field in structure */
    ret = _vio_apnd2_read(vio, fd, &(*page)->magic, areadsize, roff);
    if (ret <= 0) {
        cdb_lock_unlock(myio->lock);
        return -1;
    }

    if ((*page)->magic != PAGEMAGIC) {
        cdb_lock_unlock(myio->lock);
        cdb_seterrno(vio->db, CDB_DATAERRIDX, __FILE__, __LINE__);
        return -1;
    }

    psize = PAGESIZE(*page);
    if (ret < areadsize && ret < psize) {
        cdb_lock_unlock(myio->lock);
        cdb_seterrno(vio->db, CDB_DATAERRIDX, __FILE__, __LINE__);
        return ret;
    } else if (psize > areadsize) {
        /* need another read operation since the page is a large than default read size */
        if (psize > fixbufsize) {
            /* record is larger the stack size */
            CDBPAGE *npage = (CDBPAGE *)malloc(sizeof(CDBPAGE) + (*page)->num * sizeof(PITEM));
            memcpy(&npage->magic, &(*page)->magic, areadsize);
            *page = npage;
        }

        ret = _vio_apnd2_read(vio, fd, (char*)&(*page)->magic + areadsize,
            psize - areadsize, roff + areadsize);
        if (ret < psize - areadsize) {
            cdb_lock_unlock(myio->lock);
            cdb_seterrno(vio->db, CDB_DATAERRIDX, __FILE__, __LINE__);
            return -1;
        }
    }

    cdb_lock_unlock(myio->lock);

    /* remember where i got the page, calculate into junk space if page is discarded */
    (*page)->osize = OFFALIGNED(psize);
    (*page)->ooff = off;
    (*page)->cap = (*page)->num;
    return 0;
}

/* read a data record */
static int _vio_apnd2_readrec(CDBVIO *vio, CDBREC** rec, FOFF off, bool readval)
{
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    int ret, fd;
    uint32_t rsize;
    uint32_t fid, roff;
    /* the 'rec' is hoped to be fit in stack, the actually size is a little smaller */
    /* because some fields in CDBREC structure are not on disk */
    uint32_t fixbufsize = SBUFSIZE - (sizeof(CDBREC) - RECHSIZE);
    uint32_t areadsize = vio->db->areadsize;

    VOFF2ROFF(off, fid, roff);
    /* avoid dirty memory */
    (*rec)->magic = 0;

    cdb_lock_lock(myio->lock);
    if (fid == myio->dbuf.fid)
        /* read from current writing file? */
        fd = myio->dbuf.fd;
    else {
        /* read from old data file */
        int vfid, *fdret;
        vfid = VFIDDAT(fid);
        fdret = cdb_ht_get2(myio->fdcache, &vfid, sizeof(vfid), true);
        if (fdret == NULL) {
            fd = _vio_apnd2_loadfd(vio, fid, VIOAPND2_DATA);
            if (fd < 0) {
                cdb_lock_unlock(myio->lock);
                return -1;
            }
        } else 
            fd = *fdret;
    }

    /* NOTICE: the data on disk actually starts at 'magic' field in structure */
    ret = _vio_apnd2_read(vio, fd, &(*rec)->magic, areadsize, roff);
    if (ret <= 0) {
        cdb_lock_unlock(myio->lock);
        return -1;
    }

    if ((*rec)->magic != RECMAGIC) {
        cdb_lock_unlock(myio->lock);
        cdb_seterrno(vio->db, CDB_DATAERRDAT, __FILE__, __LINE__);
        return -1;
    }

    uint32_t ovsize = (*rec)->vsize;
    if (!readval) 
        /* read key only */
        (*rec)->vsize = 0;
    rsize = RECSIZE(*rec);

    if (ret < areadsize && ret < rsize) {
        cdb_lock_unlock(myio->lock);
        cdb_seterrno(vio->db, CDB_DATAERRDAT, __FILE__, __LINE__);
        return -1;
    } else if (rsize > areadsize) {
        /* need another read */
        if (rsize > fixbufsize) {
            /* record is larger the stack size */
            CDBREC *nrec = (CDBREC *)malloc(sizeof(CDBREC)+(*rec)->ksize+(*rec)->vsize);
            memcpy(&nrec->magic, &(*rec)->magic, areadsize);
            *rec = nrec;
        }
        ret = _vio_apnd2_read(vio, fd, (char*)&(*rec)->magic + areadsize,
            rsize - areadsize, roff + areadsize);
        if (ret != rsize - areadsize) {
            cdb_lock_unlock(myio->lock);
            cdb_seterrno(vio->db, CDB_DATAERRDAT, __FILE__, __LINE__);
            return -1;
        }
    }
    cdb_lock_unlock(myio->lock);

    /* fix pointer */
    (*rec)->key = (*rec)->buf;
    (*rec)->val = (*rec)->buf + (*rec)->ksize;

    /* even if didn't read the value, still keep the complete (old) size */
    if (!readval)
        (*rec)->osize = OFFALIGNED(rsize + ovsize);
    else
        (*rec)->osize = OFFALIGNED(rsize);

    (*rec)->ooff = off;
    return 0;
}


/* write a index page, return the written virtual offset */
static int _vio_apnd2_writepage(CDBVIO *vio, CDBPAGE *page, FOFF *off)
{
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    VIOAPND2FINFO *finfo;
    uint32_t psize = PAGESIZE(page);   
    uint32_t fid, roff;
    uint32_t ofid;

    page->magic = PAGEMAGIC;
    page->oid = cdb_genoid(vio->db);

    cdb_lock_lock(myio->lock);
    /* buffer ready? */
    if (myio->ibuf.fd < 0) {
        if (_vio_apnd2_shiftnew(vio, VIOAPND2_INDEX) < 0) {
            cdb_lock_unlock(myio->lock);
            return -1;
        }
    }

    /* if it was modified from existing page, remember the wasted space */
    if (OFFNOTNULL(page->ooff)) {
        VOFF2ROFF(page->ooff, ofid, roff);
        finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->idxmeta, &ofid, SI4, false);
        if (finfo)
            finfo->rcyled += page->osize;
    }

    if (psize > myio->ibuf.limit) {
        /* page too large  */
        _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
        fid = myio->ibuf.fid;
        roff = myio->ibuf.off; 
        _vio_apnd2_write(vio, myio->ibuf.fd, &page->magic, psize, true);
        myio->ibuf.oid = page->oid;
        _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
        cdb_lock_unlock(myio->lock);

        /* remember last wrote offset */
        ROFF2VOFF(fid, roff, *off);
        page->ooff = *off;
        page->osize = OFFALIGNED(psize);
        return 0;
    } else if (psize + myio->ibuf.pos > myio->ibuf.limit)
        /* buffer is full */
        _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);

    /* copy to buffer */
    fid = myio->ibuf.fid;
    roff = myio->ibuf.off + myio->ibuf.pos; 
    memcpy(myio->ibuf.buf + myio->ibuf.pos, &page->magic, psize);
    myio->ibuf.pos += psize;
    myio->ibuf.pos = OFFALIGNED(myio->ibuf.pos);
    myio->ibuf.oid = page->oid;
    cdb_lock_unlock(myio->lock);
    ROFF2VOFF(fid, roff, *off);

    /* remember last wrote offset */
    page->ooff = *off;
    page->osize = OFFALIGNED(psize);
    return 0;
}


/* delete a record */
static int _vio_apnd2_deleterec(CDBVIO *vio, CDBREC *rec, FOFF off)
{
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    uint32_t ofid, roff;
    
    cdb_lock_lock(myio->lock);
    myio->delbuf[myio->delbufpos] = off;
    if (++myio->delbufpos == DELBUFMAX) {
        if (_vio_apnd2_flushbuf(vio, VIOAPND2_DELLOG) < 0)
            return -1;
    }
    
    /* it is an deleted record, remember the space to be recycled */
    VOFF2ROFF(off, ofid, roff);
    if (OFFNOTNULL(rec->ooff)) {
        VIOAPND2FINFO *finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &ofid, SI4, false);
        if (finfo) {
            finfo->rcyled += rec->osize;
        }
    }
    cdb_lock_unlock(myio->lock);
    return 0;
}



/* write a data record, return the written virtual offset */
static int _vio_apnd2_writerec(CDBVIO *vio, CDBREC *rec, FOFF *off, int ptrtype) {
    VIOAPND2 *myio = (VIOAPND2*)vio->iometa;
    uint32_t rsize = RECSIZE(rec);
    uint32_t fid, roff, ofid;
    if (ptrtype == VIOAPND2_RECEXTERNAL)
        rec->magic = RECMAGIC;

    /* oid always are increment, even if it is a record moved from an old data file */
    rec->oid = cdb_genoid(vio->db);
    cdb_lock_lock(myio->lock);
    /* buffer ready? */
    if (myio->dbuf.fd < 0) {
        if (_vio_apnd2_shiftnew(vio, VIOAPND2_DATA) < 0) {
            cdb_lock_unlock(myio->lock);
            return -1;
        }
    }
    /* it is an overwritten record, remember the space to be recycled */
    if (OFFNOTNULL(rec->ooff)) {
        VOFF2ROFF(rec->ooff, ofid, roff);
        VIOAPND2FINFO *finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &ofid, SI4, false);
        if (finfo)
            finfo->rcyled += rec->osize;
    }
    if (rsize > myio->dbuf.limit) {
        /* record too large */
        _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
        fid = myio->dbuf.fid;
        roff = myio->dbuf.off;
        _vio_apnd2_write(vio, myio->dbuf.fd, &rec->magic, RECHSIZE, true);
        if (ptrtype == VIOAPND2_RECINTERNAL)
            _vio_apnd2_write(vio, myio->dbuf.fd, rec->buf, rec->ksize + rec->vsize, false);
        else {
            _vio_apnd2_write(vio, myio->dbuf.fd, rec->key, rec->ksize, false);
            _vio_apnd2_write(vio, myio->dbuf.fd, rec->val, rec->vsize, false);
        }
        /* reset the buffer */
        myio->dbuf.oid = rec->oid;
        _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
        if (rec->expire) {
            VIOAPND2FINFO *finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &fid, SI4, false);
            if (finfo) {
                if (finfo->nexpire == 0) {
                    finfo->lcktime = time(NULL);
                    finfo->nexpire = rec->expire;
                } else if (finfo->nexpire > rec->expire) {
                    finfo->nexpire = rec->expire;
                }
            }
        }
        cdb_lock_unlock(myio->lock);
        ROFF2VOFF(fid, roff, *off);
        return 0;
    } else if (rsize + myio->dbuf.pos > myio->dbuf.limit)
        /* buffer is full */
        _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
    /* copy to buffer */
    fid = myio->dbuf.fid;
    roff = myio->dbuf.off + myio->dbuf.pos;
    memcpy(myio->dbuf.buf + myio->dbuf.pos, &rec->magic, RECHSIZE);
    myio->dbuf.pos += RECHSIZE;
    if (ptrtype == VIOAPND2_RECINTERNAL) {
        memcpy(myio->dbuf.buf + myio->dbuf.pos, rec->buf, rec->ksize + rec->vsize);
        myio->dbuf.pos += rec->ksize + rec->vsize;
    } else {
        memcpy(myio->dbuf.buf + myio->dbuf.pos, rec->key, rec->ksize);
        myio->dbuf.pos += rec->ksize;
        memcpy(myio->dbuf.buf + myio->dbuf.pos, rec->val, rec->vsize);
        myio->dbuf.pos += rec->vsize;
    }
    myio->dbuf.pos = OFFALIGNED(myio->dbuf.pos);
    myio->dbuf.oid = rec->oid;
    if (rec->expire) {
        VIOAPND2FINFO *finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &fid, SI4, false);
        if (finfo) {
            if (finfo->nexpire == 0) {
                finfo->lcktime = time(NULL);
                finfo->nexpire = rec->expire;
            } else if (finfo->nexpire > rec->expire) {
                finfo->nexpire = rec->expire;
            }
        }
    }
    ROFF2VOFF(fid, roff, *off);
    cdb_lock_unlock(myio->lock);
    rec->osize = rsize;
    rec->ooff = *off;
    return 0;
}

static int _vio_apnd2_writerecexternal(CDBVIO *vio, CDBREC *rec, FOFF *off) 
{
    return _vio_apnd2_writerec(vio, rec, off, VIOAPND2_RECEXTERNAL);
}

static int _vio_apnd2_writerecinternal(CDBVIO *vio, CDBREC *rec, FOFF *off)
{
    return _vio_apnd2_writerec(vio, rec, off, VIOAPND2_RECINTERNAL);
}


/* flush buffers, and sync data to disk from OS cache */
static int _vio_apnd2_sync(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    cdb_lock_lock(myio->lock);
    _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
    _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
    if (myio->dbuf.fd > 0)
        fdatasync(myio->dbuf.fd);
    if (myio->ibuf.fd > 0)
        fdatasync(myio->ibuf.fd);
    _vio_apnd2_writehead(vio, false);
    cdb_lock_unlock(myio->lock);
    return 0;
}


/* write db information and main index table into a single file */
static int _vio_apnd2_writehead(CDBVIO *vio, bool wtable)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDB *db = vio->db;
    char buf[FILEMETASIZE];
    int pos = 0;

    memset(buf, 'X', FILEMETASIZE);
    memcpy(buf, FILEMAGICHEADER, FILEMAGICLEN);
    pos += FILEMAGICLEN;
    *(uint32_t*)(buf + pos) = db->hsize;
    pos += SI4;
    *(uint64_t*)(buf + pos) = db->oid;
    pos += SI8;
    *(uint64_t*)(buf + pos) = db->roid;
    pos += SI8;
    *(uint64_t*)(buf + pos) = db->rnum;
    pos += SI8;
    *(uint32_t*)(buf + pos) = VIOAPND2_SIGOPEN;
    pos += SI4;

    if (pwrite(myio->hfd, buf, FILEMETASIZE, 0) != FILEMETASIZE) {
        cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
        return -1;
    }

    if (wtable && pwrite(myio->hfd, db->mtable, sizeof(FOFF) * db->hsize, FILEMETASIZE)
        != sizeof(FOFF) * db->hsize) {
            cdb_seterrno(vio->db, CDB_WRITEERR, __FILE__, __LINE__);
            return -1;
    }
    return 0;
}


/* wrapped for upper layer */
static int _vio_apnd2_writehead2(CDBVIO *vio)
{
    return _vio_apnd2_writehead(vio, true);
}


/* read db information and main index table from a single file */
static int _vio_apnd2_readhead(CDBVIO *vio, bool rtable)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDB *db = vio->db;
    char buf[FILEMETASIZE];
    int pos = 0;

    if (myio->create) {
        /* the db is just created, allocate a empty main index table for db */
        db->mtable = (FOFF *)malloc(sizeof(FOFF) * db->hsize);
        memset(db->mtable, 0, sizeof(FOFF) * db->hsize);
        _vio_apnd2_writehead(vio, false);
        return 0;
    }

    if (pread(myio->hfd, buf, FILEMETASIZE, 0) != FILEMETASIZE) {
        cdb_seterrno(db, CDB_READERR, __FILE__, __LINE__);
        return -1;
    }

    if (memcmp(buf, FILEMAGICHEADER, FILEMAGICLEN)) {
        cdb_seterrno(db, CDB_DATAERRMETA, __FILE__, __LINE__);
        return -1;
    }

    pos += FILEMAGICLEN;
    db->hsize = *(uint32_t*)(buf + pos);
    pos += SI4;
    db->oid = *(uint64_t*)(buf + pos);
    pos += SI8;
    db->roid = *(uint64_t*)(buf + pos);
    pos += SI8;
    db->rnum = *(uint64_t*)(buf + pos);
    pos += SI8;
    /* 4 bytes reserved for open status */
    pos += SI4;

    if (!rtable)
        return 0;
        
    if (db->mtable)
        free(db->mtable);
    db->mtable = (FOFF *)malloc(sizeof(FOFF) * db->hsize);
    if (pread(myio->hfd, db->mtable, sizeof(FOFF) * db->hsize, FILEMETASIZE) !=
        sizeof(FOFF) * db->hsize) {
            free(db->mtable);
            cdb_seterrno(db, CDB_READERR, __FILE__, __LINE__);
            return -1;
    }
    return 0;
}


/* wrapped for upper layer */
static int _vio_apnd2_readhead2(CDBVIO *vio)
{
    return _vio_apnd2_readhead(vio, true);
}


/* check if some dat file has too large junk space */
static void _vio_apnd2_rcyledataspacetask(void *arg)
{
    CDBVIO *vio = (CDBVIO *)arg;
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDBHTITEM *item;
    uint32_t now = time(NULL);
    uint32_t posblexpnum = 0;     
    cdb_lock_lock(myio->lock);
    item = cdb_ht_iterbegin(myio->datmeta);
    while(item != NULL) {
        VIOAPND2FINFO *finfo = (VIOAPND2FINFO*)cdb_ht_itemval(myio->datmeta, item);
        if (finfo->nexpire && finfo->nexpire <= now)
            posblexpnum++;
        item = cdb_ht_iternext(myio->datmeta, item);
    }

    item = cdb_ht_iterbegin(myio->datmeta);
    while(item != NULL) {
        VIOAPND2FINFO *finfo = (VIOAPND2FINFO*)cdb_ht_itemval(myio->datmeta, item);
        uint32_t fid = finfo->fid;
        /* rcyled space size is inaccurate */
        if (finfo->rcyled * 2 < finfo->fsize 
            /* no data file possibly has expire record */
            && (posblexpnum == 0
            /* long enough time passed since last check on this file */
            || finfo->lcktime + posblexpnum * DATARCYLECHECKFACTOR > now
            /* check the data file most recent expire record */
            || finfo->nexpire > now 
            /* no expire record */
            || finfo->nexpire == 0)) {
            item = cdb_ht_iternext(myio->datmeta, item);
            continue;
        }
        
        /* do not work on the writing file or file to be deleted */
        if (finfo->fstatus != VIOAPND2_FULL || finfo->unlink) {
            item = cdb_ht_iternext(myio->datmeta, item);
            continue;
        }
        
        /* have to iterate and calculate recycle space */
        finfo->ref++;
        /* operation on this file should not in lock protection */
        cdb_lock_unlock(myio->lock);
        
        if (finfo->rcyled * 2 < finfo->fsize) {
            _vio_apnd2_rcyledatafile(vio, finfo, false);
            finfo->lcktime = now;
        }
            
        if (finfo->rcyled * 2 >= finfo->fsize) {
            _vio_apnd2_rcyledatafile(vio, finfo, true);
        }
        
        cdb_lock_lock(myio->lock);
        finfo->ref--;
        if (finfo->ref == 0 && finfo->unlink) {
            /* unlink the file */
            _vio_apnd2_unlink(vio, finfo, VIOAPND2_DATA);
            cdb_ht_del2(myio->datmeta, &fid, SI4);
        }
        item = cdb_ht_iterbegin(myio->datmeta);
    }
    cdb_lock_unlock(myio->lock);
}

/* only be called in _vio_apnd2_rcylepagespacetask; when a page is moved into a new
  index file, its ooff should be changed, also its copy in cache should be updated */
static void _vio_apnd2_fixcachepageooff(CDB *db, uint32_t bid, FOFF off)
{
    CDBPAGE *page = NULL;

    if (db->pcache) {
        cdb_lock_lock(db->pclock);
        page = cdb_ht_get2(db->pcache, &bid, SI4, true);
        cdb_lock_unlock(db->pclock);
    }

    /* not in pcache, exists in dirty page cache? */
    if (page == NULL && db->dpcache) {
        cdb_lock_lock(db->dpclock);
        page = cdb_ht_get2(db->dpcache, &bid, SI4, true);
        cdb_lock_unlock(db->dpclock);
    }

    if (page)
        page->ooff = off;
}

/* check if some index file has too large junk space */
static void _vio_apnd2_rcylepagespacetask(void *arg)
{
    CDBVIO *vio = (CDBVIO *)arg;
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDBHTITEM *item;
    
    cdb_lock_lock(myio->lock);
    item = cdb_ht_iterbegin(myio->idxmeta);
    while(item != NULL) {
        VIOAPND2FINFO *finfo = (VIOAPND2FINFO*)cdb_ht_itemval(myio->idxmeta, item);
        uint32_t fid = finfo->fid;

        /* do not work on the writing file or file to be deleted */
        if (finfo->fstatus != VIOAPND2_FULL || finfo->unlink) {
            item = cdb_ht_iternext(myio->idxmeta, item);
            continue;
        }

        /* junk space too large? */
        if (finfo->rcyled * 2 > finfo->fsize) {
            int fd;
            char filename[MAX_PATH_LEN];
            snprintf(filename, MAX_PATH_LEN, "%s/idx%08d.cdb", myio->filepath, fid);
            fd = open(filename, O_RDONLY, 0644);
            if (fd < 0) {
                cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
                item = cdb_ht_iternext(myio->idxmeta, item);
                continue;
            }
            finfo->ref++;
            /* I/O should not block the lock */
            cdb_lock_unlock(myio->lock);

            uint32_t fsize = lseek(fd, 0, SEEK_END);
            uint32_t pos = FILEMETASIZE;
            char *map = mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
            while(pos < fsize) {
                CDBPAGE *page = (CDBPAGE *)&map[pos-(sizeof(CDBPAGE) - PAGEHSIZE)];
                FOFF off;

                if (page->magic != PAGEMAGIC) {
                    pos += ALIGNBYTES;
                    continue;
                }

                ROFF2VOFF(fid, pos, off);
                page->ooff = off;
                page->osize = OFFALIGNED(PAGESIZE(page));
                if (OFFEQ(vio->db->mtable[page->bid], off)) {
                    FOFF noff;
                    _vio_apnd2_writepage(vio, page, &noff);
                    /* lock and double check */
                    cdb_lock_lock(vio->db->mlock[page->bid % MLOCKNUM]);
                    if (OFFEQ(vio->db->mtable[page->bid], off)) {
                        vio->db->mtable[page->bid] = noff;
                        _vio_apnd2_fixcachepageooff(vio->db, page->bid, noff);
                    }
                    cdb_lock_unlock(vio->db->mlock[page->bid % MLOCKNUM]);
                }
                pos += OFFALIGNED(PAGESIZE(page));
            }
            munmap(map, fsize);
            close(fd);

            cdb_lock_lock(myio->lock);
            /* drop information for the file */
            finfo->ref--;
            finfo->unlink = true;
            if (finfo->ref == 0) {
                /* unlink the file */
                _vio_apnd2_unlink(vio, finfo, VIOAPND2_INDEX);
                cdb_ht_del2(myio->idxmeta, &fid, SI4);
            }
            /* reset the iterator */
            item = cdb_ht_iterbegin(myio->idxmeta);
            continue;
        }
        item = cdb_ht_iternext(myio->idxmeta, item);
    }
    cdb_lock_unlock(myio->lock);
}


/* unlink a file and remove fd from fdcache. The function runs under lock protection */
static void _vio_apnd2_unlink(CDBVIO *vio, VIOAPND2FINFO *finfo, int dtype)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    char filename[MAX_PATH_LEN];
    char ipfx[] = "idx";
    char dpfx[] = "dat";
    char *pfx;
    uint32_t *fnum;
    uint32_t vfid, fid = finfo->fid;
    VIOAPND2FINFO **fhead, **ftail;
    CDBHTITEM *fditem = NULL;

    if (dtype == VIOAPND2_INDEX) {
        pfx = ipfx;
        vfid = VFIDIDX(fid);
        fnum = &myio->ifnum;
        fhead = &myio->idxfhead;
        ftail = &myio->idxftail;
    } else if (dtype == VIOAPND2_DATA) {
        pfx = dpfx;
        vfid = VFIDDAT(fid);
        fnum = &myio->dfnum;
        fhead = &myio->datfhead;
        ftail = &myio->datftail;
    } else 
        return;

    snprintf(filename, MAX_PATH_LEN, "%s/%s%08d.cdb", myio->filepath, pfx, fid);
    fditem = cdb_ht_del(myio->fdcache, &vfid, SI4);
    if (fditem != NULL) {
        close(*(int*)cdb_ht_itemval(myio->fdcache, fditem));
        free(fditem);
    } 
    (*fnum)--;
    unlink(filename);

    /* fix linked list of data/index files after remove a finfo from meta table */
    if (finfo->fprev)
        finfo->fprev->fnext = finfo->fnext;
    if (finfo->fnext)
        finfo->fnext->fprev = finfo->fprev;
    if (*fhead == finfo)
        *fhead = finfo->fnext;
    if (*ftail == finfo)
        *ftail = finfo->fprev;
}


/* only be used for sorting files at recovery */
typedef struct {
    uint32_t fid;
    uint64_t oidf;
} VIOAPND2SREORDER;


static int _vio_apnd2_cmpfuncsreorder(const void *p1, const void *p2)
{
    VIOAPND2SREORDER *s1, *s2;
    s1 = (VIOAPND2SREORDER *)p1;
    s2 = (VIOAPND2SREORDER *)p2;
    return s1->oidf - s2->oidf;
}


/* recovery the database if it was not close properly 
 * or force recovery from roid = 0
 * the procedure runs with no lock protection */
static int _vio_apnd2_recovery(CDBVIO *vio, bool force)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDB *db = vio->db;
    char filename[MAX_PATH_LEN];
    struct dirent **filelist;
    int fnum;
    VIOAPND2SREORDER *idxorders;
    int idxpos, idxlimit;
    VIOAPND2SREORDER *datorders;
    int datpos, datlimit;
    uint32_t imaxfid = 0, dmaxfid = 0;
    bool gotmindex = false;
    

    idxpos = datpos = 0;
    idxlimit = datlimit = 256;
    idxorders = (VIOAPND2SREORDER *)malloc(idxlimit * sizeof(VIOAPND2SREORDER));
    datorders = (VIOAPND2SREORDER *)malloc(datlimit * sizeof(VIOAPND2SREORDER));
    fnum = scandir(myio->filepath, &filelist, 0, alphasort);
    myio->dfnum = myio->ifnum = 0;
    myio->datfhead = myio->datftail = myio->idxfhead = myio->idxftail = NULL;
    /* special value to mark if found current writing file */
    myio->ibuf.fid = myio->dbuf.fid = -1;
    for(int i = 0; i < fnum; i++) {
        // Check file name/type
        const char *cstr = filelist[i]->d_name;
        if (strncmp(cstr + strlen(cstr) - 4, ".cdb", 4) != 0)
            /* not a cuttdb file*/
            continue;
        if (strcmp(cstr, "dellog.cdb") == 0) {
            snprintf(filename, MAX_PATH_LEN, "%s/%s", myio->filepath, cstr);
            myio->dfd = open(filename, O_RDONLY, 0644);
        } else if (strcmp(cstr, "mainindex.cdb") == 0) {
            gotmindex = true;
//            snprintf(filename, MAX_PATH_LEN, "%s/%s", myio->filepath, cstr);
//            myio->hfd = open(filename, O_RDONLY, 0644);
//            if (_vio_apnd2_readhead(vio, false) < 0 || db->hsize == 0) {
//                goto ERRRET;
//            }
//            db->mtable = (FOFF *)malloc(sizeof(FOFF) * db->hsize);
//            gotmindex = true;
//            memset(db->mtable, 0, sizeof(FOFF) * db->hsize);
        } else if (strcmp(cstr, "mainmeta.cdb") == 0) {
            snprintf(filename, MAX_PATH_LEN, "%s/%s", myio->filepath, cstr);
            myio->mfd = open(filename, O_RDWR, 0644);
            if (myio->mfd < 0) {
                cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
                continue;
            }
        } else if (strlen(cstr) == 15 
            && (strncmp(cstr, "dat", 3) == 0 || strncmp(cstr, "idx", 3) == 0)) {
            VIOAPND2FINFO finfo;
            uint64_t fsize = 0;
            
            snprintf(filename, MAX_PATH_LEN, "%s/%s", myio->filepath, cstr);
            int fd = open(filename, O_RDWR, 0644);
            if (fd < 0) {
                cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
                continue;
            }
            if (_vio_apnd2_readfmeta(vio, fd, &finfo) < 0) {
                close(fd);
                continue;
            }
            fsize = lseek(fd, 0, SEEK_END);
            finfo.rcyled = 0;
            finfo.ref = 0;
            finfo.unlink = false;
            finfo.fprev = finfo.fnext = NULL;
            if (finfo.ftype == VIOAPND2_INDEX) {
                if (force) {
                    /* delete all index file and rebuild them if force to recovery */
                    close(fd);
                    unlink(filename);
                } else {
                    cdb_ht_insert2(myio->idxmeta, &finfo.fid, SI4, &finfo, sizeof(VIOAPND2FINFO));
                    idxorders[idxpos].fid = finfo.fid;
                    idxorders[idxpos].oidf = finfo.oidf;
                    if (++idxpos == idxlimit) {
                        VIOAPND2SREORDER *tmp = (VIOAPND2SREORDER *)malloc(idxlimit * 2 * sizeof(VIOAPND2SREORDER));
                        memcpy(tmp, idxorders, idxlimit * sizeof(VIOAPND2SREORDER));
                        idxlimit *= 2;
                        free(idxorders);
                        idxorders = tmp;
                    }
                    if(finfo.fstatus == VIOAPND2_WRITING) {
                        myio->ibuf.fid = finfo.fid;
                        myio->ibuf.off = OFFALIGNED(fsize);
                        myio->ibuf.pos = 0;
                        myio->ibuf.fd = fd;
                    } else 
                        close(fd);
                    if (finfo.fid > imaxfid)
                        imaxfid = finfo.fid;
                    myio->ifnum++;
                }
            } else if (finfo.ftype == VIOAPND2_DATA) {
                /* no information about nearest expire record time, make a fake one(non zero) */
                finfo.nexpire = finfo.lcktime = time(NULL);
                cdb_ht_insert2(myio->datmeta, &finfo.fid, SI4, &finfo, sizeof(VIOAPND2FINFO));
                datorders[datpos].fid = finfo.fid;
                datorders[datpos].oidf = finfo.oidf;
                if (++datpos == datlimit) {
                    VIOAPND2SREORDER *tmp = (VIOAPND2SREORDER *)malloc(datlimit * 2 * sizeof(VIOAPND2SREORDER));
                    memcpy(tmp, datorders, datlimit * sizeof(VIOAPND2SREORDER));
                    datlimit *= 2;
                    free(datorders);
                    datorders = tmp;
                }
                if (finfo.fstatus == VIOAPND2_WRITING) {
                    myio->dbuf.fid = finfo.fid;
                    myio->dbuf.off = OFFALIGNED(fsize);
                    myio->dbuf.pos = 0;
                    myio->dbuf.fd = fd;
                } else
                    close(fd);
                if (finfo.fid > dmaxfid)
                    dmaxfid = finfo.fid;
                myio->dfnum++;
            } else
                close(fd);
        } /* end of else */
    } /* end of for */

   
    /* fix recycled size */
    _vio_apnd2_readmeta(vio, true);

    if (filelist) {
        for(int j = 0; j < fnum; j++)
            if (filelist[j]) {
                free(filelist[j]);
                filelist[j] = NULL;
            }
        free(filelist);
        filelist = NULL;
    }
    
    if (!gotmindex) {
        /* recovery failed */
        /* return */
        goto ERRRET;
    } else {
        if (_vio_apnd2_readhead(vio, false) < 0)
            goto ERRRET;
    }
    
    if (myio->mfd < 0) {
        snprintf(filename, MAX_PATH_LEN, "%s/mainmeta.cdb", myio->filepath);
        myio->mfd = open(filename, O_RDWR | O_CREAT, 0644);
        if (myio->mfd < 0) {
            cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
            goto ERRRET;
        }
    }

    /* index file complele broken, replay all records to build the index */
    if (myio->ifnum == 0 || force)
        db->roid = 0;
    /* re-count records num */
    db->rnum = 0;
         
    /* fix index/data file meta relation */
    qsort(datorders, datpos, sizeof(VIOAPND2SREORDER), _vio_apnd2_cmpfuncsreorder);
    qsort(idxorders, idxpos, sizeof(VIOAPND2SREORDER), _vio_apnd2_cmpfuncsreorder);
    
    VIOAPND2FINFO *lfinfo = NULL;
    for(int i = 0; i < datpos; i++) {
        VIOAPND2FINFO *cfinfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &datorders[i].fid, SI4, false);
        if (cfinfo == NULL)
            continue;
        if (lfinfo)
            lfinfo->fnext = cfinfo;
        else {
            myio->datfhead = cfinfo;
        }
        cfinfo->fprev = lfinfo;
        lfinfo = cfinfo;
    }
    myio->datftail = lfinfo;
    if (lfinfo)
        lfinfo->fnext = NULL;
    lfinfo = NULL;
    for(int i = 0; i < idxpos; i++) {
        VIOAPND2FINFO *cfinfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->idxmeta, &idxorders[i].fid, SI4, false);
        if (cfinfo == NULL)
            continue;
        if (lfinfo)
            lfinfo->fnext = cfinfo;
        else {
            myio->idxfhead = cfinfo;
        }
        cfinfo->fprev = lfinfo;
        lfinfo = cfinfo;
    }
    myio->idxftail = lfinfo;
    if (lfinfo)
        lfinfo->fnext = NULL;
    lfinfo = NULL;

    if (myio->ibuf.fid == -1) {
        myio->ibuf.fid = 0;
        _vio_apnd2_shiftnew(vio, VIOAPND2_INDEX);
    } 
    if (myio->dbuf.fid == -1) {
        myio->dbuf.fid = 0;
        _vio_apnd2_shiftnew(vio, VIOAPND2_DATA);
    } 
 
    /* fix offsets in main index table */
    db->mtable = (FOFF *)malloc(db->hsize * sizeof(FOFF));
    memset(db->mtable, 0, db->hsize * sizeof(FOFF));
    void *it = _vio_apnd2_pageiterfirst(vio, 0);
    if (it) {
        char sbuf[SBUFSIZE];
        CDBPAGE *page = (CDBPAGE *)sbuf;
        /* need not use iterator since don't care about contents in page */
        /* I'm just lazy, cpu time is cheap */
        while(_vio_apnd2_pageiternext(vio, &page, it) == 0) {
            if (OFFNOTNULL(db->mtable[page->bid])) {
                /* recalculate the space to be recycled */
                uint32_t ofid, roff;
                char sbuf[SBUFSIZE];
                CDBPAGE *opage = (CDBPAGE *)sbuf;
                _vio_apnd2_readpage(vio, &opage, db->mtable[page->bid]);
                if (OFFNOTNULL(opage->ooff)) {
                    VOFF2ROFF(opage->ooff, ofid, roff);
                    VIOAPND2FINFO *finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->idxmeta, &ofid, SI4, false);
                    if (finfo)
                        finfo->rcyled += opage->osize;
                }
                /* fix impaction of old page */
                db->rnum -= opage->num;
                if (opage != (CDBPAGE *)sbuf)
                    free(opage);
            }
            db->mtable[page->bid] = page->ooff;
            db->rnum += page->num;
            if (page != (CDBPAGE *)sbuf) {
                free(page);
                page = (CDBPAGE *)sbuf;
            }
        }
        _vio_apnd2_pageiterdestory(vio, it);
    }
    
    /* like what was did just now */
    it = _vio_apnd2_reciterfirst(vio, db->roid);
    if (it) {
        char sbuf[SBUFSIZE];
        CDBREC *rec = (CDBREC *)sbuf;
        while(_vio_apnd2_reciternext(vio, &rec, it) == 0) {
            FOFF soffs[SFOFFNUM];
            FOFF *soff = soffs, ooff;
            char sbuf2[SBUFSIZE];
            OFFZERO(ooff);
            CDBREC *rrec = (CDBREC*)sbuf2;
            uint64_t hash = CDBHASH64(rec->buf, rec->ksize);

            /* check record with duplicate key(old version/overwritten maybe */
            int retnum = cdb_getoff(db, hash, &soff, CDB_NOTLOCKED);
            for(int i = 0; i < retnum; i++) {
                if (rrec != (CDBREC*)sbuf2) {
                    free(rrec);
                    rrec = (CDBREC*)sbuf2;
                }
                
                int cret = _vio_apnd2_readrec(db->vio, &rrec, soff[i], false);
                if (cret < 0)
                    continue;
                    
                if (rec->ksize == rrec->ksize && memcmp(rrec->key, rec->key, rec->ksize) == 0) {
                    ooff = rrec->ooff;
                    break;
                }
            }
            if (soff != soffs)
                free(soff);
            if (rrec != (CDBREC*)sbuf2) 
                free(rrec);

            if (OFFNOTNULL(ooff))
                /* replace offset in index */
                cdb_replaceoff(db, hash, ooff, rec->ooff, CDB_NOTLOCKED);
            else
                cdb_updatepage(vio->db, hash, rec->ooff, CDB_PAGEINSERTOFF, CDB_NOTLOCKED);

            if (rec->oid > db->oid)
                db->oid = rec->oid;
            if (rec != (CDBREC *)sbuf) {
                free(rec);
                rec = (CDBREC *)sbuf;
            }
        }
        _vio_apnd2_reciterdestory(vio, it);
    }
    
    /* replay deletion logs */
    FOFF delitems[1024];
    for(; myio->dfd > 0;) {
        int ret = read(myio->dfd, delitems, 1024 * sizeof(FOFF));
        if (ret > 0) {
            for(int j = 0; j * sizeof(FOFF) < ret; j++) {
                char sbuf[SBUFSIZE];
                uint32_t ofid, roff;
                CDBREC *rec = (CDBREC *)sbuf;
                if (_vio_apnd2_readrec(vio, &rec, delitems[j], false) < 0)
                    continue;
                if (cdb_updatepage(db, CDBHASH64(rec->key, rec->ksize),
                                   delitems[j], CDB_PAGEDELETEOFF, CDB_NOTLOCKED) == 0)
                VOFF2ROFF(delitems[j], ofid, roff);
                VIOAPND2FINFO *finfo = (VIOAPND2FINFO *)cdb_ht_get2(myio->datmeta, &ofid, SI4, false);
                if (finfo)
                    finfo->rcyled += rec->osize;
                if (rec != (CDBREC *)sbuf)
                    free(rec);
            }
        } else {
            close(myio->dfd);
            myio->dfd = -1;
        }
    }
    
    cdb_flushalldpage(db);
    _vio_apnd2_writemeta(vio);
    _vio_apnd2_writehead(vio, true);
    cdb_ht_clean(myio->idxmeta);
    cdb_ht_clean(myio->datmeta);
    free(idxorders);
    free(datorders);
    /* mfd / dfd will be opened again after this function, but hfd won't be */
    myio->datfhead = myio->datftail = myio->idxfhead = myio->idxftail = NULL;
    if (myio->ibuf.fd > 0)
        close(myio->ibuf.fd);
    if (myio->dbuf.fd > 0)
        close(myio->dbuf.fd);
    if (myio->mfd > 0)
        close(myio->mfd);
    if (myio->dfd > 0)
        close(myio->dfd);
    return 0;

ERRRET:
    if (filelist) {
        for(int j = 0; j < fnum; j++)
            if (filelist[j]) {
                free(filelist[j]);
                filelist[j] = NULL;
            }
        free(filelist);
    }
    if (myio->hfd > 0)
        close(myio->hfd);
    if (myio->mfd > 0)
        close(myio->mfd);
    if (myio->dfd > 0)
        close(myio->dfd);
    free(datorders);
    free(idxorders);
    return -1;
}


static VIOAPND2FINFO* _vio_apnd2_fileiternext(CDBVIO *vio, int dtype, uint64_t oid)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    uint64_t foid = (uint64_t)-1;
    CDBHTITEM *item;
    CDBHASHTABLE *ht;
    VIOAPND2FINFO *finfo = NULL;

    if (dtype == VIOAPND2_INDEX)
        ht = myio->idxmeta;
    else if (dtype == VIOAPND2_DATA)
        ht = myio->datmeta;
    else
        return NULL;

    cdb_lock_lock(myio->lock);
    item = cdb_ht_iterbegin(ht);
    while(item) {
        VIOAPND2FINFO *tfinfo = (VIOAPND2FINFO *)cdb_ht_itemval(ht, item);
        if (tfinfo->oidf < foid && tfinfo->oidf >= oid) {
            foid = tfinfo->oidf;
            finfo = tfinfo;
        }
        item = cdb_ht_iternext(ht, item);
    }
    if (finfo)
        finfo->ref++;
    cdb_lock_unlock(myio->lock);
    return finfo;
}

static int _vio_apnd2_iterfirst(CDBVIO *vio, VIOAPND2ITOR *it, int dtype, int64_t oid)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    CDBHASHTABLE *tmpcache;
    char filename[MAX_PATH_LEN];
    char ipfx[] = "idx";
    char dpfx[] = "dat";
    char *pfx;

    if (dtype == VIOAPND2_INDEX) {
        pfx = ipfx;
        tmpcache = myio->idxmeta;
    } else if (dtype == VIOAPND2_DATA) {
        pfx = dpfx;
        tmpcache = myio->datmeta;
    } else 
        return -1;

    if (it->finfo == NULL)
        it->finfo = _vio_apnd2_fileiternext(vio, dtype, oid);
    if (it->finfo == NULL) {
        return -1;
    }

    snprintf(filename, MAX_PATH_LEN, "%s/%s%08d.cdb", myio->filepath, pfx, it->finfo->fid);
    it->fd = open(filename, O_RDONLY, 0644);
    if (it->fd < 0) {
        cdb_lock_lock(myio->lock);
        it->finfo->ref--;
        if (it->finfo->ref == 0 && it->finfo->unlink) {
            /* unlink the file */
            _vio_apnd2_unlink(vio, it->finfo, dtype);
            cdb_ht_del2(tmpcache, &it->finfo->fid, SI4);
        }
        cdb_lock_unlock(myio->lock);
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        return -1;
    }

    it->fsize = lseek(it->fd, 0, SEEK_END);
    it->mmap = mmap(NULL, it->fsize, PROT_READ | PROT_WRITE, MAP_PRIVATE, it->fd, 0);
    it->off = FILEMETASIZE;
    it->oid = oid;

    while(it->off < it->fsize) {
        if (dtype == VIOAPND2_INDEX) {
            CDBPAGE *page = (CDBPAGE *)(it->mmap + it->off -(sizeof(CDBPAGE) - PAGEHSIZE));
            if (page->magic != PAGEMAGIC) {
                it->off += ALIGNBYTES;
                continue;
            }
            if (page->oid >= oid) 
                break;
            it->off += OFFALIGNED(PAGESIZE(page));
        } else if (dtype == VIOAPND2_DATA) {
            CDBREC *rec = (CDBREC *)(it->mmap + it->off -(sizeof(CDBREC) - RECHSIZE));
            if (rec->magic != RECMAGIC && rec->magic != DELRECMAGIC) {
                it->off += ALIGNBYTES;
                continue;
            }
            if (rec->oid >= oid) 
                break;
            it->off += OFFALIGNED(RECSIZE(rec));
        }
    }

    if (it->off >= it->fsize) {
        munmap(it->mmap, it->fsize);
        close(it->fd);
        cdb_lock_lock(myio->lock);
        it->finfo->ref--;
        if (it->finfo->ref == 0 && it->finfo->unlink) {
            /* unlink the file */
            _vio_apnd2_unlink(vio, it->finfo, dtype);
            cdb_ht_del2(tmpcache, &it->finfo->fid, SI4);
        }
        cdb_lock_unlock(myio->lock);
        return -1;
    }
    return 0;
}


static int _vio_apnd2_pageiternext(CDBVIO *vio, CDBPAGE **page, void *iter)
{
    VIOAPND2ITOR *it = (VIOAPND2ITOR *)iter;
    CDBPAGE *cpage;
    uint32_t fixbufsize = SBUFSIZE - (sizeof(CDBPAGE) - PAGEHSIZE);

    for(;;) {
        if (it->off >= it->fsize) {
            it->oid = CDBMAX(it->oid, it->finfo->oidl);
            _vio_apnd2_iterfree(vio, VIOAPND2_INDEX, it);
            if (_vio_apnd2_iterfirst(vio, it, VIOAPND2_INDEX, it->oid) < 0)
                return -1;
        }
        cpage = (CDBPAGE *)(it->mmap + it->off -(sizeof(CDBPAGE) - PAGEHSIZE));
        if (cpage->magic != PAGEMAGIC) {
            it->off += ALIGNBYTES;
            continue;
        }
        if (PAGESIZE(cpage) <= fixbufsize)
            memcpy(&(*page)->magic, &cpage->magic, PAGESIZE(cpage));
        else {
            *page = (CDBPAGE *)malloc(sizeof(CDBPAGE) + (*page)->num * sizeof(PITEM));
            memcpy(&(*page)->magic, &cpage->magic, PAGESIZE(cpage));
        }
        (*page)->osize = PAGESIZE(cpage);
        (*page)->cap = (*page)->num;
        ROFF2VOFF(it->finfo->fid, it->off, (*page)->ooff);
        /* set iterator to next one */
        it->oid = (*page)->oid + 1;
        it->off += OFFALIGNED(PAGESIZE(cpage));
        return 0;
    }
    return -1;
}

static int _vio_apnd2_reciternext(CDBVIO *vio, CDBREC **rec, void *iter)
{
    VIOAPND2ITOR *it = (VIOAPND2ITOR *)iter;
    CDBREC *crec;
    uint32_t fixbufsize = SBUFSIZE - (sizeof(CDBREC) - RECHSIZE);

    for(;;) {
        if (it->off >= it->fsize) {
            it->oid = CDBMAX(it->oid, it->finfo->oidl);
            _vio_apnd2_iterfree(vio, VIOAPND2_DATA, it);
            if (_vio_apnd2_iterfirst(vio, it, VIOAPND2_DATA, it->oid) < 0)
                return -1;
        }
        crec = (CDBREC *)(it->mmap + it->off -(sizeof(CDBREC) - RECHSIZE));
        if (crec->magic != RECMAGIC && crec->magic != DELRECMAGIC) {
            it->off += ALIGNBYTES;
            continue;
        }
        if (RECSIZE(crec) <= fixbufsize)
            memcpy(&(*rec)->magic, &crec->magic, RECSIZE(crec));
        else {
            *rec = (CDBREC *)malloc(sizeof(CDBREC) + crec->ksize + crec->vsize);
            memcpy(&(*rec)->magic, &crec->magic, RECSIZE(crec));
        }

        (*rec)->osize = RECSIZE(crec);
        (*rec)->expire = crec->expire;
        ROFF2VOFF(it->finfo->fid, it->off, (*rec)->ooff);
        (*rec)->key = (*rec)->buf;
        (*rec)->val = (*rec)->buf + (*rec)->ksize;

        /* set iterator to next one */
        it->oid = (*rec)->oid + 1;
        it->off += OFFALIGNED(RECSIZE(crec));
        return 0;
    }
    return -1;
}


static int _vio_apnd2_iterfree(CDBVIO *vio, int dtype, VIOAPND2ITOR *it)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    if (it->mmap) {
        munmap(it->mmap, it->fsize);
        close(it->fd);
        cdb_lock_lock(myio->lock);
        it->finfo->ref--;
        if (it->finfo->ref == 0 && it->finfo->unlink) {
            /* unlink the file */
            VIOAPND2FINFO *tfinfo;
            it->finfo->fnext->fprev = it->finfo->fprev;
            it->finfo->fprev->fnext = it->finfo->fnext;
            tfinfo = it->finfo;
            it->finfo = it->finfo->fnext;
            _vio_apnd2_unlink(vio, tfinfo, dtype);
            if (dtype == VIOAPND2_INDEX)
                cdb_ht_del2(myio->idxmeta, &tfinfo->fid, SI4);
            else if (dtype == VIOAPND2_DATA)
                cdb_ht_del2(myio->datmeta, &tfinfo->fid, SI4);
        } else 
            it->finfo = it->finfo->fnext;
        if (it->finfo)
            it->finfo->ref++;
        cdb_lock_unlock(myio->lock);
        it->mmap = NULL;
    }
    return 0;
}


static void* _vio_apnd2_reciterfirst(CDBVIO *vio, uint64_t oid)
{
    VIOAPND2ITOR *it = (VIOAPND2ITOR *)malloc(sizeof(VIOAPND2ITOR));

    /* iterator won't get to buffered data */
    _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
    it->mmap = NULL;
    it->finfo = NULL;
    if (_vio_apnd2_iterfirst(vio, it, VIOAPND2_DATA, oid) < 0) {
        free(it);
        return NULL;
    }
    return (void*)it;
}


static void _vio_apnd2_reciterdestory(CDBVIO *vio, void *iter)
{
    if (iter) {
        _vio_apnd2_iterfree(vio, VIOAPND2_DATA, (VIOAPND2ITOR *)iter);
        free(iter);
    }
}

static void* _vio_apnd2_pageiterfirst(CDBVIO *vio, uint64_t oid)
{
    VIOAPND2ITOR *it = (VIOAPND2ITOR *)malloc(sizeof(VIOAPND2ITOR));

    /* iterator won't get to buffered data */
    _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
    it->mmap = NULL;
    it->finfo = NULL;
    if (_vio_apnd2_iterfirst(vio, it, VIOAPND2_INDEX, oid) < 0) {
        free(it);
        return NULL;
    }
    return (void*)it;
}


static void _vio_apnd2_pageiterdestory(CDBVIO *vio, void *iter)
{
    if (iter) {
        _vio_apnd2_iterfree(vio, VIOAPND2_INDEX, (VIOAPND2ITOR *)iter);
        free(iter);
    }
}

static int _vio_apnd2_rcyledatafile(CDBVIO *vio, VIOAPND2FINFO *finfo, bool rcyle)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    int fd;
    char filename[MAX_PATH_LEN];
    uint32_t nexpire = 0xffffffff;

    snprintf(filename, MAX_PATH_LEN, "%s/dat%08d.cdb", myio->filepath, finfo->fid);
    fd = open(filename, O_RDONLY, 0644);
    if (fd < 0) {
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
        return -1;
    }

    uint32_t frsize = 0, fsize = lseek(fd, 0, SEEK_END);
    uint32_t pos = FILEMETASIZE;
    char *map = mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    uint32_t now = time(NULL);
    while(pos < fsize) {
        CDBREC *rec = (CDBREC *)&map[pos-(sizeof(CDBREC) - RECHSIZE)];
        FOFF off;
        uint64_t hash;

        if (rec->magic != RECMAGIC && rec->magic != DELRECMAGIC) {
            pos += ALIGNBYTES;
            continue;
        }

        ROFF2VOFF(finfo->fid, pos, off);
        hash = CDBHASH64(rec->buf, rec->ksize);
        if (cdb_checkoff(vio->db, hash, off, CDB_NOTLOCKED) 
        /* not expired */
        && (rec->expire > now || rec->expire == 0)) {
            /* nearest expire record in current file */
            if (rec->expire && rec->expire < nexpire)
                nexpire = rec->expire;

            /* record exist in index, skip */
            if (rcyle) {
                FOFF noff;
                rec->ooff = off;
                rec->osize = OFFALIGNED(RECSIZE(rec));
                _vio_apnd2_writerecinternal(vio, rec, &noff);
                cdb_replaceoff(vio->db, hash, off, noff, CDB_NOTLOCKED);
            }
        } else {
            if (rcyle && rec->expire && rec->expire < now) {
                /* expired record, delete from index page */
                cdb_updatepage(vio->db, hash, off, CDB_PAGEDELETEOFF, CDB_NOTLOCKED);
            }
            frsize += OFFALIGNED(RECSIZE(rec));
        }
        pos += OFFALIGNED(RECSIZE(rec));
    }
    munmap(map, fsize);
    close(fd);
    cdb_lock_lock(myio->lock);
    /* fix metainfo about nearest expire time in current data file */
    if (nexpire == 0xffffffff) 
        finfo->nexpire = 0;
    else
        finfo->nexpire = nexpire;
    finfo->rcyled = frsize;
    if (rcyle) {
        /* unlink */
        finfo->unlink = true;
    }
    cdb_lock_unlock(myio->lock);
    return 0;
}


static void _vio_apnd2_cleanpoint(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    char filename[MAX_PATH_LEN];
    
    cdb_lock_lock(myio->lock);
    _vio_apnd2_flushbuf(vio, VIOAPND2_DATA);
    _vio_apnd2_flushbuf(vio, VIOAPND2_INDEX);
    _vio_apnd2_writehead(vio, false);
    if (myio->dfd > 0) 
        close(myio->dfd);
    snprintf(filename, MAX_PATH_LEN, "%s/dellog.cdb", myio->filepath);
    /* clean the previous deletion log */
    myio->dfd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    /* open failed, whom to tell? */
    if (myio->dfd < 0)
        cdb_seterrno(vio->db, CDB_OPENERR, __FILE__, __LINE__);
    cdb_lock_unlock(myio->lock);
}


static int _vio_apnd2_checkopensig(CDBVIO *vio)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    int pos = 0;
    uint32_t ret;
    
    if (myio->hfd < 0)
        return -1;
        
    pos += FILEMAGICLEN;
    pos += SI4;
    pos += SI8;
    pos += SI8;
    pos += SI8;
    if (pread(myio->hfd, &ret, SI4, pos) != SI4)
        return -1;
        
    return ret;
}


static int _vio_apnd2_setopensig(CDBVIO *vio, int sig)
{
    VIOAPND2 *myio = (VIOAPND2 *)vio->iometa;
    int pos = 0;
    uint32_t val = sig;
    if (myio->hfd < 0)
        return -1;
        
    pos += FILEMAGICLEN;
    pos += SI4;
    pos += SI8;
    pos += SI8;
    pos += SI8;
    if (pwrite(myio->hfd, &val, SI4, pos) != SI4)
        return -1;
    return 0;
}


