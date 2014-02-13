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
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdint.h>

#define SI4 4
#define SI8 8

/* data record */
typedef struct {
    /* disk store starts at following field */
    uint32_t magic;
    uint32_t ksize;
    uint32_t vsize;
    uint32_t expire;
    uint64_t oid;
    char buf[0];
} __attribute__((packed)) CDBREC;

/* real size of a record header when stored on disk */
#define RECHSIZE (SI4 * 4 + SI8)
/* real size of a record when stored on disk */
#define RECSIZE(r) (RECHSIZE + (r)->ksize + (r)->vsize)

#define FILEMETASIZE 64
#define ALIGNBYTES 16
#define RECMAGIC 0x19871022
#define DELRECMAGIC 0x19871023
#define FILEMAGICHEADER "CuTtDbFiLePaRtIaL"
#define FILEMAGICLEN (strlen(FILEMAGICHEADER))
#define OFFALIGNED(off) (((off) & (ALIGNBYTES - 1))? ((off) | (ALIGNBYTES - 1)) + 1: off)



void process(const char *filename)
{
#define SBUFSIZE 4096
    int fd = open(filename, O_RDONLY, 0644);
    char buf[SBUFSIZE];
    if (fd < 0)
        fprintf(stderr, "%s Open failed\n", filename);

    long filesize = lseek(fd, 0, SEEK_END);
    long pos = FILEMETASIZE;
    char *map = (char*)mmap(NULL, filesize, PROT_READ, MAP_SHARED, fd, 0);
    if (memcmp(map, FILEMAGICHEADER, FILEMAGICLEN)) {
        fprintf(stderr, "%s is not a cuttdb file\n", filename);
        close(fd);
        return;
    }

    while(pos < filesize) {
        char *kvbuf = buf;
        CDBREC *rec = (CDBREC*)&map[pos];
        if (rec->magic != RECMAGIC && rec->magic != DELRECMAGIC) {
            pos += ALIGNBYTES;
            continue;
        }

        pos += OFFALIGNED(RECSIZE(rec));
        if (rec->magic != RECMAGIC)
            continue;
        
        if (rec->ksize + rec->vsize + 2 > SBUFSIZE) {
            kvbuf = (char*)malloc(rec->ksize + rec->vsize + 2);
        }
        memcpy(kvbuf, rec->buf, rec->ksize);
        kvbuf[rec->ksize] = '\t';
        memcpy(kvbuf + rec->ksize + 1, rec->buf + rec->ksize, rec->vsize);
        kvbuf[rec->ksize + rec->vsize + 1] = '\0';
        printf("%s\t%u\n", kvbuf, rec->expire);
        if (kvbuf != buf)
            free(kvbuf);
    }

    munmap(map, filesize);
    close(fd);
}




int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s dat########.cdb dat########.cdb .... \n", argv[0]);
        return 0;
    }
    for(int i = 1; i < argc; i++)
        process(argv[i]);
    return 0;
}




