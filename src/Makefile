#!/bin/sh

OPT=-O2
DEBUG=-g
CFLAGS=-std=gnu99 -Wall -fPIC $(OPT) $(DEBUG) -DHAVE_EPOLL

CC=gcc
LCOMMON=-lrt -lpthread

ifeq ($(GOOGPERF),yes)
PROFILER=-DGOOG_PROFILER
LPROFILER=-lprofiler
endif


OBJS=cdb_bgtask.o cdb_bloomfilter.o cdb_core.o cdb_crc64.o \
     cdb_errno.o cdb_hashtable.o cdb_lock.o cdb_vio.o vio_apnd2.o

all:  libcuttdb.a libcuttdb.so cdb_dumpdb cdb_builddb cdb_dumpraw cuttdb-server move 


test: test_mt 
	mv test_mt ../build/


cdb_dumpdb: cdb_dumpdb.o libcuttdb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LCOMMON)


test_mt: test_mt.o libcuttdb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LCOMMON)

cdb_dumpraw: cdb_dumpraw.c
	$(CC) $(CFLAGS) -o $@ $^


cdb_builddb: cdb_builddb.o libcuttdb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LCOMMON)


cuttdb-server: cuttdb-server.o server-thread.o libcuttdb.a
	$(CC) -o $@ $^ $(LCOMMON)


libcuttdb.so: $(OBJS)
	$(CC) -shared -o $@ $^ $(LPROFILER) $(LCOMMON) 


libcuttdb.a: $(OBJS)
	ar cqs $@ $^  


%.o: %.c
	$(CC) -c $(CFLAGS) -o $@ $^ $(PROFILER)


clean:
	rm -rf *.o libcuttdb.so libcuttdb.a cdb_dumpdb cdb_builddb cdb_dumpraw ../build/*


cleanobj:
	rm -rf *.o

                                                                                      
move:
	mv cdb_dumpdb cdb_dumpraw cdb_builddb cuttdb-server libcuttdb.so libcuttdb.a ../build/


rebuild: clean all


install: ../build/libcuttdb.so ../build/libcuttdb.a cuttdb.h
	cp ../build/libcuttdb.a ../build/libcuttdb.so /usr/lib/
	cp cuttdb.h /usr/include/
