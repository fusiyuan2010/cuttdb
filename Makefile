#!/bin/sh
#build a prototype k-v database

OPT=-O2
DEBUG=-g
CFLAGS=-std=gnu99 -Wall -fPIC $(OPT) $(DEBUG) -DHAVE_EPOLL

CC=gcc
LCOMMON=-lrt -lpthread

ifeq ($(GOOGPERF),yes)
PROFILER=-DGOOG_PROFILER
LPROFILER=-lprofiler
endif

OBJDIR := objs
BUILDDIR := build
SRCDIR := src
OBJS := $(addprefix $(OBJDIR)/, cdb_bgtask.o cdb_bloomfilter.o cdb_core.o cdb_crc64.o cdb_errno.o cdb_hashtable.o cdb_lock.o cdb_vio.o vio_apnd2.o)

all:  library exes

library: $(BUILDDIR)/libcuttdb.a $(BUILDDIR)/libcuttdb.so
exes: $(BUILDDIR)/cuttdb-server $(BUILDDIR)/cdb_dumpraw $(BUILDDIR)/cdb_builddb $(BUILDDIR)/cdb_dumpdb
test: $(BUILDDIR)/test_mt 

$(BUILDDIR)/cdb_dumpdb: $(OBJDIR)/cdb_dumpdb.o $(BUILDDIR)/libcuttdb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LCOMMON)

$(BUILDDIR)/test_mt: $(SRCDIR)/test_mt.c $(BUILDDIR)/libcuttdb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LCOMMON) -Wno-format

$(BUILDDIR)/cdb_dumpraw: $(SRCDIR)/cdb_dumpraw.c
	$(CC) $(CFLAGS) -o $@ $^

$(BUILDDIR)/cdb_builddb: $(OBJDIR)/cdb_builddb.o $(BUILDDIR)/libcuttdb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LCOMMON)

$(BUILDDIR)/cuttdb-server: $(OBJDIR)/cuttdb-server.o $(OBJDIR)/server-thread.o $(BUILDDIR)/libcuttdb.a
	$(CC) -o $@ $^ $(LCOMMON)

$(BUILDDIR)/libcuttdb.so: $(OBJDIR) $(BUILDDIR) $(OBJS)
	$(CC) -shared -o $@ $(OBJS) $(LPROFILER) $(LCOMMON) 

$(BUILDDIR)/libcuttdb.a: $(OBJDIR) $(BUILDDIR) $(OBJS)
	ar cqs $@ $(OBJS)

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(OBJDIR):
	mkdir -p $(OBJDIR)

$(OBJDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) -o $@ $^ $(PROFILER)

clean:
	rm -rf $(OBJDIR) $(BUILDDIR)

cleanobj:
	rm -rf $(OBJDIR)

rebuild: clean all

install: library $(SRCDIR)/cuttdb.h
	cp $(BUILDDIR)/libcuttdb.a $(BUILDDIR)/libcuttdb.so /usr/lib/
	cp $(SRCDIR)/cuttdb.h /usr/include/
