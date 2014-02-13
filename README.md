CuttDB
========
A hash-based key-value database for persistent storing massive small records, initially designed to be a indexed repository for ten to hundreds of millions URLs, web pages and small documents.


Features
--------
 - It is a database library to be embeded into other programs.
 - Keys and values are arbitrary byte arrays, value is retrieved by key.
 - Very good performance, especially for insert operation, even when stored massive amount of records.
 - Designed sophisticated cache on both index pages and records to take use of memory better.
 - Data on disk is always write ahead to minimize the possibility of data loss.
 - Bloom filter is included to accelerate the query on inexistent record
 - Record expiration and space recycle are supported.
 - Multithreading is supported.
 - Server side with memcached protocols(set/add/replace/get/delete) is also supported now.

Limitations
-----------
 - SQL or relation data model is not supported.
 - The index structure is based on hash, so prefix query or ordered iteration is not supported.
 - Transaction is not supported.
 - Update operation(set for exist record) is less efficient

Performance
-----------
Simple test on a machine with Core 2 Duo E4500@2.2GHz/8GB RAM/7200RPM SATA/Linux 3.2.0-25
```sh
 Insert 200,000,000 records, key and value are both 8-byte strings:
 Overall: 200000000 / 200000000     (458.941 s, 435784 ops)
 Now program consumes 2.0 GB ram.


 Clean OS cache by 'echo 3 > /proc/sys/vm/drop_cache'.
 Retrieve 50000 records randomly:
 Overall: 50000 / 50000     (459.279 s, 108 ops)


 Retrieve these records again: 
 Overall: 50000 / 50000     (0.037 s, 1315789 ops)
 ```

a detailed test result please refer to [200,000,000 benchmark][benchmark].

Usage
-----
### compile & use the library
```sh
$ git clone "http://cuttdb.googlecode.com/svn/trunk/" cuttdb
$ cd cuttdb/src ; make ; sudo make install
$ vim test.c
$ gcc test.c -lcuttdb; mkdir testdb; ./a.out
```
contents in test.c :
```c
#include <cuttdb.h>
#include <stdio.h>

int main()
{
    CDB *db = cdb_new();
    cdb_option(db, 20000, 16, 16);
    if (cdb_open(db, "./testdb/", CDB_CREAT) < 0) {
        printf("Open Failed\n");
        return -1;
    }
    cdb_set(db, "key1", 4, "HELLO1\0", 7);

    char *val; int vsize;
    cdb_get(db, "key1", 4, (void**)&val, &vsize);
    printf("value for key1:%s[%d]\n", val, vsize);
    cdb_free_val((void**)&val);
    cdb_destroy(db);
}
```

### Server version:
```sh
$ ./cuttdb-server -H /data/testdb -r 0 -P 1024 -n 100000 -d -t 4
$ telnet 127.0.0.1 8964
 

Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
set test_key1 0 0 10
test_value
STORED
get test_key1
VALUE test_key1 0 10
test_value
END
delete test_key1
DELETED
get test_key1
END
```

The program is currently used in web crawler at cutt.com 

[benchmark]:http://cuttdb.googlecode.com/files/benchmark-20121025-200000000-8-8.result.txt
