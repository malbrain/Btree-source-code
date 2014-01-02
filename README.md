Btree-source-code
=================

A working project for High-concurrency B-tree source code in C

There are three major code sets in the btree source code:

btree2q.c       Single Threaded/MultiProcess with latching supplied by advisory file locking.  Works with network file systems.
threads2h.c     Multi-Threaded/Multi-Process with latching implemented with test & set locks in the btree pages.

The Foster set includes three types of latching:

Fosterbtreee.c  Multi-Threaded/Single Process with latches hosted by the buffer pool manager
Fosterbtreef.c  Multi-Threaded/Multi-Process with latches hosted by a latch manager using the first few pages of the btree.
Fosterbtreeg.c  Multi-Threaded/Multi-Process with latches implemented with test & set locks in the btree pages.

Compilation is achieved on linux or Windows by:

gcc -D STANDALONE threads2h.c -lpthread

or

cl /D STANDALONE threads2h.c

Please see the project home page at code.google.com/p/high-concurrency-btree
