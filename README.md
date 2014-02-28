Btree-source-code
=================

A working project for High-concurrency B-tree source code in C

There are six files in the btree source code:

btree2s.c       Single Threaded/MultiProcess version that removes keys all the way back to an original empty btree, placing removed nodes on a free list.  Operates under either memory mapping or file I/O.  Recommended btrees hosted on network file systems.

btree2t.c       Single Threaded/MultiProcess version similar to btree2s except that fcntl locking has been replaced by test & set latches in the first few btree pages.  Uses either memory mapping or file I/O.

btree2u.c		Single Threaded/MultiProcess version that implements a traditional buffer pool manager in the first n pages of the btree file.  The buffer pool accesses its pages with mmap.  Evicted pages are written back to the btree file from the buffer pool pages with pwrite. Recommended for linux.

threads2h.c     Multi-Threaded/Multi-Process with latching implemented by a latch manager with pthreads/SRW latches in the first few btree pages. Recommended for Windows.

threads2i.c     Multi-Threaded/Multi-Process with latching implemented by a latch manager with test & set latches in the first few btree pages with thread yield system calls during contention.

threads2j.c     Multi-Threaded/Multi-Process with latching implemented by a latch manager with test & set locks in the first few btree pages with Linux futex system calls during contention.

Compilation is achieved on linux or Windows by:

gcc -D STANDALONE threads2h.c -lpthread

or

cl /Ox /D STANDALONE threads2h.c

Please see the project wiki page for additional documentation
