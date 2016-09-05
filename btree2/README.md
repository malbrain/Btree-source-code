Btree-source-code/btree2
========================

A working project for High-concurrency B-tree source code in C.

Here are files in the btree source code:

btree2s.c       Single Threaded/MultiProcess version that removes keys all the way back to an original empty btree, placing removed nodes on a free list.  Operates under either memory mapping or file I/O.  Recommended btrees hosted on network file systems.

btree2t.c       Single Threaded/MultiProcess version similar to btree2s except that fcntl locking has been replaced by test & set latches in the first few btree pages.  Uses either memory mapping or file I/O.

btree2u.c		Single Threaded/MultiProcess version that implements a traditional buffer pool manager in the first n pages of the btree file.  The buffer pool accesses its pages with mmap.  Evicted pages are written back to the btree file from the buffer pool pages with pwrite. Recommended for linux.

btree2v.c		Single Threaded/MultiProcess version based on btree2u that utilizes linux only futex calls for latch contention.

Compilation is achieved on linux or Windows by:

gcc -D STANDALONE -O3 btree2*.c

or

cl /Ox /D STANDALONE btree2*.c
