Btree-source-code
=================

A working project for High-concurrency B-tree source code in C.  You probably want to download the separate database project for the latest version.  Most C files compile under both Windows and Linux.

New B-tree page management code using a skiplist instead of sorted arrays for key searching is under development in a sub-directory.  This approach removes the need for a latching protocol for page reading and writing.

Note:  The most recent running multi-threaded/multi-process B-tree and ARTree code can be found in a separate repository:  https://github.com/malbrain/database.

Here are the projects under btree-source-code:

* btree2:		Single Threaded/MultiProcess versions that remove keys all the way back to an original empty btree, placing removed nodes on a free list.  Operates under either memory mapping or file I/O.  Recommended btrees hosted on network file systems.

* threads2:		Multi-Threaded with latching implemented by a latch manager with test & set latches in the first few btree pages.

* threadskv:	Multi-Threaded/Multi-Process based on threads2i.c that generalizes key/value storage in the btree pages. The page slots are reduced to 16 or 32 bits, and the value byte storage occurs along with the key storage.
