Btree-source-code
=================

A working project for High-concurrency B-tree source code in C.  You probably want to download the database project for the latest developement version.  Most C files compile under both Windows and Linux.

Here are the projects under btree-source-code:

    * database:		Multi-Threaded/Multi-Process multiple MVCC document store with transactions and automatic index management.  Cursors enumerate documents in key order.|

    * btree2:		Single Threaded/MultiProcess versions that remove keys all the way back to an original empty btree, placing removed nodes on a free list.  Operates under either memory mapping or file I/O.  Recommended btrees hosted on network file systems.

    * threads2:		Multi-Threaded/Multi-Process with latching implemented by a latch manager with test & set latches in the first few btree pages.

    * threadskv:	Multi-Threaded/Multi-Process based on threads2i.c that generalizes key/value storage in the btree pages. The page slots are reduced to 16 or 32 bits, and the value byte storage occurs along with the key storage.
