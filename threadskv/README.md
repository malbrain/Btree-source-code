Btree-source-code/threadskv
===========================

A working project for High-concurrency B-tree source code in C.  You probably want to download threadskv10h.c for the latest developement version.

Here are files in the threadskv project:

threadskv1.c	Multi-Threaded/Multi-Process based on threads2i.c that generalizes key/value storage in the btree pages. The page slots are reduced to 16 or 32 bits, and the value byte storage occurs along with the key storage.

threadskv2.c	Multi-Threaded/Multi-Process based on threadskv1 that replaces the linear sorted key array with a red/black tree.

threadskv3.c	Multi-Threaded/Multi-Process based on threadskv1 that introduces librarian filler slots in the linear key array to minimize data movement when a new key is inserted into the middle of the array.

threadskv4b.c	Multi-Threaded/Multi-Process based on threadskv3 that manages duplicate keys added to the btree.

threadskv5.c	Multi-Threaded/Multi-Process based on threadskv4b that supports bi-directional cursors through the btree. Also supports raw disk partitions for btrees.

threadskv6.c	Multi-Threaded/Single-Process with traditional buffer pool manager using the swap device.  Based on threadskv5 and btree2u.

threadskv7.c	Multi-Threaded/Single-Process with atomic add of a set of keys under eventual consistency.  Adds an individual key lock manager.

threadskv8.c	Multi-Threaded/Single-Process with atomic-consistent add of a set of keys based on threadskv6.c.  Uses btree page latches as locking granularity.

threadskv10g.c	Multi-Threaded/Multi-Process with 2 Log-Structured-Merge (LSM) btrees based on threadskv8.c. Also adds dual leaf/interior node page sizes for each btree. Includes an experimental recovery redo log. This file is linux only.

threadskv10h.c	Multi-Threaded/Multi-Process with 2 Log-Structured-Merge (LSM) btrees based on threadskv8.c. Also adds dual leaf/interior node page sizes for each btree. This file is linux only.

Compilation is achieved on linux or Windows by:

gcc -D STANDALONE -O3 threadskv10h.c -lpthread

or

cl /Ox /D STANDALONE threadskv10h.c

Please see the project wiki page for additional documentation
