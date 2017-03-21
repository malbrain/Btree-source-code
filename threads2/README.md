Btree-source-code/threads2
==========================

Here are files in the threads2 project:

threads2i.c     Multi-Threaded with latching implemented by a latch manager with test & set latches in the first few btree pages with thread yield system calls during contention.

threads2j.c     Multi-Threaded with latching implemented by a latch manager with test & set locks in the first few btree pages with Linux futex system calls during contention.

Compilation is achieved on linux or Windows by:

gcc -D STANDALONE -O3 threads2i.c

or

cl /Ox /D STANDALONE threads2i.c
