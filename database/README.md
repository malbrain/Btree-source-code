Btree-source-code/database
==========================

A working project for High-concurrency B-tree/Database source code in C.

Compile with ./build or build.bat

The runtime options are:

    Usage: dbtest db_name cmds idx_type [page_bits leaf_xtra on_disk src_file1 src_file2 ... ]
      where db_name is the prefix name of the database file
      cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort, with one character command for each input src_file.
      idx_type is 0 for ARTree or 1 for btree
      page_bits is the btree page size in bits
      leaf_xtra is the btree leaf page extra bits
      on_disk is 1 for OnDisk, 0 for InMemory
      src_file1 thru src_filen are files of keys separated by newline

Linux compilation command:

    [root@test7x64 xlink]# cc -O3 -g -o dbtest db*.c artree/*.c btree1/*.c -lpthread

Sample single thread output from indexing 40M pennysort keys (cmd 'w'):

    [root@test7x64 xlink]# ./dbtest tstdb w 0 14 0 1 pennykeys0-3
    started indexing for pennykeys0-3
     real 0m33.022s
     user 0m28.067s
     sys  0m4.048s

    -rw-r--r-- 1 root root 4294967296 Sep 16 22:22 ARTreeIdx

Sample multiple thread output from indexing 40M pennysort keys (cmd 'w'):

    [root@test7x64 xlink]# ./dbtest tstdb w 0 14 0 1 pennykeys[0123]
    started indexing for pennykeys0
    started indexing for pennykeys1
    started indexing for pennykeys2
    started indexing for pennykeys3
     real 0m14.716s
     user 0m41.705s
     sys  0m12.839s

Sample output from storing/indexing/persisting 40M pennysort records (4GB):

    [root@test7x64 xlink]# ./dbtest tstdb p 0 13 0 1 penny0-3
    started pennysort insert for penny0-3
     real 1m53.100s
     user 0m55.810s
     sys  0m15.550s

    -rw-rw-r-- 1 karl karl    4194304 Oct  5 06:41 tstdb
    -rw-rw-r-- 1 karl karl 8589934592 Oct  5 06:43 tstdb.documents
    -rw-rw-r-- 1 karl karl 4294967296 Oct  5 06:43 tstdb.documents.ARTreeIdx

Sample output with four concurrent threads each storing 10M pennysort records:

    [root@test7x64 xlink]# ./dbtest tstdb p 1 14 0 1 penny[0123]
    started pennysort insert for penny0
    started pennysort insert for penny1
    started pennysort insert for penny2
    started pennysort insert for penny3
     real 0m42.312s
     user 2m20.475s
     sys  0m12.352s
 
    -rw-r--r-- 1 root root    1048576 Sep 16 22:15 tstdb
    -rw-r--r-- 1 root root 8589934592 Sep 16 22:16 tstdb.documents
    -rw-r--r-- 1 root root 2147483648 Sep 16 22:16 tstdb.documents.Btree1Idx

Sample cursor scan output and sort check of 40M pennysort records:

    [root@test7x64 xlink]# export LC_ALL=C
    [root@test7x64 xlink]# ./dbtest tstdb s 1 | sort -c
    started scanning
     Total keys read 40000000
     real 0m28.190s
     user 0m22.578s
     sys  0m4.005s

Sample cursor scan count of 40M pennysort records:

    [root@test7x64 xlink]# ./dbtest tstdb c 1
    started counting
     Total keys counted 40000000
     real 0m20.568s
     user 0m18.457s
     sys  0m2.123s

Please address any concerns problems, or suggestions to the program author, Karl Malbrain, malbrain@cal.berkeley.edu
