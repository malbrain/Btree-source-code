Btree-source-code/database
==========================

A working project for High-concurrency B-tree/Database source code in C.

Compile with ./build or build.bat

The runtime options are:

    Usage: dbtest db_name cmds [page_bits leaf_xtra on_disk src_file1 src_file2 ... ]
      where db_name is the prefix name of the database file
      cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennys ort, with one character command for each input src_file. Commands with no input file need a placeholder.
      page_bits is the btree page size in bits
      leaf_xtra is the btree leaf page extra bits
      on_disk is 1 for OnDisk, 0 for InMemory
      src_file1 thru src_filen are files of keys separated by newline

Sample output from indexing/persisting 10M complete pennysort records (cmd 'w'):

    [root@test7x64 xlink]# cc -O3 -g -o dbtest database/*.c -lpthread
    [root@test7x64 xlink]# ./dbtest tstdb w 14 0 1 penny0
    started indexing for penny0
     real 0m42.706s
     user 0m40.067s
     sys  0m2.673s

    -rw-r--r-- 1 root root    1048576 Sep 16 22:21 tstdb
    -rw-r--r-- 1 root root    1048576 Sep 16 22:21 tstdb.documents
    -rw-r--r-- 1 root root 2147483648 Sep 16 22:22 tstdb.documents.index1

Sample output from storing/indexing/persisting 10M pennysort records (1GB):

    [root@test7x64 xlink]# ./dbtest tstdb p 13 0 1 penny0
    started pennysort insert for penny0
     real 0m28.211s
     user 0m25.218s
     sys  0m2.023s

    -rw-r--r-- 1 root root    1048576 Sep 16 22:19 tstdb
    -rw-r--r-- 1 root root 2147483648 Sep 16 22:19 tstdb.documents
    -rw-r--r-- 1 root root  536870912 Sep 16 22:19 tstdb.documents.index0

Sample output from indexing/persisting 10M complete pennysort records (cmd 'w') InMemory:

    [root@test7x64 xlink]# ./dbtest tstdb w 14 0 0 penny0
    started indexing for penny0
     real 0m40.065s
     user 0m38.730s
     sys  0m1.368s

Sample output from storing/indexing/persisting 10M pennysort records (1GB) inMemory:

    [root@test7x64 xlink]# ./dbtest tstdb p 14 0 0 penny0
    started pennysort insert for penny0
     real 0m35.829s
     user 0m34.863s
     sys  0m0.987s

Sample output with four concurrent threads each storing 10M pennysort records:

    [root@test7x64 xlink]# ./dbtest tstdb p 14 0 1 penny[0123]
    started pennysort insert for penny0
    started pennysort insert for penny1
    started pennysort insert for penny2
    started pennysort insert for penny3
     real 0m42.312s
     user 2m20.475s
     sys  0m12.352s
 
    -rw-r--r-- 1 root root    1048576 Sep 16 22:15 tstdb
    -rw-r--r-- 1 root root 8589934592 Sep 16 22:16 tstdb.documents
    -rw-r--r-- 1 root root 2147483648 Sep 16 22:16 tstdb.documents.index0

Sample cursor scan output and sort check of 40M pennysort records:

    [root@test7x64 xlink]# export LC_ALL=C
    [root@test7x64 xlink]# ./dbtest tstdb s 14 0 1 x | sort -c
    started scanning
     Total keys read 40000000
     real 0m28.190s
     user 0m22.578s
     sys  0m4.005s

Please address any concerns problems, or suggestions to the program author, Karl Malbrain, malbrain@cal.berkeley.edu
