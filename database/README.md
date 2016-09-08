Btree-source-code/database
==========================

A working project for High-concurrency B-tree/Database source code in C.

Compilation is achieved on linux or Windows by:

gcc -O3 -o dbtest *.c -lpthread

or

cl /Ox /Fe dbtest.exe *.c

The runtime options are:

    Usage: dbtest db_name cmds [page_bits leaf_xtra on_disk src_file1 src_file2 ... ]
      where db_name is the prefix name of the database file
      cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennys ort, with one character command for each input src_file. Commands with no input file need a placeholder.
      page_bits is the btree page size in bits
      leaf_xtra is the btree leaf page extra bits
      on_disk is 1 for OnDisk, 0 for InMemory
      src_file1 thru src_filen are files of keys separated by newline

Sample output from indexing/persisting 10000000 complete pennysort records (cmd 'w'):

    [root@test7x64 xlink]# cc -O3 -g -o dbtest database/*.c -lpthread
    [root@test7x64 xlink]# ./dbtest tstdb w 14 0 1 penny0
    started indexing for /home/xlink14/xlink/penny0
     real 0m42.706s
     user 0m40.067s
     sys  0m2.673s

    -rw-r--r-- 1 root root     131072 Sep  7 17:26 tstdb
    -rw-r--r-- 1 root root 2147090432 Sep  7 17:27 tstdb.index

Sample output from storing/indexing/persisting 10000000 pennysort records (1GB):

    [root@test7x64 xlink]# ./dbtest tstdb p 13 0 1 penny0
    started pennysort insert for /home/xlink14/xlink/penny0
     real 0m38.211s
     user 0m36.218s
     sys  0m2.023s

    -rw-r--r-- 1 root root 1073610752 Sep  7 22:37 tstdb
    -rw-r--r-- 1 root root  536477696 Sep  7 22:37 tstdb.index

Please address any concerns problems, or suggestions to the program author, Karl Malbrain, malbrain@cal.berkeley.edu

