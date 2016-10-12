Btree-source-code/database
==========================

A working project for High-concurrency B-tree/Database source code in C.

Compile with ./build or build.bat

The runtime options are:

    Usage: dbtest db_name -cmds=[crwsdf]... -type=[012] -bits=# -xtra=# -onDisk -txns -noDocs -keyLen=# src_file1 src_file2 ... ]
      where db_name is the prefix name of the database file
      cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind, with a one character command for each input src_file, or a no-input command.
      type is the type of index: 0 = ART, 1 = btree1, 2 = btree2
      keyLen is key size, zero for whole line
      bits is the btree page size in bits
      xtra is the btree leaf page extra bits
      onDisk specifies resides in disk file
      noDocs specifies keys only
      txns indicates use of transactions
      src_file1 thru src_filen are files of keys/documents separated by newline

Linux compilation command:

    [karl@test7x64 xlink]# cc -O2 -g -o dbtest standalone.c db*.c artree/*.c btree1/*.c -lpthread

Sample single thread output from indexing 40M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey0-3
    started indexing for pennykey0-3
     real 0m35.022s
     user 0m31.067s
     sys  0m4.048s

    -rw-r--r-- 1 karl engr 4294967296 Sep 16 22:22 ARTreeIdx

Sample four thread output from indexing 40M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey[0123]
    started indexing for pennykey0
    started indexing for pennykey1
    started indexing for pennykey2
    started indexing for pennykey3
     real 0m14.716s
     user 0m41.705s
     sys  0m12.839s

Sample single thread output from indexing 80M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey0-7
    started indexing for pennykey0-7
     real 1m29.627s
     user 1m10.717s
     sys  0m10.965s

Sample eight thread output from indexing 80M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey[01234567]
    started indexing for pennykey0
    started indexing for pennykey1
    started indexing for pennykey2
    started indexing for pennykey3
    started indexing for pennykey4
    started indexing for pennykey5
    started indexing for pennykey6
    started indexing for pennykey7
     real 0m46.590s
     user 1m51.374s
     sys  1m3.939s

Sample output from storing/indexing/persisting 40M pennysort records (4GB):

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -keyLen=10 penny0-3
    started indexing for penny0-3
     real 3m52.100s
     user 1m5.810s
     sys  0m23.550s

    -rw-rw-r-- 1 karl engr    4194304 Oct  5 06:41 tstdb
    -rw-rw-r-- 1 karl engr 8589934592 Oct  5 06:43 tstdb.documents
    -rw-rw-r-- 1 karl engr 4294967296 Oct  5 06:43 tstdb.documents.ARTreeIdx

Sample output with four concurrent threads each storing 10M pennysort records:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -keyLen=10 penny[0123]
    started pennysort insert for penny0
    started pennysort insert for penny1
    started pennysort insert for penny2
    started pennysort insert for penny3
     real 0m42.312s
     user 2m20.475s
     sys  0m12.352s
 
    -rw-r--r-- 1 karl engr    1048576 Sep 16 22:15 tstdb
    -rw-r--r-- 1 karl engr 8589934592 Sep 16 22:16 tstdb.documents
    -rw-r--r-- 1 karl engr 2147483648 Sep 16 22:16 tstdb.documents.Btree1Idx

Sample cursor scan output and sort check of 40M pennysort records:

    [karl@test7x64 xlink]# export LC_ALL=C
    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=s
    started scanning
     Total keys read 40000000
     real 0m28.190s
     user 0m22.578s
     sys  0m4.005s

Sample cursor scan count of 40M pennysort records:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=c
    started counting
     Total keys counted 40000000
     real 0m20.568s
     user 0m18.457s
     sys  0m2.123s

Sample cursor scan with min and max values:

    [karl@test7x64 xlink]$ ./dbtest tstdb -cmds=s -startKey=aaaA -endKey=aaaK -noDocs
    started scanning min key: aaaA max key: aaaK
    aaaATP)O4j
    aaaBx&\7,4
    aaaE%(2YNR
    aaaF~E 1:w
    aaaG?n!En5
    aaaGQBoH:`
    aaaGtu4)28
    aaaGy2q2wI
    aaaH8EX{6k
    aaaJb'}Wk_
     Total keys read 10
     real 0m0.000s
     user 0m0.000s
     sys  0m0.001s

Please address any concerns problems, or suggestions to the program author, Karl Malbrain, malbrain@cal.berkeley.edu
