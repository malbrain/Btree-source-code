#include <errno.h>
#include <string.h>

#include "db.h"
#include "db_map.h"
#include "db_api.h"

#ifndef unix
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>

double getCpuTime(int type)
{
FILETIME crtime[1];
FILETIME xittime[1];
FILETIME systime[1];
FILETIME usrtime[1];
SYSTEMTIME timeconv[1];
double ans = 0;

	memset (timeconv, 0, sizeof(SYSTEMTIME));

	switch( type ) {
	case 0:
		GetSystemTimeAsFileTime (xittime);
		FileTimeToSystemTime (xittime, timeconv);
		ans = (double)timeconv->wDayOfWeek * 3600 * 24;
		break;
	case 1:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (usrtime, timeconv);
		break;
	case 2:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (systime, timeconv);
		break;
	}

	ans += (double)timeconv->wHour * 3600;
	ans += (double)timeconv->wMinute * 60;
	ans += (double)timeconv->wSecond;
	ans += (double)timeconv->wMilliseconds / 1000;
	return ans;
}
#else
#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
struct rusage used[1];
struct timeval tv[1];

	switch( type ) {
	case 0:
		gettimeofday(tv, NULL);
		return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

	case 1:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

	case 2:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
	}

	return 0;
}
#endif

//  Interface function to create a document key
//	from a document and a key specifier

enum KeyType {
	pennySort,		// 10 char keys
	wholeRec		// whole record key
};

typedef struct {
	enum KeyType type;
} KeySpec;

uint16_t keyGenerator (uint8_t *key, void *obj, uint16_t objSize, void *spec, uint16_t specSize) {
KeySpec *keySpec = (KeySpec *)spec;
uint16_t keyLen = 0;

	switch (keySpec->type) {
	case pennySort:
#ifdef DEBUG
		if(objSize < 10)
			return fprintf(stderr, "pennysort KeyGenerator record too short: %d\n", objSize), 0;
#endif
		memcpy(key, obj, 10);
		return 10;

	case wholeRec:
		memcpy(key, obj, objSize);
		return objSize;
	}
	
#ifdef DEBUG
	fprintf(stderr, "KeyGenerator invalid KeyType: %d\n", keySpec->type);
#endif
	return 0;
}

typedef struct {
	char idx;
	char *type;
	char *infile;
	void *database;
	int bits, xtra, onDisk;
	int num;
} ThreadArg;

//  standalone program to index file of keys
//  then list them onto std-out

#ifdef unix
void *index_file (void *arg)
#else
unsigned __stdcall index_file (void *arg)
#endif
{
int line = 0, found = 0, cnt = 0, idx;
int ch, len = 0, slot, type = 0;
unsigned char key[4096];
ThreadArg *args = arg;
KeySpec keySpec[1];
uint64_t objId;
void *docStore;
ObjId txnId;
void *index;
int stat;
FILE *in;

	txnId.bits = 0;

	docStore = openDocStore(args->database, "documents", strlen("documents"), args->onDisk);

	if( args->idx < strlen (args->type) )
		ch = args->type[args->idx];
	else
		ch = args->type[strlen(args->type) - 1];

	switch(ch | 0x20)
	{
	case 'd':
		type = ch;

	case 'p':
		if( !type )
			type = ch;

		if( type == 'd' )
		  fprintf(stderr, "started pennysort delete for %s\n", args->infile);
		else
		  fprintf(stderr, "started pennysort insert for %s\n", args->infile);

		keySpec->type = pennySort;

		index = createIndex(docStore, "index0", strlen("index0"), keySpec, sizeof(keySpec), args->bits, args->xtra, args->onDisk);

		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %d\n", line);
#endif
			  line++;

			  if ((stat = addDocument (docStore, key, len, &objId, txnId)))
				  fprintf(stderr, "Add Error %d Line: %d\n", stat, line), exit(0);
			  len = 0;
			  continue;
			}

			else if( len < 4096 )
				key[len++] = ch;

//		fprintf(stderr, "finished %s for %d keys: %d reads %d writes %d found\n", args->infile, line, bt->reads, bt->writes, bt->found);
		break;

	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->infile);

		keySpec->type = wholeRec;

		index = createIndex(docStore, "index1", strlen("index1"), keySpec, sizeof(keySpec), args->bits, args->xtra, args->onDisk);

		if( in = fopen (args->infile, "r") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %d\n", line);
#endif
			  line++;

			  len += addObjId(key + len, line);
			  if ((stat = insertKey(index, key, len)))
				  fprintf(stderr, "Key Error %d Line: %d\n", stat, line), exit(0);
			  len = 0;
			}
			else if( len < 4096 )
				key[len++] = ch;

//		fprintf(stderr, "finished %s for %d keys: %d reads %d writes\n", args->infile, line, bt->reads, bt->writes);
		break;

/*	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( bt_findkey (bt, key, len, NULL, 0) == 0 )
				found++;
			  else if( bt->err )
				fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line), exit(0);
			  len = 0;
			}
			else if( len < 4096 )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, found %d: %d reads %d writes\n", args->infile, line, found, bt->reads, bt->writes);
		break;

	case 's':
		fprintf(stderr, "started scanning\n");
	  	do {
			if( set->latch = bt_pinlatch (bt, page_no, 1) )
				set->page = bt_mappage (bt, set->latch);
			else
				fprintf(stderr, "unable to obtain latch"), exit(1);
			bt_lockpage (bt, BtLockRead, set->latch);
			next = bt_getid (set->page->right);

			for( slot = 0; slot++ < set->page->cnt; )
			 if( next || slot < set->page->cnt )
			  if( !slotptr(set->page, slot)->dead ) {
				ptr = keyptr(set->page, slot);
				len = ptr->len;

			    if( slotptr(set->page, slot)->type == Duplicate )
					len -= BtId;

				fwrite (ptr->key, len, 1, stdout);
				val = valptr(set->page, slot);
				fwrite (val->value, val->len, 1, stdout);
				fputc ('\n', stdout);
				cnt++;
			   }

			bt_unlockpage (bt, BtLockRead, set->latch);
			bt_unpinlatch (set->latch);
	  	} while( page_no = next );

		fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
		break;

	case 'r':
		fprintf(stderr, "started reverse scan\n");
		if( slot = bt_lastkey (bt) )
	  	   while( slot = bt_prevkey (bt, slot) ) {
			if( slotptr(bt->cursor, slot)->dead )
			  continue;

			ptr = keyptr(bt->cursor, slot);
			len = ptr->len;

			if( slotptr(bt->cursor, slot)->type == Duplicate )
				len -= BtId;

			fwrite (ptr->key, len, 1, stdout);
			val = valptr(bt->cursor, slot);
			fwrite (val->value, val->len, 1, stdout);
			fputc ('\n', stdout);
			cnt++;
		  }

		fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
		break;

	case 'c':
#ifdef unix
		posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
		fprintf(stderr, "started counting\n");
		page_no = LEAF_page;

		while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
			if( bt_readpage (bt->mgr, bt->frame, page_no) )
				break;

			if( !bt->frame->free && !bt->frame->lvl )
				cnt += bt->frame->act;

			bt->reads++;
			page_no++;
		}
		
	  	cnt--;	// remove stopper key
		fprintf(stderr, " Total keys counted %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
		break;
*/
	}

#ifdef unix
	return NULL;
#else
	return 0;
#endif
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
int idx, cnt, len, slot, err;
int xtra = 0, bits = 16;
double start, stop;
#ifdef unix
pthread_t *threads;
#else
SYSTEM_INFO info[1];
HANDLE *threads;
#endif
ThreadArg *args;
float elapsed;
int num = 0;
char key[1];
bool onDisk = true;
void *database;
void *docStore;
void *index;

#ifdef _WIN32
	GetSystemInfo(info);
	fprintf(stderr, "PageSize: %d, # Processors: %d, Allocation Granularity: %d\n\n", info->dwPageSize, info->dwNumberOfProcessors, info->dwAllocationGranularity);
#endif
	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s db_name cmds [page_bits leaf_xtra on_disk src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where db_name is the prefix name of the database file\n");
		fprintf (stderr, "  cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort, with one character command for each input src_file. Commands with no input file need a placeholder.\n");
		fprintf (stderr, "  page_bits is the btree page size in bits\n");
		fprintf (stderr, "  leaf_xtra is the btree leaf page extra bits\n");
		fprintf (stderr, "  on_disk is 1 for OnDisk, 0 for InMemory\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
		exit(0);
	}

	initialize();

	start = getCpuTime(0);

	if( argc > 3 )
		bits = atoi(argv[3]);

	if( argc > 4 )
		xtra = atoi(argv[4]);

	if( argc > 5 )
		onDisk = atoi(argv[5]);

	cnt = argc - 6;
#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc (cnt * sizeof(ThreadArg));

	database = openDatabase(argv[1], strlen(argv[1]), onDisk);

	//	fire off threads

	for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 6];
		args[idx].database = database;
		args[idx].onDisk = onDisk;
		args[idx].type = argv[2];
		args[idx].bits = bits;
		args[idx].xtra = xtra;
		args[idx].num = num;
		args[idx].idx = idx;
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
		  fprintf(stderr, "Error creating thread %d\n", err);
#else
		while ( (int64_t)(threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, index_file, args + idx, 0, NULL)) < 0LL)
		  fprintf(stderr, "Error creating thread errno = %d\n", errno);

#endif
	}

	// 	wait for termination

#ifdef unix
	for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);
#else
	WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	for( idx = 0; idx < cnt; idx++ )
		CloseHandle(threads[idx]);
#endif

	elapsed = getCpuTime(0) - start;
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

}
