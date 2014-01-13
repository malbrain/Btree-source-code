// foster btree version g
// 02 JAN 2014

// author: karl malbrain, malbrain@cal.berkeley.edu

/*
This work, including the source code, documentation
and related data, is placed into the public domain.

The orginal author is Karl Malbrain.

THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
RESULTING FROM THE USE, MODIFICATION, OR
REDISTRIBUTION OF THIS SOFTWARE.
*/

// Please see the project home page for documentation
// code.google.com/p/high-concurrency-btree

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#ifdef linux
#define _GNU_SOURCE
#endif

#ifdef unix
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <process.h>
#include <intrin.h>
#endif

#include <memory.h>
#include <string.h>

typedef unsigned long long	uid;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw

#define BT_maxbits		24					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

/*
There are five lock types for each node in three independent sets: 
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete. 
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent. 
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
5. (set 3) ParentLock: Exclusive. Have parent adopt/delete maximum foster child from the node.
*/

typedef enum{
	BtLockAccess,
	BtLockDelete,
	BtLockRead,
	BtLockWrite,
	BtLockParent
}BtLock;

//	Define the length of the page and key pointers

#define BtId 6

//	Page key slot definition.

//	If BT_maxbits is 15 or less, you can save 4 bytes
//	for each key stored by making the first two uints
//	into ushorts.  You can also save 4 bytes by removing
//	the tod field from the key.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence key (highest key) for
//	the page is always present, even after cleanup.

typedef struct {
	uint off:BT_maxbits;		// page offset for key start
	uint dead:1;				// set for deleted key
	uint tod;					// time-stamp for key
	unsigned char id[BtId];		// id associated with key
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the value
//	bytes.

typedef struct {
	unsigned char len;
	unsigned char key[1];
} *BtKey;

//	mode & definition for spin latch implementation

enum {
	Mutex = 1,
	Write = 2,
	Pending = 4,
	Share = 8
} LockMode;

// mutex locks the other fields
// exclusive is set for write access
// share is count of read accessors

typedef struct {
	volatile ushort mutex:1;
	volatile ushort exclusive:1;
	volatile ushort pending:1;
	volatile ushort share:13;
} BtSpinLatch;

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct Page {
	BtSpinLatch readwr[1];		// read/write lock
	BtSpinLatch access[1];		// access intent lock
	BtSpinLatch parent[1];		// parent SMO lock
	ushort foster;				// count of foster children
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	unsigned char bits;			// page size in bits
	unsigned char lvl:6;		// level of page
	unsigned char kill:1;		// page is being deleted
 	unsigned char dirty:1;		// page needs to be cleaned
	unsigned char right[BtId];	// page number to right
} *BtPage;

//	The memory mapping pool table buffer manager entry

typedef struct {
	unsigned long long int lru;	// number of times accessed
	uid  basepage;				// mapped base page number
	char *map;					// mapped memory pointer
	volatile ushort pin;		// mapped page pin counter
	ushort slot;				// slot index in this array
	void *hashprev;				// previous pool entry for the same hash idx
	void *hashnext;				// next pool entry for the same hash idx
#ifndef unix
	HANDLE hmap;				// Windows memory mapping handle
#endif
} BtPool;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// page size	
	uint page_bits;				// page size in bits	
	uint seg_bits;				// seg size in pages in bits
	uint mode;					// read-write mode
#ifdef unix
	int idx;
	char *pooladvise;			// bit maps for pool page advisements
#else
	HANDLE idx;
#endif
	volatile ushort poolcnt;	// highest page pool node in use
	volatile ushort evicted;	// last evicted hash table slot
	ushort poolmax;				// highest page pool node allocated
	ushort poolmask;			// total size of pages in mmap segment - 1
	ushort hashsize;			// size of Hash Table for pool entries
	ushort *hash;				// hash table of pool entries
	BtPool *pool;				// memory pool page segments
	BtSpinLatch *latch;			// latches for pool hash slots
#ifndef unix
	HANDLE halloc, hlatch;		// allocation and latch table handles
#endif
} BtMgr;

typedef struct {
	BtMgr *mgr;			// buffer manager for thread
	BtPage temp;		// temporary frame buffer (memory mapped/file IO)
	BtPage alloc;		// frame buffer for alloc page ( page 0 )
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	BtPage zero;		// page frame for zeroes at end of file
	BtPage page;		// current page
	uid page_no;		// current page number	
	uid cursor_page;	// current cursor page number	
	unsigned char *mem;	// frame, cursor, page memory buffer
	int err;			// last error
} BtDb;

typedef enum {
	BTERR_ok = 0,
	BTERR_struct,
	BTERR_ovflw,
	BTERR_lock,
	BTERR_map,
	BTERR_wrt,
	BTERR_hash,
	BTERR_latch
} BTERR;

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (BtMgr *mgr);
extern BTERR  bt_insertkey (BtDb *bt, unsigned char *key, uint len, uid id, uint tod);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
extern uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint mode, uint bits, uint poolsize, uint segsize, uint hashsize);
void bt_mgrclose (BtMgr *mgr);

//  Helper functions to return cursor slot values

extern BtKey bt_key (BtDb *bt, uint slot);
extern uid bt_uid (BtDb *bt, uint slot);
extern uint bt_tod (BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0	// allocation of new pages
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves

//	Number of levels to create in a new BTree

#define MIN_lvl			2

//  The page is allocated from low and hi ends.
//  The key offsets and row-id's are allocated
//  from the bottom, while the text of the key
//  is allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65534), and up to 253 bytes
//  of key value.  Duplicate keys are discarded.
//  Associated with each key is a 48 bit row-id.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup The fence key for a node is always
//	present, even after deletion and cleanup.

//  Groups of pages called segments from the btree are
//  cached with memory mapping. A hash table is used to keep
//  track of the cached segments.  This behaviour is controlled
//  by the cache block size parameter to bt_open.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question.

//	An adoption traversal leaves the parent node locked as the
//	tree is traversed to the level in quesiton.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page

#define slotptr(page, slot) (((BtSlot *)(page+1)) + (slot-1))
#define keyptr(page, slot) ((BtKey)((unsigned char*)(page) + slotptr(page, slot)->off))

void bt_putid(unsigned char *dest, uid id)
{
int i = BtId;

	while( i-- )
		dest[i] = (unsigned char)id, id >>= 8;
}

uid bt_getid(unsigned char *src)
{
uid id = 0;
int i;

	for( i = 0; i < BtId; i++ )
		id <<= 8, id |= *src++; 

	return id;
}

//	wait until write lock mode is clear
//	and add 1 to the share count

void bt_spinreadlock(BtSpinLatch *latch)
{
ushort prev;

  do {
#ifdef unix
	while( __sync_fetch_and_or((ushort *)latch, Mutex) & Mutex )
		sched_yield();
#else
	while( _InterlockedOr16((ushort *)latch, Mutex) & Mutex )
		SwitchToThread();
#endif
	//  see if exclusive request is granted or pending

	if( prev = !(latch->exclusive | latch->pending) )
#ifdef unix
		__sync_fetch_and_add((ushort *)latch, Share);
#else
		_InterlockedExchangeAdd16 ((ushort *)latch, Share);
#endif

#ifdef unix
	__sync_fetch_and_and ((ushort *)latch, ~Mutex);
#else
	_InterlockedAnd16((ushort *)latch, ~Mutex);
#endif

	if( prev )
		return;
#ifdef  unix
  } while( sched_yield(), 1 );
#else
  } while( SwitchToThread(), 1 );
#endif
}

//	wait for other read and write latches to relinquish

void bt_spinwritelock(BtSpinLatch *latch)
{
ushort prev;

  do {
#ifdef  unix
	while( __sync_fetch_and_or((ushort *)latch, Mutex | Pending) & Mutex )
		sched_yield();
#else
	while( _InterlockedOr16((ushort *)latch, Mutex | Pending) & Mutex )
		SwitchToThread();
#endif
	if( prev = !(latch->share | latch->exclusive) )
#ifdef unix
		__sync_fetch_and_or((ushort *)latch, Write);
#else
		_InterlockedOr16((ushort *)latch, Write);
#endif

#ifdef unix
	__sync_fetch_and_and ((ushort *)latch, ~(Mutex | Pending));
#else
	_InterlockedAnd16((ushort *)latch, ~(Mutex | Pending));
#endif

	if( prev )
		return;
#ifdef  unix
	sched_yield();
#else
	SwitchToThread();
#endif
  } while( 1 );
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 otherwise

int bt_spinwritetry(BtSpinLatch *latch)
{
ushort prev;

#ifdef unix
	if( prev = __sync_fetch_and_or((ushort *)latch, Mutex), prev & Mutex )
		return 0;
#else
	if( prev = _InterlockedOr16((ushort *)latch, Mutex), prev & Mutex )
		return 0;
#endif
	//	take write access if all bits are clear

	if( !prev )
#ifdef unix
		__sync_fetch_and_or ((ushort *)latch, Write);
#else
		_InterlockedOr16((ushort *)latch, Write);
#endif
	return !prev;
}

//	clear write mode

void bt_spinreleasewrite(BtSpinLatch *latch)
{
#ifdef unix
	__sync_fetch_and_and ((ushort *)latch, ~Write);
#else
	_InterlockedAnd16((ushort *)latch, ~Write);
#endif
}

//	decrement reader count

void bt_spinreleaseread(BtSpinLatch *latch)
{
#ifdef unix
	__sync_fetch_and_add((ushort *)latch, -Share);
#else
	_InterlockedExchangeAdd16 ((ushort *)latch, -Share);
#endif
}

void bt_mgrclose (BtMgr *mgr)
{
BtPool *pool;
uint slot;

	// release mapped pages
	//	note that slot zero is never used

	for( slot = 1; slot < mgr->poolmax; slot++ ) {
		pool = mgr->pool + slot;
		if( pool->slot )
#ifdef unix
			munmap (pool->map, (mgr->poolmask+1) << mgr->page_bits);
#else
		{
			FlushViewOfFile(pool->map, 0);
			UnmapViewOfFile(pool->map);
			CloseHandle(pool->hmap);
		}
#endif
	}

#ifdef unix
	close (mgr->idx);
	free (mgr->pool);
	free (mgr->hash);
	free (mgr->pooladvise);
	free (mgr);
#else
	FlushFileBuffers(mgr->idx);
	CloseHandle(mgr->idx);
	GlobalFree (mgr->pool);
	GlobalFree (mgr->hash);
	GlobalFree (mgr);
#endif
}

//	close and release memory

void bt_close (BtDb *bt)
{
#ifdef unix
	if ( bt->mem )
		free (bt->mem);
#else
	if ( bt->mem)
		VirtualFree (bt->mem, 0, MEM_RELEASE);
#endif
	free (bt);
}

//  open/create new btree buffer manager

//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of mapped page pool (e.g. 8192)

BtMgr *bt_mgr (char *name, uint mode, uint bits, uint poolmax, uint segsize, uint hashsize)
{
uint lvl, attr, cacheblk, last, slot, idx;
BtPage alloc;
int lockmode;
off64_t size;
uint amt[1];
BtMgr* mgr;
BtKey key;

#ifndef unix
SYSTEM_INFO sysinfo[1];
#endif

	// determine sanity of page size and buffer pool

	if( bits > BT_maxbits )
		bits = BT_maxbits;
	else if( bits < BT_minbits )
		bits = BT_minbits;

	if( !poolmax )
		return NULL;	// must have buffer pool

#ifdef unix
	mgr = calloc (1, sizeof(BtMgr));

	switch (mode & 0x7fff)
	{
	case BT_rw:
		mgr->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);
		lockmode = 1;
		break;

	case BT_ro:
	default:
		mgr->idx = open ((char*)name, O_RDONLY);
		lockmode = 0;
		break;
	}
	if( mgr->idx == -1 )
		return free(mgr), NULL;
	
	cacheblk = 4096;	// minimum mmap segment size for unix

#else
	mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtMgr));
	attr = FILE_ATTRIBUTE_NORMAL;
	switch (mode & 0x7fff)
	{
	case BT_rw:
		mgr->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);
		lockmode = 1;
		break;

	case BT_ro:
	default:
		mgr->idx = CreateFile(name, GENERIC_READ, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_EXISTING, attr, NULL);
		lockmode = 0;
		break;
	}
	if( mgr->idx == INVALID_HANDLE_VALUE )
		return GlobalFree(mgr), NULL;

	// normalize cacheblk to multiple of sysinfo->dwAllocationGranularity
	GetSystemInfo(sysinfo);
	cacheblk = sysinfo->dwAllocationGranularity;
#endif

#ifdef unix
	alloc = malloc (BT_maxpage);
	*amt = 0;

	// read minimum page size to get root info

	if( size = lseek (mgr->idx, 0L, 2) ) {
		if( pread(mgr->idx, alloc, BT_minpage, 0) == BT_minpage )
			bits = alloc->bits;
		else
			return free(mgr), free(alloc), NULL;
	} else if( mode == BT_ro )
		return bt_mgrclose (mgr), NULL;
#else
	alloc = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
	size = GetFileSize(mgr->idx, amt);

	if( size || *amt ) {
		if( !ReadFile(mgr->idx, (char *)alloc, BT_minpage, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		bits = alloc->bits;
	} else if( mode == BT_ro )
		return bt_mgrclose (mgr), NULL;
#endif

	mgr->page_size = 1 << bits;
	mgr->page_bits = bits;

	mgr->poolmax = poolmax;
	mgr->mode = mode;

	if( cacheblk < mgr->page_size )
		cacheblk = mgr->page_size;

	//  mask for partial memmaps

	mgr->poolmask = (cacheblk >> bits) - 1;

	//	see if requested size of pages per memmap is greater

	if( (1 << segsize) > mgr->poolmask )
		mgr->poolmask = (1 << segsize) - 1;

	mgr->seg_bits = 0;

	while( (1 << mgr->seg_bits) <= mgr->poolmask )
		mgr->seg_bits++;

	mgr->hashsize = hashsize;

#ifdef unix
	mgr->pool = calloc (poolmax, sizeof(BtPool));
	mgr->hash = calloc (hashsize, sizeof(ushort));
	mgr->latch = calloc (hashsize, sizeof(BtSpinLatch));
	mgr->pooladvise = calloc (poolmax, (mgr->poolmask + 8) / 8);
#else
	mgr->pool = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, poolmax * (sizeof(BtPool)));
	mgr->hash = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, hashsize * sizeof(ushort));
	mgr->latch = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, hashsize * sizeof(BtSpinLatch));
#endif
	if( size || *amt )
		goto mgrxit;

	// initializes an empty b-tree with root page and page of leaves

	memset (alloc, 0, 1 << bits);
	bt_putid(alloc->right, MIN_lvl+1);
	alloc->bits = mgr->page_bits;

#ifdef unix
	if( write (mgr->idx, alloc, mgr->page_size) < mgr->page_size )
		return bt_mgrclose (mgr), NULL;
#else
	if( !WriteFile (mgr->idx, (char *)alloc, mgr->page_size, amt, NULL) )
		return bt_mgrclose (mgr), NULL;

	if( *amt < mgr->page_size )
		return bt_mgrclose (mgr), NULL;
#endif

	memset (alloc, 0, 1 << bits);
	alloc->bits = mgr->page_bits;

	for( lvl=MIN_lvl; lvl--; ) {
		slotptr(alloc, 1)->off = mgr->page_size - 3;
		bt_putid(slotptr(alloc, 1)->id, lvl ? MIN_lvl - lvl + 1 : 0);		// next(lower) page number
		key = keyptr(alloc, 1);
		key->len = 2;			// create stopper key
		key->key[0] = 0xff;
		key->key[1] = 0xff;
		alloc->min = mgr->page_size - 3;
		alloc->lvl = lvl;
		alloc->cnt = 1;
		alloc->act = 1;
#ifdef unix
		if( write (mgr->idx, alloc, mgr->page_size) < mgr->page_size )
			return bt_mgrclose (mgr), NULL;
#else
		if( !WriteFile (mgr->idx, (char *)alloc, mgr->page_size, amt, NULL) )
			return bt_mgrclose (mgr), NULL;

		if( *amt < mgr->page_size )
			return bt_mgrclose (mgr), NULL;
#endif
	}

	// create empty page area by writing last page of first
	// segment area (other pages are zeroed by O/S)

	if( mgr->poolmask ) {
		memset(alloc, 0, mgr->page_size);
		last = mgr->poolmask;

		while( last < MIN_lvl + 1 )
			last += mgr->poolmask + 1;

#ifdef unix
		pwrite(mgr->idx, alloc, mgr->page_size, last << mgr->page_bits);
#else
		SetFilePointer (mgr->idx, last << mgr->page_bits, NULL, FILE_BEGIN);
		if( !WriteFile (mgr->idx, (char *)alloc, mgr->page_size, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		if( *amt < mgr->page_size )
			return bt_mgrclose (mgr), NULL;
#endif
	}

mgrxit:
#ifdef unix
	free (alloc);
#else
	VirtualFree (alloc, 0, MEM_RELEASE);
#endif
	return mgr;
}

//	open BTree access method
//	based on buffer manager

BtDb *bt_open (BtMgr *mgr)
{
BtDb *bt = malloc (sizeof(*bt));

	memset (bt, 0, sizeof(*bt));
	bt->mgr = mgr;
#ifdef unix
	bt->mem = malloc (3 *mgr->page_size);
#else
	bt->mem = VirtualAlloc(NULL, 3 * mgr->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
	bt->frame = (BtPage)bt->mem;
	bt->zero = (BtPage)(bt->mem + 1 * mgr->page_size);
	bt->cursor = (BtPage)(bt->mem + 2 * mgr->page_size);
	return bt;
}

//  compare two keys, returning > 0, = 0, or < 0
//  as the comparison value

int keycmp (BtKey key1, unsigned char *key2, uint len2)
{
uint len1 = key1->len;
int ans;

	if( ans = memcmp (key1->key, key2, len1 > len2 ? len2 : len1) )
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//	Buffer Pool mgr

// find segment in pool
// must be called with hashslot idx locked
//	return NULL if not there
//	otherwise return node

BtPool *bt_findpool(BtDb *bt, uid page_no, uint idx)
{
BtPool *pool;
uint slot;

	// compute start of hash chain in pool

	if( slot = bt->mgr->hash[idx] ) 
		pool = bt->mgr->pool + slot;
	else
		return NULL;

	page_no &= ~bt->mgr->poolmask;

	while( pool->basepage != page_no )
	  if( pool = pool->hashnext )
		continue;
	  else
		return NULL;

	return pool;
}

// add segment to hash table

void bt_linkhash(BtDb *bt, BtPool *pool, uid page_no, int idx)
{
BtPool *node;
uint slot;

	pool->hashprev = pool->hashnext = NULL;
	pool->basepage = page_no & ~bt->mgr->poolmask;
	pool->lru = 1;

	if( slot = bt->mgr->hash[idx] ) {
		node = bt->mgr->pool + slot;
		pool->hashnext = node;
		node->hashprev = pool;
	}

	bt->mgr->hash[idx] = pool->slot;
}

//	find best segment to evict from buffer pool

BtPool *bt_findlru (BtDb *bt, uint hashslot)
{
unsigned long long int target = ~0LL;
BtPool *pool = NULL, *node;

	if( !hashslot )
		return NULL;

	node = bt->mgr->pool + hashslot;

	//	scan pool entries under hash table slot

	do {
	  if( node->pin )
		continue;
	  if( node->lru > target )
		continue;
	  target = node->lru;
	  pool = node;
	} while( node = node->hashnext );

	return pool;
}

//  map new buffer pool segment to virtual memory

BTERR bt_mapsegment(BtDb *bt, BtPool *pool, uid page_no)
{
off64_t off = (page_no & ~bt->mgr->poolmask) << bt->mgr->page_bits;
off64_t limit = off + ((bt->mgr->poolmask+1) << bt->mgr->page_bits);
int flag;

#ifdef unix
	flag = PROT_READ | ( bt->mgr->mode == BT_ro ? 0 : PROT_WRITE );
	pool->map = mmap (0, (bt->mgr->poolmask+1) << bt->mgr->page_bits, flag, MAP_SHARED, bt->mgr->idx, off);
	if( pool->map == MAP_FAILED )
		return bt->err = BTERR_map;
	// clear out madvise issued bits
	memset (bt->mgr->pooladvise + pool->slot * ((bt->mgr->poolmask + 8) / 8), 0, (bt->mgr->poolmask + 8)/8);
#else
	flag = ( bt->mgr->mode == BT_ro ? PAGE_READONLY : PAGE_READWRITE );
	pool->hmap = CreateFileMapping(bt->mgr->idx, NULL, flag, (DWORD)(limit >> 32), (DWORD)limit, NULL);
	if( !pool->hmap )
		return bt->err = BTERR_map;

	flag = ( bt->mgr->mode == BT_ro ? FILE_MAP_READ : FILE_MAP_WRITE );
	pool->map = MapViewOfFile(pool->hmap, flag, (DWORD)(off >> 32), (DWORD)off, (bt->mgr->poolmask+1) << bt->mgr->page_bits);
	if( !pool->map )
		return bt->err = BTERR_map;
#endif
 	return bt->err = 0;
}

//	find or place requested page in segment-pool
//	return pool table entry, incrementing pin

BtPool *bt_pinpage(BtDb *bt, uid page_no)
{
BtPool *pool, *node, *next;
uint slot, idx, victim;

	//	lock hash table chain

	idx = (uint)(page_no >> bt->mgr->seg_bits) % bt->mgr->hashsize;
	bt_spinreadlock (&bt->mgr->latch[idx]);

	//	look up in hash table

	if( pool = bt_findpool(bt, page_no, idx) ) {
#ifdef unix
		__sync_fetch_and_add(&pool->pin, 1);
#else
		_InterlockedIncrement16 (&pool->pin);
#endif
		bt_spinreleaseread (&bt->mgr->latch[idx]);
		pool->lru++;
		return pool;
	}

	//	upgrade to write lock

	bt_spinreleaseread (&bt->mgr->latch[idx]);
	bt_spinwritelock (&bt->mgr->latch[idx]);

	// try to find page in pool with write lock

	if( pool = bt_findpool(bt, page_no, idx) ) {
#ifdef unix
		__sync_fetch_and_add(&pool->pin, 1);
#else
		_InterlockedIncrement16 (&pool->pin);
#endif
		bt_spinreleasewrite (&bt->mgr->latch[idx]);
		pool->lru++;
		return pool;
	}

	// allocate a new pool node
	// and add to hash table

#ifdef unix
	slot = __sync_fetch_and_add(&bt->mgr->poolcnt, 1);
#else
	slot = _InterlockedIncrement16 (&bt->mgr->poolcnt) - 1;
#endif

	if( ++slot < bt->mgr->poolmax ) {
		pool = bt->mgr->pool + slot;
		pool->slot = slot;

		if( bt_mapsegment(bt, pool, page_no) )
			return NULL;

		bt_linkhash(bt, pool, page_no, idx);
#ifdef unix
		__sync_fetch_and_add(&pool->pin, 1);
#else
		_InterlockedIncrement16 (&pool->pin);
#endif
		bt_spinreleasewrite (&bt->mgr->latch[idx]);
		return pool;
	}

	// pool table is full
	//	find best pool entry to evict

#ifdef unix
	__sync_fetch_and_add(&bt->mgr->poolcnt, -1);
#else
	_InterlockedDecrement16 (&bt->mgr->poolcnt);
#endif

	while( 1 ) {
#ifdef unix
		victim = __sync_fetch_and_add(&bt->mgr->evicted, 1);
#else
		victim = _InterlockedIncrement16 (&bt->mgr->evicted) - 1;
#endif
		victim %= bt->mgr->hashsize;

		// try to get write lock
		//	skip entry if not obtained

		if( !bt_spinwritetry (&bt->mgr->latch[victim]) )
			continue;

		//  if cache entry is empty
		//	or no slots are unpinned
		//	skip this entry

		if( !(pool = bt_findlru(bt, bt->mgr->hash[victim])) ) {
			bt_spinreleasewrite (&bt->mgr->latch[victim]);
			continue;
		}

		// unlink victim pool node from hash table

		if( node = pool->hashprev )
			node->hashnext = pool->hashnext;
		else if( node = pool->hashnext )
			bt->mgr->hash[victim] = node->slot;
		else
			bt->mgr->hash[victim] = 0;

		if( node = pool->hashnext )
			node->hashprev = pool->hashprev;

		bt_spinreleasewrite (&bt->mgr->latch[victim]);

		//	remove old file mapping
#ifdef unix
		munmap (pool->map, (bt->mgr->poolmask+1) << bt->mgr->page_bits);
#else
		FlushViewOfFile(pool->map, 0);
		UnmapViewOfFile(pool->map);
		CloseHandle(pool->hmap);
#endif
		pool->map = NULL;

		//  create new pool mapping
		//  and link into hash table

		if( bt_mapsegment(bt, pool, page_no) )
			return NULL;

		bt_linkhash(bt, pool, page_no, idx);
#ifdef unix
		__sync_fetch_and_add(&pool->pin, 1);
#else
		_InterlockedIncrement16 (&pool->pin);
#endif
		bt_spinreleasewrite (&bt->mgr->latch[idx]);
		return pool;
	}
}

// place write, read, or parent lock on requested page_no.
//	pin to buffer pool and return page pointer

BTERR bt_lockpage(BtDb *bt, uid page_no, BtLock mode, BtPage *pageptr)
{
BtPool *pool;
uint subpage;
BtPage page;

	//	find/create maping in pool table
	//	  and pin our pool slot

	if( pool = bt_pinpage(bt, page_no) )
		subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
	else
		return bt->err;

	page = (BtPage)(pool->map + (subpage << bt->mgr->page_bits));

#ifdef unix
	{
	uint idx = subpage / 8;
	uint bit = subpage % 8;

		if( ~((bt->mgr->pooladvise + pool->slot * ((bt->mgr->poolmask + 8)/8))[idx] >> bit) & 1 ) {
		  madvise (page, bt->mgr->page_size, MADV_WILLNEED);
		  (bt->mgr->pooladvise + pool->slot * ((bt->mgr->poolmask + 8)/8))[idx] |= 1 << bit;
		}
	}
#endif

	switch( mode ) {
	case BtLockRead:
		bt_spinreadlock (page->readwr);
		break;
	case BtLockWrite:
		bt_spinwritelock (page->readwr);
		break;
	case BtLockAccess:
		bt_spinreadlock (page->access);
		break;
	case BtLockDelete:
		bt_spinwritelock (page->access);
		break;
	case BtLockParent:
		bt_spinwritelock (page->parent);
		break;
	default:
		return bt->err = BTERR_lock;
	}

	if( pageptr )
		*pageptr = page;

	return bt->err = 0;
}

// remove write, read, or parent lock on requested page_no.

BTERR bt_unlockpage(BtDb *bt, uid page_no, BtLock mode)
{
BtPool *pool;
uint subpage;
BtPage page;
uint idx;

	//	since page is pinned
	//	it should still be in the buffer pool
	//	and is in no danger of being a victim for reuse

	idx = (uint)(page_no >> bt->mgr->seg_bits) % bt->mgr->hashsize;
 	bt_spinreadlock (&bt->mgr->latch[idx]);

	if( !(pool = bt_findpool(bt, page_no, idx)) )
		return bt->err = BTERR_hash;

 	bt_spinreleaseread (&bt->mgr->latch[idx]);

	subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
	page = (BtPage)(pool->map + (subpage << bt->mgr->page_bits));

	switch( mode ) {
	case BtLockRead:
		bt_spinreleaseread (page->readwr);
		break;
	case BtLockWrite:
		bt_spinreleasewrite (page->readwr);
		break;
	case BtLockAccess:
		bt_spinreleaseread (page->access);
		break;
	case BtLockDelete:
		bt_spinreleasewrite (page->access);
		break;
	case BtLockParent:
		bt_spinreleasewrite (page->parent);
		break;
	default:
		return bt->err = BTERR_lock;
	}

#ifdef  unix
	__sync_fetch_and_add(&pool->pin, -1);
#else
	_InterlockedDecrement16 (&pool->pin);
#endif
	return bt->err = 0;
}

//	deallocate a deleted page
//	place on free chain out of allocator page
//  fence key must already be removed from parent

BTERR bt_freepage(BtDb *bt, uid page_no)
{
	//  obtain delete lock on deleted page

	if( bt_lockpage(bt, page_no, BtLockDelete, NULL) )
		return bt->err;

	//  obtain write lock on deleted page

	if( bt_lockpage(bt, page_no, BtLockWrite, &bt->temp) )
		return bt->err;

	//	lock allocation page

	if ( bt_lockpage(bt, ALLOC_page, BtLockWrite, &bt->alloc) )
		return bt->err;

	//	store free chain in allocation page second right
	bt_putid(bt->temp->right, bt_getid(bt->alloc[1].right));
	bt_putid(bt->alloc[1].right, page_no);

	// unlock allocation page

	if( bt_unlockpage(bt, ALLOC_page, BtLockWrite) )
		return bt->err;

	//  remove write lock on deleted node

	if( bt_unlockpage(bt, page_no, BtLockWrite) )
		return bt->err;

	//  remove delete lock on deleted node

	if( bt_unlockpage(bt, page_no, BtLockDelete) )
		return bt->err;

	return 0;
}

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
uid new_page;
BtPage pmap;
int subpage;
int reuse;

	// lock page zero

	if( bt_lockpage(bt, ALLOC_page, BtLockWrite, &bt->alloc) )
		return 0;

	// use empty chain first
	// else allocate empty page

	if( new_page = bt_getid(bt->alloc[1].right) ) {
		if( bt_lockpage (bt, new_page, BtLockWrite, &bt->temp) )
			return 0;
		bt_putid(bt->alloc[1].right, bt_getid(bt->temp->right));
		if( bt_unlockpage (bt, new_page, BtLockWrite) )
			return 0;
		reuse = 1;
	} else {
		new_page = bt_getid(bt->alloc->right);
		bt_putid(bt->alloc->right, new_page+1);
		reuse = 0;
	}

#ifdef unix
	memset(bt->zero, 0, 3 * sizeof(BtSpinLatch)); // clear locks
	memcpy((char *)bt->zero + 3 * sizeof(BtSpinLatch), (char *)page + 3 * sizeof(BtSpinLatch), bt->mgr->page_size - 3 * sizeof(BtSpinLatch));
	if ( pwrite(bt->mgr->idx, bt->zero, bt->mgr->page_size, new_page << bt->mgr->page_bits) < bt->mgr->page_size )
		return bt->err = BTERR_wrt, 0;

	// if writing first page of pool block, zero last page in the block

	if ( !reuse && bt->mgr->poolmask > 0 && (new_page & bt->mgr->poolmask) == 0 )
	{
		// use zero buffer to write zeros
		memset(bt->zero, 0, bt->mgr->page_size);
		if ( pwrite(bt->mgr->idx,bt->zero, bt->mgr->page_size, (new_page | bt->mgr->poolmask) << bt->mgr->page_bits) < bt->mgr->page_size )
			return bt->err = BTERR_wrt, 0;
	}
#else
	//	bring new page into pool and copy page.
	//	this will extend the file into the new pages.

	if( bt_lockpage(bt, new_page, BtLockWrite, &pmap) )
		return 0;

	//  copy source page, but leave latch area intact

	memcpy((char *)pmap + 3 * sizeof(BtSpinLatch), (char *)page + 3 * sizeof(BtSpinLatch), bt->mgr->page_size - 3 * sizeof(BtSpinLatch));

	if( bt_unlockpage (bt, new_page, BtLockWrite) )
		return 0;
#endif
	// unlock allocation latch and return new page no

	if ( bt_unlockpage(bt, ALLOC_page, BtLockWrite) )
		return 0;

	return new_page;
}

//  find slot in page for given key at a given level

int bt_findslot (BtDb *bt, unsigned char *key, uint len)
{
uint diff, higher = bt->page->cnt, low = 1, slot;

	//	low is the lowest candidate, higher is already
	//	tested as .ge. the given key, loop ends when they meet

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(bt->page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot;
	}

	return higher;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtDb *bt, unsigned char *key, uint len, uint lvl, uint lock)
{
uid page_no = ROOT_page, prevpage = 0;
uint drill = 0xff, slot;
uint mode, prevmode;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (lock == BtLockWrite) && (drill == lvl) ? BtLockWrite : BtLockRead; 

	bt->page_no = page_no;

 	// obtain access lock using lock chaining with Access mode

	if( page_no > ROOT_page )
	  if( bt_lockpage(bt, page_no, BtLockAccess, NULL) )
		return 0;									

	//  now unlock our (possibly foster) parent

	if( prevpage )
	  if( bt_unlockpage(bt, prevpage, prevmode) )
		return 0;
	  else
		prevpage = 0;

 	// obtain read lock using lock chaining
	// and pin page contents

	if( bt_lockpage(bt, page_no, mode, &bt->page) )
		return 0;									

	if( page_no > ROOT_page )
	  if( bt_unlockpage(bt, page_no, BtLockAccess) )
		return 0;									

	// re-read and re-lock root after determining actual level of root

	if( bt->page_no == ROOT_page )
	  if( bt->page->lvl != drill) {
		drill = bt->page->lvl;

	    if( lock == BtLockWrite && drill == lvl )
		  if( bt_unlockpage(bt, page_no, mode) )
			return 0;
		  else
			continue;
	  }

	prevpage = bt->page_no;
	prevmode = mode;

	//	if page is being deleted,
	//	move back to preceeding page

	if( bt->page->kill ) {
		page_no = bt_getid (bt->page->right);
		continue;
	}

	//  find key on page at this level
	//  and descend to requested level

	slot = bt_findslot (bt, key, len);

	//	is this slot a foster child?

	if( slot <= bt->page->cnt - bt->page->foster )
	  if( drill == lvl )
		return slot;

	while( slotptr(bt->page, slot)->dead )
	  if( slot++ < bt->page->cnt )
		continue;
	  else
		goto slideright;

	if( slot <= bt->page->cnt - bt->page->foster )
		drill--;

	//  continue down / right using overlapping locks
	//  to protect pages being killed or split.

	page_no = bt_getid(slotptr(bt->page, slot)->id);
	continue;

slideright:
	page_no = bt_getid(bt->page->right);

  } while( page_no );

  // return error on end of chain

  bt->err = BTERR_struct;
  return 0;	// return error
}

//  find and delete key on page by marking delete flag bit
//  when page becomes empty, delete it from the btree

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl)
{
unsigned char leftkey[256], rightkey[256];
uid page_no, right;
uint slot, tod;
BtKey ptr;

	if( slot = bt_loadpage (bt, key, len, lvl, BtLockWrite) )
		ptr = keyptr(bt->page, slot);
	else
		return bt->err;

	// if key is found delete it, otherwise ignore request

	if( !keycmp (ptr, key, len) )
		if( slotptr(bt->page, slot)->dead == 0 ) {
 			slotptr(bt->page,slot)->dead = 1;
			if( slot < bt->page->cnt )
 				bt->page->dirty = 1;
 			bt->page->act--;
		}

	// return if page is not empty, or it has no right sibling

	right = bt_getid(bt->page->right);
	page_no = bt->page_no;

	if( !right || bt->page->act )
		return bt_unlockpage(bt, page_no, BtLockWrite);

	// obtain Parent lock over write lock

	if( bt_lockpage(bt, page_no, BtLockParent, NULL) )
		return bt->err;

	// cache copy of key to delete

	ptr = keyptr(bt->page, bt->page->cnt);
	memcpy(leftkey, ptr, ptr->len + 1);

	// lock and map right page

	if( bt_lockpage(bt, right, BtLockWrite, &bt->temp) )
		return bt->err;

	// pull contents of next page into current empty page 
	memcpy (bt->page, bt->temp, bt->mgr->page_size);

	//	cache copy of key to update
	ptr = keyptr(bt->temp, bt->temp->cnt);
	memcpy(rightkey, ptr, ptr->len + 1);

	//  Mark right page as deleted and point it to left page
	//	until we can post updates at higher level.

	bt_putid(bt->temp->right, page_no);
	bt->temp->kill = 1;
	bt->temp->cnt = 0;

	if( bt_unlockpage(bt, right, BtLockWrite) )
		return bt->err;
	if( bt_unlockpage(bt, page_no, BtLockWrite) )
		return bt->err;

	//  delete old lower key to consolidated node

	if( bt_deletekey (bt, leftkey + 1, *leftkey, lvl + 1) )
		return bt->err;

	//  redirect higher key directly to consolidated node

	if( slot = bt_loadpage (bt, rightkey+1, *rightkey, lvl+1, BtLockWrite) )
		ptr = keyptr(bt->page, slot);
	else
		return bt->err;

	// since key already exists, update id

	if( keycmp (ptr, rightkey+1, *rightkey) )
		return bt->err = BTERR_struct;

	slotptr(bt->page, slot)->dead = 0;
	bt_putid(slotptr(bt->page,slot)->id, page_no);

	if( bt_unlockpage(bt, bt->page_no, BtLockWrite) )
		return bt->err;

	//	obtain write lock and
	//	add right block to free chain

	if( bt_freepage (bt, right) )
		return bt->err;

	// 	remove ParentModify lock

	if( bt_unlockpage(bt, page_no, BtLockParent) )
		return bt->err;
	
	return 0;
}

//	find key in leaf level and return row-id

uid bt_findkey (BtDb *bt, unsigned char *key, uint len)
{
uint  slot;
BtKey ptr;
uid id;

	if( slot = bt_loadpage (bt, key, len, 0, BtLockRead) )
		ptr = keyptr(bt->page, slot);
	else
		return 0;

	// if key exists, return row-id
	//	otherwise return 0

	if( ptr->len == len && !memcmp (ptr->key, key, len) )
		id = bt_getid(slotptr(bt->page,slot)->id);
	else
		id = 0;

	if( bt_unlockpage (bt, bt->page_no, BtLockRead) )
		return 0;

	return id;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	1 - go ahead

uint bt_cleanpage(BtDb *bt, uint amt)
{
uint nxt = bt->mgr->page_size;
BtPage page = bt->page;
uint cnt = 0, idx = 0;
uint max = page->cnt;
BtKey key;

	if( page->min >= (max+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return 1;

	//	skip cleanup if nothing to reclaim

	if( !page->dirty )
		return 0;

	memcpy (bt->frame, page, bt->mgr->page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, bt->mgr->page_size - sizeof(*page));
	page->dirty = 0;
	page->act = 0;

	// try cleaning up page first

	while( cnt++ < max ) {
		// always leave fence key and foster children in list
		if( cnt < max - page->foster && slotptr(bt->frame,cnt)->dead )
			continue;

		// copy key
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)page + nxt, key, key->len + 1);

		// copy slot
		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		if( !(slotptr(page, idx)->dead = slotptr(bt->frame, cnt)->dead) )
			page->act++;
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
	}

	page->min = nxt;
	page->cnt = idx;

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return 1;

	return 0;
}

//	add key to current page
//	page must already be writelocked

void bt_addkeytopage (BtDb *bt, uint slot, unsigned char *key, uint len, uid id, uint tod)
{
BtPage page = bt->page;
uint idx;

	// calculate next available slot and copy key into page

	page->min -= len + 1;
	((unsigned char *)page)[page->min] = len;
	memcpy ((unsigned char *)page + page->min +1, key, len );

	for( idx = slot; idx < page->cnt; idx++ )
	  if( slotptr(page, idx)->dead )
		break;

	// now insert key into array before slot
	// preserving the fence slot

	if( idx == page->cnt )
		idx++, page->cnt++;

	page->act++;

	while( idx > slot )
		*slotptr(page, idx) = *slotptr(page, idx -1), idx--;

	bt_putid(slotptr(page,slot)->id, id);
	slotptr(page, slot)->off = page->min;
	slotptr(page, slot)->tod = tod;
	slotptr(page, slot)->dead = 0;
}

// split the root and raise the height of the btree
//	call with current page locked and page no of foster child
//	return with current page (root) unlocked

BTERR bt_splitroot(BtDb *bt, uid right)
{
uint nxt = bt->mgr->page_size;
unsigned char fencekey[256];
BtPage root = bt->page;
uid new_page;
BtKey key;

	//  Obtain an empty page to use, and copy the left page
	//  contents into it from the root.  Strip foster child key.
	//	(it's the stopper key)

	root->act--;
	root->cnt--;
	root->foster--;

	//	Save left fence key.

	key = keyptr(root, root->cnt);
	memcpy (fencekey, key, key->len + 1);

	//  copy the lower keys into a new left page

	if( !(new_page = bt_newpage(bt, root)) )
		return bt->err;

	// preserve the page info at the bottom
	// and set rest of the root to zero

	memset (root+1, 0, bt->mgr->page_size - sizeof(*root));

	// insert left fence key on empty newroot page

	nxt -= *fencekey + 1;
	memcpy ((unsigned char *)root + nxt, fencekey, *fencekey + 1);
	bt_putid(slotptr(root, 1)->id, new_page);
	slotptr(root, 1)->off = nxt;
	
	// insert stopper key on newroot page
	// and increase the root height

	nxt -= 3;
	fencekey[0] = 2;
	fencekey[1] = 0xff;
	fencekey[2] = 0xff;
	memcpy ((unsigned char *)root + nxt, fencekey, *fencekey + 1);
	bt_putid(slotptr(root, 2)->id, right);
	slotptr(root, 2)->off = nxt;

	bt_putid(root->right, 0);
	root->min = nxt;		// reset lowest used offset and key count
	root->cnt = 2;
	root->act = 2;
	root->lvl++;

	// release root (bt->page)

	return bt_unlockpage(bt, ROOT_page, BtLockWrite);
}

//  split already locked full node
//	in current page variables
//	return unlocked.

BTERR bt_splitpage (BtDb *bt)
{
uint slot, cnt, idx, max, nxt = bt->mgr->page_size;
unsigned char fencekey[256];
uid page_no = bt->page_no;
BtPage page = bt->page;
uint tod = time(NULL);
uint lvl = page->lvl;
uid new_page, right;
BtKey key;

	//	initialize frame buffer

	memset (bt->frame, 0, bt->mgr->page_size);
	max = page->cnt - page->foster;
	tod = (uint)time(NULL);
	cnt = max / 2;
	idx = 0;

	//  split higher half of keys to bt->frame
	//	leaving foster children in the left node.

	while( cnt++ < max ) {
		key = keyptr(page, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)bt->frame + nxt, key, key->len + 1);
		memcpy(slotptr(bt->frame,++idx)->id, slotptr(page,cnt)->id, BtId);
		slotptr(bt->frame, idx)->tod = slotptr(page, cnt)->tod;
		slotptr(bt->frame, idx)->off = nxt;
		bt->frame->act++;
	}

	// transfer right link node

	if( page_no > ROOT_page ) {
		right = bt_getid (page->right);
		bt_putid(bt->frame->right, right);
	}

	bt->frame->bits = bt->mgr->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	//	get new free page and write frame to it.

	if( !(new_page = bt_newpage(bt, bt->frame)) )
		return bt->err;

	//	remember fence key for new page to add
	//	as foster child

	key = keyptr(bt->frame, idx);
	memcpy (fencekey, key, key->len + 1);

	//	update lower keys and foster children to continue in old page

	memcpy (bt->frame, page, bt->mgr->page_size);
	memset (page+1, 0, bt->mgr->page_size - sizeof(*page));
	nxt = bt->mgr->page_size;
	page->act = 0;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys
	//	to remain in the old page

	while( cnt++ < max / 2 ) {
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)page + nxt, key, key->len + 1);
		memcpy (slotptr(page,++idx)->id, slotptr(bt->frame,cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
		page->act++;
	}

	//	insert new foster child at beginning of the current foster children

	nxt -= *fencekey + 1;
	memcpy ((unsigned char *)page + nxt, fencekey, *fencekey + 1);
	bt_putid (slotptr(page,++idx)->id, new_page);
	slotptr(page, idx)->tod = tod;
	slotptr(page, idx)->off = nxt;
	page->foster++;
	page->act++;

	//  continue with old foster child keys if any

	cnt = bt->frame->cnt - bt->frame->foster;

	while( cnt++ < bt->frame->cnt ) {
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)page + nxt, key, key->len + 1);
		memcpy (slotptr(page,++idx)->id, slotptr(bt->frame,cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
		page->act++;
	}

	page->min = nxt;
	page->cnt = idx;

	//	link new right page

	bt_putid (page->right, new_page);

	// if current page is the root page, split it

	if( page_no == ROOT_page )
		return bt_splitroot (bt, new_page);

	//  release wr lock on our page

	if( bt_unlockpage (bt, page_no, BtLockWrite) )
		return bt->err;

	//  obtain ParentModification lock for current page
	//	to fix fence key and highest foster child on page

	if( bt_lockpage (bt, page_no, BtLockParent, NULL) )
		return bt->err;

	//  get our highest foster child key to find in parent node

	if( bt_lockpage (bt, page_no, BtLockRead, &page) )
		return bt->err;

	key = keyptr(page, page->cnt);
	memcpy (fencekey, key, key->len+1);

	if( bt_unlockpage (bt, page_no, BtLockRead) )
		return bt->err;

	  //  update our parent
try_again:

	do {
	  slot = bt_loadpage (bt, fencekey + 1, *fencekey, lvl + 1, BtLockWrite);

	  if( !slot )
		return bt->err;

	  // check if parent page has enough space for any possible key

	  if( bt_cleanpage (bt, 256) )
		break;

	  if( bt_splitpage (bt) )
		return bt->err;
	} while( 1 );

	//  see if we are still a foster child from another node

	if( bt_getid (slotptr(bt->page, slot)->id) != page_no ) {
		if( bt_unlockpage (bt, bt->page_no, BtLockWrite) )
			return bt->err;
#ifdef  unix
		sched_yield();
#else
		SwitchToThread();
#endif
		goto try_again;
	}

	//	wait until readers from parent get their locks
	//	on our page

	if( bt_lockpage (bt, page_no, BtLockDelete, NULL) )
		return bt->err;

	//	lock our page for writing

	if( bt_lockpage (bt, page_no, BtLockWrite, &page) )
		return bt->err;

	//	switch parent fence key to foster child

	if( slotptr(page, page->cnt)->dead )
		slotptr(bt->page, slot)->dead = 1;
	else
		bt_putid (slotptr(bt->page, slot)->id, bt_getid(slotptr(page, page->cnt)->id));

	//	remove highest foster child from our page

	page->cnt--;
	page->act--;
	page->foster--;
	page->dirty = 1;
	key = keyptr(page, page->cnt);

	//	add our new fence key for foster child to our parent

	bt_addkeytopage (bt, slot, key->key, key->len, page_no, tod);

	if( bt_unlockpage (bt, bt->page_no, BtLockWrite) )
		return bt->err;

	if( bt_unlockpage (bt, page_no, BtLockDelete) )
		return bt->err;

	if( bt_unlockpage (bt, page_no, BtLockWrite) )
		return bt->err;

	return bt_unlockpage (bt, page_no, BtLockParent);
}

//  Insert new key into the btree at leaf level.

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uid id, uint tod)
{
uint slot, idx;
BtPage page;
BtKey ptr;

	while( 1 ) {
		if( slot = bt_loadpage (bt, key, len, 0, BtLockWrite) )
			ptr = keyptr(bt->page, slot);
		else
		{
			if ( !bt->err )
				bt->err = BTERR_ovflw;
			return bt->err;
		}

		// if key already exists, update id and return

		page = bt->page;

		if( !keycmp (ptr, key, len) ) {
			slotptr(page, slot)->dead = 0;
			slotptr(page, slot)->tod = tod;
			bt_putid(slotptr(page,slot)->id, id);
			return bt_unlockpage(bt, bt->page_no, BtLockWrite);
		}

		// check if page has enough space

 		if( bt_cleanpage (bt, len) )
			break;

		if( bt_splitpage (bt) )
			return bt->err;
	}

  	bt_addkeytopage (bt, slot, key, len, id, tod);

	return bt_unlockpage (bt, bt->page_no, BtLockWrite);
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
uint slot;

	// cache page for retrieval
	if( slot = bt_loadpage (bt, key, len, 0, BtLockRead) )
		memcpy (bt->cursor, bt->page, bt->mgr->page_size);
	bt->cursor_page = bt->page_no;
	if ( bt_unlockpage(bt, bt->page_no, BtLockRead) )
		return 0;

	return slot;
}

//  return next slot for cursor page
//  or slide cursor right into next page

uint bt_nextkey (BtDb *bt, uint slot)
{
BtPage page;
uid right;

  do {
	right = bt_getid(bt->cursor->right);
	while( slot++ < bt->cursor->cnt - bt->cursor->foster )
	  if( slotptr(bt->cursor,slot)->dead )
		continue;
	  else if( right || (slot < bt->cursor->cnt - bt->cursor->foster) )
		return slot;
	  else
		break;

	if( !right )
		break;

	bt->cursor_page = right;

    if( bt_lockpage(bt, right, BtLockRead, &page) )
		return 0;

	memcpy (bt->cursor, page, bt->mgr->page_size);

	if ( bt_unlockpage(bt, right, BtLockRead) )
		return 0;

	slot = 0;
  } while( 1 );

  return bt->err = 0;
}

BtKey bt_key(BtDb *bt, uint slot)
{
	return keyptr(bt->cursor, slot);
}

uid bt_uid(BtDb *bt, uint slot)
{
	return bt_getid(slotptr(bt->cursor,slot)->id);
}

uint bt_tod(BtDb *bt, uint slot)
{
	return slotptr(bt->cursor,slot)->tod;
}


#ifdef STANDALONE

typedef struct {
	char type, idx;
	char *infile;
	BtMgr *mgr;
	int num;
} ThreadArg;

//  standalone program to index file of keys
//  then list them onto std-out

#ifdef unix
void *index_file (void *arg)
#else
uint __stdcall index_file (void *arg)
#endif
{
int line = 0, found = 0, cnt = 0;
uid next, page_no = LEAF_page;	// start on first page of leaves
unsigned char key[256];
ThreadArg *args = arg;
int ch, len = 0, slot;
time_t tod[1];
BtPage page;
BtKey ptr;
BtDb *bt;
FILE *in;

	bt = bt_open (args->mgr);
	time (tod);

	switch(args->type | 0x20)
	{
	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( args->num == 1 )
		  		sprintf((char *)key+len, "%.9d", 1000000000 - line), len += 9;

			  else if( args->num )
		  		sprintf((char *)key+len, "%.9d", line + args->idx * args->num), len += 9;

			  if( bt_insertkey (bt, key, len, line, *tod) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < 255 )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'd':
		fprintf(stderr, "started deleting keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( args->num == 1 )
		  		sprintf((char *)key+len, "%.9d", 1000000000 - line), len += 9;

			  else if( args->num )
		  		sprintf((char *)key+len, "%.9d", line + args->idx * args->num), len += 9;

			  if( bt_deletekey (bt, key, len, 0) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < 255 )
				key[len++] = ch;
		fprintf(stderr, "finished %s for keys, %d \n", args->infile, line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( args->num == 1 )
		  		sprintf((char *)key+len, "%.9d", 1000000000 - line), len += 9;

			  else if( args->num )
		  		sprintf((char *)key+len, "%.9d", line + args->idx * args->num), len += 9;

			  if( bt_findkey (bt, key, len) )
				found++;
			  else if( bt->err )
				fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line), exit(0);
			  len = 0;
			}
			else if( len < 255 )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, found %d\n", args->infile, line, found);
		break;

	case 's':
		len = key[0] = 0;

		fprintf(stderr, "started reading\n");

		if( slot = bt_startkey (bt, key, len) )
		  slot--;
		else
		  fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

		while( slot = bt_nextkey (bt, slot) ) {
			ptr = bt_key(bt, slot);
			fwrite (ptr->key, ptr->len, 1, stdout);
			fputc ('\n', stdout);
		}

		break;

	case 'c':
		fprintf(stderr, "started reading\n");

	  	do {
			bt_lockpage (bt, page_no, BtLockRead, &page);
			cnt += page->act;
			next = bt_getid (page->right);
			bt_unlockpage (bt, page_no, BtLockRead);
	  	} while( page_no = next );

	  	cnt--;	// remove stopper key
		fprintf(stderr, " Total keys read %d\n", cnt);
		break;
	}

	bt_close (bt);
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
int segsize, bits = 16;
#ifdef unix
pthread_t *threads;
timer start, stop;
#else
time_t start[1], stop[1];
HANDLE *threads;
#endif
double real_time;
ThreadArg *args;
uint poolsize = 0;
int num = 0;
char key[1];
BtMgr *mgr;
BtKey ptr;
BtDb *bt;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file Read/Write/Scan/Delete/Find [page_bits mapped_segments seg_bits line_numbers src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where page_bits is the page size in bits\n");
		fprintf (stderr, "  mapped_segments is the number of mmap segments in buffer pool\n");
		fprintf (stderr, "  seg_bits is the size of individual segments in buffer pool in pages in bits\n");
		fprintf (stderr, "  line_numbers = 1 to append line numbers to keys\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
		exit(0);
	}

#ifdef unix
	gettimeofday(&start, NULL);
#else
	time(start);
#endif

	if( argc > 3 )
		bits = atoi(argv[3]);

	if( argc > 4 )
		poolsize = atoi(argv[4]);

	if( !poolsize )
		fprintf (stderr, "Warning: no mapped_pool\n");

	if( poolsize > 65535 )
		fprintf (stderr, "Warning: mapped_pool > 65535 segments\n");

	if( argc > 5 )
		segsize = atoi(argv[5]);
	else
		segsize = 4; 	// 16 pages per mmap segment

	if( argc > 6 )
		num = atoi(argv[6]);

	cnt = argc - 7;
#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc (cnt * sizeof(ThreadArg));

	mgr = bt_mgr ((argv[1]), BT_rw, bits, poolsize, segsize, poolsize / 8);

	if( !mgr ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	}

	//	fire off threads

	for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 7];
		args[idx].type = argv[2][0];
		args[idx].mgr = mgr;
		args[idx].num = num;
		args[idx].idx = idx;
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
#else
		threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, index_file, args + idx, 0, NULL);
#endif
	}

	// 	wait for termination

#ifdef unix
	for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);
	gettimeofday(&stop, NULL);
	real_time = 1000.0 * ( stop.tv_sec - start.tv_sec ) + 0.001 * (stop.tv_usec - start.tv_usec );
#else
	WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	for( idx = 0; idx < cnt; idx++ )
		CloseHandle(threads[idx]);

	time (stop);
	real_time = 1000 * (*stop - *start);
#endif
	fprintf(stderr, " Time to complete: %.2f seconds\n", real_time/1000);
	bt_mgrclose (mgr);
}

#endif	//STANDALONE
