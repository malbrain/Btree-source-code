// btree version threadskv1 sched_yield version
//	with reworked bt_deletekey code
//	and phase-fair reader writer lock
// 12 MAR 2014

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
#include <stddef.h>

typedef unsigned long long	uid;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_latchtable	128					// number of latch manager slots

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
5. (set 3) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
*/

typedef enum{
	BtLockAccess,
	BtLockDelete,
	BtLockRead,
	BtLockWrite,
	BtLockParent
} BtLock;

//	definition for phase-fair reader/writer lock implementation

typedef struct {
	ushort rin[1];
	ushort rout[1];
	ushort ticket[1];
	ushort serving[1];
} RWLock;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4

//	definition for spin latch implementation

// exclusive is set for write access
// share is count of read accessors
// grant write lock when share == 0

volatile typedef struct {
	ushort exclusive:1;
	ushort pending:1;
	ushort share:14;
} BtSpinLatch;

#define XCL 1
#define PEND 2
#define BOTH 3
#define SHARE 4

//  hash table entries

typedef struct {
	BtSpinLatch latch[1];
	volatile ushort slot;		// Latch table entry at head of chain
} BtHashEntry;

//	latch manager table structure

typedef struct {
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	RWLock parent[1];		// Posting of fence key in parent
	BtSpinLatch busy[1];		// slot is being moved between chains
	volatile ushort next;		// next entry in hash table chain
	volatile ushort prev;		// prev entry in hash table chain
	volatile ushort pin;		// number of outstanding locks
	volatile ushort hash;		// hash slot entry is under
	volatile uid page_no;		// latch set page number
} BtLatchSet;

//	Define the length of the page and key pointers

#define BtId 6

//	Page key slot definition.

//	If BT_maxbits is 15 or less, you can save 2 bytes
//	for each key stored by making the two uints
//	into ushorts.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence key (highest key) for
//	the page is always present, even after cleanup.

typedef struct {
	uint off:BT_maxbits;		// page offset for key start
	uint dead:1;				// set for deleted key
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the key
//	bytes.

typedef struct {
	unsigned char len;
	unsigned char key[1];
} *BtKey;

//	the value structure also occupies space at the upper
//	end of the page.

typedef struct {
	unsigned char len;
	unsigned char value[1];
} *BtVal;

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	unsigned char bits:7;		// page size in bits
	unsigned char free:1;		// page is on free chain
	unsigned char lvl:6;		// level of page
	unsigned char kill:1;		// page is being deleted
	unsigned char dirty:1;		// page has deleted keys
	unsigned char right[BtId];	// page number to right
} *BtPage;

//	The memory mapping pool table buffer manager entry

typedef struct {
	uid  basepage;				// mapped base page number
	char *map;					// mapped memory pointer
	ushort slot;				// slot index in this array
	ushort pin;					// mapped page pin counter
	void *hashprev;				// previous pool entry for the same hash idx
	void *hashnext;				// next pool entry for the same hash idx
#ifndef unix
	HANDLE hmap;				// Windows memory mapping handle
#endif
} BtPool;

#define CLOCK_bit 0x8000		// bit in pool->pin

//  The loadpage interface object

typedef struct {
	uid page_no;		// current page number
	BtPage page;		// current page pointer
	BtPool *pool;		// current page pool
	BtLatchSet *latch;	// current page latch set
} BtPageSet;

//	structure for latch manager on ALLOC_page

typedef struct {
	struct BtPage_ alloc[1];	// next page_no in right ptr
	unsigned char chain[BtId];	// head of free page_nos chain
	BtSpinLatch lock[1];		// allocation area lite latch
	ushort latchdeployed;		// highest number of latch entries deployed
	ushort nlatchpage;			// number of latch pages at BT_latch
	ushort latchtotal;			// number of page latch entries
	ushort latchhash;			// number of latch hash table slots
	ushort latchvictim;			// next latch entry to examine
	BtHashEntry table[0];		// the hash table
} BtLatchMgr;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// page size	
	uint page_bits;				// page size in bits	
	uint seg_bits;				// seg size in pages in bits
	uint mode;					// read-write mode
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	ushort poolcnt;				// highest page pool node in use
	ushort poolmax;				// highest page pool node allocated
	ushort poolmask;			// total number of pages in mmap segment - 1
	ushort hashsize;			// size of Hash Table for pool entries
	volatile uint evicted;		// last evicted hash table slot
	ushort *hash;				// pool index for hash entries
	BtSpinLatch *latch;			// latches for hash table slots
	BtLatchMgr *latchmgr;		// mapped latch page from allocation page
	BtLatchSet *latchsets;		// mapped latch set from latch pages
	BtPool *pool;				// memory pool page segments
#ifndef unix
	HANDLE halloc;				// allocation and latch table handle
#endif
} BtMgr;

typedef struct {
	BtMgr *mgr;			// buffer manager for thread
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	uid cursor_page;	// current cursor page number	
	unsigned char *mem;	// frame, cursor, page memory buffer
	int found;			// last delete or insert was found
	int err;			// last error
} BtDb;

typedef enum {
	BTERR_ok = 0,
	BTERR_struct,
	BTERR_ovflw,
	BTERR_lock,
	BTERR_map,
	BTERR_wrt,
	BTERR_hash
} BTERR;

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (BtMgr *mgr);
extern BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, void *value, uint vallen);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
extern int bt_findkey    (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint vallen);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint mode, uint bits, uint poolsize, uint segsize, uint hashsize);
void bt_mgrclose (BtMgr *mgr);

//  Helper functions to return slot values
//	from the cursor page.

extern BtKey bt_key (BtDb *bt, uint slot);
extern BtVal bt_val (BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0	// allocation & latch manager hash table
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves
#define LATCH_page		3	// pages for latch manager

//	Number of levels to create in a new BTree

#define MIN_lvl			2

//  The page is allocated from low and hi ends.
//  The key slots are allocated from the bottom,
//	while the text and value of the key
//  are allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65534), and up to 253 bytes
//  of key value.  Duplicate keys are discarded.
//  Associated with each key is a value byte string
//	containing any value desired.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	The b-tree pages are linked with next
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup. The fence key for a leaf node is
//	always present

//  Groups of pages called segments from the btree are optionally
//  cached with a memory mapped pool. A hash table is used to keep
//  track of the cached segments.  This behaviour is controlled
//  by the cache block size parameter to bt_open.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse. It also
//	contains the latch manager hash table.

//	The ParentModification lock on a node is obtained to serialize posting
//	or changing the fence key for a node.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page
//	Page slots use 1 based indexing.

#define slotptr(page, slot) (((BtSlot *)(page+1)) + (slot-1))
#define keyptr(page, slot) ((BtKey)((unsigned char*)(page) + slotptr(page, slot)->off))
#define valptr(page, slot) ((BtVal)(keyptr(page,slot)->key + keyptr(page,slot)->len))

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

//	Phase-Fair reader/writer lock implementation

void WriteLock (RWLock *lock)
{
ushort w, r, tix;

#ifdef unix
	tix = __sync_fetch_and_add (lock->ticket, 1);
#else
	tix = _InterlockedExchangeAdd16 (lock->ticket, 1);
#endif
	// wait for our ticket to come up

	while( tix != lock->serving[0] )
#ifdef unix
		sched_yield();
#else
		SwitchToThread ();
#endif

	w = PRES | (tix & PHID);
#ifdef  unix
	r = __sync_fetch_and_add (lock->rin, w);
#else
	r = _InterlockedExchangeAdd16 (lock->rin, w);
#endif
	while( r != *lock->rout )
#ifdef unix
		sched_yield();
#else
		SwitchToThread();
#endif
}

void WriteRelease (RWLock *lock)
{
#ifdef unix
	__sync_fetch_and_and (lock->rin, ~MASK);
#else
	_InterlockedAnd16 (lock->rin, ~MASK);
#endif
	lock->serving[0]++;
}

void ReadLock (RWLock *lock)
{
ushort w;
#ifdef unix
	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;
#else
	w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
#endif
	if( w )
	  while( w == (*lock->rin & MASK) )
#ifdef unix
		sched_yield ();
#else
		SwitchToThread ();
#endif
}

void ReadRelease (RWLock *lock)
{
#ifdef unix
	__sync_fetch_and_add (lock->rout, RINC);
#else
	_InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}

//	Spin Latch Manager

//	wait until write lock mode is clear
//	and add 1 to the share count

void bt_spinreadlock(BtSpinLatch *latch)
{
ushort prev;

  do {
#ifdef unix
	prev = __sync_fetch_and_add ((ushort *)latch, SHARE);
#else
	prev = _InterlockedExchangeAdd16((ushort *)latch, SHARE);
#endif
	//  see if exclusive request is granted or pending

	if( !(prev & BOTH) )
		return;
#ifdef unix
	prev = __sync_fetch_and_add ((ushort *)latch, -SHARE);
#else
	prev = _InterlockedExchangeAdd16((ushort *)latch, -SHARE);
#endif
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
	prev = __sync_fetch_and_or((ushort *)latch, PEND | XCL);
#else
	prev = _InterlockedOr16((ushort *)latch, PEND | XCL);
#endif
	if( !(prev & XCL) )
	  if( !(prev & ~BOTH) )
		return;
	  else
#ifdef unix
		__sync_fetch_and_and ((ushort *)latch, ~XCL);
#else
		_InterlockedAnd16((ushort *)latch, ~XCL);
#endif
#ifdef  unix
  } while( sched_yield(), 1 );
#else
  } while( SwitchToThread(), 1 );
#endif
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 otherwise

int bt_spinwritetry(BtSpinLatch *latch)
{
ushort prev;

#ifdef  unix
	prev = __sync_fetch_and_or((ushort *)latch, XCL);
#else
	prev = _InterlockedOr16((ushort *)latch, XCL);
#endif
	//	take write access if all bits are clear

	if( !(prev & XCL) )
	  if( !(prev & ~BOTH) )
		return 1;
	  else
#ifdef unix
		__sync_fetch_and_and ((ushort *)latch, ~XCL);
#else
		_InterlockedAnd16((ushort *)latch, ~XCL);
#endif
	return 0;
}

//	clear write mode

void bt_spinreleasewrite(BtSpinLatch *latch)
{
#ifdef unix
	__sync_fetch_and_and((ushort *)latch, ~BOTH);
#else
	_InterlockedAnd16((ushort *)latch, ~BOTH);
#endif
}

//	decrement reader count

void bt_spinreleaseread(BtSpinLatch *latch)
{
#ifdef unix
	__sync_fetch_and_add((ushort *)latch, -SHARE);
#else
	_InterlockedExchangeAdd16((ushort *)latch, -SHARE);
#endif
}

//	link latch table entry into latch hash table

void bt_latchlink (BtDb *bt, ushort hashidx, ushort victim, uid page_no)
{
BtLatchSet *set = bt->mgr->latchsets + victim;

	if( set->next = bt->mgr->latchmgr->table[hashidx].slot )
		bt->mgr->latchsets[set->next].prev = victim;

	bt->mgr->latchmgr->table[hashidx].slot = victim;
	set->page_no = page_no;
	set->hash = hashidx;
	set->prev = 0;
}

//	release latch pin

void bt_unpinlatch (BtLatchSet *set)
{
#ifdef unix
	__sync_fetch_and_add(&set->pin, -1);
#else
	_InterlockedDecrement16 (&set->pin);
#endif
}

//	find existing latchset or inspire new one
//	return with latchset pinned

BtLatchSet *bt_pinlatch (BtDb *bt, uid page_no)
{
ushort hashidx = page_no % bt->mgr->latchmgr->latchhash;
ushort slot, avail = 0, victim, idx;
BtLatchSet *set;

	//  obtain read lock on hash table entry

	bt_spinreadlock(bt->mgr->latchmgr->table[hashidx].latch);

	if( slot = bt->mgr->latchmgr->table[hashidx].slot ) do
	{
		set = bt->mgr->latchsets + slot;
		if( page_no == set->page_no )
			break;
	} while( slot = set->next );

	if( slot ) {
#ifdef unix
		__sync_fetch_and_add(&set->pin, 1);
#else
		_InterlockedIncrement16 (&set->pin);
#endif
	}

    bt_spinreleaseread (bt->mgr->latchmgr->table[hashidx].latch);

	if( slot )
		return set;

  //  try again, this time with write lock

  bt_spinwritelock(bt->mgr->latchmgr->table[hashidx].latch);

  if( slot = bt->mgr->latchmgr->table[hashidx].slot ) do
  {
	set = bt->mgr->latchsets + slot;
	if( page_no == set->page_no )
		break;
	if( !set->pin && !avail )
		avail = slot;
  } while( slot = set->next );

  //  found our entry, or take over an unpinned one

  if( slot || (slot = avail) ) {
	set = bt->mgr->latchsets + slot;
#ifdef unix
	__sync_fetch_and_add(&set->pin, 1);
#else
	_InterlockedIncrement16 (&set->pin);
#endif
	set->page_no = page_no;
	bt_spinreleasewrite(bt->mgr->latchmgr->table[hashidx].latch);
	return set;
  }

	//  see if there are any unused entries
#ifdef unix
	victim = __sync_fetch_and_add (&bt->mgr->latchmgr->latchdeployed, 1) + 1;
#else
	victim = _InterlockedIncrement16 (&bt->mgr->latchmgr->latchdeployed);
#endif

	if( victim < bt->mgr->latchmgr->latchtotal ) {
		set = bt->mgr->latchsets + victim;
#ifdef unix
		__sync_fetch_and_add(&set->pin, 1);
#else
		_InterlockedIncrement16 (&set->pin);
#endif
		bt_latchlink (bt, hashidx, victim, page_no);
		bt_spinreleasewrite (bt->mgr->latchmgr->table[hashidx].latch);
		return set;
	}

#ifdef unix
	victim = __sync_fetch_and_add (&bt->mgr->latchmgr->latchdeployed, -1);
#else
	victim = _InterlockedDecrement16 (&bt->mgr->latchmgr->latchdeployed);
#endif
  //  find and reuse previous lock entry

  while( 1 ) {
#ifdef unix
	victim = __sync_fetch_and_add(&bt->mgr->latchmgr->latchvictim, 1);
#else
	victim = _InterlockedIncrement16 (&bt->mgr->latchmgr->latchvictim) - 1;
#endif
	//	we don't use slot zero

	if( victim %= bt->mgr->latchmgr->latchtotal )
		set = bt->mgr->latchsets + victim;
	else
		continue;

	//	take control of our slot
	//	from other threads

	if( set->pin || !bt_spinwritetry (set->busy) )
		continue;

	idx = set->hash;

	// try to get write lock on hash chain
	//	skip entry if not obtained
	//	or has outstanding locks

	if( !bt_spinwritetry (bt->mgr->latchmgr->table[idx].latch) ) {
		bt_spinreleasewrite (set->busy);
		continue;
	}

	if( set->pin ) {
		bt_spinreleasewrite (set->busy);
		bt_spinreleasewrite (bt->mgr->latchmgr->table[idx].latch);
		continue;
	}

	//  unlink our available victim from its hash chain

	if( set->prev )
		bt->mgr->latchsets[set->prev].next = set->next;
	else
		bt->mgr->latchmgr->table[idx].slot = set->next;

	if( set->next )
		bt->mgr->latchsets[set->next].prev = set->prev;

	bt_spinreleasewrite (bt->mgr->latchmgr->table[idx].latch);
#ifdef unix
	__sync_fetch_and_add(&set->pin, 1);
#else
	_InterlockedIncrement16 (&set->pin);
#endif
	bt_latchlink (bt, hashidx, victim, page_no);
	bt_spinreleasewrite (bt->mgr->latchmgr->table[hashidx].latch);
	bt_spinreleasewrite (set->busy);
	return set;
  }
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
			munmap (pool->map, (uid)(mgr->poolmask+1) << mgr->page_bits);
#else
		{
			FlushViewOfFile(pool->map, 0);
			UnmapViewOfFile(pool->map);
			CloseHandle(pool->hmap);
		}
#endif
	}

#ifdef unix
	munmap (mgr->latchsets, mgr->latchmgr->nlatchpage * mgr->page_size);
	munmap (mgr->latchmgr, 2 * mgr->page_size);
#else
	FlushViewOfFile(mgr->latchmgr, 0);
	UnmapViewOfFile(mgr->latchmgr);
	CloseHandle(mgr->halloc);
#endif
#ifdef unix
	close (mgr->idx);
	free (mgr->pool);
	free (mgr->hash);
	free ((void *)mgr->latch);
	free (mgr);
#else
	FlushFileBuffers(mgr->idx);
	CloseHandle(mgr->idx);
	GlobalFree (mgr->pool);
	GlobalFree (mgr->hash);
	GlobalFree ((void *)mgr->latch);
	GlobalFree (mgr);
#endif
}

//	close and release memory

void bt_close (BtDb *bt)
{
#ifdef unix
	if( bt->mem )
		free (bt->mem);
#else
	if( bt->mem)
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
uint nlatchpage, latchhash;
unsigned char value[BtId];
BtLatchMgr *latchmgr;
off64_t size;
uint amt[1];
BtMgr* mgr;
BtKey key;
BtVal val;
int flag;

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

	mgr->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);

	if( mgr->idx == -1 )
		return free(mgr), NULL;
	
	cacheblk = 4096;	// minimum mmap segment size for unix

#else
	mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtMgr));
	attr = FILE_ATTRIBUTE_NORMAL;
	mgr->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);

	if( mgr->idx == INVALID_HANDLE_VALUE )
		return GlobalFree(mgr), NULL;

	// normalize cacheblk to multiple of sysinfo->dwAllocationGranularity
	GetSystemInfo(sysinfo);
	cacheblk = sysinfo->dwAllocationGranularity;
#endif

#ifdef unix
	latchmgr = malloc (BT_maxpage);
	*amt = 0;

	// read minimum page size to get root info

	if( size = lseek (mgr->idx, 0L, 2) ) {
		if( pread(mgr->idx, latchmgr, BT_minpage, 0) == BT_minpage )
			bits = latchmgr->alloc->bits;
		else
			return free(mgr), free(latchmgr), NULL;
	} else if( mode == BT_ro )
		return free(latchmgr), bt_mgrclose (mgr), NULL;
#else
	latchmgr = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
	size = GetFileSize(mgr->idx, amt);

	if( size || *amt ) {
		if( !ReadFile(mgr->idx, (char *)latchmgr, BT_minpage, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		bits = latchmgr->alloc->bits;
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
#else
	mgr->pool = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, poolmax * sizeof(BtPool));
	mgr->hash = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, hashsize * sizeof(ushort));
	mgr->latch = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, hashsize * sizeof(BtSpinLatch));
#endif

	if( size || *amt )
		goto mgrlatch;

	// initialize an empty b-tree with latch page, root page, page of leaves
	// and page(s) of latches

	memset (latchmgr, 0, 1 << bits);
	nlatchpage = BT_latchtable / (mgr->page_size / sizeof(BtLatchSet)) + 1; 
	bt_putid(latchmgr->alloc->right, MIN_lvl+1+nlatchpage);
	latchmgr->alloc->bits = mgr->page_bits;

	latchmgr->nlatchpage = nlatchpage;
	latchmgr->latchtotal = nlatchpage * (mgr->page_size / sizeof(BtLatchSet));

	//  initialize latch manager

	latchhash = (mgr->page_size - sizeof(BtLatchMgr)) / sizeof(BtHashEntry);

	//	size of hash table = total number of latchsets

	if( latchhash > latchmgr->latchtotal )
		latchhash = latchmgr->latchtotal;

	latchmgr->latchhash = latchhash;

#ifdef unix
	if( write (mgr->idx, latchmgr, mgr->page_size) < mgr->page_size )
		return bt_mgrclose (mgr), NULL;
#else
	if( !WriteFile (mgr->idx, (char *)latchmgr, mgr->page_size, amt, NULL) )
		return bt_mgrclose (mgr), NULL;

	if( *amt < mgr->page_size )
		return bt_mgrclose (mgr), NULL;
#endif

	memset (latchmgr, 0, 1 << bits);
	latchmgr->alloc->bits = mgr->page_bits;

	for( lvl=MIN_lvl; lvl--; ) {
		slotptr(latchmgr->alloc, 1)->off = mgr->page_size - 3 - (lvl ? BtId + 1: 1);
		key = keyptr(latchmgr->alloc, 1);
		key->len = 2;		// create stopper key
		key->key[0] = 0xff;
		key->key[1] = 0xff;

		bt_putid(value, MIN_lvl - lvl + 1);
		val = valptr(latchmgr->alloc, 1);
		val->len = lvl ? BtId : 0;
		memcpy (val->value, value, val->len);

		latchmgr->alloc->min = slotptr(latchmgr->alloc, 1)->off;
		latchmgr->alloc->lvl = lvl;
		latchmgr->alloc->cnt = 1;
		latchmgr->alloc->act = 1;
#ifdef unix
		if( write (mgr->idx, latchmgr, mgr->page_size) < mgr->page_size )
			return bt_mgrclose (mgr), NULL;
#else
		if( !WriteFile (mgr->idx, (char *)latchmgr, mgr->page_size, amt, NULL) )
			return bt_mgrclose (mgr), NULL;

		if( *amt < mgr->page_size )
			return bt_mgrclose (mgr), NULL;
#endif
	}

	// clear out latch manager locks
	//	and rest of pages to round out segment

	memset(latchmgr, 0, mgr->page_size);
	last = MIN_lvl + 1;

	while( last <= ((MIN_lvl + 1 + nlatchpage) | mgr->poolmask) ) {
#ifdef unix
		pwrite(mgr->idx, latchmgr, mgr->page_size, last << mgr->page_bits);
#else
		SetFilePointer (mgr->idx, last << mgr->page_bits, NULL, FILE_BEGIN);
		if( !WriteFile (mgr->idx, (char *)latchmgr, mgr->page_size, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		if( *amt < mgr->page_size )
			return bt_mgrclose (mgr), NULL;
#endif
		last++;
	}

mgrlatch:
#ifdef unix
	// mlock the root page and the latchmgr page

	flag = PROT_READ | PROT_WRITE;
	mgr->latchmgr = mmap (0, 2 * mgr->page_size, flag, MAP_SHARED, mgr->idx, ALLOC_page * mgr->page_size);
	if( mgr->latchmgr == MAP_FAILED )
		return bt_mgrclose (mgr), NULL;
	mlock (mgr->latchmgr, 2 * mgr->page_size);

	mgr->latchsets = (BtLatchSet *)mmap (0, mgr->latchmgr->nlatchpage * mgr->page_size, flag, MAP_SHARED, mgr->idx, LATCH_page * mgr->page_size);
	if( mgr->latchsets == MAP_FAILED )
		return bt_mgrclose (mgr), NULL;
	mlock (mgr->latchsets, mgr->latchmgr->nlatchpage * mgr->page_size);
#else
	flag = PAGE_READWRITE;
	mgr->halloc = CreateFileMapping(mgr->idx, NULL, flag, 0, (BT_latchtable / (mgr->page_size / sizeof(BtLatchSet)) + 1 + LATCH_page) * mgr->page_size, NULL);
	if( !mgr->halloc )
		return bt_mgrclose (mgr), NULL;

	flag = FILE_MAP_WRITE;
	mgr->latchmgr = MapViewOfFile(mgr->halloc, flag, 0, 0, (BT_latchtable / (mgr->page_size / sizeof(BtLatchSet)) + 1 + LATCH_page) * mgr->page_size);
	if( !mgr->latchmgr )
		return GetLastError(), bt_mgrclose (mgr), NULL;

	mgr->latchsets = (void *)((char *)mgr->latchmgr + LATCH_page * mgr->page_size);
#endif

#ifdef unix
	free (latchmgr);
#else
	VirtualFree (latchmgr, 0, MEM_RELEASE);
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
	bt->mem = malloc (2 *mgr->page_size);
#else
	bt->mem = VirtualAlloc(NULL, 2 * mgr->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
	bt->frame = (BtPage)bt->mem;
	bt->cursor = (BtPage)(bt->mem + 1 * mgr->page_size);
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
	pool->pin = CLOCK_bit + 1;

	if( slot = bt->mgr->hash[idx] ) {
		node = bt->mgr->pool + slot;
		pool->hashnext = node;
		node->hashprev = pool;
	}

	bt->mgr->hash[idx] = pool->slot;
}

//  map new buffer pool segment to virtual memory

BTERR bt_mapsegment(BtDb *bt, BtPool *pool, uid page_no)
{
off64_t off = (page_no & ~bt->mgr->poolmask) << bt->mgr->page_bits;
off64_t limit = off + ((bt->mgr->poolmask+1) << bt->mgr->page_bits);
int flag;

#ifdef unix
	flag = PROT_READ | ( bt->mgr->mode == BT_ro ? 0 : PROT_WRITE );
	pool->map = mmap (0, (uid)(bt->mgr->poolmask+1) << bt->mgr->page_bits, flag, MAP_SHARED, bt->mgr->idx, off);
	if( pool->map == MAP_FAILED )
		return bt->err = BTERR_map;

#else
	flag = ( bt->mgr->mode == BT_ro ? PAGE_READONLY : PAGE_READWRITE );
	pool->hmap = CreateFileMapping(bt->mgr->idx, NULL, flag, (DWORD)(limit >> 32), (DWORD)limit, NULL);
	if( !pool->hmap )
		return bt->err = BTERR_map;

	flag = ( bt->mgr->mode == BT_ro ? FILE_MAP_READ : FILE_MAP_WRITE );
	pool->map = MapViewOfFile(pool->hmap, flag, (DWORD)(off >> 32), (DWORD)off, (uid)(bt->mgr->poolmask+1) << bt->mgr->page_bits);
	if( !pool->map )
		return bt->err = BTERR_map;
#endif
 	return bt->err = 0;
}

//	calculate page within pool

BtPage bt_page (BtDb *bt, BtPool *pool, uid page_no)
{
uint subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
BtPage page;

	page = (BtPage)(pool->map + (subpage << bt->mgr->page_bits));
//	madvise (page, bt->mgr->page_size, MADV_WILLNEED);
	return page;
}

//  release pool pin

void bt_unpinpool (BtPool *pool)
{
#ifdef unix
	__sync_fetch_and_add(&pool->pin, -1);
#else
	_InterlockedDecrement16 (&pool->pin);
#endif
}

//	find or place requested page in segment-pool
//	return pool table entry, incrementing pin

BtPool *bt_pinpool(BtDb *bt, uid page_no)
{
uint slot, hashidx, idx, victim;
BtPool *pool, *node, *next;

	//	lock hash table chain

	hashidx = (uint)(page_no >> bt->mgr->seg_bits) % bt->mgr->hashsize;
	bt_spinwritelock (&bt->mgr->latch[hashidx]);

	//	look up in hash table

	if( pool = bt_findpool(bt, page_no, hashidx) ) {
#ifdef unix
		__sync_fetch_and_or(&pool->pin, CLOCK_bit);
		__sync_fetch_and_add(&pool->pin, 1);
#else
		_InterlockedOr16 (&pool->pin, CLOCK_bit);
		_InterlockedIncrement16 (&pool->pin);
#endif
		bt_spinreleasewrite (&bt->mgr->latch[hashidx]);
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

		bt_linkhash(bt, pool, page_no, hashidx);
		bt_spinreleasewrite (&bt->mgr->latch[hashidx]);
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
		victim = _InterlockedIncrement (&bt->mgr->evicted) - 1;
#endif
		victim %= bt->mgr->poolmax;
		pool = bt->mgr->pool + victim;
		idx = (uint)(pool->basepage >> bt->mgr->seg_bits) % bt->mgr->hashsize;

		if( !victim )
			continue;

		// try to get write lock
		//	skip entry if not obtained

		if( !bt_spinwritetry (&bt->mgr->latch[idx]) )
			continue;

		//	skip this entry if
		//	page is pinned
		//  or clock bit is set

		if( pool->pin ) {
#ifdef unix
			__sync_fetch_and_and(&pool->pin, ~CLOCK_bit);
#else
			_InterlockedAnd16 (&pool->pin, ~CLOCK_bit);
#endif
			bt_spinreleasewrite (&bt->mgr->latch[idx]);
			continue;
		}

		// unlink victim pool node from hash table

		if( node = pool->hashprev )
			node->hashnext = pool->hashnext;
		else if( node = pool->hashnext )
			bt->mgr->hash[idx] = node->slot;
		else
			bt->mgr->hash[idx] = 0;

		if( node = pool->hashnext )
			node->hashprev = pool->hashprev;

		bt_spinreleasewrite (&bt->mgr->latch[idx]);

		//	remove old file mapping
#ifdef unix
		munmap (pool->map, (uid)(bt->mgr->poolmask+1) << bt->mgr->page_bits);
#else
//		FlushViewOfFile(pool->map, 0);
		UnmapViewOfFile(pool->map);
		CloseHandle(pool->hmap);
#endif
		pool->map = NULL;

		//  create new pool mapping
		//  and link into hash table

		if( bt_mapsegment(bt, pool, page_no) )
			return NULL;

		bt_linkhash(bt, pool, page_no, hashidx);
		bt_spinreleasewrite (&bt->mgr->latch[hashidx]);
		return pool;
	}
}

// place write, read, or parent lock on requested page_no.

void bt_lockpage(BtLock mode, BtLatchSet *set)
{
	switch( mode ) {
	case BtLockRead:
		ReadLock (set->readwr);
		break;
	case BtLockWrite:
		WriteLock (set->readwr);
		break;
	case BtLockAccess:
		ReadLock (set->access);
		break;
	case BtLockDelete:
		WriteLock (set->access);
		break;
	case BtLockParent:
		WriteLock (set->parent);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *set)
{
	switch( mode ) {
	case BtLockRead:
		ReadRelease (set->readwr);
		break;
	case BtLockWrite:
		WriteRelease (set->readwr);
		break;
	case BtLockAccess:
		ReadRelease (set->access);
		break;
	case BtLockDelete:
		WriteRelease (set->access);
		break;
	case BtLockParent:
		WriteRelease (set->parent);
		break;
	}
}

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
BtPageSet set[1];
uid new_page;
int blk;

	//	lock allocation page

	bt_spinwritelock(bt->mgr->latchmgr->lock);

	// use empty chain first
	// else allocate empty page

	if( new_page = bt_getid(bt->mgr->latchmgr->chain) ) {
		if( set->pool = bt_pinpool (bt, new_page) )
			set->page = bt_page (bt, set->pool, new_page);
		else
			return 0;

		bt_putid(bt->mgr->latchmgr->chain, bt_getid(set->page->right));
		bt_unpinpool (set->pool);
	} else {
		new_page = bt_getid(bt->mgr->latchmgr->alloc->right);
		bt_putid(bt->mgr->latchmgr->alloc->right, new_page+1);
#ifdef unix
		// if writing first page of pool block, set file length thru last page

		if( (new_page & bt->mgr->poolmask) == 0 )
			ftruncate (bt->mgr->idx, (new_page + bt->mgr->poolmask + 1) << bt->mgr->page_bits);
#endif
	}
#ifdef unix
	// unlock allocation latch

	bt_spinreleasewrite(bt->mgr->latchmgr->lock);
#endif

	//	bring new page into pool and copy page.
	//	this will extend the file into the new pages on WIN32.

	if( set->pool = bt_pinpool (bt, new_page) )
		set->page = bt_page (bt, set->pool, new_page);
	else
		return 0;

	memcpy(set->page, page, bt->mgr->page_size);
	bt_unpinpool (set->pool);

#ifndef unix
	// unlock allocation latch

	bt_spinreleasewrite(bt->mgr->latchmgr->lock);
#endif
	return new_page;
}

//  find slot in page for given key at a given level

int bt_findslot (BtPageSet *set, unsigned char *key, uint len)
{
uint diff, higher = set->page->cnt, low = 1, slot;
uint good = 0;

	//	  make stopper key an infinite fence value

	if( bt_getid (set->page->right) )
		higher++;
	else
		good++;

	//	low is the lowest candidate.
	//  loop ends when they meet

	//  higher is already
	//	tested as .ge. the passed key.

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(set->page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on right link page

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtDb *bt, BtPageSet *set, unsigned char *key, uint len, uint lvl, BtLock lock)
{
uid page_no = ROOT_page, prevpage = 0;
uint drill = 0xff, slot;
BtLatchSet *prevlatch;
uint mode, prevmode;
BtPool *prevpool;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (drill == lvl) ? lock : BtLockRead; 

	set->latch = bt_pinlatch (bt, page_no);
	set->page_no = page_no;

	// pin page contents

	if( set->pool = bt_pinpool (bt, page_no) )
		set->page = bt_page (bt, set->pool, page_no);
	else
		return 0;

 	// obtain access lock using lock chaining with Access mode

	if( page_no > ROOT_page )
	  bt_lockpage(BtLockAccess, set->latch);

	//	release & unpin parent page

	if( prevpage ) {
	  bt_unlockpage(prevmode, prevlatch);
	  bt_unpinlatch (prevlatch);
	  bt_unpinpool (prevpool);
	  prevpage = 0;
	}

 	// obtain read lock using lock chaining

	bt_lockpage(mode, set->latch);

	if( set->page->free )
		return bt->err = BTERR_struct, 0;

	if( page_no > ROOT_page )
	  bt_unlockpage(BtLockAccess, set->latch);

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if( set->page_no != ROOT_page )
			return bt->err = BTERR_struct, 0;
			
		drill = set->page->lvl;

		if( lock != BtLockRead && drill == lvl ) {
		  bt_unlockpage(mode, set->latch);
		  bt_unpinlatch (set->latch);
		  bt_unpinpool (set->pool);
		  continue;
		}
	}

	prevpage = set->page_no;
	prevlatch = set->latch;
	prevpool = set->pool;
	prevmode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( !set->page->kill )
	 if( slot = bt_findslot (set, key, len) ) {
	  if( drill == lvl )
		return slot;

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
			continue;
		else
			goto slideright;

	  page_no = bt_getid(valptr(set->page, slot)->value);
	  drill--;
	  continue;
	 }

	//  or slide right into next page

slideright:
	page_no = bt_getid(set->page->right);

  } while( page_no );

  // return error on end of right chain

  bt->err = BTERR_struct;
  return 0;	// return error
}

//	return page to free list
//	page must be delete & write locked

void bt_freepage (BtDb *bt, BtPageSet *set)
{
	//	lock allocation page

	bt_spinwritelock (bt->mgr->latchmgr->lock);

	//	store chain
	memcpy(set->page->right, bt->mgr->latchmgr->chain, BtId);
	bt_putid(bt->mgr->latchmgr->chain, set->page_no);
	set->page->free = 1;

	// unlock released page

	bt_unlockpage (BtLockDelete, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);

	// unlock allocation page

	bt_spinreleasewrite (bt->mgr->latchmgr->lock);
}

//	a fence key was deleted from a page
//	push new fence value upwards

BTERR bt_fixfence (BtDb *bt, BtPageSet *set, uint lvl)
{
unsigned char leftkey[256], rightkey[256];
unsigned char value[BtId];
uid page_no;
BtKey ptr;

	//	remove the old fence value

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (rightkey, ptr, ptr->len + 1);

	memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
	set->page->dirty = 1;

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (leftkey, ptr, ptr->len + 1);
	page_no = set->page_no;

	bt_lockpage (BtLockParent, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	//	insert new (now smaller) fence key

	bt_putid (value, page_no);

	if( bt_insertkey (bt, leftkey+1, *leftkey, lvl+1, value, BtId) )
	  return bt->err;

	//	now delete old fence key

	if( bt_deletekey (bt, rightkey+1, *rightkey, lvl+1) )
		return bt->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch(set->latch);
	bt_unpinpool (set->pool);
	return 0;
}

//	root has a single child
//	collapse a level from the tree

BTERR bt_collapseroot (BtDb *bt, BtPageSet *root)
{
BtPageSet child[1];
uint idx;

  // find the child entry and promote as new root contents

  do {
	for( idx = 0; idx++ < root->page->cnt; )
	  if( !slotptr(root->page, idx)->dead )
		break;

	child->page_no = bt_getid (valptr(root->page, idx)->value);

	child->latch = bt_pinlatch (bt, child->page_no);
	bt_lockpage (BtLockDelete, child->latch);
	bt_lockpage (BtLockWrite, child->latch);

	if( child->pool = bt_pinpool (bt, child->page_no) )
		child->page = bt_page (bt, child->pool, child->page_no);
	else
		return bt->err;

	memcpy (root->page, child->page, bt->mgr->page_size);
	bt_freepage (bt, child);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockWrite, root->latch);
  bt_unpinlatch (root->latch);
  bt_unpinpool (root->pool);
  return 0;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl)
{
unsigned char lowerfence[256], higherfence[256];
uint slot, idx, dirty = 0, fence, found;
BtPageSet set[1], right[1];
unsigned char value[BtId];
BtKey ptr;

	if( slot = bt_loadpage (bt, set, key, len, lvl, BtLockWrite) )
		ptr = keyptr(set->page, slot);
	else
		return bt->err;

	//	are we deleting a fence slot?

	fence = slot == set->page->cnt;

	// if key is found delete it, otherwise ignore request

	if( found = !keycmp (ptr, key, len) )
	  if( found = slotptr(set->page, slot)->dead == 0 ) {
		dirty = slotptr(set->page, slot)->dead = 1;
 		set->page->dirty = 1;
 		set->page->act--;

		// collapse empty slots

		while( idx = set->page->cnt - 1 )
		  if( slotptr(set->page, idx)->dead ) {
			*slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	//	did we delete a fence key in an upper level?

	if( dirty && lvl && set->page->act && fence )
	  if( bt_fixfence (bt, set, lvl) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	is this a collapsed root?

	if( lvl > 1 && set->page_no == ROOT_page && set->page->act == 1 )
	  if( bt_collapseroot (bt, set) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	return if page is not empty

 	if( set->page->act ) {
		bt_unlockpage(BtLockWrite, set->latch);
		bt_unpinlatch (set->latch);
		bt_unpinpool (set->pool);
		return bt->found = found, 0;
	}

	//	cache copy of fence key
	//	to post in parent

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (lowerfence, ptr, ptr->len + 1);

	//	obtain lock on right page

	right->page_no = bt_getid(set->page->right);
	right->latch = bt_pinlatch (bt, right->page_no);
	bt_lockpage (BtLockWrite, right->latch);

	// pin page contents

	if( right->pool = bt_pinpool (bt, right->page_no) )
		right->page = bt_page (bt, right->pool, right->page_no);
	else
		return 0;

	if( right->page->kill )
		return bt->err = BTERR_struct;

	// pull contents of right peer into our empty page

	memcpy (set->page, right->page, bt->mgr->page_size);

	// cache copy of key to update

	ptr = keyptr(right->page, right->page->cnt);
	memcpy (higherfence, ptr, ptr->len + 1);

	// mark right page deleted and point it to left page
	//	until we can post parent updates

	bt_putid (right->page->right, set->page_no);
	right->page->kill = 1;

	bt_lockpage (BtLockParent, right->latch);
	bt_unlockpage (BtLockWrite, right->latch);

	bt_lockpage (BtLockParent, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	// redirect higher key directly to our new node contents

	bt_putid (value, set->page_no);

	if( bt_insertkey (bt, higherfence+1, *higherfence, lvl+1, value, BtId) )
	  return bt->err;

	//	delete old lower key to our node

	if( bt_deletekey (bt, lowerfence+1, *lowerfence, lvl+1) )
	  return bt->err;

	//	obtain delete and write locks to right node

	bt_unlockpage (BtLockParent, right->latch);
	bt_lockpage (BtLockDelete, right->latch);
	bt_lockpage (BtLockWrite, right->latch);
	bt_freepage (bt, right);

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	bt->found = found;
	return 0;
}

//	find key in leaf level and return number of value bytes
//	or (-1) if not found

int bt_findkey (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint valmax)
{
BtPageSet set[1];
uint  slot;
BtKey ptr;
BtVal val;
int ret;

	if( slot = bt_loadpage (bt, set, key, keylen, 0, BtLockRead) )
		ptr = keyptr(set->page, slot);
	else
		return 0;

	// if key exists, return TRUE
	//	otherwise return FALSE

	if( !slotptr(set->page, slot)->dead && !keycmp (ptr, key, keylen) ) {
		val = valptr (set->page,slot);
		if( valmax > val->len )
			valmax = val->len;
		memcpy (value, val->value, valmax);
		ret = valmax;
	} else
		ret = -1;

	bt_unlockpage (BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	return ret;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0  new slot value

uint bt_cleanpage(BtDb *bt, BtPage page, uint keylen, uint slot, uint vallen)
{
uint nxt = bt->mgr->page_size;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey key;
BtVal val;

	if( page->min >= (max+1) * sizeof(BtSlot) + sizeof(*page) + keylen + 1 + vallen + 1)
		return slot;

	//	skip cleanup if nothing to reclaim

	if( !page->dirty )
		return 0;

	memcpy (bt->frame, page, bt->mgr->page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, bt->mgr->page_size - sizeof(*page));
	page->dirty = 0;
	page->act = 0;

	// try cleaning up page first
	// by removing deleted keys

	while( cnt++ < max ) {
		if( cnt == slot )
			newslot = idx + 1;
		if( cnt < max && slotptr(bt->frame,cnt)->dead )
			continue;

		// copy the key across

		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)page + nxt, key, key->len + 1);

		// copy the value across

		val = valptr(bt->frame, cnt);
		nxt -= val->len + 1;
		((unsigned char *)page)[nxt] = val->len;
		memcpy ((unsigned char *)page + nxt + 1, val->value, val->len);

		// set up the slot

		slotptr(page, idx)->off = nxt;

		if( !(slotptr(page, idx)->dead = slotptr(bt->frame, cnt)->dead) )
			page->act++;
	}

	page->min = nxt;
	page->cnt = idx;

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+1) * sizeof(BtSlot) + sizeof(*page) + keylen + 1 + vallen + 1 )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtDb *bt, BtPageSet *root, unsigned char *leftkey, uid page_no2)
{
uint nxt = bt->mgr->page_size;
unsigned char value[BtId];
uid left;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( !(left = bt_newpage(bt, root->page)) )
		return bt->err;

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, bt->mgr->page_size - sizeof(*root->page));

	// insert lower keys page fence key on newroot page as first key

	nxt -= BtId + 1;
	bt_putid (value, left);
	((unsigned char *)root->page)[nxt] = BtId;
	memcpy ((unsigned char *)root->page + nxt + 1, value, BtId);

	nxt -= *leftkey + 1;
	memcpy ((unsigned char *)root->page + nxt, leftkey, *leftkey + 1);
	slotptr(root->page, 1)->off = nxt;
	
	// insert stopper key on newroot page
	// and increase the root height

	nxt -= 3 + BtId + 1;
	((unsigned char *)root->page)[nxt] = 2;
	((unsigned char *)root->page)[nxt+1] = 0xff;
	((unsigned char *)root->page)[nxt+2] = 0xff;

	bt_putid (value, page_no2);
	((unsigned char *)root->page)[nxt+3] = BtId;
	memcpy ((unsigned char *)root->page + nxt + 4, value, BtId);
	slotptr(root->page, 2)->off = nxt;

	bt_putid(root->page->right, 0);
	root->page->min = nxt;		// reset lowest used offset and key count
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	// release and unpin root

	bt_unlockpage(BtLockWrite, root->latch);
	bt_unpinlatch (root->latch);
	bt_unpinpool (root->pool);
	return 0;
}

//  split already locked full node
//	return unlocked.

BTERR bt_splitpage (BtDb *bt, BtPageSet *set)
{
uint cnt = 0, idx = 0, max, nxt = bt->mgr->page_size;
unsigned char fencekey[256], rightkey[256];
unsigned char value[BtId];
uint lvl = set->page->lvl;
BtPageSet right[1];
uint prev;
BtKey key;
BtVal val;

	//  split higher half of keys to bt->frame

	memset (bt->frame, 0, bt->mgr->page_size);
	max = set->page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		val = valptr(set->page, cnt);
		nxt -= val->len + 1;
		((unsigned char *)bt->frame)[nxt] = val->len;
		memcpy ((unsigned char *)bt->frame + nxt + 1, val->value, val->len);

		key = keyptr(set->page, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)bt->frame + nxt, key, key->len + 1);

		slotptr(bt->frame, ++idx)->off = nxt;

		if( !(slotptr(bt->frame, idx)->dead = slotptr(set->page, cnt)->dead) )
			bt->frame->act++;
	}

	// remember existing fence key for new page to the right

	memcpy (rightkey, key, key->len + 1);

	bt->frame->bits = bt->mgr->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	// link right node

	if( set->page_no > ROOT_page )
		memcpy (bt->frame->right, set->page->right, BtId);

	//	get new free page and write higher keys to it.

	if( !(right->page_no = bt_newpage(bt, bt->frame)) )
		return bt->err;

	//	update lower keys to continue in old page

	memcpy (bt->frame, set->page, bt->mgr->page_size);
	memset (set->page+1, 0, bt->mgr->page_size - sizeof(*set->page));
	nxt = bt->mgr->page_size;
	set->page->dirty = 0;
	set->page->act = 0;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys

	while( cnt++ < max / 2 ) {
		val = valptr(bt->frame, cnt);
		nxt -= val->len + 1;
		((unsigned char *)set->page)[nxt] = val->len;
		memcpy ((unsigned char *)set->page + nxt + 1, val->value, val->len);

		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)set->page + nxt, key, key->len + 1);
		slotptr(set->page, ++idx)->off = nxt;
		set->page->act++;
	}

	// remember fence key for smaller page

	memcpy(fencekey, key, key->len + 1);

	bt_putid(set->page->right, right->page_no);
	set->page->min = nxt;
	set->page->cnt = idx;

	// if current page is the root page, split it

	if( set->page_no == ROOT_page )
		return bt_splitroot (bt, set, fencekey, right->page_no);

	// insert new fences in their parent pages

	right->latch = bt_pinlatch (bt, right->page_no);
	bt_lockpage (BtLockParent, right->latch);

	bt_lockpage (BtLockParent, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	// insert new fence for reformulated left block of smaller keys

	bt_putid (value, set->page_no);

	if( bt_insertkey (bt, fencekey+1, *fencekey, lvl+1, value, BtId) )
		return bt->err;

	// switch fence for right block of larger keys to new right page

	bt_putid (value, right->page_no);

	if( bt_insertkey (bt, rightkey+1, *rightkey, lvl+1, value, BtId) )
		return bt->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);

	bt_unlockpage (BtLockParent, right->latch);
	bt_unpinlatch (right->latch);
	return 0;
}
//  Insert new key into the btree at given level.

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint keylen, uint lvl, void *value, uint vallen)
{
BtPageSet set[1];
uint slot, idx;
uint reuse;
BtKey ptr;
BtVal val;

	while( 1 ) {
		if( slot = bt_loadpage (bt, set, key, keylen, lvl, BtLockWrite) )
			ptr = keyptr(set->page, slot);
		else
		{
			if( !bt->err )
				bt->err = BTERR_ovflw;
			return bt->err;
		}

		// if key already exists, update value and return

		if( reuse = !keycmp (ptr, key, keylen) )
		  if( val = valptr(set->page, slot), val->len >= vallen ) {
			if( slotptr(set->page, slot)->dead )
				set->page->act++;
			slotptr(set->page, slot)->dead = 0;
			val->len = vallen;
			memcpy (val->value, value, vallen);
			bt_unlockpage(BtLockWrite, set->latch);
			bt_unpinlatch (set->latch);
			bt_unpinpool (set->pool);
			return 0;
		  } else {
			if( !slotptr(set->page, slot)->dead )
				set->page->act--;
			slotptr(set->page, slot)->dead = 1;
			set->page->dirty = 1;
		  }

		// check if page has enough space

 		if( slot = bt_cleanpage (bt, set->page, keylen, slot, vallen) )
			break;

		if( bt_splitpage (bt, set) )
			return bt->err;
	}

	// calculate next available slot and copy key into page

	set->page->min -= vallen + 1; // reset lowest used offset
	((unsigned char *)set->page)[set->page->min] = vallen;
	memcpy ((unsigned char *)set->page + set->page->min +1, value, vallen );

	set->page->min -= keylen + 1; // reset lowest used offset
	((unsigned char *)set->page)[set->page->min] = keylen;
	memcpy ((unsigned char *)set->page + set->page->min +1, key, keylen );

	for( idx = slot; idx < set->page->cnt; idx++ )
	  if( slotptr(set->page, idx)->dead )
		break;

	// now insert key into array before slot

	if( !reuse && idx == set->page->cnt )
		idx++, set->page->cnt++;

	set->page->act++;

	while( idx > slot )
		*slotptr(set->page, idx) = *slotptr(set->page, idx -1), idx--;

	slotptr(set->page, slot)->off = set->page->min;
	slotptr(set->page, slot)->dead = 0;

	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	return 0;
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint slot;

	// cache page for retrieval

	if( slot = bt_loadpage (bt, set, key, len, 0, BtLockRead) )
	  memcpy (bt->cursor, set->page, bt->mgr->page_size);
	else
	  return 0;

	bt->cursor_page = set->page_no;

	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	return slot;
}

//  return next slot for cursor page
//  or slide cursor right into next page

uint bt_nextkey (BtDb *bt, uint slot)
{
BtPageSet set[1];
uid right;

  do {
	right = bt_getid(bt->cursor->right);

	while( slot++ < bt->cursor->cnt )
	  if( slotptr(bt->cursor,slot)->dead )
		continue;
	  else if( right || (slot < bt->cursor->cnt) ) // skip infinite stopper
		return slot;
	  else
		break;

	if( !right )
		break;

	bt->cursor_page = right;

	if( set->pool = bt_pinpool (bt, right) )
		set->page = bt_page (bt, set->pool, right);
	else
		return 0;

	set->latch = bt_pinlatch (bt, right);
    bt_lockpage(BtLockRead, set->latch);

	memcpy (bt->cursor, set->page, bt->mgr->page_size);

	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	slot = 0;

  } while( 1 );

  return bt->err = 0;
}

BtKey bt_key(BtDb *bt, uint slot)
{
	return keyptr(bt->cursor, slot);
}

BtVal bt_val(BtDb *bt, uint slot)
{
	return valptr(bt->cursor,slot);
}

#ifdef STANDALONE

#ifndef unix
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

uint bt_latchaudit (BtDb *bt)
{
ushort idx, hashidx;
uid next, page_no;
BtLatchSet *latch;
uint cnt = 0;
BtKey ptr;

#ifdef unix
	posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
	if( *(ushort *)(bt->mgr->latchmgr->lock) )
		fprintf(stderr, "Alloc page locked\n");
	*(ushort *)(bt->mgr->latchmgr->lock) = 0;

	for( idx = 1; idx <= bt->mgr->latchmgr->latchdeployed; idx++ ) {
		latch = bt->mgr->latchsets + idx;
		if( *latch->readwr->rin & MASK )
			fprintf(stderr, "latchset %d rwlocked for page %.8x\n", idx, latch->page_no);
		memset ((ushort *)latch->readwr, 0, sizeof(RWLock));

		if( *latch->access->rin & MASK )
			fprintf(stderr, "latchset %d accesslocked for page %.8x\n", idx, latch->page_no);
		memset ((ushort *)latch->access, 0, sizeof(RWLock));

		if( *latch->parent->rin & MASK )
			fprintf(stderr, "latchset %d parentlocked for page %.8x\n", idx, latch->page_no);
		memset ((ushort *)latch->parent, 0, sizeof(RWLock));

		if( latch->pin ) {
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, latch->page_no);
			latch->pin = 0;
		}
	}

	for( hashidx = 0; hashidx < bt->mgr->latchmgr->latchhash; hashidx++ ) {
	  if( *(ushort *)(bt->mgr->latchmgr->table[hashidx].latch) )
			fprintf(stderr, "hash entry %d locked\n", hashidx);

	  *(ushort *)(bt->mgr->latchmgr->table[hashidx].latch) = 0;

	  if( idx = bt->mgr->latchmgr->table[hashidx].slot ) do {
		latch = bt->mgr->latchsets + idx;
		if( *(ushort *)latch->busy )
			fprintf(stderr, "latchset %d busylocked for page %.8x\n", idx, latch->page_no);
		*(ushort *)latch->busy = 0;
		if( latch->pin )
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, latch->page_no);
	  } while( idx = latch->next );
	}

	next = bt->mgr->latchmgr->nlatchpage + LATCH_page;
	page_no = LEAF_page;

	while( page_no < bt_getid(bt->mgr->latchmgr->alloc->right) ) {
	off64_t off = page_no << bt->mgr->page_bits;
#ifdef unix
		pread (bt->mgr->idx, bt->frame, bt->mgr->page_size, off);
#else
		DWORD amt[1];

		  SetFilePointer (bt->mgr->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

		  if( !ReadFile(bt->mgr->idx, bt->frame, bt->mgr->page_size, amt, NULL))
			fprintf(stderr, "page %.8x unable to read\n", page_no);

		  if( *amt <  bt->mgr->page_size )
			fprintf(stderr, "page %.8x unable to read\n", page_no);
#endif
		if( !bt->frame->free ) {
		 for( idx = 0; idx++ < bt->frame->cnt - 1; ) {
		  ptr = keyptr(bt->frame, idx+1);
		  if( keycmp (keyptr(bt->frame, idx), ptr->key, ptr->len) >= 0 )
			fprintf(stderr, "page %.8x idx %.2x out of order\n", page_no, idx);
		 }
		 if( !bt->frame->lvl )
			cnt += bt->frame->act;
		}
		if( page_no > LEAF_page )
			next = page_no + 1;
		page_no = next;
	}
	return cnt - 1;
}

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
BtPageSet set[1];
BtKey ptr;
BtDb *bt;
FILE *in;

	bt = bt_open (args->mgr);

	switch(args->type | 0x20)
	{
	case 'a':
		fprintf(stderr, "started latch mgr audit\n");
		cnt = bt_latchaudit (bt);
		fprintf(stderr, "finished latch mgr audit, found %d keys\n", cnt);
		break;

	case 'p':
		fprintf(stderr, "started pennysort for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( bt_insertkey (bt, key, 10, 0, key + 10, len - 10) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < 255 )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

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

			  if( bt_insertkey (bt, key, len, 0, NULL, 0) )
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

			  if( bt_findkey (bt, key, len, NULL, 0) == 0 )
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
		fprintf(stderr, "started scanning\n");
	  	do {
			if( set->pool = bt_pinpool (bt, page_no) )
				set->page = bt_page (bt, set->pool, page_no);
			else
				break;
			set->latch = bt_pinlatch (bt, page_no);
			bt_lockpage (BtLockRead, set->latch);
			next = bt_getid (set->page->right);
			cnt += set->page->act;

			for( slot = 0; slot++ < set->page->cnt; )
			 if( next || slot < set->page->cnt )
			  if( !slotptr(set->page, slot)->dead ) {
				ptr = keyptr(set->page, slot);
				fwrite (ptr->key, ptr->len, 1, stdout);
				fputc ('\n', stdout);
			  }

			bt_unlockpage (BtLockRead, set->latch);
			bt_unpinlatch (set->latch);
			bt_unpinpool (set->pool);
	  	} while( page_no = next );

	  	cnt--;	// remove stopper key
		fprintf(stderr, " Total keys read %d\n", cnt);
		break;

	case 'c':
#ifdef unix
		posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
		fprintf(stderr, "started counting\n");
		next = bt->mgr->latchmgr->nlatchpage + LATCH_page;
		page_no = LEAF_page;

		while( page_no < bt_getid(bt->mgr->latchmgr->alloc->right) ) {
		uid off = page_no << bt->mgr->page_bits;
#ifdef unix
		  pread (bt->mgr->idx, bt->frame, bt->mgr->page_size, off);
#else
		DWORD amt[1];

		  SetFilePointer (bt->mgr->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

		  if( !ReadFile(bt->mgr->idx, bt->frame, bt->mgr->page_size, amt, NULL))
			return bt->err = BTERR_map;

		  if( *amt <  bt->mgr->page_size )
			return bt->err = BTERR_map;
#endif
			if( !bt->frame->free && !bt->frame->lvl )
				cnt += bt->frame->act;
			if( page_no > LEAF_page )
				next = page_no + 1;
			page_no = next;
		}
		
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
double start, stop;
#ifdef unix
pthread_t *threads;
#else
HANDLE *threads;
#endif
ThreadArg *args;
uint poolsize = 0;
float elapsed;
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

	start = getCpuTime(0);

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

	bt_mgrclose (mgr);
}

#endif	//STANDALONE
