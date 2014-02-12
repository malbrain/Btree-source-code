// btree version threads2h pthread rw lock version
//	with reworked bt_deletekey code
// 12 FEB 2014

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

//	mode & definition for latch implementation

enum {
	Mutex = 1,
	Write = 2,
	Pending = 4,
	Share = 8
} LockMode;

// exclusive is set for write access
// share is count of read accessors
// grant write lock when share == 0

typedef struct {
	volatile ushort mutex:1;
	volatile ushort exclusive:1;
	volatile ushort pending:1;
	volatile ushort share:13;
} BtSpinLatch;

//  hash table entries

typedef struct {
	BtSpinLatch latch[1];
	volatile ushort slot;		// Latch table entry at head of chain
} BtHashEntry;

//	latch manager table structure

typedef struct {
#ifdef unix
	pthread_rwlock_t lock[1];
#else
	SRWLOCK srw[1];
#endif
} BtLatch;

typedef struct {
	BtLatch readwr[1];			// read/write page lock
	BtLatch access[1];			// Access Intent/Page delete
	BtLatch parent[1];			// Posting of fence key in parent
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

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	unsigned char bits:7;		// page size in bits
	unsigned char free:1;		// page is on free list
	unsigned char lvl:4;		// level of page
	unsigned char kill:1;		// page is being killed
	unsigned char dirty:1;		// page has deleted keys
	unsigned char posted:1;		// page fence is posted
	unsigned char goright:1;	// page is being killed, continue to right
	unsigned char right[BtId];	// page number to right
	unsigned char fence[256];	// page fence key
} *BtPage;

//	The memory mapping pool table buffer manager entry

typedef struct {
	unsigned long long int lru;	// number of times accessed
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

//  The loadpage interface object

typedef struct {
	uid page_no;		// current page number
	BtPage page;		// current page pointer
	BtPool *pool;		// current page pool
	BtLatchSet *latch;	// current page latch set
} BtPageSet;

//	structure for latch manager on ALLOC_page

typedef struct {
	struct BtPage_ alloc[2];	// next & free page_nos in right ptr
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
	BtPage zero;		// page frame for zeroes at end of file
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
extern BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uid id, uint tod, uint lvl);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len);
extern uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	internal functions
BTERR bt_removepage (BtDb *bt, BtPageSet *set, uint lvl, unsigned char *pagefence);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint mode, uint bits, uint poolsize, uint segsize, uint hashsize);
void bt_mgrclose (BtMgr *mgr);

//  Helper functions to return slot values

extern BtKey bt_key (BtDb *bt, uint slot);
extern uid bt_uid (BtDb *bt, uint slot);
extern uint bt_tod (BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0	// allocation & lock manager hash table
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves
#define LATCH_page		3	// pages for lock manager

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

//	The b-tree pages are linked with next
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup The fence key for a node is
//	present in a special array.

//  Groups of pages called segments from the btree are optionally
//  cached with a memory mapped pool. A hash table is used to keep
//  track of the cached segments.  This behaviour is controlled
//  by the cache block size parameter to bt_open.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse.

//	The ParentModification lock on a node is obtained to serialize posting
//	or changing the fence key for a node.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page.
//	Page slots use 1 based indexing.

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

//	Latch Manager

//	wait until write lock mode is clear
//	and add 1 to the share count

void bt_spinreadlock(BtSpinLatch *latch)
{
ushort prev;

  do {
	//	obtain latch mutex
#ifdef unix
	if( __sync_fetch_and_or((ushort *)latch, Mutex) & Mutex )
		continue;
#else
	if( prev = _InterlockedOr16((ushort *)latch, Mutex) & Mutex )
		continue;
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
  do {
#ifdef  unix
	if( __sync_fetch_and_or((ushort *)latch, Mutex | Pending) & Mutex )
		continue;
#else
	if( _InterlockedOr16((ushort *)latch, Mutex | Pending) & Mutex )
		continue;
#endif
	if( !(latch->share | latch->exclusive) ) {
#ifdef unix
		__sync_fetch_and_or((ushort *)latch, Write);
		__sync_fetch_and_and ((ushort *)latch, ~(Mutex | Pending));
#else
		_InterlockedOr16((ushort *)latch, Write);
		_InterlockedAnd16((ushort *)latch, ~(Mutex | Pending));
#endif
		return;
	}

#ifdef unix
	__sync_fetch_and_and ((ushort *)latch, ~Mutex);
#else
	_InterlockedAnd16((ushort *)latch, ~Mutex);
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

#ifdef unix
	__sync_fetch_and_and ((ushort *)latch, ~Mutex);
#else
	_InterlockedAnd16((ushort *)latch, ~Mutex);
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

void bt_readlock(BtLatch *latch)
{
#ifdef unix
	pthread_rwlock_rdlock (latch->lock);
#else
	AcquireSRWLockShared (latch->srw);
#endif
}

//	wait for other read and write latches to relinquish

void bt_writelock(BtLatch *latch)
{
#ifdef unix
	pthread_rwlock_wrlock (latch->lock);
#else
	AcquireSRWLockExclusive (latch->srw);
#endif
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 if already write or read locked

int bt_writetry(BtLatch *latch)
{
int result = 0;

#ifdef unix
	result = !pthread_rwlock_trywrlock (latch->lock);
#else
	result = TryAcquireSRWLockExclusive (latch->srw);
#endif
	return result;
}

//	clear write mode

void bt_releasewrite(BtLatch *latch)
{
#ifdef unix
	pthread_rwlock_unlock (latch->lock);
#else
	ReleaseSRWLockExclusive (latch->srw);
#endif
}

//	decrement reader count

void bt_releaseread(BtLatch *latch)
{
#ifdef unix
	pthread_rwlock_unlock (latch->lock);
#else
	ReleaseSRWLockShared (latch->srw);
#endif
}

void bt_initlockset (BtLatchSet *set)
{
#ifdef unix
pthread_rwlockattr_t rwattr[1];

	pthread_rwlockattr_init (rwattr);
	pthread_rwlockattr_setpshared (rwattr, PTHREAD_PROCESS_SHARED);

	pthread_rwlock_init (set->readwr->lock, rwattr);
	pthread_rwlock_init (set->access->lock, rwattr);
	pthread_rwlock_init (set->parent->lock, rwattr);
	pthread_rwlockattr_destroy (rwattr);
#else
	InitializeSRWLock (set->readwr->srw);
	InitializeSRWLock (set->access->srw);
	InitializeSRWLock (set->parent->srw);
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
		bt_initlockset (set);
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
	munmap (mgr->latchsets, mgr->latchmgr->nlatchpage * mgr->page_size);
	munmap (mgr->latchmgr, mgr->page_size);
#else
	FlushViewOfFile(mgr->latchmgr, 0);
	UnmapViewOfFile(mgr->latchmgr);
	CloseHandle(mgr->halloc);
#endif
#ifdef unix
	close (mgr->idx);
	free (mgr->pool);
	free (mgr->hash);
	free (mgr->latch);
	free (mgr);
#else
	FlushFileBuffers(mgr->idx);
	CloseHandle(mgr->idx);
	GlobalFree (mgr->pool);
	GlobalFree (mgr->hash);
	GlobalFree (mgr->latch);
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
uint nlatchpage, latchhash;
BtLatchMgr *latchmgr;
off64_t size;
uint amt[1];
BtMgr* mgr;
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
		slotptr(latchmgr->alloc, 1)->off = offsetof(struct BtPage_, fence);
		bt_putid(slotptr(latchmgr->alloc, 1)->id, lvl ? MIN_lvl - lvl + 1 : 0);		// next(lower) page number
		latchmgr->alloc->fence[0] = 2;
		latchmgr->alloc->fence[1] = 0xff;
		latchmgr->alloc->fence[2] = 0xff;
		latchmgr->alloc->min = mgr->page_size;
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
	flag = PROT_READ | PROT_WRITE;
	mgr->latchmgr = mmap (0, mgr->page_size, flag, MAP_SHARED, mgr->idx, ALLOC_page * mgr->page_size);
	if( mgr->latchmgr == MAP_FAILED )
		return bt_mgrclose (mgr), NULL;
	mgr->latchsets = (BtLatchSet *)mmap (0, mgr->latchmgr->nlatchpage * mgr->page_size, flag, MAP_SHARED, mgr->idx, LATCH_page * mgr->page_size);
	if( mgr->latchsets == MAP_FAILED )
		return bt_mgrclose (mgr), NULL;
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
	bt->mem = malloc (3 *mgr->page_size);
#else
	bt->mem = VirtualAlloc(NULL, 3 * mgr->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
	bt->frame = (BtPage)bt->mem;
	bt->zero = (BtPage)(bt->mem + 1 * mgr->page_size);
	bt->cursor = (BtPage)(bt->mem + 2 * mgr->page_size);

	memset (bt->zero, 0, mgr->page_size);
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

	//  scan pool entries under hash table slot

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
	pool->map = mmap (0, (bt->mgr->poolmask+1) << bt->mgr->page_bits, flag, MAP_SHARED | MAP_POPULATE, bt->mgr->idx, off);
	if( pool->map == MAP_FAILED )
		return bt->err = BTERR_map;
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

//	calculate page within pool

BtPage bt_page (BtDb *bt, BtPool *pool, uid page_no)
{
uint subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
BtPage page;

	page = (BtPage)(pool->map + (subpage << bt->mgr->page_bits));
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
		victim = _InterlockedIncrement (&bt->mgr->evicted) - 1;
#endif
		victim %= bt->mgr->hashsize;

		// try to get write lock
		//	skip entry if not obtained

		if( !bt_spinwritetry (&bt->mgr->latch[victim]) )
			continue;

		//  if pool entry is empty
		//	or any pages are pinned
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

void bt_lockpage(BtLock mode, BtLatchSet *set)
{
	switch( mode ) {
	case BtLockRead:
		bt_readlock (set->readwr);
		break;
	case BtLockWrite:
		bt_writelock (set->readwr);
		break;
	case BtLockAccess:
		bt_readlock (set->access);
		break;
	case BtLockDelete:
		bt_writelock (set->access);
		break;
	case BtLockParent:
		bt_writelock (set->parent);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *set)
{
	switch( mode ) {
	case BtLockRead:
		bt_releaseread (set->readwr);
		break;
	case BtLockWrite:
		bt_releasewrite (set->readwr);
		break;
	case BtLockAccess:
		bt_releaseread (set->access);
		break;
	case BtLockDelete:
		bt_releasewrite (set->access);
		break;
	case BtLockParent:
		bt_releasewrite (set->parent);
		break;
	}
}

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
BtPageSet set[1];
uid new_page;
int reuse;

	//	lock allocation page

	bt_spinwritelock(bt->mgr->latchmgr->lock);

	// use empty chain first
	// else allocate empty page

	if( new_page = bt_getid(bt->mgr->latchmgr->alloc[1].right) ) {
		if( set->pool = bt_pinpool (bt, new_page) )
			set->page = bt_page (bt, set->pool, new_page);
		else
			return 0;

		bt_putid(bt->mgr->latchmgr->alloc[1].right, bt_getid(set->page->right));
		bt_unpinpool (set->pool);
		reuse = 1;
	} else {
		new_page = bt_getid(bt->mgr->latchmgr->alloc->right);
		bt_putid(bt->mgr->latchmgr->alloc->right, new_page+1);
		reuse = 0;
	}
#ifdef unix
	if ( pwrite(bt->mgr->idx, page, bt->mgr->page_size, new_page << bt->mgr->page_bits) < bt->mgr->page_size )
		return bt->err = BTERR_wrt, 0;

	// if writing first page of pool block, zero last page in the block

	if ( !reuse && bt->mgr->poolmask > 0 && (new_page & bt->mgr->poolmask) == 0 )
	{
		// use zero buffer to write zeros
		if ( pwrite(bt->mgr->idx,bt->zero, bt->mgr->page_size, (new_page | bt->mgr->poolmask) << bt->mgr->page_bits) < bt->mgr->page_size )
			return bt->err = BTERR_wrt, 0;
	}
#else
	//	bring new page into pool and copy page.
	//	this will extend the file into the new pages.

	if( set->pool = bt_pinpool (bt, new_page) )
		set->page = bt_page (bt, set->pool, new_page);
	else
		return 0;

	memcpy(set->page, page, bt->mgr->page_size);
	bt_unpinpool (set->pool);
#endif
	// unlock allocation latch and return new page no

	bt_spinreleasewrite(bt->mgr->latchmgr->lock);
	return new_page;
}

//  find slot in page for given key at a given level

int bt_findslot (BtPageSet *set, unsigned char *key, uint len)
{
uint diff, higher = set->page->cnt, low = 1, slot;

	//	  make stopper key an infinite fence value

	if( bt_getid (set->page->right) )
		higher++;

	//	low is the lowest candidate.
	//  loop ends when they meet

	//  higher is already
	//	tested as .ge. the given key.

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(set->page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot;
	}

	if( higher <= set->page->cnt )
		return higher;

	//	if leaf page, compare against fence value

	//	return zero if key is on right link page
	//	or return slot beyond last key

	if( set->page->lvl || keycmp ((BtKey)set->page->fence, key, len) < 0 )
 		return 0;

	return higher;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtDb *bt, BtPageSet *set, unsigned char *key, uint len, uint lvl, uint lock)
{
uid page_no = ROOT_page, prevpage = 0;
uint drill = 0xff, slot;
BtLatchSet *prevlatch;
uint mode, prevmode;
BtPool *prevpool;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (lock == BtLockWrite) && (drill == lvl) ? BtLockWrite : BtLockRead; 

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

	if( page_no > ROOT_page )
	  bt_unlockpage(BtLockAccess, set->latch);

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if ( set->page_no != ROOT_page )
			return bt->err = BTERR_struct, 0;
			
		drill = set->page->lvl;

		if( lock == BtLockWrite && drill == lvl ) {
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

	//	if page is being deleted and we should continue right

	if( set->page->kill && set->page->goright ) {
		page_no = bt_getid (set->page->right);
		continue;
	}

	//	otherwise, wait for deleted node to clear

	if( set->page->kill ) {
		bt_unlockpage(mode, set->latch);
		bt_unpinlatch (set->latch);
		bt_unpinpool (set->pool);
		page_no = ROOT_page;
		prevpage = 0;
		drill = 0xff;
#ifdef unix
		sched_yield();
#else
		SwitchToThread();
#endif
		continue;
	}
	
	//  find key on page at this level
	//  and descend to requested level

	if( slot = bt_findslot (set, key, len) ) {
	  if( drill == lvl )
		return slot;

	  if( slot > set->page->cnt )
		return bt->err = BTERR_struct;

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
			continue;
		else
			return bt->err = BTERR_struct, 0;

	  page_no = bt_getid(slotptr(set->page, slot)->id);
	  drill--;
	  continue;
	}

	//  or slide right into next page

	page_no = bt_getid(set->page->right);

  } while( page_no );

  // return error on end of right chain

  bt->err = BTERR_struct;
  return 0;	// return error
}

// drill down fixing fence values for left sibling tree

//  call with set write locked
//	return with set unlocked & unpinned.

BTERR bt_fixfences (BtDb *bt, BtPageSet *set, unsigned char *newfence)
{
unsigned char oldfence[256];
BtPageSet next[1];
int chk;

  memcpy (oldfence, set->page->fence, 256);

  while( !set->page->kill && set->page->lvl ) {
	next->page_no = bt_getid(slotptr(set->page, set->page->cnt)->id);
	next->latch = bt_pinlatch (bt, next->page_no);
	bt_lockpage (BtLockParent, next->latch);
	bt_lockpage (BtLockAccess, next->latch);
	bt_lockpage (BtLockWrite, next->latch);
	bt_unlockpage (BtLockAccess, next->latch);

	if( next->pool = bt_pinpool (bt, next->page_no) )
		next->page = bt_page (bt, next->pool, next->page_no);
	else
		return bt->err;

	chk = keycmp ((BtKey)next->page->fence, oldfence + 1, *oldfence);

	if( chk < 0 ) {
	  next->page_no = bt_getid (next->page->right);
	  bt_unlockpage (BtLockWrite, next->latch);
	  bt_unlockpage (BtLockParent, next->latch);
	  bt_unpinlatch (next->latch);
	  bt_unpinpool (next->pool);
	  continue;
	}

	if( chk > 0 )
		return bt->err = BTERR_struct;

	if( bt_fixfences (bt, next, newfence) )
		return bt->err;

	break;
  }

  memcpy (set->page->fence, newfence, 256);

  bt_unlockpage (BtLockWrite, set->latch);
  bt_unlockpage (BtLockParent, set->latch);
  bt_unpinlatch (set->latch);
  bt_unpinpool (set->pool);
  return 0;
}

//	return page to free list
//	page must be delete & write locked

void bt_freepage (BtDb *bt, BtPageSet *set)
{
	//	lock allocation page

	bt_spinwritelock (bt->mgr->latchmgr->lock);

	//	store chain in second right
	bt_putid(set->page->right, bt_getid(bt->mgr->latchmgr->alloc[1].right));
	bt_putid(bt->mgr->latchmgr->alloc[1].right, set->page_no);
	set->page->free = 1;

	// unlock released page

	bt_unlockpage (BtLockDelete, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);

	// unlock allocation page

	bt_spinreleasewrite (bt->mgr->latchmgr->lock);
}

//	remove the root level by promoting its only child
//	call with parent and child pages

BTERR bt_removeroot (BtDb *bt, BtPageSet *root, BtPageSet *child)
{
uid next = 0;

  do {
	if( next ) {
	  child->latch = bt_pinlatch (bt, next);
	  bt_lockpage (BtLockDelete, child->latch);
	  bt_lockpage (BtLockWrite, child->latch);

	  if( child->pool = bt_pinpool (bt, next) )
		child->page = bt_page (bt, child->pool, next);
	  else
		return bt->err;

	  child->page_no = next;
	}

	memcpy (root->page, child->page, bt->mgr->page_size);
	next = bt_getid (slotptr(child->page, child->page->cnt)->id);
	bt_freepage (bt, child);
  } while( root->page->lvl > 1 && root->page->cnt == 1 );

  bt_unlockpage (BtLockWrite, root->latch);
  bt_unpinlatch (root->latch);
  bt_unpinpool (root->pool);
  return 0;
}

//  pull right page over ourselves in simple merge

BTERR bt_mergeright (BtDb *bt, BtPageSet *set, BtPageSet *parent, BtPageSet *right, uint slot, uint idx)
{
	//  install ourselves as child page
	//	and delete ourselves from parent

	bt_putid (slotptr(parent->page, idx)->id, set->page_no);
	slotptr(parent->page, slot)->dead = 1;
	parent->page->act--;

	//	collapse any empty slots

	while( idx = parent->page->cnt - 1 )
	  if( slotptr(parent->page, idx)->dead ) {
		*slotptr(parent->page, idx) = *slotptr(parent->page, idx + 1);
		memset (slotptr(parent->page, parent->page->cnt--), 0, sizeof(BtSlot));
	  } else
		  break;

	memcpy (set->page, right->page, bt->mgr->page_size);
	bt_unlockpage (BtLockParent, right->latch);

	bt_freepage (bt, right);

	//	do we need to remove a btree level?
	//	(leave the first page of leaves alone)

	if( parent->page_no == ROOT_page && parent->page->cnt == 1 )
	  if( set->page->lvl )
		return bt_removeroot (bt, parent, set);

	bt_unlockpage (BtLockWrite, parent->latch);
	bt_unlockpage (BtLockDelete, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (parent->latch);
	bt_unpinpool (parent->pool);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	return 0;
}

//	remove both child and parent from the btree
//	from the fence position in the parent
//	call with both pages locked for writing

BTERR bt_removeparent (BtDb *bt, BtPageSet *child, BtPageSet *parent, BtPageSet *right, BtPageSet *rparent, uint lvl)
{
unsigned char pagefence[256];
uint idx;

	//  pull right sibling over ourselves and unlock

	memcpy (child->page, right->page, bt->mgr->page_size);

	bt_unlockpage (BtLockWrite, child->latch);
	bt_unpinlatch (child->latch);
	bt_unpinpool (child->pool);

	//  install ourselves into right link of old right page

	bt_putid (right->page->right, child->page_no);
	right->page->goright = 1;	// tell bt_loadpage to go right to us
	right->page->kill = 1;

	bt_unlockpage (BtLockWrite, right->latch);

	//	remove our slot from our parent
	//	signal to move right

	parent->page->goright = 1;	// tell bt_loadpage to go right to rparent
	parent->page->kill = 1;
	parent->page->act--;

	//	redirect right page pointer in right parent to us

	for( idx = 0; idx++ < rparent->page->cnt; )
	  if( !slotptr(rparent->page, idx)->dead )
		break;

	if( bt_getid (slotptr(rparent->page, idx)->id) != right->page_no )
		return bt->err = BTERR_struct;

	bt_putid (slotptr(rparent->page, idx)->id, child->page_no);
	bt_unlockpage (BtLockWrite, rparent->latch);
	bt_unpinlatch (rparent->latch);
	bt_unpinpool (rparent->pool);

	//	free the right page

	bt_lockpage (BtLockDelete, right->latch);
	bt_lockpage (BtLockWrite, right->latch);
	bt_freepage (bt, right);

	//	save parent page fence value

	memcpy (pagefence, parent->page->fence, 256);
	bt_unlockpage (BtLockWrite, parent->latch);

	return bt_removepage (bt, parent, lvl, pagefence);
}

//	remove page from btree
//	call with page unlocked
//	returns with page on free list

BTERR bt_removepage (BtDb *bt, BtPageSet *set, uint lvl, unsigned char *pagefence)
{
BtPageSet parent[1], sibling[1], rparent[1];
unsigned char newfence[256];
uint slot, idx;
BtKey ptr;

	//	load and lock our parent

retry:
	if( !(slot = bt_loadpage (bt, parent, pagefence+1, *pagefence, lvl+1, BtLockWrite)) )
		return bt->err;

	//	can we do a simple merge entirely
	//	between siblings on the parent page?

	if( slot < parent->page->cnt ) {
		// find our right neighbor
		//	right must exist because the stopper prevents
		//	the rightmost page from deleting

		for( idx = slot; idx++ < parent->page->cnt; )
		  if( !slotptr(parent->page, idx)->dead )
			break;

		sibling->page_no = bt_getid (slotptr (parent->page, idx)->id);

		bt_lockpage (BtLockDelete, set->latch);
		bt_lockpage (BtLockWrite, set->latch);

		//	merge right if sibling shows up in
		//  our parent and is not being killed

		if( sibling->page_no == bt_getid (set->page->right) ) {
		  sibling->latch = bt_pinlatch (bt, sibling->page_no);
		  bt_lockpage (BtLockParent, sibling->latch);
		  bt_lockpage (BtLockDelete, sibling->latch);
		  bt_lockpage (BtLockWrite, sibling->latch);

		  if( sibling->pool = bt_pinpool (bt, sibling->page_no) )
			sibling->page = bt_page (bt, sibling->pool, sibling->page_no);
		  else
			return bt->err;

		  if( !sibling->page->kill )
			return bt_mergeright(bt, set, parent, sibling, slot, idx);

		  //  try again later

		  bt_unlockpage (BtLockWrite, sibling->latch);
		  bt_unlockpage (BtLockParent, sibling->latch);
		  bt_unlockpage (BtLockDelete, sibling->latch);
		  bt_unpinlatch (sibling->latch);
		  bt_unpinpool (sibling->pool);
		}

		bt_unlockpage (BtLockDelete, set->latch);
		bt_unlockpage (BtLockWrite, set->latch);
		bt_unlockpage (BtLockWrite, parent->latch);
		bt_unpinlatch (parent->latch);
		bt_unpinpool (parent->pool);
#ifdef linux
		sched_yield();
#else
		SwitchToThread();
#endif
		goto retry;
	}

	//  find our left neighbor in our parent page

	for( idx = slot; --idx; )
	  if( !slotptr(parent->page, idx)->dead )
		break;

	//	if no left neighbor, delete ourselves and our parent

	if( !idx ) {
		bt_lockpage (BtLockAccess, set->latch);
		bt_lockpage (BtLockWrite, set->latch);
		bt_unlockpage (BtLockAccess, set->latch);

		rparent->page_no = bt_getid (parent->page->right);
		rparent->latch = bt_pinlatch (bt, rparent->page_no);

		bt_lockpage (BtLockAccess, rparent->latch);
		bt_lockpage (BtLockWrite, rparent->latch);
		bt_unlockpage (BtLockAccess, rparent->latch);

		if( rparent->pool = bt_pinpool (bt, rparent->page_no) )
			rparent->page = bt_page (bt, rparent->pool, rparent->page_no);
		else
			return bt->err;

		if( !rparent->page->kill ) {
		  sibling->page_no = bt_getid (set->page->right);
		  sibling->latch = bt_pinlatch (bt, sibling->page_no);

		  bt_lockpage (BtLockAccess, sibling->latch);
		  bt_lockpage (BtLockWrite, sibling->latch);
		  bt_unlockpage (BtLockAccess, sibling->latch);

		  if( sibling->pool = bt_pinpool (bt, sibling->page_no) )
			sibling->page = bt_page (bt, sibling->pool, sibling->page_no);
		  else
			return bt->err;

		  if( !sibling->page->kill )
			return bt_removeparent (bt, set, parent, sibling, rparent, lvl+1);

		  //  try again later

		  bt_unlockpage (BtLockWrite, sibling->latch);
		  bt_unpinlatch (sibling->latch);
		  bt_unpinpool (sibling->pool);
		}

		bt_unlockpage (BtLockWrite, set->latch);
		bt_unlockpage (BtLockWrite, rparent->latch);
		bt_unpinlatch (rparent->latch);
		bt_unpinpool (rparent->pool);

		bt_unlockpage (BtLockWrite, parent->latch);
		bt_unpinlatch (parent->latch);
		bt_unpinpool (parent->pool);
#ifdef linux
		sched_yield();
#else
		SwitchToThread();
#endif
		goto retry;
	}

	// redirect parent to our left sibling
	// lock and map our left sibling's page

	sibling->page_no = bt_getid (slotptr(parent->page, idx)->id);
	sibling->latch = bt_pinlatch (bt, sibling->page_no);

	//	wait our turn on fence key maintenance

	bt_lockpage(BtLockParent, sibling->latch);
	bt_lockpage(BtLockAccess, sibling->latch);
	bt_lockpage(BtLockWrite, sibling->latch);
	bt_unlockpage(BtLockAccess, sibling->latch);

	if( sibling->pool = bt_pinpool (bt, sibling->page_no) )
		sibling->page = bt_page (bt, sibling->pool, sibling->page_no);
	else
		return bt->err;

	//  wait until left sibling is in our parent

	if( bt_getid (sibling->page->right) != set->page_no ) {
		bt_unlockpage (BtLockWrite, parent->latch);
		bt_unlockpage (BtLockWrite, sibling->latch);
		bt_unlockpage (BtLockParent, sibling->latch);
		bt_unpinlatch (parent->latch);
		bt_unpinpool (parent->pool);
		bt_unpinlatch (sibling->latch);
		bt_unpinpool (sibling->pool);
#ifdef linux
		sched_yield();
#else
		SwitchToThread();
#endif
		goto retry;
	}

	//	delete our left sibling from parent

	slotptr(parent->page,idx)->dead = 1;
	parent->page->dirty = 1;
	parent->page->act--;

	//	redirect our parent slot to our left sibling

	bt_putid (slotptr(parent->page, slot)->id, sibling->page_no);
	memcpy (sibling->page->right, set->page->right, BtId);

	//	collapse dead slots from parent

	while( idx = parent->page->cnt - 1 )
	  if( slotptr(parent->page, idx)->dead ) {
		*slotptr(parent->page, idx) = *slotptr(parent->page, parent->page->cnt);
		memset (slotptr(parent->page, parent->page->cnt--), 0, sizeof(BtSlot));
	  } else
		  break;

	//  free our original page

	bt_lockpage (BtLockDelete, set->latch);
	bt_lockpage (BtLockWrite, set->latch);
	bt_freepage (bt, set);

	//	go down the left node's fence keys to the leaf level
	//	and update the fence keys in each page

	memcpy (newfence, parent->page->fence, 256);

	if( bt_fixfences (bt, sibling, newfence) )
		return bt->err;

	//  promote sibling as new root?

	if( parent->page_no == ROOT_page && parent->page->cnt == 1 )
	 if( sibling->page->lvl ) {
	  sibling->latch = bt_pinlatch (bt, sibling->page_no);
	  bt_lockpage (BtLockDelete, sibling->latch);
	  bt_lockpage (BtLockWrite, sibling->latch);

	  if( sibling->pool = bt_pinpool (bt, sibling->page_no) )
		sibling->page = bt_page (bt, sibling->pool, sibling->page_no);
	  else
		return bt->err;

	  return bt_removeroot (bt, parent, sibling);
	 }

	bt_unlockpage (BtLockWrite, parent->latch);
	bt_unpinlatch (parent->latch);
	bt_unpinpool (parent->pool);

	return 0;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len)
{
unsigned char pagefence[256];
uint slot, idx, found;
BtPageSet set[1];
BtKey ptr;

	if( slot = bt_loadpage (bt, set, key, len, 0, BtLockWrite) )
		ptr = keyptr(set->page, slot);
	else
		return bt->err;

	// if key is found delete it, otherwise ignore request

	if( found = slot <= set->page->cnt )
	  if( found = !keycmp (ptr, key, len) )
		if( found = slotptr(set->page, slot)->dead == 0 ) {
 		  slotptr(set->page,slot)->dead = 1;
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

 	if( set->page->act ) {
		bt_unlockpage(BtLockWrite, set->latch);
		bt_unpinlatch (set->latch);
		bt_unpinpool (set->pool);
		return bt->found = found, 0;
	}

	memcpy (pagefence, set->page->fence, 256);
	set->page->kill = 1;

	bt_unlockpage (BtLockWrite, set->latch);

	if( bt_removepage (bt, set, 0, pagefence) )
		return bt->err;

	bt->found = found;
	return 0;
}

//	find key in leaf level and return row-id

uid bt_findkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint  slot;
BtKey ptr;
uid id;

	if( slot = bt_loadpage (bt, set, key, len, 0, BtLockRead) )
		ptr = keyptr(set->page, slot);
	else
		return 0;

	// if key exists, return row-id
	//	otherwise return 0

	if( !keycmp (ptr, key, len) )
		id = bt_getid(slotptr(set->page,slot)->id);
	else
		id = 0;

	bt_unlockpage (BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	return id;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0  new slot value

uint bt_cleanpage(BtDb *bt, BtPage page, uint amt, uint slot)
{
uint nxt = bt->mgr->page_size, off;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey key;

	if( page->min >= (max+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
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
		if( slotptr(bt->frame,cnt)->dead )
			continue;

		// if its not the fence key,
		// copy the key across

		off = slotptr(bt->frame,cnt)->off;

		if( off >= sizeof(*page) ) {
			key = keyptr(bt->frame, cnt);
			off = nxt -= key->len + 1;
			memcpy ((unsigned char *)page + nxt, key, key->len + 1);
		}

		// copy slot

		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = off;
		page->act++;
	}

	page->min = nxt;
	page->cnt = idx;

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtDb *bt, BtPageSet *root, uid page_no2)
{
uint nxt = bt->mgr->page_size;
unsigned char leftkey[256];
uid new_page;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	memcpy (leftkey, root->page->fence, 256);
	root->page->posted = 1;

	if( !(new_page = bt_newpage(bt, root->page)) )
		return bt->err;

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, bt->mgr->page_size - sizeof(*root->page));
	memset(root->page->fence, 0, 256);
	root->page->fence[0] = 2;
	root->page->fence[1] = 0xff;
	root->page->fence[2] = 0xff;

	// insert lower keys page fence key on newroot page

	nxt -= *leftkey + 1;
	memcpy ((unsigned char *)root->page + nxt, leftkey, *leftkey + 1);
	bt_putid(slotptr(root->page, 1)->id, new_page);
	slotptr(root->page, 1)->off = nxt;
	
	// insert stopper key on newroot page
	// and increase the root height

	bt_putid(slotptr(root->page, 2)->id, page_no2);
	slotptr(root->page, 2)->off = offsetof(struct BtPage_, fence);

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
uint cnt = 0, idx = 0, max, nxt = bt->mgr->page_size, off;
unsigned char fencekey[256];
uint lvl = set->page->lvl;
uid right;
BtKey key;

	//  split higher half of keys to bt->frame

	memset (bt->frame, 0, bt->mgr->page_size);
	max = set->page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		if( !lvl || cnt < max ) {
			key = keyptr(set->page, cnt);
			off = nxt -= key->len + 1;
			memcpy ((unsigned char *)bt->frame + nxt, key, key->len + 1);
		} else
			off = offsetof(struct BtPage_, fence);

		memcpy(slotptr(bt->frame,++idx)->id, slotptr(set->page,cnt)->id, BtId);
		slotptr(bt->frame, idx)->tod = slotptr(set->page, cnt)->tod;
		slotptr(bt->frame, idx)->off = off;
		bt->frame->act++;
	}

	if( set->page_no == ROOT_page )
		bt->frame->posted = 1;

	memcpy (bt->frame->fence, set->page->fence, 256);
	bt->frame->bits = bt->mgr->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	// link right node

	if( set->page_no > ROOT_page )
		memcpy (bt->frame->right, set->page->right, BtId);

	//	get new free page and write higher keys to it.

	if( !(right = bt_newpage(bt, bt->frame)) )
		return bt->err;

	//	update lower keys to continue in old page

	memcpy (bt->frame, set->page, bt->mgr->page_size);
	memset (set->page+1, 0, bt->mgr->page_size - sizeof(*set->page));
	nxt = bt->mgr->page_size;
	set->page->posted = 0;
	set->page->dirty = 0;
	set->page->act = 0;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys

	while( cnt++ < max / 2 ) {
		key = keyptr(bt->frame, cnt);

		if( !lvl || cnt < max / 2 ) {
			off = nxt -= key->len + 1;
			memcpy ((unsigned char *)set->page + nxt, key, key->len + 1);
		} else 
			off = offsetof(struct BtPage_, fence);

		memcpy(slotptr(set->page,++idx)->id, slotptr(bt->frame,cnt)->id, BtId);
		slotptr(set->page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(set->page, idx)->off = off;
		set->page->act++;
	}

	// install fence key for smaller key page

	memset(set->page->fence, 0, 256);
	memcpy(set->page->fence, key, key->len + 1);

	bt_putid(set->page->right, right);
	set->page->min = nxt;
	set->page->cnt = idx;

	// if current page is the root page, split it

	if( set->page_no == ROOT_page )
		return bt_splitroot (bt, set, right);

	bt_unlockpage (BtLockWrite, set->latch);

	// insert new fences in their parent pages

	while( 1 ) {
		bt_lockpage (BtLockParent, set->latch);
		bt_lockpage (BtLockWrite, set->latch);

		memcpy (fencekey, set->page->fence, 256);
		right = bt_getid (set->page->right);

		if( set->page->posted ) {
			bt_unlockpage (BtLockParent, set->latch);
			bt_unlockpage (BtLockWrite, set->latch);
			bt_unpinlatch (set->latch);
			bt_unpinpool (set->pool);
			return 0;
		}

		set->page->posted = 1;
		bt_unlockpage (BtLockWrite, set->latch);

		if( bt_insertkey (bt, fencekey+1, *fencekey, set->page_no, time(NULL), lvl+1) )
			return bt->err;

		bt_unlockpage (BtLockParent, set->latch);
		bt_unpinlatch (set->latch);
		bt_unpinpool (set->pool);

		if( !(set->page_no = right) )
			break;

		set->latch = bt_pinlatch (bt, right);

		if( set->pool = bt_pinpool (bt, right) )
			set->page = bt_page (bt, set->pool, right);
		else
			return bt->err;
	}

	return 0;
}

//  Insert new key into the btree at given level.

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uid id, uint tod, uint lvl)
{
BtPageSet set[1];
uint slot, idx;
BtKey ptr;

	while( 1 ) {
		if( slot = bt_loadpage (bt, set, key, len, lvl, BtLockWrite) )
			ptr = keyptr(set->page, slot);
		else
		{
			if ( !bt->err )
				bt->err = BTERR_ovflw;
			return bt->err;
		}

		// if key already exists, update id and return

		if( slot <= set->page->cnt )
		  if( !keycmp (ptr, key, len) ) {
			if( slotptr(set->page, slot)->dead )
				set->page->act++;
			slotptr(set->page, slot)->dead = 0;
			slotptr(set->page, slot)->tod = tod;
			bt_putid(slotptr(set->page,slot)->id, id);
			bt_unlockpage(BtLockWrite, set->latch);
			bt_unpinlatch (set->latch);
			bt_unpinpool (set->pool);
			return 0;
		}

		// check if page has enough space

 		if( slot = bt_cleanpage (bt, set->page, len, slot) )
			break;

		if( bt_splitpage (bt, set) )
			return bt->err;
	}

	// calculate next available slot and copy key into page

	set->page->min -= len + 1; // reset lowest used offset
	((unsigned char *)set->page)[set->page->min] = len;
	memcpy ((unsigned char *)set->page + set->page->min +1, key, len );

	for( idx = slot; idx <= set->page->cnt; idx++ )
	  if( slotptr(set->page, idx)->dead )
		break;

	// now insert key into array before slot

	if( idx > set->page->cnt )
		set->page->cnt++;

	set->page->act++;

	while( idx > slot )
		*slotptr(set->page, idx) = *slotptr(set->page, idx -1), idx--;

	bt_putid(slotptr(set->page,slot)->id, id);
	slotptr(set->page, slot)->off = set->page->min;
	slotptr(set->page, slot)->tod = tod;
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

uid bt_uid(BtDb *bt, uint slot)
{
	return bt_getid(slotptr(bt->cursor,slot)->id);
}

uint bt_tod(BtDb *bt, uint slot)
{
	return slotptr(bt->cursor,slot)->tod;
}


#ifdef STANDALONE

void bt_latchaudit (BtDb *bt)
{
ushort idx, hashidx;
BtPageSet set[1];

#ifdef unix
	for( idx = 1; idx < bt->mgr->latchmgr->latchdeployed; idx++ ) {
		set->latch = bt->mgr->latchsets + idx;
		if( set->latch->pin ) {
			fprintf(stderr, "latchset %d pinned for page %.6x\n", idx, set->latch->page_no);
			set->latch->pin = 0;
		}
	}

	for( hashidx = 0; hashidx < bt->mgr->latchmgr->latchhash; hashidx++ ) {
	  if( idx = bt->mgr->latchmgr->table[hashidx].slot ) do {
		set->latch = bt->mgr->latchsets + idx;
		if( set->latch->hash != hashidx )
			fprintf(stderr, "latchset %d wrong hashidx\n", idx);
		if( set->latch->pin )
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, set->latch->page_no);
	  } while( idx = set->latch->next );
	}

	set->page_no = bt_getid(bt->mgr->latchmgr->alloc[1].right);

	while( set->page_no ) {
		fprintf(stderr, "free: %.6x\n", (uint)set->page_no);

		if( set->pool = bt_pinpool (bt, set->page_no) )
			set->page = bt_page (bt, set->pool, set->page_no);
		else
			return;

	    set->page_no = bt_getid(set->page->right);
		bt_unpinpool (set->pool);
	}
#endif
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
time_t tod[1];
BtKey ptr;
BtDb *bt;
FILE *in;

	bt = bt_open (args->mgr);
	time (tod);

	switch(args->type | 0x20)
	{
	case 'a':
		fprintf(stderr, "started latch mgr audit\n");
		bt_latchaudit (bt);
		fprintf(stderr, "finished latch mgr audit\n");
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

			  if( bt_insertkey (bt, key, len, line, *tod, 0) )
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

			  if( bt_deletekey (bt, key, len) )
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
		fprintf(stderr, "started counting\n");

	  	do {
			if( set->pool = bt_pinpool (bt, page_no) )
				set->page = bt_page (bt, set->pool, page_no);
			else
				break;
			set->latch = bt_pinlatch (bt, page_no);
			bt_lockpage (BtLockRead, set->latch);
			cnt += set->page->act;
			next = bt_getid (set->page->right);
			bt_unlockpage (BtLockRead, set->latch);
			bt_unpinlatch (set->latch);
			bt_unpinpool (set->pool);
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
