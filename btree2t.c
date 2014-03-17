// btree version 2t  sched_yield version of spinlocks
//	with reworked bt_deletekey code
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
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#endif

#include <memory.h>
#include <string.h>

typedef unsigned long long	uid;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_latchtable	8192					// number of latch manager slots

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw
#define BT_fl 0x6c66	// fl

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
}BtLock;

//	definition for latch implementation

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
	BtSpinLatch readwr[1];		// read/write page lock
	BtSpinLatch access[1];		// Access Intent/Page delete
	BtSpinLatch parent[1];		// Posting of fence key in parent
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
//	for each key stored by making the first two uints
//	into ushorts.  You can also save 4 bytes by removing
//	the tod field from the key.

//	Keys are marked dead, but remain on the page until
//	cleanup is called. The fence key (highest key) for
//	the page is always present, even if dead.

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
	unsigned char key[0];
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
	unsigned char lvl:6;		// level of page
	unsigned char kill:1;		// page is being deleted
	unsigned char dirty:1;		// page is dirty
	unsigned char right[BtId];	// page number to right
} *BtPage;

//	The memory mapping hash table entry

typedef struct {
	BtPage page;		// mapped page pointer
	uid  page_no;		// mapped page number
	void *lruprev;		// least recently used previous cache block
	void *lrunext;		// lru next cache block
	void *hashprev;		// previous cache block for the same hash idx
	void *hashnext;		// next cache block for the same hash idx
#ifndef unix
	HANDLE hmap;
#endif
}BtHash;

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

typedef struct _BtDb {
	uint page_size;		// each page size	
	uint page_bits;		// each page size in bits	
	uint seg_bits;		// segment size in pages in bits
	uid page_no;		// current page number	
	uid cursor_page;	// current cursor page number	
	int  err;
	uint mode;			// read-write mode
	uint mapped_io;		// use memory mapping
	BtPage temp;		// temporary frame buffer (memory mapped/file IO)
	BtPage alloc;		// frame buffer for alloc page ( page 0 )
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	BtPage zero;		// zeroes frame buffer (never mapped)
	BtPage page;		// current page
	BtLatchSet *latch;		// current page latch
	BtLatchMgr *latchmgr;	// mapped latch page from allocation page
	BtLatchSet *latchsets;	// mapped latch set from latch pages
#ifdef unix
	int idx;
#else
	HANDLE idx;
	HANDLE halloc;		// allocation and latch table handle
#endif
	unsigned char *mem;	// frame, cursor, page memory buffer
	int nodecnt;		// highest page cache segment in use
	int nodemax;		// highest page cache segment allocated
	int hashmask;		// number of pages in segments - 1
	int hashsize;		// size of hash table
	int found;			// last deletekey found key
	BtHash *lrufirst;	// lru list head
	BtHash *lrulast;	// lru list tail
	ushort *cache;		// hash table for cached segments
	BtHash *nodes;		// segment cache
} BtDb;

typedef enum {
BTERR_ok = 0,
BTERR_notfound,
BTERR_struct,
BTERR_ovflw,
BTERR_lock,
BTERR_hash,
BTERR_kill,
BTERR_map,
BTERR_wrt,
BTERR_eof
} BTERR;

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (char *name, uint mode, uint bits, uint cacheblk, uint pgblk, uint hashsize);
extern BTERR  bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, uid id, uint tod);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
extern uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	internal functions
BTERR bt_update (BtDb *bt, BtPage page, uid page_no);
BTERR bt_mappage (BtDb *bt, BtPage *page, uid page_no);
//  Helper functions to return slot values

extern BtKey bt_key (BtDb *bt, uint slot);
extern uid bt_uid (BtDb *bt, uint slot);
extern uint bt_tod (BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0
#define ROOT_page		1
#define LEAF_page		2
#define LATCH_page		3

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

//	The b-tree pages are linked with right
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup The fence key for a node is always
//	present, even after deletion and cleanup.

//  Deleted leaf pages are reclaimed  on a free list.
//	The upper levels of the btree are fixed on creation.

//  Groups of pages from the btree are optionally
//  cached with memory mapping. A hash table is used to keep
//  track of the cached pages.  This behaviour is controlled
//  by the number of cache blocks parameter and pages per block
//	given to bt_open.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 (ALLOC page) is dedicated to lock for new page extensions,
//	and chains empty leaf pages together for reuse.

//	Parent locks are obtained to prevent resplitting or deleting a node
//	before its fence is posted into its upper level.

//	A special open mode of BT_fl is provided to safely access files on
//	WIN32 networks. WIN32 network operations should not use memory mapping.
//	This WIN32 mode sets FILE_FLAG_NOBUFFERING and FILE_FLAG_WRITETHROUGH
//	to prevent local caching of network file contents.

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

BTERR bt_abort (BtDb *bt, BtPage page, uid page_no, BTERR err)
{
BtKey ptr;

	fprintf(stderr, "\n Btree2 abort, error %d on page %.8x\n", err, page_no);
	fprintf(stderr, "level=%d kill=%d free=%d cnt=%x act=%x\n", page->lvl, page->kill, page->free, page->cnt, page->act);
	ptr = keyptr(page, page->cnt);
	fprintf(stderr, "fence='%.*s'\n", ptr->len, ptr->key);
	fprintf(stderr, "right=%.8x\n", bt_getid(page->right));
	return bt->err = err;
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
BtLatchSet *latch = bt->latchsets + victim;

	if( latch->next = bt->latchmgr->table[hashidx].slot )
		bt->latchsets[latch->next].prev = victim;

	bt->latchmgr->table[hashidx].slot = victim;
	latch->page_no = page_no;
	latch->hash = hashidx;
	latch->prev = 0;
}

//	release latch pin

void bt_unpinlatch (BtLatchSet *latch)
{
#ifdef unix
	__sync_fetch_and_add(&latch->pin, -1);
#else
	_InterlockedDecrement16 (&latch->pin);
#endif
}

//	find existing latchset or inspire new one
//	return with latchset pinned

BtLatchSet *bt_pinlatch (BtDb *bt, uid page_no)
{
ushort hashidx = page_no % bt->latchmgr->latchhash;
ushort slot, avail = 0, victim, idx;
BtLatchSet *latch;

	//  obtain read lock on hash table entry

	bt_spinreadlock(bt->latchmgr->table[hashidx].latch);

	if( slot = bt->latchmgr->table[hashidx].slot ) do
	{
		latch = bt->latchsets + slot;
		if( page_no == latch->page_no )
			break;
	} while( slot = latch->next );

	if( slot ) {
#ifdef unix
		__sync_fetch_and_add(&latch->pin, 1);
#else
		_InterlockedIncrement16 (&latch->pin);
#endif
	}

    bt_spinreleaseread (bt->latchmgr->table[hashidx].latch);

	if( slot )
		return latch;

  //  try again, this time with write lock

  bt_spinwritelock(bt->latchmgr->table[hashidx].latch);

  if( slot = bt->latchmgr->table[hashidx].slot ) do
  {
	latch = bt->latchsets + slot;
	if( page_no == latch->page_no )
		break;
	if( !latch->pin && !avail )
		avail = slot;
  } while( slot = latch->next );

  //  found our entry, or take over an unpinned one

  if( slot || (slot = avail) ) {
	latch = bt->latchsets + slot;
#ifdef unix
	__sync_fetch_and_add(&latch->pin, 1);
#else
	_InterlockedIncrement16 (&latch->pin);
#endif
	latch->page_no = page_no;
	bt_spinreleasewrite(bt->latchmgr->table[hashidx].latch);
	return latch;
  }

	//  see if there are any unused entries
#ifdef unix
	victim = __sync_fetch_and_add (&bt->latchmgr->latchdeployed, 1) + 1;
#else
	victim = _InterlockedIncrement16 (&bt->latchmgr->latchdeployed);
#endif

	if( victim < bt->latchmgr->latchtotal ) {
		latch = bt->latchsets + victim;
#ifdef unix
		__sync_fetch_and_add(&latch->pin, 1);
#else
		_InterlockedIncrement16 (&latch->pin);
#endif
		bt_latchlink (bt, hashidx, victim, page_no);
		bt_spinreleasewrite (bt->latchmgr->table[hashidx].latch);
		return latch;
	}

#ifdef unix
	victim = __sync_fetch_and_add (&bt->latchmgr->latchdeployed, -1);
#else
	victim = _InterlockedDecrement16 (&bt->latchmgr->latchdeployed);
#endif
  //  find and reuse previous lock entry

  while( 1 ) {
#ifdef unix
	victim = __sync_fetch_and_add(&bt->latchmgr->latchvictim, 1);
#else
	victim = _InterlockedIncrement16 (&bt->latchmgr->latchvictim) - 1;
#endif
	//	we don't use slot zero

	if( victim %= bt->latchmgr->latchtotal )
		latch = bt->latchsets + victim;
	else
		continue;

	//	take control of our slot
	//	from other threads

	if( latch->pin || !bt_spinwritetry (latch->busy) )
		continue;

	idx = latch->hash;

	// try to get write lock on hash chain
	//	skip entry if not obtained
	//	or has outstanding locks

	if( !bt_spinwritetry (bt->latchmgr->table[idx].latch) ) {
		bt_spinreleasewrite (latch->busy);
		continue;
	}

	if( latch->pin ) {
		bt_spinreleasewrite (latch->busy);
		bt_spinreleasewrite (bt->latchmgr->table[idx].latch);
		continue;
	}

	//  unlink our available victim from its hash chain

	if( latch->prev )
		bt->latchsets[latch->prev].next = latch->next;
	else
		bt->latchmgr->table[idx].slot = latch->next;

	if( latch->next )
		bt->latchsets[latch->next].prev = latch->prev;

	bt_spinreleasewrite (bt->latchmgr->table[idx].latch);
#ifdef unix
	__sync_fetch_and_add(&latch->pin, 1);
#else
	_InterlockedIncrement16 (&latch->pin);
#endif
	bt_latchlink (bt, hashidx, victim, page_no);
	bt_spinreleasewrite (bt->latchmgr->table[hashidx].latch);
	bt_spinreleasewrite (latch->busy);
	return latch;
  }
}

//	close and release memory

void bt_close (BtDb *bt)
{
BtHash *hash;
#ifdef unix
	munmap (bt->latchsets, bt->latchmgr->nlatchpage * bt->page_size);
	munmap (bt->latchmgr, bt->page_size);
#else
	FlushViewOfFile(bt->latchmgr, 0);
	UnmapViewOfFile(bt->latchmgr);
	CloseHandle(bt->halloc);
#endif
#ifdef unix
	// release mapped pages

	if( hash = bt->lrufirst )
		do munmap (hash->page, (bt->hashmask+1) << bt->page_bits);
		while(hash = hash->lrunext);

	if( bt->mem )
		free (bt->mem);
	close (bt->idx);
	free (bt->cache);
	free (bt);
#else
	if( hash = bt->lrufirst )
	  do
	  {
		FlushViewOfFile(hash->page, 0);
		UnmapViewOfFile(hash->page);
		CloseHandle(hash->hmap);
	  } while(hash = hash->lrunext);

	if( bt->mem)
		VirtualFree (bt->mem, 0, MEM_RELEASE);
	FlushFileBuffers(bt->idx);
	CloseHandle(bt->idx);
	GlobalFree (bt->cache);
	GlobalFree (bt);
#endif
}
//  open/create new btree

//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of mapped page pool (e.g. 8192)

BtDb *bt_open (char *name, uint mode, uint bits, uint nodemax, uint segsize, uint hashsize)
{
uint lvl, attr, cacheblk, last, slot, idx;
uint nlatchpage, latchhash;
BtLatchMgr *latchmgr;
off64_t size;
uint amt[1];
BtKey key;
BtDb* bt;
int flag;

#ifndef unix
SYSTEM_INFO sysinfo[1];
OVERLAPPED ovl[1];
uint len, flags;
#else
struct flock lock[1];
#endif

	// determine sanity of page size and buffer pool

	if( bits > BT_maxbits )
		bits = BT_maxbits;
	else if( bits < BT_minbits )
		bits = BT_minbits;

#ifdef unix
	bt = calloc (1, sizeof(BtDb));

	bt->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);

	if( bt->idx == -1 )
		return free(bt), NULL;
	
	cacheblk = 4096;	// minimum mmap segment size for unix

#else
	bt = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtDb));
	attr = FILE_ATTRIBUTE_NORMAL;
	bt->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);

	if( bt->idx == INVALID_HANDLE_VALUE )
		return GlobalFree(bt), NULL;

	// normalize cacheblk to multiple of sysinfo->dwAllocationGranularity
	GetSystemInfo(sysinfo);
	cacheblk = sysinfo->dwAllocationGranularity;
#endif

#ifdef unix
	memset (lock, 0, sizeof(lock));

	lock->l_type = F_WRLCK;
	lock->l_len = sizeof(struct BtPage_);
	lock->l_whence = 0;

	if( fcntl (bt->idx, F_SETLKW, lock) < 0 )
		return bt_close (bt), NULL;
#else
	memset (ovl, 0, sizeof(ovl));
	len = sizeof(struct BtPage_);

	//	use large offsets to
	//	simulate advisory locking

	ovl->OffsetHigh |= 0x80000000;

	if( mode == BtLockDelete || mode == BtLockWrite || mode == BtLockParent )
		flags |= LOCKFILE_EXCLUSIVE_LOCK;

	if( LockFileEx (bt->idx, flags, 0, len, 0L, ovl) )
		return bt_close (bt), NULL;
#endif 
#ifdef unix
	latchmgr = malloc (BT_maxpage);
	*amt = 0;

	// read minimum page size to get root info

	if( size = lseek (bt->idx, 0L, 2) ) {
		if( pread(bt->idx, latchmgr, BT_minpage, 0) == BT_minpage )
			bits = latchmgr->alloc->bits;
		else
			return free(bt), free(latchmgr), NULL;
	} else if( mode == BT_ro )
		return free(latchmgr), bt_close (bt), NULL;
#else
	latchmgr = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
	size = GetFileSize(bt->idx, amt);

	if( size || *amt ) {
		if( !ReadFile(bt->idx, (char *)latchmgr, BT_minpage, amt, NULL) )
			return bt_close (bt), NULL;
		bits = latchmgr->alloc->bits;
	} else if( mode == BT_ro )
		return bt_close (bt), NULL;
#endif

	bt->page_size = 1 << bits;
	bt->page_bits = bits;

	bt->mode = mode;

	if( cacheblk < bt->page_size )
		cacheblk = bt->page_size;

	//  mask for partial memmaps

	bt->hashmask = (cacheblk >> bits) - 1;

	//	see if requested size of pages per memmap is greater

	if( (1 << segsize) > bt->hashmask )
		bt->hashmask = (1 << segsize) - 1;

	bt->seg_bits = 0;

	while( (1 << bt->seg_bits) <= bt->hashmask )
		bt->seg_bits++;

	bt->hashsize = hashsize;

	if( bt->nodemax = nodemax++ ) {
#ifdef unix
	  bt->nodes = calloc (nodemax, sizeof(BtHash));
	  bt->cache = calloc (hashsize, sizeof(ushort));
#else
	  bt->nodes = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, nodemax * sizeof(BtHash));
	  bt->cache = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, hashsize * sizeof(ushort));
#endif
	  bt->mapped_io = 1;
	}

	if( size || *amt ) {
		goto btlatch;
	}

	// initialize an empty b-tree with latch page, root page, page of leaves
	// and page(s) of latches

	memset (latchmgr, 0, 1 << bits);

	nlatchpage = BT_latchtable;
	if( nlatchpage > nodemax )
		nlatchpage = nodemax;
	nlatchpage *= sizeof(BtLatchSet);
	nlatchpage += bt->page_size - 1;
	nlatchpage /= bt->page_size;

	bt_putid(latchmgr->alloc->right, MIN_lvl+1+nlatchpage);
	latchmgr->alloc->bits = bt->page_bits;

	latchmgr->nlatchpage = nlatchpage;
	latchmgr->latchtotal = nlatchpage * bt->page_size / sizeof(BtLatchSet);

	//  initialize latch manager

	latchhash = (bt->page_size - sizeof(BtLatchMgr)) / sizeof(BtHashEntry);

	//	size of hash table = total number of latchsets

	if( latchhash > latchmgr->latchtotal )
		latchhash = latchmgr->latchtotal;

	latchmgr->latchhash = latchhash;

#ifdef unix
	if( write (bt->idx, latchmgr, bt->page_size) < bt->page_size )
		return bt_close (bt), NULL;
#else
	if( !WriteFile (bt->idx, (char *)latchmgr, bt->page_size, amt, NULL) )
		return bt_close (bt), NULL;

	if( *amt < bt->page_size )
		return bt_close (bt), NULL;
#endif

	memset (latchmgr, 0, 1 << bits);
	latchmgr->alloc->bits = bt->page_bits;

	for( lvl=MIN_lvl; lvl--; ) {
		slotptr(latchmgr->alloc, 1)->off = bt->page_size - 3;
		bt_putid(slotptr(latchmgr->alloc, 1)->id, lvl ? MIN_lvl - lvl + 1 : 0);		// next(lower) page number
		key = keyptr(latchmgr->alloc, 1);
		key->len = 2;		// create stopper key
		key->key[0] = 0xff;
		key->key[1] = 0xff;
		latchmgr->alloc->min = bt->page_size - 3;
		latchmgr->alloc->lvl = lvl;
		latchmgr->alloc->cnt = 1;
		latchmgr->alloc->act = 1;
#ifdef unix
		if( write (bt->idx, latchmgr, bt->page_size) < bt->page_size )
			return bt_close (bt), NULL;
#else
		if( !WriteFile (bt->idx, (char *)latchmgr, bt->page_size, amt, NULL) )
			return bt_close (bt), NULL;

		if( *amt < bt->page_size )
			return bt_close (bt), NULL;
#endif
	}

	// clear out latch manager locks
	//	and rest of pages to round out segment

	memset(latchmgr, 0, bt->page_size);
	last = MIN_lvl + 1;

	while( last <= ((MIN_lvl + 1 + nlatchpage) | bt->hashmask) ) {
#ifdef unix
		pwrite(bt->idx, latchmgr, bt->page_size, last << bt->page_bits);
#else
		SetFilePointer (bt->idx, last << bt->page_bits, NULL, FILE_BEGIN);
		if( !WriteFile (bt->idx, (char *)latchmgr, bt->page_size, amt, NULL) )
			return bt_close (bt), NULL;
		if( *amt < bt->page_size )
			return bt_close (bt), NULL;
#endif
		last++;
	}

btlatch:
#ifdef unix
	lock->l_type = F_UNLCK;
	if( fcntl (bt->idx, F_SETLK, lock) < 0 )
		return bt_close (bt), NULL;
#else
	if( !UnlockFileEx (bt->idx, 0, sizeof(struct BtPage_), 0, ovl) )
		return bt_close (bt), NULL;
#endif
#ifdef unix
	flag = PROT_READ | PROT_WRITE;
	bt->latchmgr = mmap (0, bt->page_size, flag, MAP_SHARED, bt->idx, ALLOC_page * bt->page_size);
	if( bt->latchmgr == MAP_FAILED )
		return bt_close (bt), NULL;
	bt->latchsets = (BtLatchSet *)mmap (0, bt->latchmgr->nlatchpage * bt->page_size, flag, MAP_SHARED, bt->idx, LATCH_page * bt->page_size);
	if( bt->latchsets == MAP_FAILED )
		return bt_close (bt), NULL;
#else
	flag = PAGE_READWRITE;
	bt->halloc = CreateFileMapping(bt->idx, NULL, flag, 0, (BT_latchtable / (bt->page_size / sizeof(BtLatchSet)) + 1 + LATCH_page) * bt->page_size, NULL);
	if( !bt->halloc )
		return bt_close (bt), NULL;

	flag = FILE_MAP_WRITE;
	bt->latchmgr = MapViewOfFile(bt->halloc, flag, 0, 0, (BT_latchtable / (bt->page_size / sizeof(BtLatchSet)) + 1 + LATCH_page) * bt->page_size);
	if( !bt->latchmgr )
		return GetLastError(), bt_close (bt), NULL;

	bt->latchsets = (void *)((char *)bt->latchmgr + LATCH_page * bt->page_size);
#endif

#ifdef unix
	free (latchmgr);
#else
	VirtualFree (latchmgr, 0, MEM_RELEASE);
#endif

#ifdef unix
	bt->mem = malloc (6 * bt->page_size);
#else
	bt->mem = VirtualAlloc(NULL, 6 * bt->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
	bt->frame = (BtPage)bt->mem;
	bt->cursor = (BtPage)(bt->mem + bt->page_size);
	bt->page = (BtPage)(bt->mem + 2 * bt->page_size);
	bt->alloc = (BtPage)(bt->mem + 3 * bt->page_size);
	bt->temp = (BtPage)(bt->mem + 4 * bt->page_size);
	bt->zero = (BtPage)(bt->mem + 5 * bt->page_size);

	memset (bt->zero, 0, bt->page_size);
	return bt;
}

// place write, read, or parent lock on requested page_no.

void bt_lockpage(BtLock mode, BtLatchSet *latch)
{
	switch( mode ) {
	case BtLockRead:
		bt_spinreadlock (latch->readwr);
		break;
	case BtLockWrite:
		bt_spinwritelock (latch->readwr);
		break;
	case BtLockAccess:
		bt_spinreadlock (latch->access);
		break;
	case BtLockDelete:
		bt_spinwritelock (latch->access);
		break;
	case BtLockParent:
		bt_spinwritelock (latch->parent);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *latch)
{
	switch( mode ) {
	case BtLockRead:
		bt_spinreleaseread (latch->readwr);
		break;
	case BtLockWrite:
		bt_spinreleasewrite (latch->readwr);
		break;
	case BtLockAccess:
		bt_spinreleaseread (latch->access);
		break;
	case BtLockDelete:
		bt_spinreleasewrite (latch->access);
		break;
	case BtLockParent:
		bt_spinreleasewrite (latch->parent);
		break;
	}
}

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
uid new_page;
int reuse;

	//	lock allocation page

	bt_spinwritelock(bt->latchmgr->lock);

	// use empty chain first
	// else allocate empty page

	if( new_page = bt_getid(bt->latchmgr->alloc[1].right) ) {
		if( bt_mappage (bt, &bt->temp, new_page) )
			return 0;
		bt_putid(bt->latchmgr->alloc[1].right, bt_getid(bt->temp->right));
		reuse = 1;
	} else {
		new_page = bt_getid(bt->latchmgr->alloc->right);
		bt_putid(bt->latchmgr->alloc->right, new_page+1);
		reuse = 0;
	}

	bt_spinreleasewrite(bt->latchmgr->lock);

	if( !bt->mapped_io )
	  if( bt_update(bt, page, new_page) )
		return 0;	//don't unlock on error
	  else
		return new_page;

#ifdef unix
	if( pwrite(bt->idx, page, bt->page_size, new_page << bt->page_bits) < bt->page_size )
		return bt->err = BTERR_wrt, 0;

	// if writing first page of pool block, zero last page in the block

	if( !reuse && bt->hashmask > 0 && (new_page & bt->hashmask) == 0 )
	{
		// use zero buffer to write zeros
		if( pwrite(bt->idx,bt->zero, bt->page_size, (new_page | bt->hashmask) << bt->page_bits) < bt->page_size )
			return bt->err = BTERR_wrt, 0;
	}
#else
	//	bring new page into pool and copy page.
	//	this will extend the file into the new pages.

	if( bt_mappage (bt, &bt->temp, new_page) )
		return 0;

	memcpy(bt->temp, page, bt->page_size);
#endif
	return new_page;
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

//  Update current page of btree by writing file contents
//	or flushing mapped area to disk.

BTERR bt_update (BtDb *bt, BtPage page, uid page_no)
{
off64_t off = page_no << bt->page_bits;

#ifdef unix
    if( !bt->mapped_io )
	 if( pwrite(bt->idx, page, bt->page_size, off) != bt->page_size )
		 return bt->err = BTERR_wrt;
#else
uint amt[1];
	if( !bt->mapped_io )
	{
		SetFilePointer (bt->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);
		if( !WriteFile (bt->idx, (char *)page, bt->page_size, amt, NULL) )
			return GetLastError(), bt->err = BTERR_wrt;

		if( *amt < bt->page_size )
			return GetLastError(), bt->err = BTERR_wrt;
	} 
	else if( bt->mode == BT_fl ) {
			FlushViewOfFile(page, bt->page_size);
			FlushFileBuffers(bt->idx);
	}
#endif
	return 0;
}

// find page in cache 

BtHash *bt_findhash(BtDb *bt, uid page_no)
{
BtHash *hash;
uint idx;

	// compute cache block first page and hash idx 

	page_no &= ~bt->hashmask;
	idx = (uint)(page_no >> bt->seg_bits) % bt->hashsize;

	if( bt->cache[idx] ) 
		hash = bt->nodes + bt->cache[idx];
	else
		return NULL;

	do if( hash->page_no == page_no )
		 break;
	while(hash = hash->hashnext );

	return hash;
}

// add page cache entry to hash index

void bt_linkhash(BtDb *bt, BtHash *node, uid page_no)
{
uint idx = (uint)(page_no >> bt->seg_bits) % bt->hashsize;
BtHash *hash;

	if( bt->cache[idx] ) {
		node->hashnext = hash = bt->nodes + bt->cache[idx];
		hash->hashprev = node;
	}

	node->hashprev = NULL;
	bt->cache[idx] = (ushort)(node - bt->nodes);
}

// remove cache entry from hash table

void bt_unlinkhash(BtDb *bt, BtHash *node)
{
uint idx = (uint)(node->page_no >> bt->seg_bits) % bt->hashsize;
BtHash *hash;

	// unlink node
	if( hash = node->hashprev )
		hash->hashnext = node->hashnext;
	else if( hash = node->hashnext )
		bt->cache[idx] = (ushort)(hash - bt->nodes);
	else
		bt->cache[idx] = 0;

	if( hash = node->hashnext )
		hash->hashprev = node->hashprev;
}

// add cache page to lru chain and map pages

BtPage bt_linklru(BtDb *bt, BtHash *hash, uid page_no)
{
int flag;
off64_t off = (page_no & ~(uid)bt->hashmask) << bt->page_bits;
off64_t limit = off + ((bt->hashmask+1) << bt->page_bits);
BtHash *node;

	memset(hash, 0, sizeof(BtHash));
	hash->page_no = (page_no & ~(uid)bt->hashmask);
	bt_linkhash(bt, hash, page_no);

	if( node = hash->lrunext = bt->lrufirst )
		node->lruprev = hash;
	else
		bt->lrulast = hash;

	bt->lrufirst = hash;

#ifdef unix
	flag = PROT_READ | ( bt->mode == BT_ro ? 0 : PROT_WRITE );
	hash->page = (BtPage)mmap (0, (bt->hashmask+1) << bt->page_bits, flag, MAP_SHARED, bt->idx, off);
	if( hash->page == MAP_FAILED )
		return bt->err = BTERR_map, (BtPage)NULL;

#else
	flag = ( bt->mode == BT_ro ? PAGE_READONLY : PAGE_READWRITE );
	hash->hmap = CreateFileMapping(bt->idx, NULL, flag,	(DWORD)(limit >> 32), (DWORD)limit, NULL);
	if( !hash->hmap )
		return bt->err = BTERR_map, NULL;

	flag = ( bt->mode == BT_ro ? FILE_MAP_READ : FILE_MAP_WRITE );
	hash->page = MapViewOfFile(hash->hmap, flag, (DWORD)(off >> 32), (DWORD)off, (bt->hashmask+1) << bt->page_bits);
	if( !hash->page )
		return bt->err = BTERR_map, NULL;
#endif

	return (BtPage)((char*)hash->page + ((uint)(page_no & bt->hashmask) << bt->page_bits));
}

//	find or place requested page in page-cache
//	return memory address where page is located.

BtPage bt_hashpage(BtDb *bt, uid page_no)
{
BtHash *hash, *node, *next;
BtPage page;

	// find page in cache and move to top of lru list  

	if( hash = bt_findhash(bt, page_no) ) {
		page = (BtPage)((char*)hash->page + ((uint)(page_no & bt->hashmask) << bt->page_bits));
		// swap node in lru list
		if( node = hash->lruprev ) {
			if( next = node->lrunext = hash->lrunext )
				next->lruprev = node;
			else
				bt->lrulast = node;

			if( next = hash->lrunext = bt->lrufirst )
				next->lruprev = hash;
			else
				return bt->err = BTERR_hash, (BtPage)NULL;

			hash->lruprev = NULL;
			bt->lrufirst = hash;
		}
		return page;
	}

	// map pages and add to cache entry

	if( bt->nodecnt < bt->nodemax ) {
		hash = bt->nodes + ++bt->nodecnt;
		return bt_linklru(bt, hash, page_no);
	}

	// hash table is already full, replace last lru entry from the cache

	if( hash = bt->lrulast ) {
		// unlink from lru list
		if( node = bt->lrulast = hash->lruprev )
			node->lrunext = NULL;
		else
			return bt->err = BTERR_hash, (BtPage)NULL;

#ifdef unix
		munmap (hash->page, (bt->hashmask+1) << bt->page_bits);
#else
//		FlushViewOfFile(hash->page, 0);
		UnmapViewOfFile(hash->page);
		CloseHandle(hash->hmap);
#endif
		// unlink from hash table

		bt_unlinkhash(bt, hash);

		// map and add to cache

		return bt_linklru(bt, hash, page_no);
	}

	return bt->err = BTERR_hash, (BtPage)NULL;
}

//  map a btree page onto current page

BTERR bt_mappage (BtDb *bt, BtPage *page, uid page_no)
{
off64_t off = page_no << bt->page_bits;
#ifndef unix
int amt[1];
#endif
	
	if( bt->mapped_io ) {
		bt->err = 0;
		*page = bt_hashpage(bt, page_no);
		return bt->err;
	}
#ifdef unix
	if( pread(bt->idx, *page, bt->page_size, off) < bt->page_size )
		return bt->err = BTERR_map;
#else
	SetFilePointer (bt->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

	if( !ReadFile(bt->idx, *page, bt->page_size, amt, NULL) )
		return bt->err = BTERR_map;

	if( *amt <  bt->page_size )
		return bt->err = BTERR_map;
#endif
	return 0;
}

//	deallocate a deleted page 
//	place on free chain out of allocator page
//	call with page latched for Writing and Deleting

BTERR bt_freepage(BtDb *bt, uid page_no, BtPage page, BtLatchSet *latch)
{
	if( bt_mappage (bt, &page, page_no) )
		return bt->err;

	//	lock allocation page

	bt_spinwritelock (bt->latchmgr->lock);

	//	store chain in second right
	bt_putid(page->right, bt_getid(bt->latchmgr->alloc[1].right));
	bt_putid(bt->latchmgr->alloc[1].right, page_no);
	page->free = 1;

	if( bt_update(bt, page, page_no) )
		return bt->err;

	// unlock released page

	bt_unlockpage (BtLockDelete, latch);
	bt_unlockpage (BtLockWrite, latch);
	bt_unpinlatch (latch);

	// unlock allocation page

	bt_spinreleasewrite (bt->latchmgr->lock);
	return 0;
}

//  find slot in page for given key at a given level

int bt_findslot (BtDb *bt, unsigned char *key, uint len)
{
uint diff, higher = bt->page->cnt, low = 1, slot;
uint good = 0;

	//	make stopper key an infinite fence value

	if( bt_getid (bt->page->right) )
		higher++;
	else
		good++;

	//	low is the lowest candidate, higher is already
	//	tested as .ge. the given key, loop ends when they meet

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(bt->page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on right link page

 	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtDb *bt, unsigned char *key, uint len, uint lvl, uint lock)
{
uid page_no = ROOT_page, prevpage = 0;
uint drill = 0xff, slot;
BtLatchSet *prevlatch;
uint mode, prevmode;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (lock == BtLockWrite) && (drill == lvl) ? BtLockWrite : BtLockRead; 

	bt->latch = bt_pinlatch(bt, page_no);
	bt->page_no = page_no;

 	// obtain access lock using lock chaining

	if( page_no > ROOT_page )
		bt_lockpage(BtLockAccess, bt->latch);

	if( prevpage ) {
		bt_unlockpage(prevmode, prevlatch);
		bt_unpinlatch(prevlatch);
		prevpage = 0;
	}

 	// obtain read lock using lock chaining

	bt_lockpage(mode, bt->latch);

	if( page_no > ROOT_page )
		bt_unlockpage(BtLockAccess, bt->latch);

	//	map/obtain page contents

	if( bt_mappage (bt, &bt->page, page_no) )
		return 0;

	// re-read and re-lock root after determining actual level of root

	if( bt->page->lvl != drill) {
		if( bt->page_no != ROOT_page )
			return bt->err = BTERR_struct, 0;
			
		drill = bt->page->lvl;

		if( lock != BtLockRead && drill == lvl ) {
			bt_unlockpage(mode, bt->latch);
			bt_unpinlatch(bt->latch);
			continue;
		}
	}

	prevpage = bt->page_no;
	prevlatch = bt->latch;
	prevmode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( !bt->page->kill )
	 if( slot = bt_findslot (bt, key, len) ) {
	  if( drill == lvl )
		return slot;

	  while( slotptr(bt->page, slot)->dead )
		if( slot++ < bt->page->cnt )
			continue;
		else
			goto slideright;

	  page_no = bt_getid(slotptr(bt->page, slot)->id);
	  drill--;
	  continue;
	 }

	//  or slide right into next page

slideright:
	page_no = bt_getid(bt->page->right);

  } while( page_no );

  // return error on end of right chain

  bt->err = BTERR_eof;
  return 0;	// return error
}

//	a fence key was deleted from a page
//	push new fence value upwards

BTERR bt_fixfence (BtDb *bt, uid page_no, uint lvl)
{
unsigned char leftkey[256], rightkey[256];
BtLatchSet *latch = bt->latch;
BtKey ptr;

	// remove deleted key, the old fence value

	ptr = keyptr(bt->page, bt->page->cnt);
	memcpy(rightkey, ptr, ptr->len + 1);

	memset (slotptr(bt->page, bt->page->cnt--), 0, sizeof(BtSlot));
	bt->page->dirty = 1;

	ptr = keyptr(bt->page, bt->page->cnt);
	memcpy(leftkey, ptr, ptr->len + 1);

	if( bt_update (bt, bt->page, page_no) )
		return bt->err;

	bt_lockpage (BtLockParent, latch);
	bt_unlockpage (BtLockWrite, latch);

	//  insert new (now smaller) fence key

	if( bt_insertkey (bt, leftkey+1, *leftkey, lvl + 1, page_no, time(NULL)) )
		return bt->err;

	//  remove old (larger) fence key

	if( bt_deletekey (bt, rightkey+1, *rightkey, lvl + 1) )
		return bt->err;

	bt_unlockpage (BtLockParent, latch);
	bt_unpinlatch (latch);
	return 0;
}

//	root has a single child
//	collapse a level from the btree
//	call with root locked in bt->page

BTERR bt_collapseroot (BtDb *bt, BtPage root)
{
BtLatchSet *latch;
uid child;
uint idx;

	// find the child entry
	//	and promote to new root

  do {
	for( idx = 0; idx++ < root->cnt; )
	  if( !slotptr(root, idx)->dead )
		break;

	child = bt_getid (slotptr(root, idx)->id);
	latch = bt_pinlatch (bt, child);

	bt_lockpage (BtLockDelete, latch);
	bt_lockpage (BtLockWrite, latch);

	if( bt_mappage (bt, &bt->temp, child) )
		return bt->err;

	memcpy (root, bt->temp, bt->page_size);

	if( bt_update (bt, root, ROOT_page) )
		return bt->err;

	if( bt_freepage (bt, child, bt->temp, latch) )
		return bt->err;

  } while( root->lvl > 1 && root->act == 1 );

  bt_unlockpage (BtLockWrite, bt->latch);
  bt_unpinlatch (bt->latch);
  return 0;
}

//  find and delete key on page by marking delete flag bit
//  when page becomes empty, delete it

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl)
{
unsigned char lowerkey[256], higherkey[256];
uint slot, dirty = 0, idx, fence, found;
BtLatchSet *latch, *rlatch;
uid page_no, right;
BtKey ptr;

	if( slot = bt_loadpage (bt, key, len, lvl, BtLockWrite) )
		ptr = keyptr(bt->page, slot);
	else
		return bt->err;

	// are we deleting a fence slot?

	fence = slot == bt->page->cnt;

	// if key is found delete it, otherwise ignore request

	if( found = !keycmp (ptr, key, len) )
	  if( found = slotptr(bt->page, slot)->dead == 0 ) {
 		dirty = slotptr(bt->page,slot)->dead = 1;
 		bt->page->dirty = 1;
 		bt->page->act--;

		// collapse empty slots

		while( idx = bt->page->cnt - 1 )
		  if( slotptr(bt->page, idx)->dead ) {
			*slotptr(bt->page, idx) = *slotptr(bt->page, idx + 1);
			memset (slotptr(bt->page, bt->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	right = bt_getid(bt->page->right);
	page_no = bt->page_no;
	latch = bt->latch;

	if( !dirty ) {
	  if( lvl )
		return bt_abort (bt, bt->page, page_no, BTERR_notfound);
	  bt_unlockpage(BtLockWrite, latch);
	  bt_unpinlatch (latch);
	  return bt->found = found, 0;
	}

	//	did we delete a fence key in an upper level?

	if( lvl && bt->page->act && fence )
	  if( bt_fixfence (bt, page_no, lvl) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	is this a collapsed root?

	if( lvl > 1 && page_no == ROOT_page && bt->page->act == 1 )
	  if( bt_collapseroot (bt, bt->page) )
		return bt->err;
	  else
		return bt->found = found, 0;

	// return if page is not empty

	if( bt->page->act ) {
	  if( bt_update(bt, bt->page, page_no) )
		return bt->err;
	  bt_unlockpage(BtLockWrite, latch);
	  bt_unpinlatch (latch);
	  return bt->found = found, 0;
	}

	// cache copy of fence key
	//	in order to find parent

	ptr = keyptr(bt->page, bt->page->cnt);
	memcpy(lowerkey, ptr, ptr->len + 1);

	// obtain lock on right page

	rlatch = bt_pinlatch (bt, right);
	bt_lockpage(BtLockWrite, rlatch);

	if( bt_mappage (bt, &bt->temp, right) )
		return bt->err;

	if( bt->temp->kill ) {
		bt_abort(bt, bt->temp, right, 0);
		return bt_abort(bt, bt->page, bt->page_no, BTERR_kill);
	}

	// pull contents of next page into current empty page 

	memcpy (bt->page, bt->temp, bt->page_size);

	//	cache copy of key to update

	ptr = keyptr(bt->temp, bt->temp->cnt);
	memcpy(higherkey, ptr, ptr->len + 1);

	//  Mark right page as deleted and point it to left page
	//	until we can post updates at higher level.

	bt_putid(bt->temp->right, page_no);
	bt->temp->kill = 1;

	if( bt_update(bt, bt->page, page_no) )
		return bt->err;

	if( bt_update(bt, bt->temp, right) )
		return bt->err;

	bt_lockpage(BtLockParent, latch);
	bt_unlockpage(BtLockWrite, latch);

	bt_lockpage(BtLockParent, rlatch);
	bt_unlockpage(BtLockWrite, rlatch);

	//  redirect higher key directly to consolidated node

	if( bt_insertkey (bt, higherkey+1, *higherkey, lvl+1, page_no, time(NULL)) )
		return bt->err;

	//  delete old lower key to consolidated node

	if( bt_deletekey (bt, lowerkey + 1, *lowerkey, lvl + 1) )
		return bt->err;

	//  obtain write & delete lock on deleted node
	//	add right block to free chain

	bt_lockpage(BtLockDelete, rlatch);
	bt_lockpage(BtLockWrite, rlatch);
	bt_unlockpage(BtLockParent, rlatch);

	if( bt_freepage (bt, right, bt->temp, rlatch) )
		return bt->err;

	bt_unlockpage(BtLockParent, latch);
	bt_unpinlatch(latch);
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

	bt_unlockpage (BtLockRead, bt->latch);
	bt_unpinlatch (bt->latch);
	return id;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0 - go ahead with new slot
 
uint bt_cleanpage(BtDb *bt, uint amt, uint slot)
{
uint nxt = bt->page_size;
BtPage page = bt->page;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = slot;
BtKey key;
int ret;

	if( page->min >= (max+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return slot;

	//	skip cleanup if nothing to reclaim

	if( !page->dirty )
		return 0;

	memcpy (bt->frame, page, bt->page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, bt->page_size - sizeof(*page));
	page->act = 0;

	while( cnt++ < max ) {
		if( cnt == slot )
			newslot = idx + 1;
		// always leave fence key in list
		if( cnt < max && slotptr(bt->frame,cnt)->dead )
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

	if( page->min >= (max+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtDb *bt, unsigned char *leftkey, uid page_no2)
{
uint nxt = bt->page_size;
BtPage root = bt->page;
uid right;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it

	if( !(right = bt_newpage(bt, root)) )
		return bt->err;

	// preserve the page info at the bottom
	// and set rest to zero

	memset(root+1, 0, bt->page_size - sizeof(*root));

	// insert first key on newroot page

	nxt -= *leftkey + 1;
	memcpy ((unsigned char *)root + nxt, leftkey, *leftkey + 1);
	bt_putid(slotptr(root, 1)->id, right);
	slotptr(root, 1)->off = nxt;
	
	// insert second key on newroot page
	// and increase the root height

	nxt -= 3;
	((unsigned char *)root)[nxt] = 2;
	((unsigned char *)root)[nxt+1] = 0xff;
	((unsigned char *)root)[nxt+2] = 0xff;
	bt_putid(slotptr(root, 2)->id, page_no2);
	slotptr(root, 2)->off = nxt;

	bt_putid(root->right, 0);
	root->min = nxt;		// reset lowest used offset and key count
	root->cnt = 2;
	root->act = 2;
	root->lvl++;

	// update and release root (bt->page)

	if( bt_update(bt, root, bt->page_no) )
		return bt->err;

	bt_unlockpage(BtLockWrite, bt->latch);
	bt_unpinlatch(bt->latch);
	return 0;
}

//  split already locked full node
//	return unlocked.

BTERR bt_splitpage (BtDb *bt)
{
uint cnt = 0, idx = 0, max, nxt = bt->page_size;
unsigned char fencekey[256], rightkey[256];
uid page_no = bt->page_no, right;
BtLatchSet *latch, *rlatch;
BtPage page = bt->page;
uint lvl = page->lvl;
BtKey key;

	latch = bt->latch;

	//  split higher half of keys to bt->frame
	//	the last key (fence key) might be dead

	memset (bt->frame, 0, bt->page_size);
	max = page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		key = keyptr(page, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)bt->frame + nxt, key, key->len + 1);
		memcpy(slotptr(bt->frame,++idx)->id, slotptr(page,cnt)->id, BtId);
		if( !(slotptr(bt->frame, idx)->dead = slotptr(page, cnt)->dead) )
			bt->frame->act++;
		slotptr(bt->frame, idx)->tod = slotptr(page, cnt)->tod;
		slotptr(bt->frame, idx)->off = nxt;
	}

	//	remember fence key for new right page

	memcpy (rightkey, key, key->len + 1);

	bt->frame->bits = bt->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	// link right node

	if( page_no > ROOT_page )
		memcpy (bt->frame->right, page->right, BtId);

	//	get new free page and write frame to it.

	if( !(right = bt_newpage(bt, bt->frame)) )
		return bt->err;

	//	update lower keys to continue in old page

	memcpy (bt->frame, page, bt->page_size);
	memset (page+1, 0, bt->page_size - sizeof(*page));
	nxt = bt->page_size;
	page->dirty = 0;
	page->act = 0;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys
	//	(they're all active keys)

	while( cnt++ < max / 2 ) {
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy ((unsigned char *)page + nxt, key, key->len + 1);
		memcpy(slotptr(page,++idx)->id, slotptr(bt->frame,cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
		page->act++;
	}

	// remember fence key for smaller page

	memcpy (fencekey, key, key->len + 1);

	bt_putid(page->right, right);
	page->min = nxt;
	page->cnt = idx;

	// if current page is the root page, split it

	if( page_no == ROOT_page )
		return bt_splitroot (bt, fencekey, right);

	//	lock right page

	rlatch = bt_pinlatch (bt, right);
	bt_lockpage (BtLockParent, rlatch);

	// update left (containing) node

	if( bt_update(bt, page, page_no) )
		return bt->err;

	bt_lockpage (BtLockParent, latch);
	bt_unlockpage (BtLockWrite, latch);

	// insert new fence for reformulated left block

	if( bt_insertkey (bt, fencekey+1, *fencekey, lvl+1, page_no, time(NULL)) )
		return bt->err;

	//	switch fence for right block of larger keys to new right page

	if( bt_insertkey (bt, rightkey+1, *rightkey, lvl+1, right, time(NULL)) )
		return bt->err;

	bt_unlockpage (BtLockParent, latch);
	bt_unlockpage (BtLockParent, rlatch);

	bt_unpinlatch (rlatch);
	bt_unpinlatch (latch);
	return 0;
}

//  Insert new key into the btree at requested level.
//  Pages are unlocked at exit.

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, uid id, uint tod)
{
uint slot, idx;
BtPage page;
BtKey ptr;

  while( 1 ) {
	if( slot = bt_loadpage (bt, key, len, lvl, BtLockWrite) )
		ptr = keyptr(bt->page, slot);
	else
	{
		if( !bt->err )
			bt->err = BTERR_ovflw;
		return bt->err;
	}

	// if key already exists, update id and return

	page = bt->page;

	if( !keycmp (ptr, key, len) ) {
	  if( slotptr(page, slot)->dead )
		page->act++;
	  slotptr(page, slot)->dead = 0;
	  slotptr(page, slot)->tod = tod;
	  bt_putid(slotptr(page,slot)->id, id);
	  if( bt_update(bt, bt->page, bt->page_no) )
		return bt->err;
	  bt_unlockpage(BtLockWrite, bt->latch);
	  bt_unpinlatch (bt->latch);
	  return 0;
	}

	// check if page has enough space

	if( slot = bt_cleanpage (bt, len, slot) )
		break;

	if( bt_splitpage (bt) )
		return bt->err;
  }

  // calculate next available slot and copy key into page

  page->min -= len + 1; // reset lowest used offset
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

  if( bt_update(bt, bt->page, bt->page_no) )
	  return bt->err;

  bt_unlockpage(BtLockWrite, bt->latch);
  bt_unpinlatch(bt->latch);
  return 0;
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
uint slot;

	// cache page for retrieval

	if( slot = bt_loadpage (bt, key, len, 0, BtLockRead) )
	  memcpy (bt->cursor, bt->page, bt->page_size);
	else
	  return 0;

	bt_unlockpage(BtLockRead, bt->latch);
	bt->cursor_page = bt->page_no;
	bt_unpinlatch (bt->latch);
	return slot;
}

//  return next slot for cursor page
//  or slide cursor right into next page

uint bt_nextkey (BtDb *bt, uint slot)
{
BtLatchSet *latch;
off64_t right;

  do {
	right = bt_getid(bt->cursor->right);

	while( slot++ < bt->cursor->cnt )
	  if( slotptr(bt->cursor,slot)->dead )
		continue;
	  else if( right || (slot < bt->cursor->cnt))
		return slot;
	  else
		break;

	if( !right )
		break;

	bt->cursor_page = right;
	latch = bt_pinlatch (bt, right);
    bt_lockpage(BtLockRead, latch);

	if( bt_mappage (bt, &bt->page, right) )
		return 0;

	memcpy (bt->cursor, bt->page, bt->page_size);
	bt_unlockpage(BtLockRead, latch);
	bt_unpinlatch (latch);
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

uint bt_audit (BtDb *bt)
{
ushort idx, hashidx;
uid next, page_no;
BtLatchSet *latch;
uint cnt = 0;
BtKey ptr;

#ifdef unix
	posix_fadvise( bt->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
	if( *(ushort *)(bt->latchmgr->lock) )
		fprintf(stderr, "Alloc page locked\n");
	*(ushort *)(bt->latchmgr->lock) = 0;

	for( idx = 1; idx <= bt->latchmgr->latchdeployed; idx++ ) {
		latch = bt->latchsets + idx;
		if( *(ushort *)latch->readwr )
			fprintf(stderr, "latchset %d rwlocked for page %.8x\n", idx, latch->page_no);
		*(ushort *)latch->readwr = 0;

		if( *(ushort *)latch->access )
			fprintf(stderr, "latchset %d accesslocked for page %.8x\n", idx, latch->page_no);
		*(ushort *)latch->access = 0;

		if( *(ushort *)latch->parent )
			fprintf(stderr, "latchset %d parentlocked for page %.8x\n", idx, latch->page_no);
		*(ushort *)latch->parent = 0;

		if( latch->pin ) {
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, latch->page_no);
			latch->pin = 0;
		}
	}

	for( hashidx = 0; hashidx < bt->latchmgr->latchhash; hashidx++ ) {
	  if( *(ushort *)(bt->latchmgr->table[hashidx].latch) )
			fprintf(stderr, "hash entry %d locked\n", hashidx);

	  *(ushort *)(bt->latchmgr->table[hashidx].latch) = 0;

	  if( idx = bt->latchmgr->table[hashidx].slot ) do {
		latch = bt->latchsets + idx;
		if( *(ushort *)latch->busy )
			fprintf(stderr, "latchset %d busylocked for page %.8x\n", idx, latch->page_no);
		*(ushort *)latch->busy = 0;
		if( latch->hash != hashidx )
			fprintf(stderr, "latchset %d wrong hashidx\n", idx);
		if( latch->pin )
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, latch->page_no);
	  } while( idx = latch->next );
	}

	next = bt->latchmgr->nlatchpage + LATCH_page;
	page_no = LEAF_page;

	while( page_no < bt_getid(bt->latchmgr->alloc->right) ) {
	off64_t off = page_no << bt->page_bits;
#ifdef unix
		pread (bt->idx, bt->frame, bt->page_size, off);
#else
		DWORD amt[1];

		  SetFilePointer (bt->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

		  if( !ReadFile(bt->idx, bt->frame, bt->page_size, amt, NULL))
			fprintf(stderr, "page %.8x unable to read\n", page_no);

		  if( *amt <  bt->page_size )
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

//  standalone program to index file of keys
//  then list them onto std-out

int main (int argc, char **argv)
{
uint slot, line = 0, off = 0, found = 0;
int ch, cnt = 0, bits = 12;
unsigned char key[256];
double done, start;
uid next, page_no;
uint pgblk = 0;
float elapsed;
time_t tod[1];
uint scan = 0;
uint len = 0;
uint map = 0;
BtKey ptr;
BtDb *bt;
FILE *in;

	if( argc < 4 ) {
		fprintf (stderr, "Usage: %s idx_file src_file Read/Write/Scan/Delete/Find [page_bits mapped_pool_segments pages_per_segment start_line_number]\n", argv[0]);
		fprintf (stderr, "  page_bits: size of btree page in bits\n");
		fprintf (stderr, "  mapped_pool_segments: size of buffer pool in segments\n");
		fprintf (stderr, "  pages_per_segment: size of buffer pool segment in pages in bits\n");
		exit(0);
	}

	start = getCpuTime(0);
	time(tod);

	if( argc > 4 )
		bits = atoi(argv[4]);

	if( argc > 5 )
		map = atoi(argv[5]);

	if( map > 65536 )
		fprintf (stderr, "Warning: buffer_pool > 65536 segments\n");

	if( map && map < 8 )
		fprintf (stderr, "Buffer_pool too small\n");

	if( argc > 6 )
		pgblk = atoi(argv[6]);

	if( bits + pgblk > 30 )
		fprintf (stderr, "Warning: very large buffer pool segment size\n");

	if( argc > 7 )
		off = atoi(argv[7]);

	bt = bt_open ((argv[1]), BT_rw, bits, map, pgblk, map /  8);

	if( !bt ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	}

	switch(argv[3][0]| 0x20)
	{
	case 'a':
		fprintf(stderr, "started audit for %s\n", argv[2]);
		cnt = bt_audit (bt);
		fprintf(stderr, "finished audit for %s, %d keys\n", argv[2], cnt);
		break;

	case 'w':
		fprintf(stderr, "started indexing for %s\n", argv[2]);
		if( argc > 2 && (in = fopen (argv[2], "rb")) )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  if( off )
		  		sprintf((char *)key+len, "%.9d", line + off), len += 9;

			  if( bt_insertkey (bt, key, len, 0, ++line, *tod) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < 245 )
				key[len++] = ch;
		fprintf(stderr, "finished adding keys for %s, %d \n", argv[2], line);
		break;

	case 'd':
		fprintf(stderr, "started deleting keys for %s\n", argv[2]);
		if( argc > 2 && (in = fopen (argv[2], "rb")) )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  if( off )
		  		sprintf((char *)key+len, "%.9d", line + off), len += 9;
			  line++;
			  if( bt_deletekey (bt, key, len, 0) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < 245 )
				key[len++] = ch;
		fprintf(stderr, "finished deleting keys for %s, %d \n", argv[2], line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", argv[2]);
		if( argc > 2 && (in = fopen (argv[2], "rb")) )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  if( off )
		  		sprintf((char *)key+len, "%.9d", line + off), len += 9;
			  line++;
			  if( bt_findkey (bt, key, len) )
				found++;
			  else if( bt->err )
				fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line), exit(0);
			  len = 0;
			}
			else if( len < 245 )
				key[len++] = ch;
		fprintf(stderr, "finished search of %d keys for %s, found %d\n", line, argv[2], found);
		break;

	case 's':
		fprintf(stderr, "started scaning\n");
		cnt = len = key[0] = 0;

		if( slot = bt_startkey (bt, key, len) )
		  slot--;
		else
		  fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

		while( slot = bt_nextkey (bt, slot) ) {
			ptr = bt_key(bt, slot);
			fwrite (ptr->key, ptr->len, 1, stdout);
			fputc ('\n', stdout);
			cnt++;
	  	}

		fprintf(stderr, " Total keys read %d\n", cnt - 1);
		break;

	case 'c':
	  fprintf(stderr, "started counting\n");

	  next = bt->latchmgr->nlatchpage + LATCH_page;
	  page_no = LEAF_page;
	  cnt = 0;

	  while( page_no < bt_getid(bt->latchmgr->alloc->right) ) {
	  uid off = page_no << bt->page_bits;
#ifdef unix
	  	pread (bt->idx, bt->frame, bt->page_size, off);
#else
		DWORD amt[1];

	  	SetFilePointer (bt->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

	  	if( !ReadFile(bt->idx, bt->frame, bt->page_size, amt, NULL))
			fprintf (stderr, "unable to read page %.8x", page_no);

	  	if( *amt <  bt->page_size )
			fprintf (stderr, "unable to read page %.8x", page_no);
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

	done = getCpuTime(0);
	elapsed = (float)(done - start);
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	return 0;
}

#endif	//STANDALONE
