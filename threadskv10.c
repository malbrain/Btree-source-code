// btree version threadskv10 futex version
//	with reworked bt_deletekey code,
//	phase-fair reader writer lock,
//	librarian page split code,
//	duplicate key management
//	bi-directional cursors
//	traditional buffer pool manager
//	ACID batched key-value updates
//	redo log for failure recovery
//	and LSM B-trees for write optimization

// 11 OCT 2014

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
#include <linux/futex.h>
#define SYS_futex 202
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
#include <limits.h>
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
typedef unsigned long long	logseqno;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw

#define BT_maxbits		26					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

//  BTree page number constants
#define ALLOC_page		0	// allocation page
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves
#define REDO_page		3	// first page of redo buffer

//	Number of levels to create in a new BTree

#define MIN_lvl			2

/*
There are six lock types for each node in four independent sets: 
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete. 
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent. 
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
5. (set 3) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
6. (set 4) AtomicModification: Exclusive. Atomic Update including node is underway. Incompatible with another AtomicModification. 
*/

typedef enum{
	BtLockAccess = 1,
	BtLockDelete = 2,
	BtLockRead   = 4,
	BtLockWrite  = 8,
	BtLockParent = 16,
	BtLockAtomic = 32
} BtLock;

//	lite weight spin latch

volatile typedef struct {
	ushort exclusive:1;
	ushort pending:1;
	ushort share:14;
} BtMutexLatch;

#define XCL 1
#define PEND 2
#define BOTH 3
#define SHARE 4

/*
// exclusive is set for write access

volatile typedef struct {
	unsigned char exclusive[1];
	unsigned char filler;
} BtMutexLatch;
*/
/*
typedef struct {
  union {
	struct {
	  volatile uint xlock:1;	// one writer has exclusive lock
	  volatile uint wrt:31;		// count of other writers waiting
	} bits[1];
	uint value[1];
  };
} BtMutexLatch;
*/
#define XCL 1
#define WRT 2

//	definition for phase-fair reader/writer lock implementation

typedef struct {
	volatile ushort rin[1];
	volatile ushort rout[1];
	volatile ushort ticket[1];
	volatile ushort serving[1];
	volatile ushort tid[1];
	volatile ushort dup[1];
} RWLock;

//	write only lock

typedef struct {
	BtMutexLatch xcl[1];
	volatile ushort tid[1];
	volatile ushort dup[1];
} WOLock;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4

//	mode & definition for lite latch implementation

enum {
	QueRd = 1,		// reader queue
	QueWr = 2		// writer queue
} RWQueue;

//  hash table entries

typedef struct {
	uint entry;		// Latch table entry at head of chain
	BtMutexLatch latch[1];
} BtHashEntry;

//	latch manager table structure

typedef struct {
	uid page_no;			// latch set page number
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	WOLock parent[1];		// Posting of fence key in parent
	WOLock atomic[1];		// Atomic update in progress
	uint split;				// right split page atomic insert
	uint entry;				// entry slot in latch table
	uint next;				// next entry in hash table chain
	uint prev;				// prev entry in hash table chain
	ushort pin;				// number of accessing threads
	unsigned char dirty;	// page in cache is dirty (atomic set)
	BtMutexLatch modify[1];	// modify entry lite latch
} BtLatchSet;

//	Define the length of the page record numbers

#define BtId 6

//	Page key slot definition.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence key (highest key) for
//	a leaf page is always present, even after cleanup.

//	Slot types

//	In addition to the Unique keys that occupy slots
//	there are Librarian and Duplicate key
//	slots occupying the key slot array.

//	The Librarian slots are dead keys that
//	serve as filler, available to add new Unique
//	or Dup slots that are inserted into the B-tree.

//	The Duplicate slots have had their key bytes extended
//	by 6 bytes to contain a binary duplicate key uniqueifier.

typedef enum {
	Unique,
	Librarian,
	Duplicate,
	Delete
} BtSlotType;

typedef struct {
	uint off:BT_maxbits;	// page offset for key start
	uint type:3;			// type of slot
	uint dead:1;			// set for deleted slot
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the key
//	bytes.

typedef struct {
	unsigned char len;		// this can be changed to a ushort or uint
	unsigned char key[0];
} BtKey;

//	the value structure also occupies space at the upper
//	end of the page. Each key is immediately followed by a value.

typedef struct {
	unsigned char len;		// this can be changed to a ushort or uint
	unsigned char value[0];
} BtVal;

#define BT_maxkey	255		// maximum number of bytes in a key
#define BT_keyarray (BT_maxkey + sizeof(BtKey))

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

//	note that this structure size
//	must be a multiple of 8 bytes
//	in order to place dups correctly.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	uint garbage;				// page garbage in bytes
	unsigned char bits:7;		// page size in bits
	unsigned char free:1;		// page is on free chain
	unsigned char lvl:7;		// level of page
	unsigned char kill:1;		// page is being deleted
	unsigned char right[BtId];	// page number to right
	unsigned char left[BtId];	// page number to left
	unsigned char filler[2];	// padding to multiple of 8
	logseqno lsn;				// log sequence number applied
	uid page_no;				// this page number
} *BtPage;

//  The loadpage interface object

typedef struct {
	BtPage page;		// current page pointer
	BtLatchSet *latch;	// current page latch set
} BtPageSet;

//	structure for latch manager on ALLOC_page

typedef struct {
	struct BtPage_ alloc[1];		// next page_no in right ptr
	unsigned long long dups[1];		// global duplicate key uniqueifier
	unsigned char freechain[BtId];	// head of free page_nos chain
	unsigned long long activepages;	// number of active pages pages
} BtPageZero;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// page size	
	uint page_bits;				// page size in bits	
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	BtPageZero *pagezero;		// mapped allocation page
	BtHashEntry *hashtable;		// the buffer pool hash table entries
	BtLatchSet *latchsets;		// mapped latch set from buffer pool
	unsigned char *pagepool;	// mapped to the buffer pool pages
	unsigned char *redobuff;	// mapped recovery buffer pointer
	logseqno lsn, flushlsn;		// current & first lsn flushed
	BtMutexLatch redo[1];		// redo area lite latch
	BtMutexLatch lock[1];		// allocation area lite latch
	BtMutexLatch maps[1];		// mapping segments lite latch
	ushort thread_no[1];		// next thread number
	uint nlatchpage;			// number of latch pages at BT_latch
	uint latchtotal;			// number of page latch entries
	uint latchhash;				// number of latch hash table slots
	uint latchvictim;			// next latch entry to examine
	uint latchpromote;			// next latch entry to promote
	uint redopages;				// size of recovery buff in pages
	uint redolast;				// last msync size of recovery buff
	uint redoend;				// eof/end element in recovery buff
	int err;					// last error
	int line;					// last error line no
	int found;					// number of keys found by delete
	int reads, writes;			// number of reads and writes
#ifndef unix
	HANDLE halloc;				// allocation handle
	HANDLE hpool;				// buffer pool handle
#endif
	uint segments;				// number of memory mapped segments
	unsigned char *pages[64000];// memory mapped segments of b-tree
} BtMgr;

typedef struct {
	BtMgr *mgr;					// buffer manager for entire process
	BtMgr *main;				// buffer manager for main btree
	BtPage cursor;				// cached page frame for start/next
	ushort thread_no;			// thread number
	unsigned char key[BT_keyarray];	// last found complete key
} BtDb;

//	Catastrophic errors

typedef enum {
	BTERR_ok = 0,
	BTERR_struct,
	BTERR_ovflw,
	BTERR_lock,
	BTERR_map,
	BTERR_read,
	BTERR_wrt,
	BTERR_atomic,
	BTERR_recovery
} BTERR;

#define CLOCK_bit 0x8000

// recovery manager entry types

typedef enum {
	BTRM_eof = 0,	// rest of buffer is emtpy
	BTRM_add,		// add a unique key-value to btree
	BTRM_dup,		// add a duplicate key-value to btree
	BTRM_del,		// delete a key-value from btree
	BTRM_upd,		// update a key with a new value
	BTRM_new,		// allocate a new empty page
	BTRM_old		// reuse an old empty page
} BTRM;

// recovery manager entry
//	structure followed by BtKey & BtVal

typedef struct {
	logseqno reqlsn;	// log sequence number required
	logseqno lsn;		// log sequence number for entry
	uint len;			// length of entry
	unsigned char type;	// type of entry
	unsigned char lvl;	// level of btree entry pertains to
} BtLogHdr;

// B-Tree functions

extern void bt_close (BtDb *bt);
extern BtDb *bt_open (BtMgr *mgr, BtMgr *main);
extern BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no, int syncit);
extern BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no);
extern void bt_lockpage(BtLock mode, BtLatchSet *latch, ushort thread_no);
extern void bt_unlockpage(BtLock mode, BtLatchSet *latch);
extern BTERR bt_insertkey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, void *value, uint vallen, BtSlotType type, ushort thread_no);
extern BTERR  bt_deletekey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, ushort thread_no);

extern int bt_findkey (BtDb *db, unsigned char *key, uint keylen, unsigned char *value, uint valmax);

extern uint bt_startkey (BtDb *db, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);
extern uint bt_prevkey (BtDb *db, uint slot);
extern uint bt_lastkey (BtDb *db);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint bits, uint poolsize, uint redopages);
extern void bt_mgrclose (BtMgr *mgr);
extern logseqno bt_newredo (BtMgr *mgr, BTRM type, int lvl, BtKey *key, BtVal *val, ushort thread_no);
extern logseqno bt_txnredo (BtMgr *mgr, BtPage page, ushort thread_no);

//	transaction functions
BTERR bt_txnpromote (BtDb *bt);

//  The page is allocated from low and hi ends.
//  The key slots are allocated from the bottom,
//	while the text and value of the key
//  are allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65535), and up to 253 bytes
//  of key value.

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
#define keyptr(page, slot) ((BtKey*)((unsigned char*)(page) + slotptr(page, slot)->off))
#define valptr(page, slot) ((BtVal*)(keyptr(page,slot)->key + keyptr(page,slot)->len))

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

uid bt_newdup (BtMgr *mgr)
{
#ifdef unix
	return __sync_fetch_and_add (mgr->pagezero->dups, 1) + 1;
#else
	return _InterlockedIncrement64(mgr->pagezero->dups, 1);
#endif
}

//	lite weight spin lock Latch Manager

int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}

void bt_mutexlock(BtMutexLatch *latch)
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
/*
unsigned char prev;

  do {
#ifdef  unix
	prev = __sync_fetch_and_or(latch->exclusive, XCL);
#else
	prev = _InterlockedOr8(latch->exclusive, XCL);
#endif
	if( !(prev & XCL) )
		return;
#ifdef  unix
  } while( sched_yield(), 1 );
#else
  } while( SwitchToThread(), 1 );
#endif
*/
/*
BtMutexLatch prev[1];
uint slept = 0;

  while( 1 ) {
	*prev->value = __sync_fetch_and_or(latch->value, XCL);

	if( !prev->bits->xlock ) {			// did we set XCL bit?
	    if( slept )
		  __sync_fetch_and_sub(latch->value, WRT);
		return;
	}

	if( !slept ) {
		prev->bits->wrt++;
		__sync_fetch_and_add(latch->value, WRT);
	}

	sys_futex (latch->value, FUTEX_WAIT_BITSET, *prev->value, NULL, NULL, QueWr);
	slept = 1;
  } */
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 otherwise

int bt_mutextry(BtMutexLatch *latch)
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
/*
unsigned char prev;

#ifdef  unix
	prev = __sync_fetch_and_or(latch->exclusive, XCL);
#else
	prev = _InterlockedOr8(latch->exclusive, XCL);
#endif
	//	take write access if all bits are clear

	return !(prev & XCL);
*/
/*
BtMutexLatch prev[1];

	*prev->value = __sync_fetch_and_or(latch->value, XCL);

	//	take write access if exclusive bit is clear

	return !prev->bits->xlock;
*/
}

//	clear write mode

void bt_releasemutex(BtMutexLatch *latch)
{
#ifdef unix
	__sync_fetch_and_and((ushort *)latch, ~BOTH);
#else
	_InterlockedAnd16((ushort *)latch, ~BOTH);
#endif
/*
	*latch->exclusive = 0;
*/
/*
BtMutexLatch prev[1];

	*prev->value = __sync_fetch_and_and(latch->value, ~XCL);

	if( prev->bits->wrt )
	  sys_futex( latch->value, FUTEX_WAKE_BITSET, 1, NULL, NULL, QueWr );
*/
}

//	Write-Only Queue Lock

void WriteOLock (WOLock *lock, ushort tid)
{
	if( *lock->tid == tid ) {
		*lock->dup += 1;
		return;
	}

	bt_mutexlock(lock->xcl);
	*lock->tid = tid;
}

void WriteORelease (WOLock *lock)
{
	if( *lock->dup ) {
		*lock->dup -= 1;
		return;
	}

	*lock->tid = 0;
	bt_releasemutex(lock->xcl);
}

//	Phase-Fair reader/writer lock implementation

void WriteLock (RWLock *lock, ushort tid)
{
ushort w, r, tix;

	if( *lock->tid == tid ) {
		*lock->dup += 1;
		return;
	}
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
	*lock->tid = tid;
}

void WriteRelease (RWLock *lock)
{
	if( *lock->dup ) {
		*lock->dup -= 1;
		return;
	}

	*lock->tid = 0;
#ifdef unix
	__sync_fetch_and_and (lock->rin, ~MASK);
#else
	_InterlockedAnd16 (lock->rin, ~MASK);
#endif
	lock->serving[0]++;
}

//	try to obtain read lock
//  return 1 if successful

int ReadTry (RWLock *lock, ushort tid)
{
ushort w;

	//  OK if write lock already held by same thread

	if( *lock->tid == tid ) {
		*lock->dup += 1;
		return 1;
	}
#ifdef unix
	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;
#else
	w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
#endif
	if( w )
	  if( w == (*lock->rin & MASK) ) {
#ifdef unix
		__sync_fetch_and_add (lock->rin, -RINC);
#else
		_InterlockedExchangeAdd16 (lock->rin, -RINC);
#endif
		return 0;
	  }

	return 1;
}

void ReadLock (RWLock *lock, ushort tid)
{
ushort w;

	if( *lock->tid == tid ) {
		*lock->dup += 1;
		return;
	}
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
	if( *lock->dup ) {
		*lock->dup -= 1;
		return;
	}

#ifdef unix
	__sync_fetch_and_add (lock->rout, RINC);
#else
	_InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}

//	recovery manager -- flush dirty pages

void bt_flushlsn (BtMgr *mgr, ushort thread_no)
{
uint cnt3 = 0, cnt2 = 0, cnt = 0;
BtLatchSet *latch;
BtPage page;
uint entry;

  //	flush dirty pool pages to the btree

fprintf(stderr, "Start flushlsn  ");
  for( entry = 1; entry < mgr->latchtotal; entry++ ) {
	page = (BtPage)(((uid)entry << mgr->page_bits) + mgr->pagepool);
	latch = mgr->latchsets + entry;
	bt_mutexlock (latch->modify);
	bt_lockpage(BtLockRead, latch, thread_no);

	if( latch->dirty ) {
	  bt_writepage(mgr, page, latch->page_no, 0);
	  latch->dirty = 0, cnt++;
	}
if( latch->pin & ~CLOCK_bit )
cnt2++;
	bt_unlockpage(BtLockRead, latch);
	bt_releasemutex (latch->modify);
  }
fprintf(stderr, "End flushlsn %d pages  %d pinned\n", cnt, cnt2);
fprintf(stderr, "begin sync");
	sync_file_range (mgr->idx, 0, 0, SYNC_FILE_RANGE_WAIT_AFTER);
fprintf(stderr, " end sync\n");
}

//	recovery manager -- process current recovery buff on startup
//	this won't do much if previous session was properly closed.

BTERR bt_recoveryredo (BtMgr *mgr)
{
BtLogHdr *hdr, *eof;
uint offset = 0;
BtKey *key;
BtVal *val;

  pread (mgr->idx, mgr->redobuff, mgr->redopages << mgr->page_size, REDO_page << mgr->page_size);

  hdr = (BtLogHdr *)mgr->redobuff;
  mgr->flushlsn = hdr->lsn;

  while( 1 ) {
	hdr = (BtLogHdr *)(mgr->redobuff + offset);
	switch( hdr->type ) {
	case BTRM_eof:
		mgr->lsn = hdr->lsn;
		return 0;
	case BTRM_add:		// add a unique key-value to btree
	
	case BTRM_dup:		// add a duplicate key-value to btree
	case BTRM_del:		// delete a key-value from btree
	case BTRM_upd:		// update a key with a new value
	case BTRM_new:		// allocate a new empty page
	case BTRM_old:		// reuse an old empty page
		return 0;
	}
  }
}

//	recovery manager -- append new entry to recovery log
//	flush to disk when it overflows.

logseqno bt_newredo (BtMgr *mgr, BTRM type, int lvl, BtKey *key, BtVal *val, ushort thread_no)
{
uint size = mgr->page_size * mgr->redopages - sizeof(BtLogHdr);
uint amt = sizeof(BtLogHdr);
BtLogHdr *hdr, *eof;
uint last, end;

	bt_mutexlock (mgr->redo);

	if( key )
	  amt += key->len + val->len + sizeof(BtKey) + sizeof(BtVal);

	//	see if new entry fits in buffer
	//	flush and reset if it doesn't

	if( amt > size - mgr->redoend ) {
	  mgr->flushlsn = mgr->lsn;
	  msync (mgr->redobuff + (mgr->redolast & 0xfff), mgr->redoend - mgr->redolast + sizeof(BtLogHdr) + 4096, MS_SYNC);
	  mgr->redolast = 0;
	  mgr->redoend = 0;
	  bt_flushlsn(mgr, thread_no);
	}

	//	fill in new entry & either eof or end block

	hdr = (BtLogHdr *)(mgr->redobuff + mgr->redoend);

	hdr->len = amt;
	hdr->type = type;
	hdr->lvl = lvl;
	hdr->lsn = ++mgr->lsn;

	mgr->redoend += amt;

	eof = (BtLogHdr *)(mgr->redobuff + mgr->redoend);
	memset (eof, 0, sizeof(BtLogHdr));

	//  fill in key and value

	if( key ) {
	  memcpy ((unsigned char *)(hdr + 1), key, key->len + sizeof(BtKey));
	  memcpy ((unsigned char *)(hdr + 1) + key->len + sizeof(BtKey), val, val->len + sizeof(BtVal));
	}

	eof = (BtLogHdr *)(mgr->redobuff + mgr->redoend);
	memset (eof, 0, sizeof(BtLogHdr));
	eof->lsn = mgr->lsn;

	last = mgr->redolast & 0xfff;
	end = mgr->redoend;
	mgr->redolast = end;

	bt_releasemutex(mgr->redo);

	msync (mgr->redobuff + last, end - last + sizeof(BtLogHdr), MS_SYNC);
	return hdr->lsn;
}

//	recovery manager -- append transaction to recovery log
//	flush to disk when it overflows.

logseqno bt_txnredo (BtMgr *mgr, BtPage source, ushort thread_no)
{
uint size = mgr->page_size * mgr->redopages - sizeof(BtLogHdr);
uint amt = 0, src, type;
BtLogHdr *hdr, *eof;
uint last, end;
logseqno lsn;
BtKey *key;
BtVal *val;

	//	determine amount of redo recovery log space required

	for( src = 0; src++ < source->cnt; ) {
	  key = keyptr(source,src);
	  val = valptr(source,src);
	  amt += key->len + val->len + sizeof(BtKey) + sizeof(BtVal);
	  amt += sizeof(BtLogHdr);
	}

	bt_mutexlock (mgr->redo);

	//	see if new entry fits in buffer
	//	flush and reset if it doesn't

	if( amt > size - mgr->redoend ) {
	  mgr->flushlsn = mgr->lsn;
	  msync (mgr->redobuff + (mgr->redolast & 0xfff), mgr->redoend - mgr->redolast + sizeof(BtLogHdr) + 4096, MS_SYNC);
	  mgr->redolast = 0;
	  mgr->redoend = 0;
	  bt_flushlsn (mgr, thread_no);
	}

	//	assign new lsn to transaction

	lsn = ++mgr->lsn;

	//	fill in new entries

	for( src = 0; src++ < source->cnt; ) {
	  key = keyptr(source, src);
	  val = valptr(source, src);

	  switch( slotptr(source, src)->type ) {
	  case Unique:
		type = BTRM_add;
		break;
	  case Duplicate:
		type = BTRM_dup;
		break;
	  case Delete:
		type = BTRM_del;
		break;
	  }

	  amt = key->len + val->len + sizeof(BtKey) + sizeof(BtVal);
	  amt += sizeof(BtLogHdr);

	  hdr = (BtLogHdr *)(mgr->redobuff + mgr->redoend);
	  hdr->len = amt;
	  hdr->type = type;
	  hdr->lsn = lsn;
	  hdr->lvl = 0;

	  //  fill in key and value

	  memcpy ((unsigned char *)(hdr + 1), key, key->len + sizeof(BtKey));
	  memcpy ((unsigned char *)(hdr + 1) + key->len + sizeof(BtKey), val, val->len + sizeof(BtVal));

	  mgr->redoend += amt;
	}

	eof = (BtLogHdr *)(mgr->redobuff + mgr->redoend);
	memset (eof, 0, sizeof(BtLogHdr));
	eof->lsn = lsn;

	last = mgr->redolast & 0xfff;
	end = mgr->redoend;
	mgr->redolast = end;
	bt_releasemutex(mgr->redo);

	msync (mgr->redobuff + last, end - last + sizeof(BtLogHdr), MS_SYNC);
	bt_releasemutex(mgr->redo);
	return lsn;
}

//	read page into buffer pool from permanent location in Btree file

BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no)
{
off64_t off = page_no << mgr->page_bits;

	if( pread (mgr->idx, page, mgr->page_size, page_no << mgr->page_bits) < mgr->page_size ) {
		fprintf (stderr, "Unable to read page %d errno = %d\n", page_no, errno);
		return BTERR_read;
	}
if( page->page_no != page_no )
abort();
	mgr->reads++;
	return 0;
/*
int flag = PROT_READ | PROT_WRITE;
uint segment = page_no >> 32;
unsigned char *perm;

  while( 1 ) {
	if( segment < mgr->segments ) {
	  perm = mgr->pages[segment] + ((page_no & 0xffffffff) << mgr->page_bits);
		memcpy (page, perm, mgr->page_size);
if( page->page_no != page_no )
abort();
		mgr->reads++;
		return 0;
	}

	bt_mutexlock (mgr->maps);

	if( segment < mgr->segments ) {
		bt_releasemutex (mgr->maps);
		continue;
	}

	mgr->pages[mgr->segments] = mmap (0, (uid)65536 << mgr->page_bits, flag, MAP_SHARED, mgr->idx, mgr->segments << (mgr->page_bits + 16));
	mgr->segments++;

	bt_releasemutex (mgr->maps);
  }  */
}

//	write page to permanent location in Btree file
//	clear the dirty bit

BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no, int syncit)
{
off64_t off = page_no << mgr->page_bits;

	if( pwrite(mgr->idx, page, mgr->page_size, off) < mgr->page_size ) {
		fprintf (stderr, "Unable to write page %d errno = %d\n", page_no, errno);
		return BTERR_wrt;
	}
	mgr->writes++;
	return 0;
/*
int flag = PROT_READ | PROT_WRITE;
uint segment = page_no >> 32;
unsigned char *perm;

  while( 1 ) {
	if( segment < mgr->segments ) {
	  perm = mgr->pages[segment] + ((page_no & 0xffffffff) << mgr->page_bits);
		memcpy (perm, page, mgr->page_size);
		if( syncit )
			msync (perm, mgr->page_size, MS_SYNC);
		mgr->writes++;
		return 0;
	}

	bt_mutexlock (mgr->maps);

	if( segment < mgr->segments ) {
		bt_releasemutex (mgr->maps);
		continue;
	}

	mgr->pages[mgr->segments] = mmap (0, (uid)65536 << mgr->page_bits, flag, MAP_SHARED, mgr->idx, mgr->segments << (mgr->page_bits + 16));
	bt_releasemutex (mgr->maps);
	mgr->segments++;
  } */
}

//	set CLOCK bit in latch
//	decrement pin count

void bt_unpinlatch (BtMgr *mgr, BtLatchSet *latch)
{
	bt_mutexlock(latch->modify);
	latch->pin |= CLOCK_bit;
	latch->pin--;

	bt_releasemutex(latch->modify);
}

//  return the btree cached page address

BtPage bt_mappage (BtMgr *mgr, BtLatchSet *latch)
{
BtPage page = (BtPage)(((uid)latch->entry << mgr->page_bits) + mgr->pagepool);

  return page;
}

//  return next available latch entry
//	  and with latch entry locked

uint bt_availnext (BtMgr *mgr)
{
BtLatchSet *latch;
uint entry;

  while( 1 ) {
#ifdef unix
	entry = __sync_fetch_and_add (&mgr->latchvictim, 1) + 1;
#else
	entry = _InterlockedIncrement (&mgr->latchvictim);
#endif
	entry %= mgr->latchtotal;

	if( !entry )
		continue;

	latch = mgr->latchsets + entry;

	if( !bt_mutextry(latch->modify) )
		continue;

	//  return this entry if it is not pinned

	if( !latch->pin )
		return entry;

	//	if the CLOCK bit is set
	//	reset it to zero.

	latch->pin &= ~CLOCK_bit;
	bt_releasemutex(latch->modify);
  }
}

//	pin page in buffer pool
//	return with latchset pinned

BtLatchSet *bt_pinlatch (BtMgr *mgr, uid page_no, BtPage contents, ushort thread_id)
{
uint hashidx = page_no % mgr->latchhash;
BtLatchSet *latch;
uint entry, idx;
BtPage page;

  //  try to find our entry

  bt_mutexlock(mgr->hashtable[hashidx].latch);

  if( entry = mgr->hashtable[hashidx].entry ) do
  {
	latch = mgr->latchsets + entry;
	if( page_no == latch->page_no )
		break;
  } while( entry = latch->next );

  //  found our entry: increment pin

  if( entry ) {
	latch = mgr->latchsets + entry;
	bt_mutexlock(latch->modify);
	latch->pin |= CLOCK_bit;
	latch->pin++;
if(contents)
abort();
	bt_releasemutex(latch->modify);
	bt_releasemutex(mgr->hashtable[hashidx].latch);
	return latch;
  }

  //  find and reuse unpinned entry

trynext:

  entry = bt_availnext (mgr);
  latch = mgr->latchsets + entry;

  idx = latch->page_no % mgr->latchhash;

  //  if latch is on a different hash chain
  //	unlink from the old page_no chain

  if( latch->page_no )
   if( idx != hashidx ) {

	//  skip over this entry if latch not available

    if( !bt_mutextry (mgr->hashtable[idx].latch) ) {
	  bt_releasemutex(latch->modify);
	  goto trynext;
	}

	if( latch->prev )
	  mgr->latchsets[latch->prev].next = latch->next;
	else
	  mgr->hashtable[idx].entry = latch->next;

	if( latch->next )
	  mgr->latchsets[latch->next].prev = latch->prev;

    bt_releasemutex (mgr->hashtable[idx].latch);
   }

  page = (BtPage)(((uid)entry << mgr->page_bits) + mgr->pagepool);

  //  update permanent page area in btree from buffer pool
  //	no read-lock is required since page is not pinned.

  if( latch->dirty )
	if( mgr->err = bt_writepage (mgr, page, latch->page_no, 0) )
	  return mgr->line = __LINE__, NULL;
	else
	  latch->dirty = 0;

  if( contents ) {
	memcpy (page, contents, mgr->page_size);
	latch->dirty = 1;
  } else if( bt_readpage (mgr, page, page_no) )
	  return mgr->line = __LINE__, NULL;

  //  link page as head of hash table chain
  //  if this is a never before used entry,
  //  or it was previously on a different
  //  hash table chain. Otherwise, just
  //  leave it in its current hash table
  //  chain position.

  if( !latch->page_no || hashidx != idx ) {
	if( latch->next = mgr->hashtable[hashidx].entry )
	  mgr->latchsets[latch->next].prev = entry;

	mgr->hashtable[hashidx].entry = entry;
    latch->prev = 0;
  }

  //  fill in latch structure

  latch->pin = CLOCK_bit | 1;
  latch->page_no = page_no;
  latch->entry = entry;
  latch->split = 0;

  bt_releasemutex (latch->modify);
  bt_releasemutex (mgr->hashtable[hashidx].latch);
  return latch;
}
  
void bt_mgrclose (BtMgr *mgr)
{
BtLatchSet *latch;
BtLogHdr *eof;
uint num = 0;
BtPage page;
uint slot;

	//	flush previously written dirty pages
	//	and write recovery buffer to disk

	fdatasync (mgr->idx);

	if( mgr->redoend ) {
		eof = (BtLogHdr *)(mgr->redobuff + mgr->redoend);
		memset (eof, 0, sizeof(BtLogHdr));

		pwrite (mgr->idx, mgr->redobuff, mgr->redoend + sizeof(BtLogHdr), REDO_page << mgr->page_bits);
	}

	//	write remaining dirty pool pages to the btree

	for( slot = 1; slot < mgr->latchtotal; slot++ ) {
		page = (BtPage)(((uid)slot << mgr->page_bits) + mgr->pagepool);
		latch = mgr->latchsets + slot;

		if( latch->dirty ) {
			bt_writepage(mgr, page, latch->page_no, 0);
			latch->dirty = 0, num++;
		}
	}

	//	flush last batch to disk and clear
	//	redo recovery buffer on disk.

	fdatasync (mgr->idx);

	if( mgr->redopages ) {
		eof = (BtLogHdr *)mgr->redobuff;
		memset (eof, 0, sizeof(BtLogHdr));
		eof->lsn = mgr->lsn;

		pwrite (mgr->idx, mgr->redobuff, sizeof(BtLogHdr), REDO_page << mgr->page_bits);

		sync_file_range (mgr->idx, REDO_page << mgr->page_bits, sizeof(BtLogHdr), SYNC_FILE_RANGE_WAIT_AFTER);
	}

	fprintf(stderr, "%d buffer pool pages flushed\n", num);

#ifdef unix
	while( mgr->segments )
		munmap (mgr->pages[--mgr->segments], (uid)65536 << mgr->page_bits);

	munmap (mgr->pagepool, (uid)mgr->nlatchpage << mgr->page_bits);
	munmap (mgr->pagezero, mgr->page_size);
#else
	FlushViewOfFile(mgr->pagezero, 0);
	UnmapViewOfFile(mgr->pagezero);
	UnmapViewOfFile(mgr->pagepool);
	CloseHandle(mgr->halloc);
	CloseHandle(mgr->hpool);
#endif
#ifdef unix
	free (mgr->redobuff);
	close (mgr->idx);
	free (mgr);
#else
	VirtualFree (mgr->redobuff, 0, MEM_RELEASE);
	FlushFileBuffers(mgr->idx);
	CloseHandle(mgr->idx);
	GlobalFree (mgr);
#endif
}

//	close and release memory

void bt_close (BtDb *bt)
{
#ifdef unix
	if( bt->cursor )
		free (bt->cursor);
#else
	if( bt->cursor)
		VirtualFree (bt->cursor, 0, MEM_RELEASE);
#endif
	free (bt);
}

//  open/create new btree buffer manager

//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of page pool (e.g. 262144)

BtMgr *bt_mgr (char *name, uint bits, uint nodemax, uint redopages)
{
uint lvl, attr, last, slot, idx;
uint nlatchpage, latchhash;
unsigned char value[BtId];
int flag, initit = 0;
BtPageZero *pagezero;
BtLatchSet *latch;
off64_t size;
uint amt[1];
BtMgr* mgr;
BtKey* key;
BtVal *val;

	// determine sanity of page size and buffer pool

	if( bits > BT_maxbits )
		bits = BT_maxbits;
	else if( bits < BT_minbits )
		bits = BT_minbits;
#ifdef unix
	mgr = calloc (1, sizeof(BtMgr));

	mgr->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);

	if( mgr->idx == -1 ) {
		fprintf (stderr, "Unable to create/open btree file %s\n", name);
		return free(mgr), NULL;
	}
#else
	mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtMgr));
	attr = FILE_ATTRIBUTE_NORMAL;
	mgr->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);

	if( mgr->idx == INVALID_HANDLE_VALUE ) {
		fprintf (stderr, "Unable to create/open btree file %s\n", name);
		return GlobalFree(mgr), NULL;
	}
#endif

#ifdef unix
	pagezero = valloc (BT_maxpage);
	*amt = 0;

	// read minimum page size to get root info
	//	to support raw disk partition files
	//	check if bits == 0 on the disk.

	if( size = lseek (mgr->idx, 0L, 2) )
		if( pread(mgr->idx, pagezero, BT_minpage, 0) == BT_minpage )
			if( pagezero->alloc->bits )
				bits = pagezero->alloc->bits;
			else
				initit = 1;
		else
			return free(mgr), free(pagezero), NULL;
	else
		initit = 1;
#else
	pagezero = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
	size = GetFileSize(mgr->idx, amt);

	if( size || *amt ) {
		if( !ReadFile(mgr->idx, (char *)pagezero, BT_minpage, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		bits = pagezero->alloc->bits;
	} else
		initit = 1;
#endif

	mgr->page_size = 1 << bits;
	mgr->page_bits = bits;

	//  calculate number of latch hash table entries

	mgr->nlatchpage = ((uid)nodemax/16 * sizeof(BtHashEntry) + mgr->page_size - 1) / mgr->page_size;

	mgr->nlatchpage += nodemax;		// size of the buffer pool in pages
	mgr->nlatchpage += (sizeof(BtLatchSet) * (uid)nodemax + mgr->page_size - 1)/mgr->page_size;
	mgr->latchtotal = nodemax;

	if( !initit )
		goto mgrlatch;

	// initialize an empty b-tree with latch page, root page, page of leaves
	// and page(s) of latches and page pool cache

	memset (pagezero, 0, 1 << bits);
	pagezero->alloc->lvl = MIN_lvl - 1;
	pagezero->alloc->bits = mgr->page_bits;
	bt_putid(pagezero->alloc->right, redopages + MIN_lvl+1);
	pagezero->activepages = 2;

	//  initialize left-most LEAF page in
	//	alloc->left and count of active leaf pages.

	bt_putid (pagezero->alloc->left, LEAF_page);

	ftruncate (mgr->idx, REDO_page << mgr->page_bits);

	if( bt_writepage (mgr, pagezero->alloc, 0, 1) ) {
		fprintf (stderr, "Unable to create btree page zero\n");
		return bt_mgrclose (mgr), NULL;
	}

	memset (pagezero, 0, 1 << bits);
	pagezero->alloc->bits = mgr->page_bits;

	for( lvl=MIN_lvl; lvl--; ) {
	BtSlot *node = slotptr(pagezero->alloc, 1);
		node->off = mgr->page_size - 3 - (lvl ? BtId + sizeof(BtVal): sizeof(BtVal));
		key = keyptr(pagezero->alloc, 1);
		key->len = 2;		// create stopper key
		key->key[0] = 0xff;
		key->key[1] = 0xff;

		bt_putid(value, MIN_lvl - lvl + 1);
		val = valptr(pagezero->alloc, 1);
		val->len = lvl ? BtId : 0;
		memcpy (val->value, value, val->len);

		pagezero->alloc->min = node->off;
		pagezero->alloc->lvl = lvl;
		pagezero->alloc->cnt = 1;
		pagezero->alloc->act = 1;
		pagezero->alloc->page_no = MIN_lvl - lvl;

		if( bt_writepage (mgr, pagezero->alloc, MIN_lvl - lvl, 1) ) {
			fprintf (stderr, "Unable to create btree page\n");
			return bt_mgrclose (mgr), NULL;
		}
	}

mgrlatch:
#ifdef unix
	free (pagezero);
#else
	VirtualFree (pagezero, 0, MEM_RELEASE);
#endif
#ifdef unix
	// mlock the pagezero page

	flag = PROT_READ | PROT_WRITE;
	mgr->pagezero = mmap (0, mgr->page_size, flag, MAP_SHARED, mgr->idx, ALLOC_page << mgr->page_bits);
	if( mgr->pagezero == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap btree page zero, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}
	mlock (mgr->pagezero, mgr->page_size);

	mgr->pagepool = mmap (0, (uid)mgr->nlatchpage << mgr->page_bits, flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if( mgr->pagepool == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap anonymous buffer pool pages, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}
	if( mgr->redopages = redopages ) {
		ftruncate (mgr->idx, (REDO_page + redopages) << mgr->page_bits);
		mgr->redobuff = valloc (redopages * mgr->page_size);
	}
#else
	flag = PAGE_READWRITE;
	mgr->halloc = CreateFileMapping(mgr->idx, NULL, flag, 0, mgr->page_size, NULL);
	if( !mgr->halloc ) {
		fprintf (stderr, "Unable to create page zero memory mapping, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}

	flag = FILE_MAP_WRITE;
	mgr->pagezero = MapViewOfFile(mgr->halloc, flag, 0, 0, mgr->page_size);
	if( !mgr->pagezero ) {
		fprintf (stderr, "Unable to map page zero, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}

	flag = PAGE_READWRITE;
	size = (uid)mgr->nlatchpage << mgr->page_bits;
	mgr->hpool = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, flag, size >> 32, size, NULL);
	if( !mgr->hpool ) {
		fprintf (stderr, "Unable to create buffer pool memory mapping, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}

	flag = FILE_MAP_WRITE;
	mgr->pagepool = MapViewOfFile(mgr->pool, flag, 0, 0, size);
	if( !mgr->pagepool ) {
		fprintf (stderr, "Unable to map buffer pool, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}
	if( mgr->redopages = redopages )
		mgr->redobuff = VirtualAlloc (NULL, redopages * mgr->page_size | MEM_COMMIT, PAGE_READWRITE);
#endif

	mgr->latchsets = (BtLatchSet *)(mgr->pagepool + ((uid)mgr->latchtotal << mgr->page_bits));
	mgr->hashtable = (BtHashEntry *)(mgr->latchsets + mgr->latchtotal);
	mgr->latchhash = (mgr->pagepool + ((uid)mgr->nlatchpage << mgr->page_bits) - (unsigned char *)mgr->hashtable) / sizeof(BtHashEntry);

	return mgr;
}

//	open BTree access method
//	based on buffer manager

BtDb *bt_open (BtMgr *mgr, BtMgr *main)
{
BtDb *bt = malloc (sizeof(*bt));

	memset (bt, 0, sizeof(*bt));
	bt->main = main;
	bt->mgr = mgr;
#ifdef unix
	bt->cursor = valloc (mgr->page_size);
#else
	bt->cursor = VirtualAlloc(NULL, mgr->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
#ifdef unix
	bt->thread_no = __sync_fetch_and_add (mgr->thread_no, 1) + 1;
#else
	bt->thread_no = _InterlockedIncrement16(mgr->thread_no, 1);
#endif
	return bt;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: keys are same
//  -1: key2 > key1
//  +1: key2 < key1
//  as the comparison value

int keycmp (BtKey* key1, unsigned char *key2, uint len2)
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

// place write, read, or parent lock on requested page_no.

void bt_lockpage(BtLock mode, BtLatchSet *latch, ushort thread_no)
{
	switch( mode ) {
	case BtLockRead:
		ReadLock (latch->readwr, thread_no);
		break;
	case BtLockWrite:
		WriteLock (latch->readwr, thread_no);
		break;
	case BtLockAccess:
		ReadLock (latch->access, thread_no);
		break;
	case BtLockDelete:
		WriteLock (latch->access, thread_no);
		break;
	case BtLockParent:
		WriteOLock (latch->parent, thread_no);
		break;
	case BtLockAtomic:
		WriteOLock (latch->atomic, thread_no);
		break;
	case BtLockAtomic | BtLockRead:
		WriteOLock (latch->atomic, thread_no);
		ReadLock (latch->readwr, thread_no);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *latch)
{
	switch( mode ) {
	case BtLockRead:
		ReadRelease (latch->readwr);
		break;
	case BtLockWrite:
		WriteRelease (latch->readwr);
		break;
	case BtLockAccess:
		ReadRelease (latch->access);
		break;
	case BtLockDelete:
		WriteRelease (latch->access);
		break;
	case BtLockParent:
		WriteORelease (latch->parent);
		break;
	case BtLockAtomic:
		WriteORelease (latch->atomic);
		break;
	case BtLockAtomic | BtLockRead:
		WriteORelease (latch->atomic);
		ReadRelease (latch->readwr);
		break;
	}
}

//	allocate a new page
//	return with page latched, but unlocked.

int bt_newpage(BtMgr *mgr, BtPageSet *set, BtPage contents, ushort thread_id)
{
uid page_no;
int blk;

	//	lock allocation page

	bt_mutexlock(mgr->lock);

	// use empty chain first
	// else allocate empty page

	if( page_no = bt_getid(mgr->pagezero->freechain) ) {
		if( set->latch = bt_pinlatch (mgr, page_no, NULL, thread_id) )
			set->page = bt_mappage (mgr, set->latch);
		else
			return mgr->line = __LINE__, mgr->err = BTERR_struct;

		bt_putid(mgr->pagezero->freechain, bt_getid(set->page->right));
		mgr->pagezero->activepages++;

		bt_releasemutex(mgr->lock);

		memcpy (set->page, contents, mgr->page_size);
		set->latch->dirty = 1;
		return 0;
	}

	page_no = bt_getid(mgr->pagezero->alloc->right);
	bt_putid(mgr->pagezero->alloc->right, page_no+1);

	// unlock allocation latch and
	//	extend file into new page.

	mgr->pagezero->activepages++;

	ftruncate (mgr->idx, (uid)(page_no + 1) << mgr->page_bits);
	bt_releasemutex(mgr->lock);

	//	don't load cache from btree page, load it from contents

	contents->page_no = page_no;

	if( set->latch = bt_pinlatch (mgr, page_no, contents, thread_id) )
		set->page = bt_mappage (mgr, set->latch);
	else
		return mgr->err;

	return 0;
}

//  find slot in page for given key at a given level

int bt_findslot (BtPage page, unsigned char *key, uint len)
{
uint diff, higher = page->cnt, low = 1, slot;
uint good = 0;

	//	  make stopper key an infinite fence value

	if( bt_getid (page->right) )
		higher++;
	else
		good++;

	//	low is the lowest candidate.
	//  loop ends when they meet

	//  higher is already
	//	tested as .ge. the passed key.

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on right link page

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtMgr *mgr, BtPageSet *set, unsigned char *key, uint len, uint lvl, BtLock lock, ushort thread_no)
{
uid page_no = ROOT_page, prevpage = 0;
uint drill = 0xff, slot;
BtLatchSet *prevlatch;
uint mode, prevmode;
BtVal *val;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (drill == lvl) ? lock : BtLockRead; 

	if( !(set->latch = bt_pinlatch (mgr, page_no, NULL, thread_no)) )
	  return 0;

 	// obtain access lock using lock chaining with Access mode

	if( page_no > ROOT_page )
	  bt_lockpage(BtLockAccess, set->latch, thread_no);

	set->page = bt_mappage (mgr, set->latch);

	//	release & unpin parent or left sibling page

	if( prevpage ) {
	  bt_unlockpage(prevmode, prevlatch);
	  bt_unpinlatch (mgr, prevlatch);
	  prevpage = 0;
	}

 	// obtain mode lock using lock chaining through AccessLock

	bt_lockpage(mode, set->latch, thread_no);

	if( set->page->free )
		return mgr->err = BTERR_struct, mgr->line = __LINE__, 0;

	if( page_no > ROOT_page )
	  bt_unlockpage(BtLockAccess, set->latch);

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if( set->latch->page_no != ROOT_page )
			return mgr->err = BTERR_struct, mgr->line = __LINE__, 0;
			
		drill = set->page->lvl;

		if( lock != BtLockRead && drill == lvl ) {
		  bt_unlockpage(mode, set->latch);
		  bt_unpinlatch (mgr, set->latch);
		  continue;
		}
	}

	prevpage = set->latch->page_no;
	prevlatch = set->latch;
	prevmode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( !set->page->kill )
	 if( slot = bt_findslot (set->page, key, len) ) {
	  if( drill == lvl )
		return slot;

	  // find next non-dead slot -- the fence key if nothing else

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
		  continue;
		else
  		  return mgr->err = BTERR_struct, mgr->line = __LINE__, 0;

	  val = valptr(set->page, slot);

	  if( val->len == BtId )
	  	page_no = bt_getid(valptr(set->page, slot)->value);
	  else
  		return mgr->line = __LINE__, mgr->err = BTERR_struct, 0;

	  drill--;
	  continue;
	 }

	//  slide right into next page

	page_no = bt_getid(set->page->right);
  } while( page_no );

  // return error on end of right chain

  mgr->line = __LINE__, mgr->err = BTERR_struct;
  return 0;	// return error
}

//	return page to free list
//	page must be delete & write locked

void bt_freepage (BtMgr *mgr, BtPageSet *set)
{
	//	lock allocation page

	bt_mutexlock (mgr->lock);

	//	store chain

	memcpy(set->page->right, mgr->pagezero->freechain, BtId);
	bt_putid(mgr->pagezero->freechain, set->latch->page_no);
	set->latch->dirty = 1;
	set->page->free = 1;

	// decrement active page count
	// and unlock allocation page

	mgr->pagezero->activepages--;
	bt_releasemutex (mgr->lock);

	// unlock released page

	bt_unlockpage (BtLockDelete, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (mgr, set->latch);
}

//	a fence key was deleted from a page
//	push new fence value upwards

BTERR bt_fixfence (BtMgr *mgr, BtPageSet *set, uint lvl, ushort thread_no)
{
unsigned char leftkey[BT_keyarray], rightkey[BT_keyarray];
unsigned char value[BtId];
BtKey* ptr;
uint idx;

	//	remove the old fence value

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));
	memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
	set->latch->dirty = 1;

	//  cache new fence value

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	bt_lockpage (BtLockParent, set->latch, thread_no);
	bt_unlockpage (BtLockWrite, set->latch);

	//	insert new (now smaller) fence key

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey*)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
	  return mgr->err;

	//	now delete old fence key

	ptr = (BtKey*)rightkey;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1, thread_no) )
		return mgr->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch(mgr, set->latch);
	return 0;
}

//	root has a single child
//	collapse a level from the tree

BTERR bt_collapseroot (BtMgr *mgr, BtPageSet *root, ushort thread_no)
{
BtPageSet child[1];
uid page_no;
BtVal *val;
uint idx;

  // find the child entry and promote as new root contents

  do {
	for( idx = 0; idx++ < root->page->cnt; )
	  if( !slotptr(root->page, idx)->dead )
		break;

	val = valptr(root->page, idx);

	if( val->len == BtId )
		page_no = bt_getid (valptr(root->page, idx)->value);
	else
  		return mgr->line = __LINE__, mgr->err = BTERR_struct;

	if( child->latch = bt_pinlatch (mgr, page_no, NULL, thread_no) )
		child->page = bt_mappage (mgr, child->latch);
	else
		return mgr->err;

	bt_lockpage (BtLockDelete, child->latch, thread_no);
	bt_lockpage (BtLockWrite, child->latch, thread_no);

	memcpy (root->page, child->page, mgr->page_size);
	root->latch->dirty = 1;

	bt_freepage (mgr, child);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockWrite, root->latch);
  bt_unpinlatch (mgr, root->latch);
  return 0;
}

//  delete a page and manage keys
//  call with page writelocked

BTERR bt_deletepage (BtMgr *mgr, BtPageSet *set, ushort thread_no)
{
unsigned char lowerfence[BT_keyarray], higherfence[BT_keyarray];
unsigned char value[BtId];
uint lvl = set->page->lvl;
BtPageSet right[1];
uid page_no;
BtKey *ptr;

	//	cache copy of fence key
	//	to remove in parent

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (lowerfence, ptr, ptr->len + sizeof(BtKey));

	//	obtain lock on right page

	page_no = bt_getid(set->page->right);

	if( right->latch = bt_pinlatch (mgr, page_no, NULL, thread_no) )
		right->page = bt_mappage (mgr, right->latch);
	else
		return 0;

	bt_lockpage (BtLockWrite, right->latch, thread_no);

	// cache copy of key to update

	ptr = keyptr(right->page, right->page->cnt);
	memcpy (higherfence, ptr, ptr->len + sizeof(BtKey));

	if( right->page->kill )
		return mgr->line = __LINE__, mgr->err = BTERR_struct;

	// pull contents of right peer into our empty page

	memcpy (set->page, right->page, mgr->page_size);
	set->latch->dirty = 1;

	// mark right page deleted and point it to left page
	//	until we can post parent updates that remove access
	//	to the deleted page.

	bt_putid (right->page->right, set->latch->page_no);
	right->latch->dirty = 1;
	right->page->kill = 1;

	bt_lockpage (BtLockParent, right->latch, thread_no);
	bt_unlockpage (BtLockWrite, right->latch);

	bt_lockpage (BtLockParent, set->latch, thread_no);
	bt_unlockpage (BtLockWrite, set->latch);

	// redirect higher key directly to our new node contents

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey*)higherfence;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
	  return mgr->err;

	//	delete old lower key to our node

	ptr = (BtKey*)lowerfence;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1, thread_no) )
	  return mgr->err;

	//	obtain delete and write locks to right node

	bt_unlockpage (BtLockParent, right->latch);
	bt_lockpage (BtLockDelete, right->latch, thread_no);
	bt_lockpage (BtLockWrite, right->latch, thread_no);
	bt_freepage (mgr, right);

	bt_unlockpage (BtLockParent, set->latch);
	return 0;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, ushort thread_no)
{
uint slot, idx, found, fence;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

	if( slot = bt_loadpage (mgr, set, key, len, lvl, BtLockWrite, thread_no) ) {
		node = slotptr(set->page, slot);
		ptr = keyptr(set->page, slot);
	} else
		return mgr->err;

	// if librarian slot, advance to real slot

	if( node->type == Librarian ) {
		ptr = keyptr(set->page, ++slot);
		node = slotptr(set->page, slot);
	}

	fence = slot == set->page->cnt;

	// delete the key, ignore request if already dead

	if( found = !keycmp (ptr, key, len) )
	  if( found = node->dead == 0 ) {
		val = valptr(set->page,slot);
 		set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
 		set->page->act--;

		//  mark node type as delete

		node->type = Delete;
		node->dead = 1;

		// collapse empty slots beneath the fence
		// on interiour nodes

		if( lvl )
		 while( idx = set->page->cnt - 1 )
		  if( slotptr(set->page, idx)->dead ) {
			*slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	//	did we delete a fence key in an upper level?

	if( found && lvl && set->page->act && fence )
	  if( bt_fixfence (mgr, set, lvl, thread_no) )
		return mgr->err;
	  else
		return 0;

	//	do we need to collapse root?

	if( lvl > 1 && set->latch->page_no == ROOT_page && set->page->act == 1 )
	  if( bt_collapseroot (mgr, set, thread_no) )
		return mgr->err;
	  else
		return 0;

	//	delete empty page

 	if( !set->page->act ) {
	  if( bt_deletepage (mgr, set, thread_no) )
		return mgr->err;
	} else {
	  set->latch->dirty = 1;
	  bt_unlockpage(BtLockWrite, set->latch);
	}

	bt_unpinlatch (mgr, set->latch);
	return 0;
}

//	advance to next slot

uint bt_findnext (BtDb *bt, BtPageSet *set, uint slot)
{
BtLatchSet *prevlatch;
uid page_no;

	if( slot < set->page->cnt )
		return slot + 1;

	prevlatch = set->latch;

	if( page_no = bt_getid(set->page->right) )
	  if( set->latch = bt_pinlatch (bt->mgr, page_no, NULL, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	  else
		return 0;
	else
	  return bt->mgr->err = BTERR_struct, bt->mgr->line = __LINE__, 0;

 	// obtain access lock using lock chaining with Access mode

	bt_lockpage(BtLockAccess, set->latch, bt->thread_no);

	bt_unlockpage(BtLockRead, prevlatch);
	bt_unpinlatch (bt->mgr, prevlatch);

	bt_lockpage(BtLockRead, set->latch, bt->thread_no);
	bt_unlockpage(BtLockAccess, set->latch);
	return 1;
}

//	find unique key == given key, or first duplicate key in
//	leaf level and return number of value bytes
//	or (-1) if not found.

int bt_findkey (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint valmax)
{
BtPageSet set[1];
uint len, slot;
int ret = -1;
BtKey *ptr;
BtVal *val;

  if( slot = bt_loadpage (bt->mgr, set, key, keylen, 0, BtLockRead, bt->thread_no) )
   do {
	ptr = keyptr(set->page, slot);

	//	skip librarian slot place holder

	if( slotptr(set->page, slot)->type == Librarian )
		ptr = keyptr(set->page, ++slot);

	//	return actual key found

	memcpy (bt->key, ptr, ptr->len + sizeof(BtKey));
	len = ptr->len;

	if( slotptr(set->page, slot)->type == Duplicate )
		len -= BtId;

	//	not there if we reach the stopper key

	if( slot == set->page->cnt )
	  if( !bt_getid (set->page->right) )
		break;

	// if key exists, return >= 0 value bytes copied
	//	otherwise return (-1)

	if( slotptr(set->page, slot)->dead )
		continue;

	if( keylen == len )
	  if( !memcmp (ptr->key, key, len) ) {
		val = valptr (set->page,slot);
		if( valmax > val->len )
			valmax = val->len;
		memcpy (value, val->value, valmax);
		ret = valmax;
	  }

	break;

   } while( slot = bt_findnext (bt, set, slot) );

  bt_unlockpage (BtLockRead, set->latch);
  bt_unpinlatch (bt->mgr, set->latch);
  return ret;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0  new slot value

uint bt_cleanpage(BtMgr *mgr, BtPageSet *set, uint keylen, uint slot, uint vallen)
{
BtPage page = set->page, frame;
uint nxt = mgr->page_size;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey *key;
BtVal *val;

	if( page->min >= (max+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal))
		return slot;

	//	skip cleanup and proceed to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < nxt / 5 )
		return 0;

	frame = malloc (mgr->page_size);
	memcpy (frame, page, mgr->page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, mgr->page_size - sizeof(*page));
	set->latch->dirty = 1;

	page->garbage = 0;
	page->act = 0;

	// clean up page first by
	// removing deleted keys

	while( cnt++ < max ) {
		if( cnt == slot )
			newslot = idx + 2;

		if( cnt < max || frame->lvl )
		  if( slotptr(frame,cnt)->dead )
			continue;

		// copy the value across

		val = valptr(frame, cnt);
		nxt -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)page + nxt, val, val->len + sizeof(BtVal));

		// copy the key across

		key = keyptr(frame, cnt);
		nxt -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)page + nxt, key, key->len + sizeof(BtKey));

		// make a librarian slot

		slotptr(page, ++idx)->off = nxt;
		slotptr(page, idx)->type = Librarian;
		slotptr(page, idx)->dead = 1;

		// set up the slot

		slotptr(page, ++idx)->off = nxt;
		slotptr(page, idx)->type = slotptr(frame, cnt)->type;

		if( !(slotptr(page, idx)->dead = slotptr(frame, cnt)->dead) )
			page->act++;
	}

	page->min = nxt;
	page->cnt = idx;
	free (frame);

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal) )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtMgr *mgr, BtPageSet *root, BtLatchSet *right, ushort page_no)
{  
unsigned char leftkey[BT_keyarray];
uint nxt = mgr->page_size;
unsigned char value[BtId];
BtPageSet left[1];
uid left_page_no;
BtKey *ptr;
BtVal *val;

	//	save left page fence key for new root

	ptr = keyptr(root->page, root->page->cnt);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( bt_newpage(mgr, left, root->page, page_no) )
		return mgr->err;

	left_page_no = left->latch->page_no;
	bt_unpinlatch (mgr, left->latch);

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, mgr->page_size - sizeof(*root->page));

	// insert stopper key at top of newroot page
	// and increase the root height

	nxt -= BtId + sizeof(BtVal);
	bt_putid (value, right->page_no);
	val = (BtVal *)((unsigned char *)root->page + nxt);
	memcpy (val->value, value, BtId);
	val->len = BtId;

	nxt -= 2 + sizeof(BtKey);
	slotptr(root->page, 2)->off = nxt;
	ptr = (BtKey *)((unsigned char *)root->page + nxt);
	ptr->len = 2;
	ptr->key[0] = 0xff;
	ptr->key[1] = 0xff;

	// insert lower keys page fence key on newroot page as first key

	nxt -= BtId + sizeof(BtVal);
	bt_putid (value, left_page_no);
	val = (BtVal *)((unsigned char *)root->page + nxt);
	memcpy (val->value, value, BtId);
	val->len = BtId;

	ptr = (BtKey *)leftkey;
	nxt -= ptr->len + sizeof(BtKey);
	slotptr(root->page, 1)->off = nxt;
	memcpy ((unsigned char *)root->page + nxt, leftkey, ptr->len + sizeof(BtKey));
	
	bt_putid(root->page->right, 0);
	root->page->min = nxt;		// reset lowest used offset and key count
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	mgr->pagezero->alloc->lvl = root->page->lvl;

	// release and unpin root pages

	bt_unlockpage(BtLockWrite, root->latch);
	bt_unpinlatch (mgr, root->latch);

	bt_unpinlatch (mgr, right);
	return 0;
}

//  split already locked full node
//	leave it locked.
//	return pool entry for new right
//	page, unlocked

uint bt_splitpage (BtMgr *mgr, BtPageSet *set, ushort thread_no)
{
uint cnt = 0, idx = 0, max, nxt = mgr->page_size;
BtPage frame = malloc (mgr->page_size);
uint lvl = set->page->lvl;
BtPageSet right[1];
BtKey *key, *ptr;
BtVal *val, *src;
uid right2;
uint prev;

	//  split higher half of keys to frame

	memset (frame, 0, mgr->page_size);
	max = set->page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		if( cnt < max || set->page->lvl )
		  if( slotptr(set->page, cnt)->dead )
			continue;

		src = valptr(set->page, cnt);
		nxt -= src->len + sizeof(BtVal);
		memcpy ((unsigned char *)frame + nxt, src, src->len + sizeof(BtVal));

		key = keyptr(set->page, cnt);
		nxt -= key->len + sizeof(BtKey);
		ptr = (BtKey*)((unsigned char *)frame + nxt);
		memcpy (ptr, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(frame, ++idx)->off = nxt;
		slotptr(frame, idx)->type = Librarian;
		slotptr(frame, idx)->dead = 1;

		//  add actual slot

		slotptr(frame, ++idx)->off = nxt;
		slotptr(frame, idx)->type = slotptr(set->page, cnt)->type;

		if( !(slotptr(frame, idx)->dead = slotptr(set->page, cnt)->dead) )
			frame->act++;
	}

	frame->bits = mgr->page_bits;
	frame->min = nxt;
	frame->cnt = idx;
	frame->lvl = lvl;

	// link right node

	if( set->latch->page_no > ROOT_page )
		bt_putid (frame->right, bt_getid (set->page->right));

	//	get new free page and write higher keys to it.

	if( bt_newpage(mgr, right, frame, thread_no) )
		return 0;

	// process lower keys

	memcpy (frame, set->page, mgr->page_size);
	memset (set->page+1, 0, mgr->page_size - sizeof(*set->page));
	set->latch->dirty = 1;

	nxt = mgr->page_size;
	set->page->garbage = 0;
	set->page->act = 0;
	max /= 2;
	cnt = 0;
	idx = 0;

	if( slotptr(frame, max)->type == Librarian )
		max--;

	//  assemble page of smaller keys

	while( cnt++ < max ) {
		if( slotptr(frame, cnt)->dead )
			continue;
		val = valptr(frame, cnt);
		nxt -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)set->page + nxt, val, val->len + sizeof(BtVal));

		key = keyptr(frame, cnt);
		nxt -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)set->page + nxt, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(set->page, ++idx)->off = nxt;
		slotptr(set->page, idx)->type = Librarian;
		slotptr(set->page, idx)->dead = 1;

		//	add actual slot

		slotptr(set->page, ++idx)->off = nxt;
		slotptr(set->page, idx)->type = slotptr(frame, cnt)->type;
		set->page->act++;
	}

	bt_putid(set->page->right, right->latch->page_no);
	set->page->min = nxt;
	set->page->cnt = idx;

	return right->latch->entry;
}

//	fix keys for newly split page
//	call with page locked, return
//	unlocked

BTERR bt_splitkeys (BtMgr *mgr, BtPageSet *set, BtLatchSet *right, ushort thread_no)
{
unsigned char leftkey[BT_keyarray], rightkey[BT_keyarray];
unsigned char value[BtId];
uint lvl = set->page->lvl;
BtPage page;
BtKey *ptr;

	// if current page is the root page, split it

	if( set->latch->page_no == ROOT_page )
		return bt_splitroot (mgr, set, right, thread_no);

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	page = bt_mappage (mgr, right);

	ptr = keyptr(page, page->cnt);
	memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));

	// insert new fences in their parent pages

	bt_lockpage (BtLockParent, right, thread_no);

	bt_lockpage (BtLockParent, set->latch, thread_no);
	bt_unlockpage (BtLockWrite, set->latch);

	// insert new fence for reformulated left block of smaller keys

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey *)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
		return mgr->err;

	// switch fence for right block of larger keys to new right page

	bt_putid (value, right->page_no);
	ptr = (BtKey *)rightkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
		return mgr->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch (mgr, set->latch);

	bt_unlockpage (BtLockParent, right);
	bt_unpinlatch (mgr, right);
	return 0;
}

//	install new key and value onto page
//	page must already be checked for
//	adequate space

BTERR bt_insertslot (BtMgr *mgr, BtPageSet *set, uint slot, unsigned char *key,uint keylen, unsigned char *value, uint vallen, uint type, uint release)
{
uint idx, librarian;
BtSlot *node;
BtKey *ptr;
BtVal *val;

	//	if found slot > desired slot and previous slot
	//	is a librarian slot, use it

	if( slot > 1 )
	  if( slotptr(set->page, slot-1)->type == Librarian )
		slot--;

	// copy value onto page

	set->page->min -= vallen + sizeof(BtVal);
	val = (BtVal*)((unsigned char *)set->page + set->page->min);
	memcpy (val->value, value, vallen);
	val->len = vallen;

	// copy key onto page

	set->page->min -= keylen + sizeof(BtKey);
	ptr = (BtKey*)((unsigned char *)set->page + set->page->min);
	memcpy (ptr->key, key, keylen);
	ptr->len = keylen;
	
	//	find first empty slot

	for( idx = slot; idx < set->page->cnt; idx++ )
	  if( slotptr(set->page, idx)->dead )
		break;

	// now insert key into array before slot

	if( idx == set->page->cnt )
		idx += 2, set->page->cnt += 2, librarian = 2;
	else
		librarian = 1;

	set->latch->dirty = 1;
	set->page->act++;

	while( idx > slot + librarian - 1 )
		*slotptr(set->page, idx) = *slotptr(set->page, idx - librarian), idx--;

	//	add librarian slot

	if( librarian > 1 ) {
		node = slotptr(set->page, slot++);
		node->off = set->page->min;
		node->type = Librarian;
		node->dead = 1;
	}

	//	fill in new slot

	node = slotptr(set->page, slot);
	node->off = set->page->min;
	node->type = type;
	node->dead = 0;

	if( release ) {
		bt_unlockpage (BtLockWrite, set->latch);
		bt_unpinlatch (mgr, set->latch);
	}

	return 0;
}

//  Insert new key into the btree at given level.
//	either add a new key or update/add an existing one

BTERR bt_insertkey (BtMgr *mgr, unsigned char *key, uint keylen, uint lvl, void *value, uint vallen, BtSlotType type, ushort thread_no)
{
uint slot, idx, len, entry;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

  while( 1 ) { // find the page and slot for the current key
	if( slot = bt_loadpage (mgr, set, key, keylen, lvl, BtLockWrite, thread_no) ) {
		node = slotptr(set->page, slot);
		ptr = keyptr(set->page, slot);
	} else {
		if( !mgr->err )
			mgr->line = __LINE__, mgr->err = BTERR_ovflw;
		return mgr->err;
	}

	// if librarian slot == found slot, advance to real slot

	if( node->type == Librarian )
	  if( !keycmp (ptr, key, keylen) ) {
		ptr = keyptr(set->page, ++slot);
		node = slotptr(set->page, slot);
	  }

	//  if inserting a duplicate key or unique
	//	key that doesn't exist on the page,
	//	check for adequate space on the page
	//	and insert the new key before slot.

	switch( type ) {
	case Unique:
	case Duplicate:
	  if( keycmp (ptr, key, keylen) )
	   if( slot = bt_cleanpage (mgr, set, keylen, slot, vallen) )
	    return bt_insertslot (mgr, set, slot, key, keylen, value, vallen, type, 1);
	   else if( !(entry = bt_splitpage (mgr, set, thread_no)) )
		return mgr->err;
	   else if( bt_splitkeys (mgr, set, mgr->latchsets + entry, thread_no) )
		return mgr->err;
	   else
		continue;

	  // if key already exists, update value and return

	  val = valptr(set->page, slot);

	  if( val->len >= vallen ) {
		if( slotptr(set->page, slot)->dead )
			set->page->act++;
		node->type = type;
		node->dead = 0;

		set->page->garbage += val->len - vallen;
		set->latch->dirty = 1;
		val->len = vallen;
		memcpy (val->value, value, vallen);
		bt_unlockpage(BtLockWrite, set->latch);
		bt_unpinlatch (mgr, set->latch);
		return 0;
	  }

	  //  new update value doesn't fit in existing value area
	  //	make sure page has room

	  if( !node->dead )
		set->page->garbage += val->len + ptr->len + sizeof(BtKey) + sizeof(BtVal);
	  else
		set->page->act++;

	  node->type = type;
	  node->dead = 0;

	  if( !(slot = bt_cleanpage (mgr, set, keylen, slot, vallen)) )
	   if( !(entry = bt_splitpage (mgr, set, thread_no)) )
		return mgr->err;
	   else if( bt_splitkeys (mgr, set, mgr->latchsets + entry, thread_no) )
		return mgr->err;
	   else
		continue;

	  //  copy key and value onto page and update slot

	  set->page->min -= vallen + sizeof(BtVal);
	  val = (BtVal*)((unsigned char *)set->page + set->page->min);
	  memcpy (val->value, value, vallen);
	  val->len = vallen;

	  set->latch->dirty = 1;
	  set->page->min -= keylen + sizeof(BtKey);
	  ptr = (BtKey*)((unsigned char *)set->page + set->page->min);
	  memcpy (ptr->key, key, keylen);
	  ptr->len = keylen;
	
	  node->off = set->page->min;
	  bt_unlockpage(BtLockWrite, set->latch);
	  bt_unpinlatch (mgr, set->latch);
	  return 0;
    }
  }
  return 0;
}

typedef struct {
	logseqno reqlsn;	// redo log seq no required
	logseqno lsn;		// redo log sequence number
	uint entry;			// latch table entry number
	uint slot:31;		// page slot number
	uint reuse:1;		// reused previous page
} AtomicTxn;

typedef struct {
	uid page_no;		// page number for split leaf
	void *next;			// next key to insert
	uint entry:29;		// latch table entry number
	uint type:2;		// 0 == insert, 1 == delete, 2 == free
	uint nounlock:1;	// don't unlock ParentModification
	unsigned char leafkey[BT_keyarray];
} AtomicKey;

//	determine actual page where key is located
//  return slot number

uint bt_atomicpage (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint src, BtPageSet *set)
{
BtKey *key = keyptr(source,src);
uint slot = locks[src].slot;
uint entry;

	if( src > 1 && locks[src].reuse )
	  entry = locks[src-1].entry, slot = 0;
	else
	  entry = locks[src].entry;

	if( slot ) {
		set->latch = mgr->latchsets + entry;
		set->page = bt_mappage (mgr, set->latch);
		return slot;
	}

	//	is locks->reuse set? or was slot zeroed?
	//	if so, find where our key is located 
	//	on current page or pages split on
	//	same page txn operations.

	do {
		set->latch = mgr->latchsets + entry;
		set->page = bt_mappage (mgr, set->latch);

		if( slot = bt_findslot(set->page, key->key, key->len) ) {
		  if( slotptr(set->page, slot)->type == Librarian )
			slot++;
		  if( locks[src].reuse )
			locks[src].entry = entry;
		  return slot;
		}
	} while( entry = set->latch->split );

	mgr->line = __LINE__, mgr->err = BTERR_atomic;
	return 0;
}

BTERR bt_atomicinsert (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint src, ushort thread_no)
{
BtKey *key = keyptr(source, src);
BtVal *val = valptr(source, src);
BtLatchSet *latch;
BtPageSet set[1];
uint entry, slot;

  while( slot = bt_atomicpage (mgr, source, locks, src, set) ) {
	if( slot = bt_cleanpage(mgr, set, key->len, slot, val->len) ) {
	  if( bt_insertslot (mgr, set, slot, key->key, key->len, val->value, val->len, slotptr(source,src)->type, 0) )
		return mgr->err;
	  set->page->lsn = locks[src].lsn;
	  return 0;
	}

	if( entry = bt_splitpage (mgr, set, thread_no) )
	  latch = mgr->latchsets + entry;
	else
	  return mgr->err;

	//	splice right page into split chain
	//	and WriteLock it.

	bt_lockpage(BtLockWrite, latch, thread_no);
	latch->split = set->latch->split;
	set->latch->split = entry;
	locks[src].slot = 0;
  }

  return mgr->line = __LINE__, mgr->err = BTERR_atomic;
}

//	perform delete from smaller btree
//  insert a delete slot if not found there

BTERR bt_atomicdelete (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint src, ushort thread_no)
{
BtKey *key = keyptr(source, src);
BtPageSet set[1];
uint idx, slot;
BtSlot *node;
BtKey *ptr;
BtVal *val;

	if( slot = bt_atomicpage (mgr, source, locks, src, set) ) {
	  node = slotptr(set->page, slot);
	  ptr = keyptr(set->page, slot);
	  val = valptr(set->page, slot);
	} else
	  return mgr->line = __LINE__, mgr->err = BTERR_struct;

	//  if slot is not found, insert a delete slot

	if( keycmp (ptr, key->key, key->len) )
	  return bt_insertslot (mgr, set, slot, key->key, key->len, NULL, 0, Delete, 0);

	//	if node is already dead,
	//	ignore the request.

	if( node->dead )
		return 0;

	set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
	set->latch->dirty = 1;
	set->page->lsn = locks[src].lsn;
 	set->page->act--;

	node->dead = 0;
	mgr->found++;
	return 0;
}

//	delete an empty master page for a transaction

//	note that the far right page never empties because
//	it always contains (at least) the infinite stopper key
//	and that all pages that don't contain any keys are
//	deleted, or are being held under Atomic lock.

BTERR bt_atomicfree (BtMgr *mgr, BtPageSet *prev, ushort thread_no)
{
BtPageSet right[1], temp[1];
unsigned char value[BtId];
uid right_page_no;
BtKey *ptr;

	bt_lockpage(BtLockWrite, prev->latch, thread_no);

	//	grab the right sibling

	if( right->latch = bt_pinlatch(mgr, bt_getid (prev->page->right), NULL, thread_no) )
		right->page = bt_mappage (mgr, right->latch);
	else
		return mgr->err;

	bt_lockpage(BtLockAtomic, right->latch, thread_no);
	bt_lockpage(BtLockWrite, right->latch, thread_no);

	//	and pull contents over empty page
	//	while preserving master's left link

	memcpy (right->page->left, prev->page->left, BtId);
	memcpy (prev->page, right->page, mgr->page_size);

	//	forward seekers to old right sibling
	//	to new page location in set

	bt_putid (right->page->right, prev->latch->page_no);
	right->latch->dirty = 1;
	right->page->kill = 1;

	//	remove pointer to right page for searchers by
	//	changing right fence key to point to the master page

	ptr = keyptr(right->page,right->page->cnt);
	bt_putid (value, prev->latch->page_no);

	if( bt_insertkey (mgr, ptr->key, ptr->len, 1, value, BtId, Unique, thread_no) )
		return mgr->err;

	//  now that master page is in good shape we can
	//	remove its locks.

	bt_unlockpage (BtLockAtomic, prev->latch);
	bt_unlockpage (BtLockWrite, prev->latch);

	//  fix master's right sibling's left pointer
	//	to remove scanner's poiner to the right page

	if( right_page_no = bt_getid (prev->page->right) ) {
	  if( temp->latch = bt_pinlatch (mgr, right_page_no, NULL, thread_no) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return mgr->err;

	  bt_lockpage (BtLockWrite, temp->latch, thread_no);
	  bt_putid (temp->page->left, prev->latch->page_no);
	  temp->latch->dirty = 1;

	  bt_unlockpage (BtLockWrite, temp->latch);
	  bt_unpinlatch (mgr, temp->latch);
	} else {	// master is now the far right page
	  bt_mutexlock (mgr->lock);
	  bt_putid (mgr->pagezero->alloc->left, prev->latch->page_no);
	  bt_releasemutex(mgr->lock);
	}

	//	now that there are no pointers to the right page
	//	we can delete it after the last read access occurs

	bt_unlockpage (BtLockWrite, right->latch);
	bt_unlockpage (BtLockAtomic, right->latch);
	bt_lockpage (BtLockDelete, right->latch, thread_no);
	bt_lockpage (BtLockWrite, right->latch, thread_no);
	bt_freepage (mgr, right);
	return 0;
}

//	atomic modification of a batch of keys.

//	return -1 if BTERR is set
//	otherwise return slot number
//	causing the key constraint violation
//	or zero on successful completion.

BTERR bt_atomictxn (BtDb *bt, BtPage source)
{
uint src, idx, slot, samepage, entry, que = 0;
AtomicKey *head, *tail, *leaf;
BtPageSet set[1], prev[1];
unsigned char value[BtId];
BtKey *key, *ptr, *key2;
BtLatchSet *latch;
AtomicTxn *locks;
int result = 0;
BtSlot temp[1];
logseqno lsn;
BtPage page;
BtVal *val;
uid right;
int type;

  locks = calloc (source->cnt + 1, sizeof(AtomicTxn));
  head = NULL;
  tail = NULL;

  // stable sort the list of keys into order to
  //	prevent deadlocks between threads.

  for( src = 1; src++ < source->cnt; ) {
	*temp = *slotptr(source,src);
	key = keyptr (source,src);

	for( idx = src; --idx; ) {
	  key2 = keyptr (source,idx);
	  if( keycmp (key, key2->key, key2->len) < 0 ) {
		*slotptr(source,idx+1) = *slotptr(source,idx);
		*slotptr(source,idx) = *temp;
	  } else
		break;
	}
  }

  //  add entries to redo log

  if( bt->mgr->redopages )
   if( lsn = bt_txnredo (bt->mgr, source, bt->thread_no) )
    for( src = 0; src++ < source->cnt; )
	 locks[src].lsn = lsn;
    else
  	 return bt->mgr->err;

  // Load the leaf page for each key
  // group same page references with reuse bit
  // and determine any constraint violations

  for( src = 0; src++ < source->cnt; ) {
	key = keyptr(source, src);
	slot = 0;

	// first determine if this modification falls
	// on the same page as the previous modification
	//	note that the far right leaf page is a special case

	if( samepage = src > 1 )
	  if( samepage = !bt_getid(set->page->right) || keycmp (keyptr(set->page, set->page->cnt), key->key, key->len) >= 0 )
		slot = bt_findslot(set->page, key->key, key->len);
	  else
	 	bt_unlockpage(BtLockRead, set->latch); 

	if( !slot )
	  if( slot = bt_loadpage(bt->mgr, set, key->key, key->len, 0, BtLockRead | BtLockAtomic, bt->thread_no) )
		set->latch->split = 0;
	  else
  	 	return bt->mgr->err;

	if( slotptr(set->page, slot)->type == Librarian )
	  ptr = keyptr(set->page, ++slot);
	else
	  ptr = keyptr(set->page, slot);

	if( !samepage ) {
	  locks[src].entry = set->latch->entry;
	  locks[src].slot = slot;
	  locks[src].reuse = 0;
	} else {
	  locks[src].entry = 0;
	  locks[src].slot = 0;
	  locks[src].reuse = 1;
	}

	//	capture current lsn for master page

	locks[src].reqlsn = set->page->lsn;
  }

  //  unlock last loadpage lock

  if( source->cnt )
	bt_unlockpage(BtLockRead, set->latch);

  //  obtain write lock for each master page
  //  sync flushed pages to disk

  for( src = 0; src++ < source->cnt; ) {
	if( locks[src].reuse )
	  continue;

	set->latch = bt->mgr->latchsets + locks[src].entry;
	bt_lockpage (BtLockWrite, set->latch, bt->thread_no);
  }

  // insert or delete each key
  // process any splits or merges
  // release Write & Atomic latches
  // set ParentModifications and build
  // queue of keys to insert for split pages
  // or delete for deleted pages.

  // run through txn list backwards

  samepage = source->cnt + 1;

  for( src = source->cnt; src; src-- ) {
	if( locks[src].reuse )
	  continue;

	//  perform the txn operations
	//	from smaller to larger on
	//  the same page

	for( idx = src; idx < samepage; idx++ )
	 switch( slotptr(source,idx)->type ) {
	 case Delete:
	  if( bt_atomicdelete (bt->mgr, source, locks, idx, bt->thread_no) )
  	 	return bt->mgr->err;
	  break;

	case Duplicate:
	case Unique:
	  if( bt_atomicinsert (bt->mgr, source, locks, idx, bt->thread_no) )
  	 	return bt->mgr->err;
	  break;
	}

	//	after the same page operations have finished,
	//  process master page for splits or deletion.

	latch = prev->latch = bt->mgr->latchsets + locks[src].entry;
	prev->page = bt_mappage (bt->mgr, prev->latch);
	samepage = src;

	//  pick-up all splits from master page
	//	each one is already WriteLocked.

	entry = prev->latch->split;

	while( entry ) {
	  set->latch = bt->mgr->latchsets + entry;
	  set->page = bt_mappage (bt->mgr, set->latch);
	  entry = set->latch->split;

	  // delete empty master page by undoing its split
	  //  (this is potentially another empty page)
	  //  note that there are no new left pointers yet

	  if( !prev->page->act ) {
		memcpy (set->page->left, prev->page->left, BtId);
		memcpy (prev->page, set->page, bt->mgr->page_size);
		bt_lockpage (BtLockDelete, set->latch, bt->thread_no);
		bt_freepage (bt->mgr, set);

		prev->latch->split = set->latch->split;
		prev->latch->dirty = 1;
		continue;
	  }

	  // remove empty page from the split chain
	  // and return it to the free list.

	  if( !set->page->act ) {
		memcpy (prev->page->right, set->page->right, BtId);
		prev->latch->split = set->latch->split;
		bt_lockpage (BtLockDelete, set->latch, bt->thread_no);
		bt_freepage (bt->mgr, set);
		continue;
	  }

	  //  schedule prev fence key update

	  ptr = keyptr(prev->page,prev->page->cnt);
	  leaf = calloc (sizeof(AtomicKey), 1), que++;

	  memcpy (leaf->leafkey, ptr, ptr->len + sizeof(BtKey));
	  leaf->page_no = prev->latch->page_no;
	  leaf->entry = prev->latch->entry;
	  leaf->type = 0;

	  if( tail )
		tail->next = leaf;
	  else
		head = leaf;

	  tail = leaf;

	  // splice in the left link into the split page

	  bt_putid (set->page->left, prev->latch->page_no);
	  bt_lockpage(BtLockParent, prev->latch, bt->thread_no);
	  bt_unlockpage(BtLockWrite, prev->latch);
	  *prev = *set;
	}

	//  update left pointer in next right page from last split page
	//	(if all splits were reversed, latch->split == 0)

	if( latch->split ) {
	  //  fix left pointer in master's original (now split)
	  //  far right sibling or set rightmost page in page zero

	  if( right = bt_getid (prev->page->right) ) {
		if( set->latch = bt_pinlatch (bt->mgr, right, NULL, bt->thread_no) )
	  	  set->page = bt_mappage (bt->mgr, set->latch);
	 	else
  	 	  return bt->mgr->err;

	    bt_lockpage (BtLockWrite, set->latch, bt->thread_no);
	    bt_putid (set->page->left, prev->latch->page_no);
		set->latch->dirty = 1;
	    bt_unlockpage (BtLockWrite, set->latch);
		bt_unpinlatch (bt->mgr, set->latch);
	  } else {	// prev is rightmost page
	    bt_mutexlock (bt->mgr->lock);
		bt_putid (bt->mgr->pagezero->alloc->left, prev->latch->page_no);
	    bt_releasemutex(bt->mgr->lock);
	  }

	  //  Process last page split in chain

	  ptr = keyptr(prev->page,prev->page->cnt);
	  leaf = calloc (sizeof(AtomicKey), 1), que++;

	  memcpy (leaf->leafkey, ptr, ptr->len + sizeof(BtKey));
	  leaf->page_no = prev->latch->page_no;
	  leaf->entry = prev->latch->entry;
	  leaf->type = 0;
  
	  if( tail )
		tail->next = leaf;
	  else
		head = leaf;

	  tail = leaf;

	  bt_lockpage(BtLockParent, prev->latch, bt->thread_no);
	  bt_unlockpage(BtLockWrite, prev->latch);

	  //  remove atomic lock on master page

	  bt_unlockpage(BtLockAtomic, latch);
	  continue;
	}

	//  finished if prev page occupied (either master or final split)

	if( prev->page->act ) {
	  bt_unlockpage(BtLockWrite, latch);
	  bt_unlockpage(BtLockAtomic, latch);
	  bt_unpinlatch(bt->mgr, latch);
	  continue;
	}

	// any and all splits were reversed, and the
	// master page located in prev is empty, delete it
	// by pulling over master's right sibling.

	// Remove empty master's fence key

	ptr = keyptr(prev->page,prev->page->cnt);

	if( bt_deletekey (bt->mgr, ptr->key, ptr->len, 1, bt->thread_no) )
  	 	return bt->mgr->err;

	//	perform the remainder of the delete
	//	from the FIFO queue

	leaf = calloc (sizeof(AtomicKey), 1), que++;

	memcpy (leaf->leafkey, ptr, ptr->len + sizeof(BtKey));
	leaf->page_no = prev->latch->page_no;
	leaf->entry = prev->latch->entry;
	leaf->nounlock = 1;
	leaf->type = 2;
  
	if( tail )
	  tail->next = leaf;
	else
	  head = leaf;

	tail = leaf;

	//	leave atomic lock in place until
	//	deletion completes in next phase.

	bt_unlockpage(BtLockWrite, prev->latch);
  }

  //  add & delete keys for any pages split or merged during transaction

  if( leaf = head )
    do {
	  set->latch = bt->mgr->latchsets + leaf->entry;
	  set->page = bt_mappage (bt->mgr, set->latch);

	  bt_putid (value, leaf->page_no);
	  ptr = (BtKey *)leaf->leafkey;

	  switch( leaf->type ) {
	  case 0:	// insert key
	    if( bt_insertkey (bt->mgr, ptr->key, ptr->len, 1, value, BtId, Unique, bt->thread_no) )
  	 	  return bt->mgr->err;

		break;

	  case 1:	// delete key
		if( bt_deletekey (bt->mgr, ptr->key, ptr->len, 1, bt->thread_no) )
  	 	  return bt->mgr->err;

		break;

	  case 2:	// free page
		if( bt_atomicfree (bt->mgr, set, bt->thread_no) )
  		  return bt->mgr->err;

		break;
	  }

	  if( !leaf->nounlock )
	    bt_unlockpage (BtLockParent, set->latch);

	  bt_unpinlatch (bt->mgr, set->latch);
	  tail = leaf->next;
	  free (leaf);
	} while( leaf = tail );

  // if number of active pages
  // is greater than the buffer pool
  // promote page into larger btree

  while( bt->mgr->pagezero->activepages > bt->mgr->latchtotal - 10 )
	if( bt_txnpromote (bt) )
	  return bt->mgr->err;

  // return success

  free (locks);
  return 0;
}

//  promote a page into the larger btree

BTERR bt_txnpromote (BtDb *bt)
{
uint entry, slot, idx;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

  while( 1 ) {
#ifdef unix
	entry = __sync_fetch_and_add(&bt->mgr->latchpromote, 1);
#else
	entry = _InterlockedIncrement (&bt->mgr->latchpromote) - 1;
#endif
	entry %= bt->mgr->latchtotal;

	if( !entry )
		continue;

	set->latch = bt->mgr->latchsets + entry;
	idx = set->latch->page_no % bt->mgr->latchhash;

	if( !bt_mutextry(set->latch->modify) )
		continue;

//    if( !bt_mutextry (bt->mgr->hashtable[idx].latch) ) {
//	  	bt_releasemutex(set->latch->modify);
//		continue;
//	}

	//  skip this entry if it is pinned

	if( set->latch->pin & ~CLOCK_bit ) {
	  bt_releasemutex(set->latch->modify);
//      bt_releasemutex(bt->mgr->hashtable[idx].latch);
	  continue;
	}

	set->page = bt_mappage (bt->mgr, set->latch);

	// entry never used or has no right sibling

	if( !set->latch->page_no || !bt_getid (set->page->right) ) {
	  bt_releasemutex(set->latch->modify);
//      bt_releasemutex(bt->mgr->hashtable[idx].latch);
	  continue;
	}

	bt_lockpage (BtLockAccess, set->latch, bt->thread_no);
	bt_lockpage (BtLockWrite, set->latch, bt->thread_no);
    bt_unlockpage(BtLockAccess, set->latch);

	// entry interiour node or being or was killed

	if( set->page->lvl || set->page->free || set->page->kill ) {
	  bt_releasemutex(set->latch->modify);
//      bt_releasemutex(bt->mgr->hashtable[idx].latch);
      bt_unlockpage(BtLockWrite, set->latch);
	  continue;
	}

	//  pin the page for our useage

	set->latch->pin++;
	bt_releasemutex(set->latch->modify);
//    bt_releasemutex(bt->mgr->hashtable[idx].latch);

	//	if page is dirty, then
	//	sync it to the disk first.

	if( set->latch->dirty )
	 if( bt->mgr->err = bt_writepage (bt->mgr, set->page, set->latch->page_no, 1) )
	  return bt->mgr->line = __LINE__, bt->mgr->err;
	 else
	  set->latch->dirty = 0;

	// transfer slots in our selected page to larger btree
if( !(set->latch->page_no % 100) )
fprintf(stderr, "Promote page %d, %d keys\n", set->latch->page_no, set->page->cnt);

	for( slot = 0; slot++ < set->page->cnt; ) {
		ptr = keyptr (set->page, slot);
		val = valptr (set->page, slot);
		node = slotptr(set->page, slot);

		switch( node->type ) {
		case Duplicate:
		case Unique:
		  if( bt_insertkey (bt->main, ptr->key, ptr->len, 0, val->value, val->len, node->type, bt->thread_no) )
			return bt->main->err;

		  continue;

		case Delete:
		  if( bt_deletekey (bt->main, ptr->key, ptr->len, 0, bt->thread_no) )
			return bt->main->err;

		  continue;
		}
	}

	//  now delete the page

	if( bt_deletepage (bt->mgr, set, bt->thread_no) )
		return bt->mgr->err;

	bt_unpinlatch (bt->mgr, set->latch);
	return 0;
  }
}

//	set cursor to highest slot on highest page

uint bt_lastkey (BtDb *bt)
{
uid page_no = bt_getid (bt->mgr->pagezero->alloc->left);
BtPageSet set[1];

	if( set->latch = bt_pinlatch (bt->mgr, page_no, NULL, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch, bt->thread_no);
	memcpy (bt->cursor, set->page, bt->mgr->page_size);
    bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (bt->mgr, set->latch);
	return bt->cursor->cnt;
}

//	return previous slot on cursor page

uint bt_prevkey (BtDb *bt, uint slot)
{
uid cursor_page = bt->cursor->page_no;
uid ourright, next, us = cursor_page;
BtPageSet set[1];

	if( --slot )
		return slot;

	ourright = bt_getid(bt->cursor->right);

goleft:
	if( !(next = bt_getid(bt->cursor->left)) )
		return 0;

findourself:
	cursor_page = next;

	if( set->latch = bt_pinlatch (bt->mgr, next, NULL, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch, bt->thread_no);
	memcpy (bt->cursor, set->page, bt->mgr->page_size);
	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (bt->mgr, set->latch);
	
	next = bt_getid (bt->cursor->right);

	if( bt->cursor->kill )
		goto findourself;

	if( next != us )
	  if( next == ourright )
		goto goleft;
	  else
		goto findourself;

	return bt->cursor->cnt;
}

//  return next slot on cursor page
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

	if( set->latch = bt_pinlatch (bt->mgr, right, NULL, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch, bt->thread_no);
	memcpy (bt->cursor, set->page, bt->mgr->page_size);
	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (bt->mgr, set->latch);
	slot = 0;

  } while( 1 );

  return bt->mgr->err = 0;
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint slot;

	// cache page for retrieval

	if( slot = bt_loadpage (bt->mgr, set, key, len, 0, BtLockRead, bt->thread_no) )
	  memcpy (bt->cursor, set->page, bt->mgr->page_size);
	else
	  return 0;

	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (bt->mgr, set->latch);
	return slot;
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

void bt_poolaudit (BtMgr *mgr)
{
BtLatchSet *latch;
uint entry = 0;

	while( ++entry < mgr->latchtotal ) {
		latch = mgr->latchsets + entry;

		if( *latch->readwr->rin & MASK )
			fprintf(stderr, "latchset %d rwlocked for page %d\n", entry, latch->page_no);

		if( *latch->access->rin & MASK )
			fprintf(stderr, "latchset %d accesslocked for page %d\n", entry, latch->page_no);

//		if( *latch->parent->xcl->value )
//			fprintf(stderr, "latchset %d parentlocked for page %d\n", entry, latch->page_no);

//		if( *latch->atomic->xcl->value )
//			fprintf(stderr, "latchset %d atomiclocked for page %d\n", entry, latch->page_no);

//		if( *latch->modify->value )
//			fprintf(stderr, "latchset %d modifylocked for page %d\n", entry, latch->page_no);

		if( latch->pin & ~CLOCK_bit )
			fprintf(stderr, "latchset %d pinned %d times for page %d\n", entry, latch->pin & ~CLOCK_bit, latch->page_no);
	}
}

typedef struct {
	char idx;
	char *type;
	char *infile;
	BtMgr *main;
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
int line = 0, found = 0, cnt = 0, idx;
uid next, page_no = LEAF_page;	// start on first page of leaves
int ch, len = 0, slot, type = 0;
unsigned char key[BT_maxkey];
unsigned char txn[65536];
ThreadArg *args = arg;
BtPage page, frame;
BtPageSet set[1];
uint nxt = 65536;
BtKey *ptr;
BtVal *val;
BtDb *bt;
FILE *in;

	bt = bt_open (args->mgr, args->main);
	page = (BtPage)txn;

	if( args->idx < strlen (args->type) )
		ch = args->type[args->idx];
	else
		ch = args->type[strlen(args->type) - 1];

	switch(ch | 0x20)
	{
	case 'd':
		type = Delete;

	case 'p':
		if( !type )
			type = Unique;

		if( args->num )
		 if( type == Delete )
		  fprintf(stderr, "started TXN pennysort delete for %s\n", args->infile);
		 else
		  fprintf(stderr, "started TXN pennysort insert for %s\n", args->infile);
		else
		 if( type == Delete )
		  fprintf(stderr, "started pennysort delete for %s\n", args->infile);
		 else
		  fprintf(stderr, "started pennysort insert for %s\n", args->infile);

		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( !args->num ) {
			    if( bt_insertkey (bt->mgr, key, 10, 0, key + 10, len - 10, Unique, bt->thread_no) )
				  fprintf(stderr, "Error %d Line: %d source: %d\n", bt->mgr->err, bt->mgr->line, line), exit(0);
			    len = 0;
				continue;
			  }

			  nxt -= len - 10;
			  memcpy (txn + nxt, key + 10, len - 10);
			  nxt -= 1;
			  txn[nxt] = len - 10;
			  nxt -= 10;
			  memcpy (txn + nxt, key, 10);
			  nxt -= 1;
			  txn[nxt] = 10;
			  slotptr(page,++cnt)->off  = nxt;
			  slotptr(page,cnt)->type = type;
			  len = 0;

			  if( cnt < args->num )
				continue;

			  page->cnt = cnt;
			  page->act = cnt;
			  page->min = nxt;

			  if( bt_atomictxn (bt, page) )
				fprintf(stderr, "Error %d Line: %d source: %d\n", bt->mgr->err, bt->mgr->line, line), exit(0);
			  nxt = sizeof(txn);
			  cnt = 0;

			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->infile);
		if( in = fopen (args->infile, "r") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( bt_insertkey (bt->mgr, key, len, 0, NULL, 0, Unique, bt->thread_no) )
				fprintf(stderr, "Error %d Line: %d source: %d\n", bt->mgr->err, bt->mgr->line, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( bt_findkey (bt, key, len, NULL, 0) == 0 )
				found++;
			  else if( bt->mgr->err )
				fprintf(stderr, "Error %d Syserr %d Line: %d source: %d\n", bt->mgr->err, errno, bt->mgr->line, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, found %d\n", args->infile, line, found);
		break;

	case 's':
		fprintf(stderr, "started scanning\n");
		
	  	do {
			if( set->latch = bt_pinlatch (bt->mgr, page_no, NULL, bt->thread_no) )
				set->page = bt_mappage (bt->mgr, set->latch);
			else
				fprintf(stderr, "unable to obtain latch"), exit(1);

			bt_lockpage (BtLockRead, set->latch, bt->thread_no);
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

			bt_unlockpage (BtLockRead, set->latch);
			bt_unpinlatch (bt->mgr, set->latch);
	  	} while( page_no = next );

		fprintf(stderr, " Total keys read %d\n", cnt);
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

		fprintf(stderr, " Total keys read %d\n", cnt);
		break;

	case 'c':
#ifdef unix
		posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
		fprintf(stderr, "started counting\n");
		next = LEAF_page + bt->mgr->redopages + 1;

		while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
			if( bt_readpage (bt->mgr, bt->cursor, page_no) )
				break;

			if( !bt->cursor->free && !bt->cursor->lvl )
				cnt += bt->cursor->act;

			bt->mgr->reads++;
			page_no = next++;
		}
		
	  	cnt--;	// remove stopper key
		fprintf(stderr, " Total keys counted %d\n", cnt);
		break;
	}

	fprintf(stderr, "%d reads %d writes %d found\n", bt->mgr->reads, bt->mgr->writes, bt->mgr->found);
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
uint recovery = 0;
uint mainpool = 0;
uint mainbits = 0;
float elapsed;
int num = 0;
char key[1];
BtMgr *main;
BtMgr *mgr;
BtKey *ptr;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file main_file cmds [page_bits buffer_pool_size txn_size recovery_pages main_bits main_pool src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where idx_file is the name of the cache btree file\n");
		fprintf (stderr, "  where main_file is the name of the main btree file\n");
		fprintf (stderr, "  cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort, with one character command for each input src_file. Commands with no input file need a placeholder.\n");
		fprintf (stderr, "  page_bits is the page size in bits for the cache btree\n");
		fprintf (stderr, "  buffer_pool_size is the number of pages in buffer pool for the cache btree\n");
		fprintf (stderr, "  txn_size = n to block transactions into n units, or zero for no transactions\n");
		fprintf (stderr, "  recovery_pages = n to implement recovery buffer with n pages, or zero for no recovery buffer\n");
		fprintf (stderr, "  main_bits is the page size of the main btree in bits\n");
		fprintf (stderr, "  main_pool is the number of main pages in the main buffer pool\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
		exit(0);
	}

	start = getCpuTime(0);

	if( argc > 4 )
		bits = atoi(argv[4]);

	if( argc > 5 )
		poolsize = atoi(argv[5]);

	if( !poolsize )
		fprintf (stderr, "Warning: no mapped_pool\n");

	if( argc > 6 )
		num = atoi(argv[6]);

	if( argc > 7 )
		recovery = atoi(argv[7]);

	if( argc > 8 )
		mainbits = atoi(argv[8]);

	if( argc > 9 )
		mainpool = atoi(argv[9]);

	cnt = argc - 10;
#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc ((cnt + 1) * sizeof(ThreadArg));

	mgr = bt_mgr (argv[1], bits, poolsize, recovery);

	if( !mgr ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	}

	main = bt_mgr (argv[2], mainbits, mainpool, 0);

	if( !main ) {
		fprintf(stderr, "Index Open Error %s\n", argv[2]);
		exit (1);
	}

	//	fire off threads

	if( cnt )
	  for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 10];
		args[idx].type = argv[3];
		args[idx].main = main;
		args[idx].mgr = mgr;
		args[idx].num = num;
		args[idx].idx = idx;
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
#else
		threads[idx] = (HANDLE)_beginthreadex(NULL, 131072, index_file, args + idx, 0, NULL);
#endif
	  }
	else {
		args[0].infile = argv[idx + 10];
		args[0].type = argv[3];
		args[0].main = main;
		args[0].mgr = mgr;
		args[0].num = num;
		args[0].idx = 0;
		index_file (args);
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
	bt_poolaudit(mgr);
	bt_poolaudit(main);

	bt_mgrclose (main);
	bt_mgrclose (mgr);

	elapsed = getCpuTime(0) - start;
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
}

#endif	//STANDALONE
