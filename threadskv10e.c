// btree version threadskv10e futex version
//	with reworked bt_deletekey code,
//	phase-fair re-entrant reader writer lock,
//	librarian page split code,
//	duplicate key management
//	bi-directional cursors
//	traditional buffer pool manager
//	ACID batched key-value updates
//	redo log for failure recovery
//	LSM B-trees for write optimization
//	and larger sized leaf pages than non-leaf

// 24 OCT 2014

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
#include <xmmintrin.h>
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

//	Number of levels to create in a new BTree

#define MIN_lvl			2

/*
There are six lock types for each node in four independent sets: 
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete. 
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent. 
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
5. (set 3) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
6. (set 4) LinkModification: Exclusive. Update of a node's left link is underway. Incompatible with another LinkModification. 
*/

typedef enum{
	BtLockAccess = 1,
	BtLockDelete = 2,
	BtLockRead   = 4,
	BtLockWrite  = 8,
	BtLockParent = 16,
	BtLockLink   = 32
} BtLock;

typedef struct {
  union {
	struct {
	  volatile unsigned char xcl[1];
	  volatile unsigned char filler;
	  volatile ushort waiters[1];
	} bits[1];
	uint value[1];
  };
} MutexLatch;

//	definition for reader/writer reentrant lock implementation

typedef struct {
  MutexLatch xcl[1];
  MutexLatch wrt[1];
  uint readers;
  ushort dup;		// re-entrant locks
  ushort tid;		// owner thread-no
  uint line;		// owner line #
} RWLock;

//  hash table entries

typedef struct {
	MutexLatch latch[1];
	uint entry;		// Latch table entry at head of chain
} BtHashEntry;

//	latch manager table structure

typedef struct {
	uid page_no;			// latch set page number
	MutexLatch modify[1];	// modify entry lite latch
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	RWLock parent[1];		// Posting of fence key in parent
	RWLock link[1];			// left link update in progress
	uint split;				// right split page atomic insert
	uint next;				// next entry in hash table chain
	uint prev;				// prev entry in hash table chain
	volatile ushort pin;	// number of accessing threads
	unsigned char dirty;	// page in cache is dirty
	unsigned char leaf;		// page in cache is a leaf
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
	Update,
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

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	uint garbage;				// page garbage in bytes
	unsigned char lvl;			// level of page
	unsigned char free;			// page is on free chain
	unsigned char kill;			// page is being deleted
	unsigned char nopromote;	// page is being constructed
	unsigned char filler1[6];	// padding to multiple of 8 bytes
	unsigned char right[BtId];	// page number to right
	unsigned char left[BtId];	// page number to left
	unsigned char filler2[2];	// padding to multiple of 8 bytes
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
	unsigned char freechain[BtId];	// head of free page_nos chain
	unsigned char leafchain[BtId];	// head of leaf page_nos chain
	unsigned long long leafpages;	// number of active leaf pages
	uint redopages;					// number of redo pages in file
	unsigned char leaf_xtra;		// leaf page size in xtra bits
	unsigned char page_bits;		// base page size in bits
} BtPageZero;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// base page size	
	uint page_bits;				// base page size in bits	
	uint leaf_xtra;				// leaf xtra bits	
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	BtPageZero *pagezero;		// mapped allocation page
	BtHashEntry *hashtable;		// the buffer pool hash table entries
	BtHashEntry *leaftable;		// the buffer pool hash table entries
	BtLatchSet *latchsets;		// mapped latch set from buffer pool
	BtLatchSet *leafsets;		// mapped latch set from buffer pool
	unsigned char *pagepool;	// mapped to the buffer pool pages
	unsigned char *leafpool;	// mapped to the leaf pool pages
	unsigned char *redobuff;	// mapped recovery buffer pointer
	logseqno lsn, flushlsn;		// current & first lsn flushed
	MutexLatch redo[1];			// redo area lite latch
	MutexLatch lock[1];			// allocation area lite latch
	MutexLatch maps[1];			// mapping segments lite latch
	ushort thread_no[1];		// highest thread number issued
	ushort err_thread;			// error thread number
	uint nlatchpage;			// size of buffer pool & latchsets
	uint latchtotal;			// number of page latch entries
	uint latchhash;				// number of latch hash table slots
	uint latchvictim;			// next latch entry to examine
	uint nleafpage;				// size of leaf pool & leafsets
	uint leaftotal;				// number of leaf latch entries
	uint leafhash;				// number of leaf hash table slots
	uint leafvictim;			// next leaf entry to examine
	uint leafpromote;			// next leaf entry to promote
	uint redopage;				// page number of redo buffer
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
	volatile uint segments;		// number of memory mapped segments
	unsigned char *pages[64000];// memory mapped segments of b-tree
} BtMgr;

typedef struct {
	BtMgr *mgr;					// buffer manager for entire process
	BtMgr *main;				// buffer manager for main btree
	BtPage cursor;				// cached page frame for start/next
	ushort thread_no;			// thread number
	unsigned char key[BT_keyarray];	// last found complete key
} BtDb;

//	atomic txn structures

typedef struct {
	logseqno reqlsn;	// redo log seq no required
	uint entry:31;		// latch table entry number
	uint reuse:1;		// reused previous page
} AtomicTxn;

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
extern BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no, uint leaf);
extern BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no, uint leaf);
extern void bt_lockpage(BtLock mode, BtLatchSet *latch, ushort thread_no, uint line);
extern void bt_unlockpage(BtLock mode, BtLatchSet *latch, ushort thread_no, uint line);
extern BTERR bt_insertkey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, void *value, uint vallen, BtSlotType type, ushort thread_no);
extern BTERR  bt_deletekey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, ushort thread_no);

extern int bt_findkey (BtDb *db, unsigned char *key, uint keylen, unsigned char *value, uint valmax);

extern uint bt_startkey (BtDb *db, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);
extern uint bt_prevkey (BtDb *db, uint slot);
extern uint bt_lastkey (BtDb *db);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint bits, uint leaf_xtra, uint poolsize, uint leafpool, uint redopages);
extern void bt_mgrclose (BtMgr *mgr);
extern logseqno bt_newredo (BtMgr *mgr, BTRM type, int lvl, BtKey *key, BtVal *val, ushort thread_no);
extern logseqno bt_txnredo (BtMgr *mgr, BtPage page, ushort thread_no);

//	atomic transaction functions
BTERR bt_atomicexec(BtMgr *mgr, BtPage source, logseqno lsn, int lsm, ushort thread_no);
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

//	lite weight spin lock Latch Manager

int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}

void bt_mutexlock(MutexLatch *latch)
{
uint idx, waited = 0;
MutexLatch prev[1];

 while( 1 ) {
  for( idx = 0; idx < 100; idx++ ) {
	*prev->value = __sync_fetch_and_or (latch->value, 1);
	if( !*prev->bits->xcl ) {
	  if( waited )
		__sync_fetch_and_sub (latch->bits->waiters, 1);
	  return;
	}
  }

  if( !waited ) {
	__sync_fetch_and_add (latch->bits->waiters, 1);
	*prev->bits->waiters += 1;
	waited++;
  }

  sys_futex (latch->value, FUTEX_WAIT_PRIVATE, *prev->value, NULL, NULL, 0);
 }
}

int bt_mutextry(MutexLatch *latch)
{
	return !__sync_lock_test_and_set (latch->bits->xcl, 1);
}

void bt_releasemutex(MutexLatch *latch)
{
	*latch->bits->xcl = 0;

	if( *latch->bits->waiters )
		sys_futex( latch->value, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0 );
}

//	reader/writer lock implementation

void WriteLock (RWLock *lock, ushort tid, uint line)
{
	if( lock->tid == tid ) {
		lock->dup++;
		return;
	}
	bt_mutexlock (lock->xcl);
	bt_mutexlock (lock->wrt);
	bt_releasemutex (lock->xcl);

	lock->line = line;
	lock->tid = tid;
}

void WriteRelease (RWLock *lock)
{
	if( lock->dup ) {
		lock->dup--;
		return;
	}
	lock->tid = 0;
	bt_releasemutex (lock->wrt);
}

void ReadLock (RWLock *lock, ushort tid)
{
	bt_mutexlock (lock->xcl);

	if( !__sync_fetch_and_add (&lock->readers, 1) )
		bt_mutexlock (lock->wrt);

	bt_releasemutex (lock->xcl);
}

void ReadRelease (RWLock *lock)
{
	if( __sync_fetch_and_sub (&lock->readers, 1) == 1 )
		bt_releasemutex (lock->wrt);
}

//	recovery manager -- flush dirty pages

void bt_flushlsn (BtMgr *mgr, ushort thread_no)
{
uint cnt3 = 0, cnt2 = 0, cnt = 0;
uint entry, segment;
BtLatchSet *latch;
BtPage page;

  //	flush dirty pool pages to the btree

fprintf(stderr, "Start flushlsn  ");
  for( entry = 1; entry < mgr->latchtotal; entry++ ) {
	page = (BtPage)(((uid)entry << mgr->page_bits) + mgr->pagepool);
	latch = mgr->latchsets + entry;
	bt_mutexlock (latch->modify);
	bt_lockpage(BtLockRead, latch, thread_no, __LINE__);

	if( latch->dirty ) {
	  bt_writepage(mgr, page, latch->page_no, 0);
	  latch->dirty = 0, cnt++;
	}
if( latch->pin & ~CLOCK_bit )
cnt2++;
	bt_unlockpage(BtLockRead, latch, thread_no, __LINE__);
	bt_releasemutex (latch->modify);
  }
  for( entry = 1; entry < mgr->leaftotal; entry++ ) {
	page = (BtPage)(((uid)entry << mgr->page_bits << mgr->leaf_xtra) + mgr->leafpool);
	latch = mgr->leafsets + entry;
	bt_mutexlock (latch->modify);
	bt_lockpage(BtLockRead, latch, thread_no, __LINE__);

	if( latch->dirty ) {
	  bt_writepage(mgr, page, latch->page_no, 1);
	  latch->dirty = 0, cnt++;
	}
if( latch->pin & ~CLOCK_bit )
cnt2++;
	bt_unlockpage(BtLockRead, latch, thread_no, __LINE__);
	bt_releasemutex (latch->modify);
  }
fprintf(stderr, "End flushlsn %d pages  %d pinned\n", cnt, cnt2);
fprintf(stderr, "begin sync");
	for( segment = 0; segment < mgr->segments; segment++ )
	  if( msync (mgr->pages[segment], (uid)65536 << mgr->page_bits, MS_SYNC) < 0 )
		fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
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
//	flush dirty pages to disk when it overflows.

logseqno bt_newredo (BtMgr *mgr, BTRM type, int lvl, BtKey *key, BtVal *val, ushort thread_no)
{
uint size = mgr->page_size * mgr->pagezero->redopages - sizeof(BtLogHdr);
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
	  if( msync (mgr->redobuff + (mgr->redolast & ~0xfff), mgr->redoend - (mgr->redolast & ~0xfff) + sizeof(BtLogHdr), MS_SYNC) < 0 )
		fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
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

	last = mgr->redolast & ~0xfff;
	end = mgr->redoend;

	if( end - last + sizeof(BtLogHdr) >= 32768 )
	  if( msync (mgr->redobuff + last, end - last + sizeof(BtLogHdr), MS_SYNC) < 0 )
		fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
	  else
		mgr->redolast = end;

	bt_releasemutex(mgr->redo);
	return hdr->lsn;
}

//	recovery manager -- append transaction to recovery log
//	flush dirty pages to disk when it overflows.

logseqno bt_txnredo (BtMgr *mgr, BtPage source, ushort thread_no)
{
uint size = mgr->page_size * mgr->pagezero->redopages - sizeof(BtLogHdr);
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
	  if( msync (mgr->redobuff + (mgr->redolast & ~0xfff), mgr->redoend - (mgr->redolast & ~0xfff) + sizeof(BtLogHdr), MS_SYNC) < 0 )
		fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
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

	last = mgr->redolast & ~0xfff;
	end = mgr->redoend;

	if( end - last + sizeof(BtLogHdr) >= 32768 )
	  if( msync (mgr->redobuff + last, end - last + sizeof(BtLogHdr), MS_SYNC) < 0 )
		fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
	  else
		mgr->redolast = end;

	bt_releasemutex(mgr->redo);
	return lsn;
}

//	sync a single btree page to disk

BTERR bt_syncpage (BtMgr *mgr, BtPage page, BtLatchSet *latch)
{
uint segment = latch->page_no >> 16;
uint page_size = mgr->page_size;
BtPage perm;

	if( bt_writepage (mgr, page, latch->page_no, latch->leaf) )
		return mgr->err;

	if( !page->lvl )
		page_size <<= mgr->leaf_xtra;

	perm = (BtPage)(mgr->pages[segment] + ((latch->page_no & 0xffff) << mgr->page_bits));

	if( msync (perm, page_size, MS_SYNC) < 0 )
	  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);

	latch->dirty = 0;
	return 0;
}

//	read page into buffer pool from permanent location in Btree file

BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no, uint leaf)
{
int flag = PROT_READ | PROT_WRITE;
uint page_size = mgr->page_size;
uint off = 0, segment, fragment;
unsigned char *perm;

  if( leaf )
	page_size <<= mgr->leaf_xtra;

  fragment = page_no & 0xffff;
  segment = page_no >> 16;
  mgr->reads++;

  while( off < page_size ) {
	if( fragment >> 16 )
		segment++, fragment = 0;

	if( segment < mgr->segments ) {
	  perm = mgr->pages[segment] + ((uid)fragment << mgr->page_bits);

	  memcpy ((unsigned char *)page + off, perm, mgr->page_size);
	  off += mgr->page_size;
	  fragment++;
	  continue;
	}

	bt_mutexlock (mgr->maps);

	if( segment < mgr->segments ) {
		bt_releasemutex (mgr->maps);
		continue;
	}

	mgr->pages[mgr->segments] = mmap (0, (uid)65536 << mgr->page_bits, flag, MAP_SHARED, mgr->idx, (uid)mgr->segments << (mgr->page_bits + 16));
	mgr->segments++;

	bt_releasemutex (mgr->maps);
  }

  return 0;
}

//	write page to permanent location in Btree file

BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no, uint leaf)
{
int flag = PROT_READ | PROT_WRITE;
uint page_size = mgr->page_size;
uint off = 0, segment, fragment;
unsigned char *perm;

  if( leaf )
	page_size <<= mgr->leaf_xtra;

  fragment = page_no & 0xffff;
  segment = page_no >> 16;
  mgr->writes++;

  while( off < page_size ) {
	if( fragment >> 16 )
		segment++, fragment = 0;

	if( segment < mgr->segments ) {
	  perm = mgr->pages[segment] + ((uid)fragment << mgr->page_bits);
	  memcpy (perm, (unsigned char *)page + off, mgr->page_size);
	  off += mgr->page_size;
	  fragment++;
	  continue;
	}

	bt_mutexlock (mgr->maps);

	if( segment < mgr->segments ) {
		bt_releasemutex (mgr->maps);
		continue;
	}

	mgr->pages[mgr->segments] = mmap (0, (uid)65536 << mgr->page_bits, flag, MAP_SHARED, mgr->idx, (uid)mgr->segments << (mgr->page_bits + 16));
	mgr->segments++;
	bt_releasemutex (mgr->maps);
  }

  return 0;
}

//	set CLOCK bit in latch
//	decrement pin count

int value;

void bt_unpinlatch (BtLatchSet *latch, ushort thread_no, uint line)
{
	bt_mutexlock(latch->modify);
	latch->pin |= CLOCK_bit;
	latch->pin--;

	bt_releasemutex(latch->modify);
}

//  return the btree cached page address

BtPage bt_mappage (BtMgr *mgr, BtLatchSet *latch)
{
uid entry = latch - (latch->leaf ? mgr->leafsets : mgr->latchsets);
BtPage page;

  if( latch->leaf )
	page = (BtPage)((entry << mgr->page_bits << mgr->leaf_xtra) + mgr->leafpool);
  else
	page = (BtPage)((entry << mgr->page_bits) + mgr->pagepool);

  return page;
}

//  return next available leaf entry
//	  and with latch entry locked

uint bt_availleaf (BtMgr *mgr)
{
BtLatchSet *latch;
uint entry;

  while( 1 ) {
#ifdef unix
	entry = __sync_fetch_and_add (&mgr->leafvictim, 1) + 1;
#else
	entry = _InterlockedIncrement (&mgr->leafvictim);
#endif
	entry %= mgr->leaftotal;

	if( !entry )
		continue;

	latch = mgr->leafsets + entry;

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

//	pin leaf in leaf buffer pool
//	return with latchset pinned

BtLatchSet *bt_pinleaf (BtMgr *mgr, uid page_no, ushort thread_no)
{
uint hashidx = page_no % mgr->leafhash;
BtLatchSet *latch;
uint entry, idx;
BtPage page;

  //  try to find our entry

  bt_mutexlock(mgr->leaftable[hashidx].latch);

  if( entry = mgr->leaftable[hashidx].entry )
   do {
	latch = mgr->leafsets + entry;

	if( page_no == latch->page_no )
		break;
   } while( entry = latch->next );

  //  found our entry: increment pin

  if( entry ) {
	latch = mgr->leafsets + entry;
	bt_mutexlock(latch->modify);
	latch->pin |= CLOCK_bit;
	latch->pin++;

	bt_releasemutex(latch->modify);
	bt_releasemutex(mgr->leaftable[hashidx].latch);
	return latch;
  }

  //  find and reuse unpinned entry

trynext:
  entry = bt_availleaf (mgr);
  latch = mgr->leafsets + entry;

  idx = latch->page_no % mgr->leafhash;

  //  if latch is on a different hash chain
  //	unlink from the old page_no chain

  if( latch->page_no )
   if( idx != hashidx ) {

	//  skip over this entry if latch not available

    if( !bt_mutextry (mgr->leaftable[idx].latch) ) {
	  bt_releasemutex(latch->modify);
	  goto trynext;
	}

	if( latch->prev )
	  mgr->leafsets[latch->prev].next = latch->next;
	else
	  mgr->leaftable[idx].entry = latch->next;

	if( latch->next )
	  mgr->leafsets[latch->next].prev = latch->prev;

    bt_releasemutex (mgr->leaftable[idx].latch);
   }

  page = (BtPage)(((uid)entry << mgr->page_bits << mgr->leaf_xtra) + mgr->leafpool);

  //  update permanent page area in btree from buffer pool
  //	no read-lock is required since page is not pinned.

  if( latch->dirty )
	if( mgr->err = bt_writepage (mgr, page, latch->page_no, 1) )
	  return mgr->line = __LINE__, mgr->err_thread = thread_no, NULL;
	else
	  latch->dirty = 0;

  if( bt_readpage (mgr, page, page_no, 1) )
	return mgr->line = __LINE__, NULL;

  //  link page as head of hash table chain
  //  if this is a never before used entry,
  //  or it was previously on a different
  //  hash table chain. Otherwise, just
  //  leave it in its current hash table
  //  chain position.

  if( !latch->page_no || hashidx != idx ) {
	if( latch->next = mgr->leaftable[hashidx].entry )
	  mgr->leafsets[latch->next].prev = entry;

	mgr->leaftable[hashidx].entry = entry;
    latch->prev = 0;
  }

  //  fill in latch structure

  latch->pin = CLOCK_bit | 1;
  latch->page_no = page_no;
  latch->leaf = 1;

  bt_releasemutex (latch->modify);
  bt_releasemutex (mgr->leaftable[hashidx].latch);
  return latch;
}

//	pin page in non-leaf buffer pool
//	return with latchset pinned

BtLatchSet *bt_pinlatch (BtMgr *mgr, uid page_no, ushort thread_no)
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
	  return mgr->line = __LINE__, mgr->err_thread = thread_no, NULL;
	else
	  latch->dirty = 0;

  if( bt_readpage (mgr, page, page_no, 0) )
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
  latch->leaf = 0;

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

	//	write remaining dirty leaf pages to the btree

	for( slot = 1; slot < mgr->leaftotal; slot++ ) {
		page = (BtPage)(((uid)slot << mgr->page_bits << mgr->leaf_xtra) + mgr->leafpool);
		latch = mgr->leafsets + slot;

		if( latch->dirty ) {
			bt_writepage(mgr, page, latch->page_no, 1);
			latch->dirty = 0, num++;
		}
	}

	//	clear redo recovery buffer on disk.

	if( mgr->pagezero->redopages ) {
		eof = (BtLogHdr *)mgr->redobuff;
		memset (eof, 0, sizeof(BtLogHdr));
		eof->lsn = mgr->lsn;
		if( msync (mgr->redobuff, 4096, MS_SYNC) < 0 )
		  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
	}

	fprintf(stderr, "%d buffer pool pages flushed\n", num);

#ifdef unix
	while( mgr->segments )
		munmap (mgr->pages[--mgr->segments], (uid)65536 << mgr->page_bits);

	munmap (mgr->pagepool, (uid)mgr->nlatchpage << mgr->page_bits);
	munmap (mgr->leafpool, (uid)mgr->nleafpage << mgr->page_bits);
	munmap (mgr->pagezero, mgr->page_size);
#else
	FlushViewOfFile(mgr->pagezero, 0);
	UnmapViewOfFile(mgr->pagezero);
	UnmapViewOfFile(mgr->pagepool);
	CloseHandle(mgr->halloc);
	CloseHandle(mgr->hpool);
#endif
#ifdef unix
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
//		size of page pool (e.g. 262144) and leaf pool

BtMgr *bt_mgr (char *name, uint pagebits, uint leafxtra, uint nodemax, uint leafmax, uint redopages)
{
uint lvl, attr, last, slot, idx;
uint nlatchpage, latchhash;
uint nleafpage, leafhash;
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

	if( leafxtra + pagebits > BT_maxbits )
		fprintf (stderr, "pagebits + leafxtra > maxbits\n"), exit(1);

	if( pagebits < BT_minbits )
		fprintf (stderr, "pagebits < minbits\n"), exit(1);

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
	//	check if page_bits == 0 on the disk.

	if( size = lseek (mgr->idx, 0L, 2) )
		if( pread(mgr->idx, pagezero, BT_minpage, 0) == BT_minpage )
			if( pagezero->page_bits ) {
				pagebits = pagezero->page_bits;
				leafxtra = pagezero->leaf_xtra;
			} else
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
		pagebits = pagezero->page_bits;
		leafxtra = pagezero->leaf_xtra;
	} else
		initit = 1;
#endif

	mgr->page_size = 1 << pagebits;
	mgr->page_bits = pagebits;
	mgr->leaf_xtra = leafxtra;

	//  calculate number of latch hash table entries

	mgr->nlatchpage = ((uid)nodemax/16 * sizeof(BtHashEntry) + mgr->page_size - 1) >> mgr->page_bits;

	mgr->nlatchpage += nodemax;		// size of the buffer pool in pages
	mgr->nlatchpage += (sizeof(BtLatchSet) * nodemax + mgr->page_size - 1) >> mgr->page_bits;
	mgr->latchtotal = nodemax;

	//  calculate number of leaf hash table entries

	mgr->nleafpage = ((uid)leafmax/16 * sizeof(BtHashEntry) + mgr->page_size - 1) >> mgr->page_bits;

	mgr->nleafpage += leafmax << leafxtra;		// size of the leaf pool in pages
	mgr->nleafpage += (sizeof(BtLatchSet) * leafmax + mgr->page_size - 1) >> mgr->page_bits;
	mgr->leaftotal = leafmax;

	mgr->redopage = LEAF_page + (1 << leafxtra);

	if( !initit )
		goto mgrlatch;

	// initialize an empty b-tree with latch page, root page, page of leaves
	// and page(s) of latches and page pool cache

	ftruncate (mgr->idx, (mgr->redopage + pagezero->redopages) << mgr->page_bits);
	memset (pagezero, 0, 1 << pagebits);
	pagezero->alloc->lvl = MIN_lvl - 1;
	pagezero->redopages = redopages;
	pagezero->page_bits = mgr->page_bits;
	pagezero->leaf_xtra = leafxtra;

	bt_putid(pagezero->alloc->right, mgr->redopage + pagezero->redopages);
	pagezero->leafpages = 1;

	//  initialize left-most LEAF page in
	//	alloc->left and count of active leaf pages.

	bt_putid (pagezero->alloc->left, LEAF_page);

	if( bt_writepage (mgr, pagezero->alloc, 0, 0) ) {
		fprintf (stderr, "Unable to create btree page zero\n");
		return bt_mgrclose (mgr), NULL;
	}

	memset (pagezero, 0, 1 << pagebits);

	for( lvl=MIN_lvl; lvl--; ) {
	BtSlot *node = slotptr(pagezero->alloc, 1);
		node->off = mgr->page_size;

		if( !lvl )
			node->off <<= mgr->leaf_xtra;

		node->off -= 3 + (lvl ? BtId + sizeof(BtVal): sizeof(BtVal));
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

		if( bt_writepage (mgr, pagezero->alloc, MIN_lvl - lvl, !lvl) ) {
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

	// mlock the first segment of 64K pages

	flag = PROT_READ | PROT_WRITE;
	mgr->pages[0] = mmap (0, (uid)65536 << mgr->page_bits, flag, MAP_SHARED, mgr->idx, 0);
	mgr->segments = 1;

	if( mgr->pages[0] == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap first btree segment, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}

	mgr->pagezero = (BtPageZero *)mgr->pages[0];
	mlock (mgr->pagezero, mgr->page_size);

	mgr->redobuff = mgr->pages[0] + (mgr->redopage << mgr->page_bits);
	mlock (mgr->redobuff, mgr->pagezero->redopages << mgr->page_bits);

	//	allocate pool buffers

	mgr->pagepool = mmap (0, (uid)mgr->nlatchpage << mgr->page_bits, flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if( mgr->pagepool == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap anonymous buffer pool pages, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}

	mgr->leafpool = mmap (0, (uid)mgr->nleafpage << mgr->page_bits, flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if( mgr->leafpool == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap anonymous leaf pool pages, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}

	mgr->latchsets = (BtLatchSet *)(mgr->pagepool + ((uid)mgr->latchtotal << mgr->page_bits));
	mgr->hashtable = (BtHashEntry *)(mgr->latchsets + mgr->latchtotal);
	mgr->latchhash = (mgr->pagepool + ((uid)mgr->nlatchpage << mgr->page_bits) - (unsigned char *)mgr->hashtable) / sizeof(BtHashEntry);

	mgr->leafsets = (BtLatchSet *)(mgr->leafpool + ((uid)mgr->leaftotal << mgr->page_bits << mgr->leaf_xtra));
	mgr->leaftable = (BtHashEntry *)(mgr->leafsets + mgr->leaftotal);
	mgr->leafhash = (mgr->leafpool + ((uid)mgr->nleafpage << mgr->page_bits) - (unsigned char *)mgr->leaftable) / sizeof(BtHashEntry);

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
	bt->cursor = valloc (mgr->page_size << mgr->leaf_xtra);
#else
	bt->cursor = VirtualAlloc(NULL, mgr->page_size << mgr->leaf_xtra, MEM_COMMIT, PAGE_READWRITE);
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

void bt_lockpage(BtLock mode, BtLatchSet *latch, ushort thread_no, uint line)
{
	switch( mode ) {
	case BtLockRead:
		ReadLock (latch->readwr, thread_no);
		break;
	case BtLockWrite:
//if(latch->leaf)
//fprintf(stderr, "WrtRqst %d by %d at %d\n", (uint)latch->page_no, thread_no, line); 
		WriteLock (latch->readwr, thread_no, line);
//if(latch->leaf)
//fprintf(stderr, "WrtLock %d by %d at %d\n", (uint)latch->page_no, thread_no, line); 
		break;
	case BtLockAccess:
		ReadLock (latch->access, thread_no);
		break;
	case BtLockDelete:
		WriteLock (latch->access, thread_no, line);
		break;
	case BtLockParent:
		WriteLock (latch->parent, thread_no, line);
		break;
	case BtLockLink:
		WriteLock (latch->link, thread_no, line);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *latch, ushort thread_no, uint line)
{
	switch( mode ) {
	case BtLockRead:
		ReadRelease (latch->readwr);
		break;
	case BtLockWrite:
//if(latch->leaf)
//fprintf(stderr, "Un Lock %d by %d at %d\n", (uint)latch->page_no, thread_no, line); 
		WriteRelease (latch->readwr);
		break;
	case BtLockAccess:
		ReadRelease (latch->access);
		break;
	case BtLockDelete:
		WriteRelease (latch->access);
		break;
	case BtLockParent:
		WriteRelease (latch->parent);
		break;
	case BtLockLink:
		WriteRelease (latch->link);
		break;
	}
}

//	allocate a new page
//	return with page latched, but unlocked.

int bt_newpage(BtMgr *mgr, BtPageSet *set, BtPage contents, ushort thread_no)
{
uint page_size = mgr->page_size, page_xtra = 0;
unsigned char *freechain;
uid page_no;

	if( contents->lvl )
		freechain = mgr->pagezero->freechain;
	else {
		freechain = mgr->pagezero->leafchain;
		mgr->pagezero->leafpages++;
		page_xtra = mgr->leaf_xtra;
		page_size <<= page_xtra;
	}

	//	lock allocation page

	bt_mutexlock(mgr->lock);

	// use empty chain first
	// else allocate new page

	if( page_no = bt_getid(freechain) ) {
		if( set->latch = contents->lvl ? bt_pinlatch (mgr, page_no, thread_no) : bt_pinleaf (mgr, page_no, thread_no) )
			set->page = bt_mappage (mgr, set->latch);
		else
			return mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_struct;

		bt_putid(freechain, bt_getid(set->page->right));

		//  the page is currently nopromote and this
		//  will keep bt_txnpromote out.

		//	contents will replace this bit
		//  and pin will keep bt_txnpromote out

		contents->page_no = page_no;
		contents->nopromote = 0;
		set->latch->dirty = 1;

		memcpy (set->page, contents, page_size);

//		if( msync (mgr->pagezero, mgr->page_size, MS_SYNC) < 0 )
//		  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);

		bt_releasemutex(mgr->lock);
		return 0;
	}

	page_no = bt_getid(mgr->pagezero->alloc->right);
	bt_putid(mgr->pagezero->alloc->right, page_no+(1 << page_xtra));

	// unlock allocation latch and
	//	extend file into new page.

//	if( msync (mgr->pagezero, mgr->page_size, MS_SYNC) < 0 )
//	  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);
	bt_releasemutex(mgr->lock);

	//	keep bt_txnpromote out of this page
	contents->nopromote = 1;
	contents->page_no = page_no;
	if( pwrite (mgr->idx, contents, page_size, page_no << mgr->page_bits) < page_size )
		fprintf(stderr, "Write %d error %d\n", (uint)page_no, errno);
	if( set->latch = contents->lvl ? bt_pinlatch (mgr, page_no, thread_no) : bt_pinleaf (mgr, page_no, thread_no) )
		set->page = bt_mappage (mgr, set->latch);
	else
		return mgr->err_thread = thread_no, mgr->err;

	// now pin will keep bt_txnpromote out

	set->page->nopromote = 0;
	set->latch->dirty = 1;
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
uid page_no = ROOT_page, prevpage_no = 0;
uint drill = 0xff, slot;
BtLatchSet *prevlatch;
uint mode, prevmode;
BtPage prevpage;
BtVal *val;
BtKey *ptr;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (drill == lvl) ? lock : BtLockRead; 

	if( set->latch = drill ? bt_pinlatch (mgr, page_no, thread_no) : bt_pinleaf (mgr, page_no, thread_no) )
	  set->page = bt_mappage (mgr, set->latch);
	else
	  return 0;

 	// obtain access lock using lock chaining with Access mode

	if( page_no > ROOT_page )
	  bt_lockpage(BtLockAccess, set->latch, thread_no, __LINE__);

	//	release & unpin parent or left sibling page

	if( prevpage_no ) {
	  bt_unlockpage(prevmode, prevlatch, thread_no, __LINE__);
	  bt_unpinlatch (prevlatch, thread_no, __LINE__);
	  prevpage_no = 0;
	}

 	// obtain mode lock using lock coupling through AccessLock

	bt_lockpage(mode, set->latch, thread_no, __LINE__);

	// grab our fence key

	ptr=keyptr(set->page,set->page->cnt);

	if( set->page->free )
		return mgr->err = BTERR_struct, mgr->err_thread = thread_no, mgr->line = __LINE__, 0;

	if( page_no > ROOT_page )
	  bt_unlockpage(BtLockAccess, set->latch, thread_no, __LINE__);

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if( set->latch->page_no != ROOT_page )
			return mgr->err = BTERR_struct, mgr->err_thread = thread_no, mgr->line = __LINE__, 0;
			
		drill = set->page->lvl;

		if( lock != BtLockRead && drill == lvl ) {
		  bt_unlockpage(mode, set->latch, thread_no, __LINE__);
		  bt_unpinlatch (set->latch, thread_no, __LINE__);
		  continue;
		}
	}

	prevpage_no = set->latch->page_no;
	prevlatch = set->latch;
	prevpage = set->page;
	prevmode = mode;

	//  if requested key is beyond our fence,
	//	slide to the right

	if( keycmp (ptr, key, len) < 0 )
	  if( page_no = bt_getid(set->page->right) )
	  	continue;

	//  if page is part of a delete operation,
	//	slide to the left;

	if( set->page->kill ) {
	  bt_lockpage(BtLockLink, set->latch, thread_no, __LINE__);
	  page_no = bt_getid(set->page->left);
	  bt_unlockpage(BtLockLink, set->latch, thread_no, __LINE__);
	  continue;
	}

	//  find key on page at this level
	//  and descend to requested level

	if( slot = bt_findslot (set->page, key, len) ) {
	  if( drill == lvl )
		return slot;

	  // find next non-dead slot -- the fence key if nothing else

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
		  continue;
		else
  		  return mgr->err = BTERR_struct, mgr->err_thread = thread_no, mgr->line = __LINE__, 0;

	  val = valptr(set->page, slot);

	  if( val->len == BtId )
	  	page_no = bt_getid(val->value);
	  else
  		return mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_struct, 0;

	  drill--;
	  continue;
	 }

	//  slide right into next page

	page_no = bt_getid(set->page->right);

  } while( page_no );

  // return error on end of right chain

  mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_struct;
  return 0;	// return error
}

//	return page to free list
//	page must be delete & write locked
//	and have no keys pointing to it.

void bt_freepage (BtMgr *mgr, BtPageSet *set, ushort thread_no)
{
unsigned char *freechain;

	if( set->page->lvl )
		freechain = mgr->pagezero->freechain;
	else {
		freechain = mgr->pagezero->leafchain;
		mgr->pagezero->leafpages--;
	}

	//	lock allocation page

	bt_mutexlock (mgr->lock);

	//	store chain

	memcpy(set->page->right, freechain, BtId);
	bt_putid(freechain, set->latch->page_no);
	set->latch->dirty = 1;
	set->page->free = 1;

//	if( msync (mgr->pagezero, mgr->page_size, MS_SYNC) < 0 )
//	  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);

	// unlock released page
	// and unlock allocation page

	bt_unlockpage (BtLockDelete, set->latch, thread_no, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, thread_no, __LINE__);
	bt_unpinlatch (set->latch, thread_no, __LINE__);
	bt_releasemutex (mgr->lock);
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

	bt_lockpage (BtLockParent, set->latch, thread_no, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, thread_no, __LINE__);

	//	insert new (now smaller) fence key

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey*)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
	  return mgr->err_thread = thread_no, mgr->err;

	//	now delete old fence key

	ptr = (BtKey*)rightkey;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;

	bt_unlockpage (BtLockParent, set->latch, thread_no, __LINE__);
	bt_unpinlatch(set->latch, thread_no, __LINE__);
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
  		return mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_struct;

	if( child->latch = bt_pinlatch (mgr, page_no, thread_no) )
		child->page = bt_mappage (mgr, child->latch);
	else
		return mgr->err_thread = thread_no, mgr->err;

	bt_lockpage (BtLockDelete, child->latch, thread_no, __LINE__);
	bt_lockpage (BtLockWrite, child->latch, thread_no, __LINE__);

	memcpy (root->page, child->page, mgr->page_size);
	root->latch->dirty = 1;

	bt_freepage (mgr, child, thread_no);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockWrite, root->latch, thread_no, __LINE__);
  bt_unpinlatch (root->latch, thread_no, __LINE__);
  return 0;
}

//  delete a page and manage key
//  call with page writelocked

//	returns with page unpinned
//	from the page pool.

BTERR bt_deletepage (BtMgr *mgr, BtPageSet *set, ushort thread_no, uint lvl)
{
unsigned char lowerfence[BT_keyarray];
uint page_size = mgr->page_size;
BtPageSet right[1], temp[1];
unsigned char value[BtId];
uid page_no, right2;
BtKey *ptr;

	if( !lvl )
		page_size <<= mgr->leaf_xtra;

	//  cache copy of original fence key
	//	that is being deleted.

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (lowerfence, ptr, ptr->len + sizeof(BtKey));

	//	obtain locks on right page

	page_no = bt_getid(set->page->right);

	if( right->latch = lvl ? bt_pinlatch (mgr, page_no, thread_no) : bt_pinleaf (mgr, page_no, thread_no) )
		right->page = bt_mappage (mgr, right->latch);
	else
		return 0;

	bt_lockpage (BtLockWrite, right->latch, thread_no, __LINE__);

	if( right->page->kill )
		return mgr->line = __LINE__, mgr->err = BTERR_struct;

	// pull contents of right peer into our empty page
	//	preserving our left page number.

	bt_lockpage (BtLockLink, set->latch, thread_no, __LINE__);
	page_no = bt_getid(set->page->left);
	memcpy (set->page, right->page, page_size);
	bt_putid (set->page->left, page_no);
	bt_unlockpage (BtLockLink, set->latch, thread_no, __LINE__);

	set->page->page_no = set->latch->page_no;
	set->latch->dirty = 1;

	if( right2 = bt_getid (right->page->right) ) {
	  if( temp->latch = lvl ? bt_pinlatch (mgr, right2, thread_no) : bt_pinleaf (mgr, right2, thread_no) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return 0;

      bt_lockpage(BtLockLink, temp->latch, thread_no, __LINE__);
	  bt_putid (temp->page->left, set->latch->page_no);
	  bt_unlockpage(BtLockLink, temp->latch, thread_no, __LINE__);
	  bt_unpinlatch (temp->latch, thread_no, __LINE__);
	} else {	// page is rightmost
	  bt_mutexlock (mgr->lock);
	  bt_putid (mgr->pagezero->alloc->left, set->latch->page_no);
	  bt_releasemutex(mgr->lock);
	}

	// mark right page deleted and release lock

	right->latch->dirty = 1;
	right->page->kill = 1;

	// redirect higher key directly to our new node contents

	ptr = keyptr(right->page, right->page->cnt);
	bt_putid (value, set->latch->page_no);

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Update, thread_no) )
	  return mgr->err;

	bt_unlockpage (BtLockWrite, right->latch, thread_no, __LINE__);

	//	delete orignal fence key in parent
	//	and unlock our page.

	ptr = (BtKey *)lowerfence;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1, thread_no) )
	  return mgr->err;

	bt_unlockpage (BtLockWrite, set->latch, thread_no, __LINE__);
	bt_unpinlatch (set->latch, thread_no, __LINE__);

	//	obtain delete and write locks to right node

	bt_lockpage (BtLockDelete, right->latch, thread_no, __LINE__);
	bt_lockpage (BtLockWrite, right->latch, thread_no, __LINE__);
	bt_freepage (mgr, right, thread_no);
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
		return mgr->err_thread = thread_no, mgr->err;

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

	if( !found )
		return 0;

	//	did we delete a fence key in an upper level?

	if( lvl && set->page->act && fence )
	  if( bt_fixfence (mgr, set, lvl, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;
	  else
		return 0;

	//	do we need to collapse root?

	if( set->latch->page_no == ROOT_page && set->page->act == 1 )
	  if( bt_collapseroot (mgr, set, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;
	  else
		return 0;

	//	delete empty page

 	if( !set->page->act )
	  return bt_deletepage (mgr, set, thread_no, set->page->lvl);

	set->latch->dirty = 1;
	bt_unlockpage(BtLockWrite, set->latch, thread_no, __LINE__);
	bt_unpinlatch (set->latch, thread_no, __LINE__);
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
	  if( set->latch = bt_pinleaf (bt->mgr, page_no, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	  else
		return 0;
	else
	  return bt->mgr->err = BTERR_struct, bt->mgr->err_thread = bt->thread_no, bt->mgr->line = __LINE__, 0;

 	// obtain access lock using lock chaining with Access mode

	bt_lockpage(BtLockAccess, set->latch, bt->thread_no, __LINE__);

	bt_unlockpage(BtLockRead, prevlatch, bt->thread_no, __LINE__);
	bt_unpinlatch (prevlatch, bt->thread_no, __LINE__);

	bt_lockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	bt_unlockpage(BtLockAccess, set->latch, bt->thread_no, __LINE__);
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

  bt_unlockpage (BtLockRead, set->latch, bt->thread_no, __LINE__);
  bt_unpinlatch (set->latch, bt->thread_no, __LINE__);
  return ret;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0  new slot value

uint bt_cleanpage(BtMgr *mgr, BtPageSet *set, uint keylen, uint slot, uint vallen)
{
uint page_size = mgr->page_size;
BtPage page = set->page, frame;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey *key;
BtVal *val;

	if( !set->page->lvl )
		page_size <<= mgr->leaf_xtra;

	if( page->min >= (max+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal))
		return slot;

	//	skip cleanup and proceed to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < page_size / 5 )
		return 0;

	frame = malloc (page_size);
	memcpy (frame, page, page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, page_size - sizeof(*page));
	set->latch->dirty = 1;

	page->min = page_size;
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
		page->min -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)page + page->min, val, val->len + sizeof(BtVal));

		// copy the key across

		key = keyptr(frame, cnt);
		page->min -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)page + page->min, key, key->len + sizeof(BtKey));

		// make a librarian slot

		slotptr(page, ++idx)->off = page->min;
		slotptr(page, idx)->type = Librarian;
		slotptr(page, idx)->dead = 1;

		// set up the slot

		slotptr(page, ++idx)->off = page->min;
		slotptr(page, idx)->type = slotptr(frame, cnt)->type;

		if( !(slotptr(page, idx)->dead = slotptr(frame, cnt)->dead) )
			page->act++;
	}

	page->cnt = idx;
	free (frame);

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal) )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtMgr *mgr, BtPageSet *root, BtLatchSet *right, ushort thread_no)
{  
unsigned char leftkey[BT_keyarray];
uint nxt = mgr->page_size;
unsigned char value[BtId];
BtPageSet left[1];
uid left_page_no;
BtPage frame;
BtKey *ptr;
BtVal *val;

	frame = malloc (mgr->page_size);
	memcpy (frame, root->page, mgr->page_size);

	//	save left page fence key for new root

	ptr = keyptr(root->page, root->page->cnt);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( bt_newpage(mgr, left, frame, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;

	left_page_no = left->latch->page_no;
	bt_unpinlatch (left->latch, thread_no, __LINE__);
	free (frame);

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

	bt_unlockpage(BtLockWrite, root->latch, thread_no, __LINE__);
	bt_unpinlatch (root->latch, thread_no, __LINE__);

	bt_unpinlatch (right, thread_no, __LINE__);
	return 0;
}

//  split already locked full node
//	leave it locked.
//	return pool entry for new right
//	page, pinned & unlocked

uint bt_splitpage (BtMgr *mgr, BtPageSet *set, ushort thread_no, uint linkleft)
{
uint page_size = mgr->page_size;
BtPageSet right[1], temp[1];
uint cnt = 0, idx = 0, max;
uint lvl = set->page->lvl;
BtKey *key, *ptr;
BtVal *val, *src;
BtPage frame;
uid right2;
uint entry;
uint prev;

	if( !set->page->lvl )
		page_size <<= mgr->leaf_xtra;

	//  split higher half of keys to frame

	frame = malloc (page_size);
	memset (frame, 0, page_size);
	frame->min = page_size;
	max = set->page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		if( cnt < max || set->page->lvl )
		  if( slotptr(set->page, cnt)->dead )
			continue;

		src = valptr(set->page, cnt);
		frame->min -= src->len + sizeof(BtVal);
		memcpy ((unsigned char *)frame + frame->min, src, src->len + sizeof(BtVal));

		key = keyptr(set->page, cnt);
		frame->min -= key->len + sizeof(BtKey);
		ptr = (BtKey*)((unsigned char *)frame + frame->min);
		memcpy (ptr, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(frame, ++idx)->off = frame->min;
		slotptr(frame, idx)->type = Librarian;
		slotptr(frame, idx)->dead = 1;

		//  add actual slot

		slotptr(frame, ++idx)->off = frame->min;
		slotptr(frame, idx)->type = slotptr(set->page, cnt)->type;

		if( !(slotptr(frame, idx)->dead = slotptr(set->page, cnt)->dead) )
			frame->act++;
	}

	frame->cnt = idx;
	frame->lvl = lvl;

	// link right node

	if( set->latch->page_no > ROOT_page ) {
		right2 = bt_getid (set->page->right);
		bt_putid (frame->right, right2);
		bt_putid (frame->left, set->latch->page_no);
	}

	//	get new free page and write higher keys to it.

	if( bt_newpage(mgr, right, frame, thread_no) )
		return 0;

	//	link far right's left pointer to new page

	if( linkleft && set->latch->page_no > ROOT_page )
	 if( right2 ) {
	  if( temp->latch = lvl ? bt_pinlatch (mgr, right2, thread_no) : bt_pinleaf (mgr, right2, thread_no) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return 0;

      bt_lockpage(BtLockLink, temp->latch, thread_no, __LINE__);
	  bt_putid (temp->page->left, right->latch->page_no);
	  bt_unlockpage(BtLockLink, temp->latch, thread_no, __LINE__);
	  bt_unpinlatch (temp->latch, thread_no, __LINE__);
	 } else {	// page is rightmost
	  bt_mutexlock (mgr->lock);
	  bt_putid (mgr->pagezero->alloc->left, right->latch->page_no);
	  bt_releasemutex(mgr->lock);
	 }

	// process lower keys

	memcpy (frame, set->page, page_size);
	memset (set->page+1, 0, page_size - sizeof(*set->page));
	set->latch->dirty = 1;

	set->page->min = page_size;
	set->page->garbage = 0;
	set->page->act = 0;
	max /= 2;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys

	while( cnt++ < max ) {
		if( slotptr(frame, cnt)->dead )
			continue;
		val = valptr(frame, cnt);
		set->page->min -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)set->page + set->page->min, val, val->len + sizeof(BtVal));

		key = keyptr(frame, cnt);
		set->page->min -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)set->page + set->page->min, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(set->page, ++idx)->off = set->page->min;
		slotptr(set->page, idx)->type = Librarian;
		slotptr(set->page, idx)->dead = 1;

		//	add actual slot

		slotptr(set->page, ++idx)->off = set->page->min;
		slotptr(set->page, idx)->type = slotptr(frame, cnt)->type;
		set->page->act++;
	}

	bt_putid(set->page->right, right->latch->page_no);
	set->page->cnt = idx;
	free(frame);

	entry = right->latch - (set->page->lvl ? mgr->latchsets : mgr->leafsets);
	return entry;
}

//	fix keys for newly split page
//	call with both pages pinned & locked
//  return unlocked and unpinned

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

	bt_lockpage (BtLockParent, right, thread_no, __LINE__);

	bt_lockpage (BtLockParent, set->latch, thread_no, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, thread_no, __LINE__);

	// insert new fence for reformulated left block of smaller keys

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey *)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;

	// switch fence for right block of larger keys to new right page

	bt_putid (value, right->page_no);
	ptr = (BtKey *)rightkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;

	bt_unlockpage (BtLockParent, set->latch, thread_no, __LINE__);
	bt_unpinlatch (set->latch, thread_no, __LINE__);

	bt_unlockpage (BtLockParent, right, thread_no, __LINE__);
	bt_unpinlatch (right, thread_no, __LINE__);
	return 0;
}

//	install new key and value onto page
//	page must already be checked for
//	adequate space

BTERR bt_insertslot (BtMgr *mgr, BtPageSet *set, uint slot, unsigned char *key,uint keylen, unsigned char *value, uint vallen, uint type)
{
uint idx, librarian;
BtSlot *node;
BtKey *ptr;
BtVal *val;
int rate;

	//	if previous slot is a librarian slot, use it

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

	// now insert key into array before slot,
	//	adding as many librarian slots as
	//	makes sense.

	if( idx == set->page->cnt ) {
	int avail = 4 * set->page->min / 5 - sizeof(*set->page) - ++set->page->cnt * sizeof(BtSlot);

		librarian = ++idx - slot;
		avail /= sizeof(BtSlot);

		if( avail < 0 )
			avail = 0;

		if( librarian > avail )
			librarian = avail;

		if( librarian ) {
			rate = (idx - slot) / librarian;
			set->page->cnt += librarian;
			idx += librarian;
		} else
			rate = 0;
	} else
		librarian = 0, rate = 0;

	while( idx > slot ) {
		//  transfer slot
		*slotptr(set->page, idx) = *slotptr(set->page, idx-librarian-1);
		idx--;

		//	add librarian slot per rate

		if( librarian )
		 if( (idx - slot + 1)/2 <= librarian * rate ) {
			node = slotptr(set->page, idx--);
			node->off = node[1].off;
			node->type = Librarian;
			node->dead = 1;
			librarian--;
		  }
	}

	set->latch->dirty = 1;
	set->page->act++;

	//	fill in new slot

	node = slotptr(set->page, slot);
	node->off = set->page->min;
	node->type = type;
	node->dead = 0;
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
		return mgr->err_thread = thread_no, mgr->err;
	}

	// if librarian slot == found slot, advance to real slot

	if( node->type == Librarian ) {
//	  if( !keycmp (ptr, key, keylen) ) {
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
	  if( !keycmp (ptr, key, keylen) )
		break;

	  if( slot = bt_cleanpage (mgr, set, keylen, slot, vallen) )
	    if( bt_insertslot (mgr, set, slot, key, keylen, value, vallen, type) )
		  return mgr->err;
		else
		  goto insxit;

	  if( entry = bt_splitpage (mgr, set, thread_no, 1) )
	    if( !bt_splitkeys (mgr, set, mgr->latchsets + entry, thread_no) )
		  continue;

	  return mgr->err_thread = thread_no, mgr->err;

	case Update:
	  if( keycmp (ptr, key, keylen) )
		goto insxit;

	  break;
	}

	// if key already exists, update value and return

	val = valptr(set->page, slot);

	if( val->len >= vallen ) {
	  if( node->dead )
	  	set->page->act++;
	  node->type = type;
	  node->dead = 0;

	  set->page->garbage += val->len - vallen;
	  set->latch->dirty = 1;
	  val->len = vallen;
	  memcpy (val->value, value, vallen);
	  goto insxit;
	}

	//  new update value doesn't fit in existing value area
	//	make sure page has room

	if( !node->dead )
	  set->page->garbage += val->len + ptr->len + sizeof(BtKey) + sizeof(BtVal);
	else
	  set->page->act++;

	node->type = type;
	node->dead = 0;

	if( slot = bt_cleanpage (mgr, set, keylen, slot, vallen) )
	  break;

	if( entry = bt_splitpage (mgr, set, thread_no, 1) )
	  if( !bt_splitkeys (mgr, set, mgr->latchsets + entry, thread_no) )
		continue;

	return mgr->err_thread = thread_no, mgr->err;
  }

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
	
  slotptr(set->page,slot)->off = set->page->min;

insxit:
  bt_unlockpage(BtLockWrite, set->latch, thread_no, __LINE__);
  bt_unpinlatch (set->latch, thread_no, __LINE__);
  return 0;
}

//	determine actual page where key is located
//  return slot number

uint bt_atomicpage (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint src, BtPageSet *set)
{
BtKey *key = keyptr(source,src), *ptr;
unsigned char fence[BT_keyarray];
uint entry, slot;

	if( locks[src].reuse )
	  entry = locks[src-1].entry;
	else
	  entry = locks[src].entry;

	//	find where our key is located 
	//	on current page or pages split on
	//	same page txn operations.

	do {
		set->latch = mgr->leafsets + entry;
		set->page = bt_mappage (mgr, set->latch);

		if( slot = bt_findslot(set->page, key->key, key->len) ) {
		  if( slotptr(set->page, slot)->type == Librarian )
			slot++;
		  if( locks[src].reuse )
			locks[src].entry = entry;
		  return slot;
		}
	} while( entry = set->latch->split );

	ptr = keyptr (set->page, set->page->cnt);
	memcpy (fence, ptr, ptr->len + 1);

	mgr->line = __LINE__, mgr->err = BTERR_atomic;
	return 0;
}

BTERR bt_atomicinsert (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint src, ushort thread_no, logseqno lsn)
{
BtKey *key = keyptr(source, src);
BtVal *val = valptr(source, src);
BtLatchSet *latch;
BtPageSet set[1];
uint entry, slot;

  while( slot = bt_atomicpage (mgr, source, locks, src, set) ) {
	if( slot = bt_cleanpage(mgr, set, key->len, slot, val->len) ) {
	  if( bt_insertslot (mgr, set, slot, key->key, key->len, val->value, val->len, slotptr(source,src)->type) )
		return mgr->err_thread = thread_no, mgr->err;

	  set->page->lsn = lsn;
	  return 0;
	}

	//  split page

	if( entry = bt_splitpage (mgr, set, thread_no, 0) )
	  latch = mgr->leafsets + entry;
	else
	  return mgr->err_thread = thread_no, mgr->err;

	//	splice right page into split chain
	//	and WriteLock it

	bt_lockpage(BtLockWrite, latch, thread_no, __LINE__);
	latch->split = set->latch->split;
	set->latch->split = entry;
  }

  return mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_atomic;
}

//	perform delete from smaller btree
//  insert a delete slot if not found there

BTERR bt_atomicdelete (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint src, ushort thread_no, logseqno lsn)
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
	  return mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_struct;

	//  if slot is not found, insert a delete slot

	if( keycmp (ptr, key->key, key->len) )
	  if( bt_insertslot (mgr, set, slot, key->key, key->len, NULL, 0, Delete) )
		return mgr->err;

	//	if node is already dead,
	//	ignore the request.

	if( node->dead )
		return 0;

	set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
	set->latch->dirty = 1;
	set->page->lsn = lsn;
 	set->page->act--;

	node->dead = 0;
	__sync_fetch_and_add(&mgr->found, 1);
	return 0;
}

//	release master's splits from right to left

void bt_atomicrelease (BtMgr *mgr, uint entry, ushort thread_no)
{
BtLatchSet *latch = mgr->leafsets + entry;

	if( latch->split )
		bt_atomicrelease (mgr, latch->split, thread_no);

	latch->split = 0;
	bt_unlockpage(BtLockWrite, latch, thread_no, __LINE__);
	bt_unpinlatch(latch, thread_no, __LINE__);
}

int qsortcmp (BtSlot *slot1, BtSlot *slot2, BtPage page)
{
BtKey *key1 = (BtKey *)((char *)page + slot1->off);
BtKey *key2 = (BtKey *)((char *)page + slot2->off);

	return keycmp (key1, key2->key, key2->len);
}
//	atomic modification of a batch of keys.

BTERR bt_atomictxn (BtDb *bt, BtPage source)
{
uint src, idx, slot, samepage, entry, que = 0;
BtKey *key, *ptr, *key2;
int result = 0;
BtSlot temp[1];
logseqno lsn;
int type;

  // stable sort the list of keys into order to
  //	prevent deadlocks between threads.
/*
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
*/
	qsort_r (slotptr(source,1), source->cnt, sizeof(BtSlot), (__compar_d_fn_t)qsortcmp, source);
  //  add entries to redo log

  if( bt->mgr->pagezero->redopages )
	lsn = bt_txnredo (bt->mgr, source, bt->thread_no);
  else
	lsn = 0;

  //  perform the individual actions in the transaction

  if( bt_atomicexec (bt->mgr, source, lsn, 0, bt->thread_no) )
	return bt->mgr->err;

  // if number of active pages
  // is greater than the buffer pool
  // promote page into larger btree

  if( bt->main )
   while( bt->mgr->pagezero->leafpages > bt->mgr->leaftotal - *bt->mgr->thread_no )
	if( bt_txnpromote (bt) )
	  return bt->mgr->err;

  // return success

  return 0;
}

//	execute the source list of inserts/deletes

BTERR bt_atomicexec(BtMgr *mgr, BtPage source, logseqno lsn, int lsm, ushort thread_no)
{
uint src, idx, samepage, entry;
BtPageSet set[1], prev[1];
unsigned char value[BtId];
BtLatchSet *latch;
uid right_page_no;
AtomicTxn *locks;
BtKey *key, *ptr;
BtPage page;
BtVal *val;

  locks = calloc (source->cnt + 1, sizeof(AtomicTxn));

  // Load the leaf page for each key
  // group same page references with reuse bit

  for( src = 0; src++ < source->cnt; ) {
	key = keyptr(source, src);

	// first determine if this modification falls
	// on the same page as the previous modification
	//	note that the far right leaf page is a special case

	if( samepage = src > 1 )
	  samepage = !bt_getid(set->page->right) || keycmp (ptr, key->key, key->len) >= 0;

	if( !samepage )
	  if( bt_loadpage(mgr, set, key->key, key->len, 0, BtLockWrite, thread_no) )
		ptr = keyptr(set->page, set->page->cnt), set->latch->split = 0;
	  else
  	 	return mgr->err;

	entry = set->latch - mgr->leafsets;
	locks[src].reuse = samepage;
	locks[src].entry = entry;

	//	capture current lsn for master page

	locks[src].reqlsn = set->page->lsn;
  }

  // insert or delete each key
  // process any splits or merges
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
	  if( bt_atomicdelete (mgr, source, locks, idx, thread_no, lsn) )
  	 	return mgr->err;
	  break;

	case Duplicate:
	case Unique:
	  if( bt_atomicinsert (mgr, source, locks, idx, thread_no, lsn) )
  	 	return mgr->err;
	  break;

	default:
  	  bt_atomicpage (mgr, source, locks, idx, set);
	  break;
	}

	//	after the same page operations have finished,
	//  process master page for splits or deletion.

	latch = prev->latch = mgr->leafsets + locks[src].entry;
	prev->page = bt_mappage (mgr, prev->latch);
	samepage = src;

	//  pick-up all splits from master page
	//	each one is already pinned & WriteLocked.

	while( entry = prev->latch->split ) {
	  set->latch = mgr->leafsets + entry;
	  set->page = bt_mappage (mgr, set->latch);

	  // delete empty master page by undoing its split
	  //  (this is potentially another empty page)
	  //  note that there are no pointers to it yet

	  if( !prev->page->act ) {
		memcpy (set->page->left, prev->page->left, BtId);
		memcpy (prev->page, set->page, mgr->page_size << mgr->leaf_xtra);
		bt_lockpage (BtLockDelete, set->latch, thread_no, __LINE__);
		prev->latch->split = set->latch->split;
		prev->latch->dirty = 1;
		bt_freepage (mgr, set, thread_no);
		continue;
	  }

	  // remove empty split page from the split chain
	  // and return it to the free list. No other
	  // thread has its page number yet.

	  if( !set->page->act ) {
		memcpy (prev->page->right, set->page->right, BtId);
		prev->latch->split = set->latch->split;

		bt_lockpage (BtLockDelete, set->latch, thread_no, __LINE__);
		bt_freepage (mgr, set, thread_no);
		continue;
	  }

	  //  update prev's fence key

	  ptr = keyptr(prev->page,prev->page->cnt);
	  bt_putid (value, prev->latch->page_no);

	  if( bt_insertkey (mgr, ptr->key, ptr->len, 1, value, BtId, Unique, thread_no) )
		return mgr->err;

	  // splice in the left link into the split page

	  bt_putid (set->page->left, prev->latch->page_no);

	  if( lsm )
		bt_syncpage (mgr, prev->page, prev->latch);

	  *prev = *set;
	}

	//  update left pointer in next right page from last split page
	//	(if all splits were reversed or none occurred, latch->split == 0)

	if( latch->split ) {
	  //  fix left pointer in master's original (now split)
	  //  far right sibling or set rightmost page in page zero

	  if( right_page_no = bt_getid (prev->page->right) ) {
		if( set->latch = bt_pinleaf (mgr, right_page_no, thread_no) )
	  	  set->page = bt_mappage (mgr, set->latch);
	 	else
  	 	  return mgr->err;

	    bt_lockpage (BtLockLink, set->latch, thread_no, __LINE__);
	    bt_putid (set->page->left, prev->latch->page_no);
		set->latch->dirty = 1;

	    bt_unlockpage (BtLockLink, set->latch, thread_no, __LINE__);
		bt_unpinlatch (set->latch, thread_no, __LINE__);
	  } else {	// prev is rightmost page
	    bt_mutexlock (mgr->lock);
		bt_putid (mgr->pagezero->alloc->left, prev->latch->page_no);
	    bt_releasemutex(mgr->lock);
	  }

	  //  switch the original fence key from the
	  //  master page to the last split page.

	  ptr = keyptr(prev->page,prev->page->cnt);
	  bt_putid (value, prev->latch->page_no);

	  if( bt_insertkey (mgr, ptr->key, ptr->len, 1, value, BtId, Update, thread_no) )
		return mgr->err;

	  if( lsm )
		bt_syncpage (mgr, prev->page, prev->latch);

	  //  unlock and unpin the split pages

	  bt_atomicrelease (mgr, latch->split, thread_no);

	  //  unlock and unpin the master page

	  latch->split = 0;
	  bt_unlockpage(BtLockWrite, latch, thread_no, __LINE__);
	  bt_unpinlatch(latch, thread_no, __LINE__);
	  continue;
	}

	//	since there are no splits remaining, we're
	//  finished if master page occupied

	if( prev->page->act ) {
	  bt_unlockpage(BtLockWrite, prev->latch, thread_no, __LINE__);

	  if( lsm )
		bt_syncpage (mgr, prev->page, prev->latch);

	  bt_unpinlatch(prev->latch, thread_no, __LINE__);
	  continue;
	}

	// any and all splits were reversed, and the
	// master page located in prev is empty, delete it

	if( bt_deletepage (mgr, prev, thread_no, 0) )
		return mgr->err;
  }

  free (locks);
  return 0;
}

//  pick & promote a page into the larger btree

BTERR bt_txnpromote (BtDb *bt)
{
uint entry, slot, idx;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

  while( 1 ) {
#ifdef unix
	entry = __sync_fetch_and_add(&bt->mgr->leafpromote, 1);
#else
	entry = _InterlockedIncrement (&bt->mgr->leafpromote) - 1;
#endif
	entry %= bt->mgr->leaftotal;

	if( !entry )
		continue;

	set->latch = bt->mgr->leafsets + entry;

	//  skip this entry if it has never been used

	if( !set->latch->page_no )
	  continue;

	if( !bt_mutextry(set->latch->modify) )
		continue;

	//  skip this entry if it is pinned

	if( set->latch->pin & ~CLOCK_bit ) {
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	set->page = bt_mappage (bt->mgr, set->latch);

	// entry has no right sibling

	if( !bt_getid (set->page->right) ) {
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	// entry interiour node or being killed or constructed

	if( set->page->lvl || set->page->nopromote || set->page->kill ) {
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	//  pin the page for our access
	//	and leave it locked for the
	//	duration of the promotion.

	set->latch->pin++;
	bt_lockpage (BtLockWrite, set->latch, bt->thread_no, __LINE__);
	bt_releasemutex(set->latch->modify);

	// transfer slots in our selected page to the main btree
if( !(entry % 100) )
fprintf(stderr, "Promote entry %d page %d, %d keys\n", entry, set->latch->page_no, set->page->act);

	if( bt_atomicexec (bt->main, set->page, 0, bt->mgr->pagezero->redopages ? 1 : 0, bt->thread_no) ) {
		fprintf (stderr, "Promote error = %d line = %d\n", bt->main->err, bt->main->line);
		return bt->main->err;
	}

	//  now delete the page

	if( bt_deletepage (bt->mgr, set, bt->thread_no, 0) )
		fprintf (stderr, "Promote: delete page err = %d, threadno = %d\n", bt->mgr->err, bt->mgr->err_thread);

	return bt->mgr->err;
  }
}

//	set cursor to highest slot on highest page

uint bt_lastkey (BtDb *bt)
{
uid page_no = bt_getid (bt->mgr->pagezero->alloc->left);
BtPageSet set[1];

	if( set->latch = bt_pinleaf (bt->mgr, page_no, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	memcpy (bt->cursor, set->page, bt->mgr->page_size << bt->mgr->leaf_xtra);
    bt_unlockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	bt_unpinlatch (set->latch, bt->thread_no, __LINE__);
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

	if( set->latch = bt_pinleaf (bt->mgr, next, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	memcpy (bt->cursor, set->page, bt->mgr->page_size << bt->mgr->leaf_xtra);
	bt_unlockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	bt_unpinlatch (set->latch, bt->thread_no, __LINE__);
	
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

	if( set->latch = bt_pinleaf (bt->mgr, right, bt->thread_no) )
		set->page = bt_mappage (bt->mgr, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	memcpy (bt->cursor, set->page, bt->mgr->page_size << bt->mgr->leaf_xtra);
	bt_unlockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	bt_unpinlatch (set->latch, bt->thread_no, __LINE__);
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
	  memcpy (bt->cursor, set->page, bt->mgr->page_size << bt->mgr->leaf_xtra);
	else
	  return 0;

	bt_unlockpage(BtLockRead, set->latch, bt->thread_no, __LINE__);
	bt_unpinlatch (set->latch, bt->thread_no, __LINE__);
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

void bt_poolaudit (BtMgr *mgr, char *type)
{
BtLatchSet *latch, test[1];
uint entry;

	memset (test, 0, sizeof(test));

	if( memcmp (test, mgr->latchsets, sizeof(test)) )
		fprintf(stderr, "%s latchset zero overwritten\n", type);

	if( memcmp (test, mgr->leafsets, sizeof(test)) )
		fprintf(stderr, "%s leafset zero overwritten\n", type);

	for( entry = 0; ++entry < mgr->latchtotal; ) {
		latch = mgr->latchsets + entry;

		if( *latch->modify->value )
			fprintf(stderr, "%s latchset %d modifylocked for page %d\n", type, entry, latch->page_no);

		if( latch->pin & ~CLOCK_bit )
			fprintf(stderr, "%s latchset %d pinned %d times for page %d\n", type, entry, latch->pin & ~CLOCK_bit, latch->page_no);
	}

	for( entry = 0; ++entry < mgr->leaftotal; ) {
		latch = mgr->leafsets + entry;

		if( *latch->modify->value )
			fprintf(stderr, "%s leafset %d modifylocked for page %d\n", type, entry, latch->page_no);

		if( latch->pin & ~CLOCK_bit )
			fprintf(stderr, "%s leafset %d pinned %d times for page %d\n", type, entry, latch->pin & ~CLOCK_bit, latch->page_no);
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
				fprintf(stderr, "Error %d Line: %d by %d source: %d\n", bt->mgr->err, bt->mgr->line, bt->mgr->err_thread, line), exit(0);
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
			if( set->latch = bt_pinleaf (bt->mgr, page_no, bt->thread_no) )
				set->page = bt_mappage (bt->mgr, set->latch);
			else
				fprintf(stderr, "unable to obtain latch"), exit(1);

			bt_lockpage (BtLockRead, set->latch, bt->thread_no, __LINE__);
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

			bt_unlockpage (BtLockRead, set->latch, bt->thread_no, __LINE__);
			bt_unpinlatch (set->latch, bt->thread_no, __LINE__);
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
		next = bt->mgr->redopage + bt->mgr->pagezero->redopages;

		while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
			if( bt_readpage (bt->mgr, bt->cursor, page_no, 1) )
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
double start, stop;
#ifdef unix
pthread_t *threads;
#else
HANDLE *threads;
#endif
ThreadArg *args;
uint mainleafpool = 0;
uint mainleafxtra = 0;
uint redopages = 0;
uint poolsize = 0;
uint leafpool = 0;
uint leafxtra = 0;
uint mainpool = 0;
uint mainbits = 0;
int bits = 16;
float elapsed;
int num = 0;
char key[1];
BtMgr *main;
BtMgr *mgr;
BtKey *ptr;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file main_file cmds [pagebits leafbits poolsize leafpool txnsize redopages mainbits mainleafbits mainpool mainleafpool src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where idx_file is the name of the cache btree file\n");
		fprintf (stderr, "  where main_file is the name of the main btree file\n");
		fprintf (stderr, "  cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort, with one character command for each input src_file. Commands with no input file need a placeholder.\n");
		fprintf (stderr, "  pagebits is the page size in bits for the cache btree\n");
		fprintf (stderr, "  leafbits is the number of xtra bits for a leaf page\n");
		fprintf (stderr, "  poolsize is the number of pages in buffer pool for the cache btree\n");
		fprintf (stderr, "  leafpool is the number of leaf pages in leaf pool for the cache btree\n");
		fprintf (stderr, "  txnsize = n to block transactions into n units, or zero for no transactions\n");
		fprintf (stderr, "  redopages = n to implement recovery buffer with n pages, or zero for no recovery buffer\n");
		fprintf (stderr, "  mainbits is the page size of the main btree in bits\n");
		fprintf (stderr, "  mainpool is the number of main pages in the main buffer pool\n");
		fprintf (stderr, "  mainleaf is the number of main pages in the main leaf pool\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
		exit(0);
	}

	start = getCpuTime(0);

	if( argc > 4 )
		bits = atoi(argv[4]);

	if( argc > 5 )
		leafxtra = atoi(argv[5]);

	if( argc > 6 )
		poolsize = atoi(argv[6]);

	if( !poolsize )
		fprintf (stderr, "no page pool\n"), exit(1);

	if( argc > 7 )
		leafpool = atoi(argv[7]);

	if( argc > 8 )
		num = atoi(argv[8]);

	if( argc > 9 )
		redopages = atoi(argv[9]);

	if( redopages > 65535 )
		fprintf (stderr, "Recovery buffer too large\n"), exit(1);

	if( argc > 10 )
		mainbits = atoi(argv[10]);

	if( argc > 11 )
		mainleafxtra = atoi(argv[11]);

	if( argc > 12 )
		mainpool = atoi(argv[12]);

	if( argc > 13 )
		mainleafpool = atoi(argv[13]);

	cnt = argc - 14;
#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc ((cnt + 1) * sizeof(ThreadArg));

	mgr = bt_mgr (argv[1], bits, leafxtra, poolsize, leafpool, redopages);

	if( !mgr ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	}

	if( mainbits ) {
		main = bt_mgr (argv[2], mainbits, mainleafxtra, mainpool, mainleafpool, 0);

		if( !main ) {
			fprintf(stderr, "Index Open Error %s\n", argv[2]);
			exit (1);
		}
	} else
		main = NULL;

	//	fire off threads

	if( cnt )
	  for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 14];
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
	bt_poolaudit(mgr, "cache");

	if( main )
		bt_poolaudit(main, "main");

	fprintf(stderr, "cache %d reads %d writes %d found\n", mgr->reads, mgr->writes, mgr->found);
	if( main )
		fprintf(stderr, "main %d reads %d writes %d found\n", main->reads, main->writes, main->found);

	if( main )
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
