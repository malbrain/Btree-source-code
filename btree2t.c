// btree version 2t
// 04 FEB 2014

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
#include <stddef.h>
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

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw
#define BT_fl 0x6c66	// fl

#define BT_maxbits		24					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size

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

//	Define the length of the page and key pointers

#define BtId 6

//	Page key slot definition.

//	If BT_maxbits is 15 or less, you can save 2 bytes
//	for each key stored by making the first two uints
//	into ushorts.  You can also save 4 bytes by removing
//	the tod field from the key.

//	Keys are marked dead, but remain on the page until
//	cleanup is called.

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
	unsigned char bits;			// page size in bits
	unsigned char lvl:6;		// level of page
	unsigned char dirty:1;		// page is dirty
	unsigned char posted:1;		// page fence is posted
	unsigned char right[BtId];	// page number to right
	unsigned char fence[256];	// page fence key
} *BtPage;

//  The loadpage interface object

typedef struct {
	uid page_no;
	BtPage page;
} BtPageSet;

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
} BtHash;

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
	BtPage parent;		// current page's parent node (memory mapped/file IO)
	BtPage alloc;		// frame for alloc page  (memory mapped/file IO)
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	BtPage zero;		// zeroes frame buffer (never mapped)
	BtPage page;		// temporary page (memory mapped/file IO)
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	unsigned char *mem;	// frame, cursor, page memory buffer
	int nodecnt;		// highest page cache segment in use
	int nodemax;		// highest page cache segment allocated
	int hashmask;		// number of pages in segments - 1
	int hashsize;		// size of hash table
	int posted;			// last loadpage found posted key
	int found;			// last insert/delete found key
	BtHash *lrufirst;	// lru list head
	BtHash *lrulast;	// lru list tail
	ushort *cache;		// hash table for cached segments
	BtHash nodes[1];	// segment cache follows
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
extern BtDb *bt_open (char *name, uint mode, uint bits, uint cacheblk, uint pgblk);
extern BTERR  bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, uid id, uint tod);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len);
extern uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	Internal functions

BTERR bt_removepage (BtDb *bt, uid page_no, uint lvl, unsigned char *pagefence);

//  Helper functions to return slot values

extern BtKey bt_key (BtDb *bt, uint slot);
extern uid bt_uid (BtDb *bt, uint slot);
extern uint bt_tod (BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0
#define ROOT_page		1

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
//	page cleanup

//  Groups of pages from the btree are optionally
//  cached with memory mapping. A hash table is used to keep
//  track of the cached pages.  This behaviour is controlled
//  by the number of cache blocks parameter and pages per block
//	given to bt_open.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse.

//	Parent locks are obtained to prevent resplitting or deleting a node
//	before its fence is posted into its upper level.

//	Empty nodes are chained together through the ALLOC page and reused.

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

// place write, read, or parent lock on requested page_no.

BTERR bt_lockpage(BtDb *bt, uid page_no, BtLock mode)
{
off64_t off = page_no << bt->page_bits;
#ifdef unix
int flag = PROT_READ | ( bt->mode == BT_ro ? 0 : PROT_WRITE );
struct flock lock[1];
#else
uint flags = 0, len;
OVERLAPPED ovl[1];
#endif

	if( mode == BtLockRead || mode == BtLockWrite )
		off +=  1 * sizeof(*bt->page);	// use second segment

	if( mode == BtLockParent )
		off +=  2 * sizeof(*bt->page);	// use third segment

#ifdef unix
	memset (lock, 0, sizeof(lock));

	lock->l_start = off;
	lock->l_type = (mode == BtLockDelete || mode == BtLockWrite || mode == BtLockParent) ? F_WRLCK : F_RDLCK;
	lock->l_len = sizeof(*bt->page);
	lock->l_whence = 0;

	if( fcntl (bt->idx, F_SETLKW, lock) < 0 )
		return bt->err = BTERR_lock;

	return 0;
#else
	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = (uint)(off >> 32);
	ovl->Offset = (uint)off;
	len = sizeof(*bt->page);

	//	use large offsets to
	//	simulate advisory locking

	ovl->OffsetHigh |= 0x80000000;

	if( mode == BtLockDelete || mode == BtLockWrite || mode == BtLockParent )
		flags |= LOCKFILE_EXCLUSIVE_LOCK;

	if( LockFileEx (bt->idx, flags, 0, len, 0L, ovl) )
		return bt->err = 0;

	return bt->err = BTERR_lock;
#endif 
}

// remove write, read, or parent lock on requested page_no.

BTERR bt_unlockpage(BtDb *bt, uid page_no, BtLock mode)
{
off64_t off = page_no << bt->page_bits;
#ifdef unix
struct flock lock[1];
#else
OVERLAPPED ovl[1];
uint len;
#endif

	if( mode == BtLockRead || mode == BtLockWrite )
		off +=  1 * sizeof(*bt->page);	// use second segment

	if( mode == BtLockParent )
		off +=  2 * sizeof(*bt->page);	// use third segment

#ifdef unix
	memset (lock, 0, sizeof(lock));

	lock->l_start = off;
	lock->l_type = F_UNLCK;
	lock->l_len = sizeof(*bt->page);
	lock->l_whence = 0;

	if( fcntl (bt->idx, F_SETLK, lock) < 0 )
		return bt->err = BTERR_lock;
#else
	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = (uint)(off >> 32);
	ovl->Offset = (uint)off;
	len = sizeof(*bt->page);

	//	use large offsets to
	//	simulate advisory locking

	ovl->OffsetHigh |= 0x80000000;

	if( !UnlockFileEx (bt->idx, 0, len, 0, ovl) )
		return GetLastError(), bt->err = BTERR_lock;
#endif

	return bt->err = 0;
}

//	close and release memory

void bt_close (BtDb *bt)
{
BtHash *hash;
#ifdef unix
	// release mapped pages

	if( hash = bt->lrufirst )
		do munmap (hash->page, (bt->hashmask+1) << bt->page_bits);
		while(hash = hash->lrunext);

	if ( bt->mem )
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

	if ( bt->mem)
		VirtualFree (bt->mem, 0, MEM_RELEASE);
	FlushFileBuffers(bt->idx);
	CloseHandle(bt->idx);
	GlobalFree (bt->cache);
	GlobalFree (bt);
#endif
}

//  open/create new btree
//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of mapped page cache (e.g. 8192) or zero for no mapping.

BtDb *bt_open (char *name, uint mode, uint bits, uint nodemax, uint pgblk)
{
uint lvl, attr, cacheblk, last;
BtLock lockmode = BtLockWrite;
BtPage alloc;
off64_t size;
uint amt[1];
BtDb* bt;

#ifndef unix
SYSTEM_INFO sysinfo[1];
#endif

#ifdef unix
	bt = malloc (sizeof(BtDb) + nodemax * sizeof(BtHash));
	memset (bt, 0, sizeof(BtDb));

	switch (mode & 0x7fff)
	{
	case BT_fl:
	case BT_rw:
		bt->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);
		break;

	case BT_ro:
	default:
		bt->idx = open ((char*)name, O_RDONLY);
		lockmode = BtLockRead;
		break;
	}
	if( bt->idx == -1 )
		return free(bt), NULL;
	
	if( nodemax )
		cacheblk = 4096;	// page size for unix
	else
		cacheblk = 0;

#else
	bt = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtDb) + nodemax * sizeof(BtHash));
	attr = FILE_ATTRIBUTE_NORMAL;
	switch (mode & 0x7fff)
	{
	case BT_fl:
		attr |= FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING;

	case BT_rw:
		bt->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);
		break;

	case BT_ro:
	default:
		bt->idx = CreateFile(name, GENERIC_READ, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_EXISTING, attr, NULL);
		lockmode = BtLockRead;
		break;
	}
	if( bt->idx == INVALID_HANDLE_VALUE )
		return GlobalFree(bt), NULL;

	// normalize cacheblk to multiple of sysinfo->dwAllocationGranularity
	GetSystemInfo(sysinfo);

	if( nodemax )
		cacheblk = sysinfo->dwAllocationGranularity;
	else
		cacheblk = 0;
#endif

	// determine sanity of page size

	if( bits > BT_maxbits )
		bits = BT_maxbits;
	else if( bits < BT_minbits )
		bits = BT_minbits;

	if ( bt_lockpage(bt, ALLOC_page, lockmode) )
		return bt_close (bt), NULL;

#ifdef unix
	*amt = 0;

	// read minimum page size to get root info

	if( size = lseek (bt->idx, 0L, 2) ) {
		alloc = malloc (BT_minpage);
		pread(bt->idx, alloc, BT_minpage, 0);
		bits = alloc->bits;
		free (alloc);
	} else if( mode == BT_ro )
		return bt_close (bt), NULL;
#else
	size = GetFileSize(bt->idx, amt);

	if( size || *amt ) {
		alloc = VirtualAlloc(NULL, BT_minpage, MEM_COMMIT, PAGE_READWRITE);
		if( !ReadFile(bt->idx, (char *)alloc, BT_minpage, amt, NULL) )
			return bt_close (bt), NULL;
		bits = alloc->bits;
		VirtualFree (alloc, 0, MEM_RELEASE);
	} else if( mode == BT_ro )
		return bt_close (bt), NULL;
#endif

	bt->page_size = 1 << bits;
	bt->page_bits = bits;

	bt->nodemax = nodemax;
	bt->mode = mode;

	// setup cache mapping

	if( cacheblk ) {
		if( cacheblk < bt->page_size )
			cacheblk = bt->page_size;

		bt->hashsize = nodemax / 8;
		bt->hashmask = (cacheblk >> bits) - 1;
		bt->mapped_io = 1;
	}

	//	requested number of pages per memmap segment

	if( cacheblk )
	  if( (1 << pgblk) > bt->hashmask )
		bt->hashmask = (1 << pgblk) - 1;

	bt->seg_bits = 0;

	while( (1 << bt->seg_bits) <= bt->hashmask )
		bt->seg_bits++;

#ifdef unix
	bt->mem = malloc (7 *bt->page_size);
	bt->cache = calloc (bt->hashsize, sizeof(ushort));
#else
	bt->mem = VirtualAlloc(NULL, 7 * bt->page_size, MEM_COMMIT, PAGE_READWRITE);
	bt->cache = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, bt->hashsize * sizeof(ushort));
#endif
	bt->frame = (BtPage)bt->mem;
	bt->cursor = (BtPage)(bt->mem + bt->page_size);
	bt->page = (BtPage)(bt->mem + 2 * bt->page_size);
	bt->alloc = (BtPage)(bt->mem + 3 * bt->page_size);
	bt->temp = (BtPage)(bt->mem + 4 * bt->page_size);
	bt->parent = (BtPage)(bt->mem + 5 * bt->page_size);
	bt->zero = (BtPage)(bt->mem + 6 * bt->page_size);

	if( size || *amt ) {
		if ( bt_unlockpage(bt, ALLOC_page, lockmode) )
			return bt_close (bt), NULL;

		return bt;
	}

	// initializes an empty b-tree with root page and page of leaves

	memset (bt->alloc, 0, bt->page_size);
	bt_putid(bt->alloc->right, MIN_lvl+1);
	bt->alloc->bits = bt->page_bits;

#ifdef unix
	if( write (bt->idx, bt->alloc, bt->page_size) < bt->page_size )
		return bt_close (bt), NULL;
#else
	if( !WriteFile (bt->idx, (char *)bt->alloc, bt->page_size, amt, NULL) )
		return bt_close (bt), NULL;

	if( *amt < bt->page_size )
		return bt_close (bt), NULL;
#endif

	memset (bt->frame, 0, bt->page_size);
	bt->frame->bits = bt->page_bits;
	bt->frame->posted = 1;

	for( lvl=MIN_lvl; lvl--; ) {
		slotptr(bt->frame, 1)->off = offsetof(struct BtPage_, fence);
		bt_putid(slotptr(bt->frame, 1)->id, lvl ? MIN_lvl - lvl + 1 : 0);		// next(lower) page number
		bt->frame->fence[0] = 2;
		bt->frame->fence[1] = 0xff;
		bt->frame->fence[2] = 0xff;
		bt->frame->min = bt->page_size;
		bt->frame->lvl = lvl;
		bt->frame->cnt = 1;
		bt->frame->act = 1;
#ifdef unix
		if( write (bt->idx, bt->frame, bt->page_size) < bt->page_size )
			return bt_close (bt), NULL;
#else
		if( !WriteFile (bt->idx, (char *)bt->frame, bt->page_size, amt, NULL) )
			return bt_close (bt), NULL;

		if( *amt < bt->page_size )
			return bt_close (bt), NULL;
#endif
	}

	// create empty page area by writing last page of first
	// cache area (other pages are zeroed by O/S)

	if( bt->mapped_io && bt->hashmask ) {
		memset(bt->frame, 0, bt->page_size);
		last = bt->hashmask;

		while( last < MIN_lvl + 1 )
			last += bt->hashmask + 1;
#ifdef unix
		pwrite(bt->idx, bt->frame, bt->page_size, last << bt->page_bits);
#else
		SetFilePointer (bt->idx, last << bt->page_bits, NULL, FILE_BEGIN);
		if( !WriteFile (bt->idx, (char *)bt->frame, bt->page_size, amt, NULL) )
			return bt_close (bt), NULL;
		if( *amt < bt->page_size )
			return bt_close (bt), NULL;
#endif
	}

	if( bt_unlockpage(bt, ALLOC_page, lockmode) )
		return bt_close (bt), NULL;

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

//  Update current page of btree by writing file contents
//	or flushing mapped area to disk.

BTERR bt_update (BtDb *bt, BtPage page, uid page_no)
{
off64_t off = page_no << bt->page_bits;

#ifdef unix
    if ( !bt->mapped_io )
	 if ( pwrite(bt->idx, page, bt->page_size, off) != bt->page_size )
		 return bt->err = BTERR_wrt;
#else
uint amt[1];
	if ( !bt->mapped_io )
	{
		SetFilePointer (bt->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);
		if( !WriteFile (bt->idx, (char *)page, bt->page_size, amt, NULL) )
			return GetLastError(), bt->err = BTERR_wrt;

		if( *amt < bt->page_size )
			return GetLastError(), bt->err = BTERR_wrt;
	} 
	else if ( bt->mode == BT_fl ) {
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
off64_t off = (page_no & ~bt->hashmask) << bt->page_bits;
off64_t limit = off + ((bt->hashmask+1) << bt->page_bits);
BtHash *node;

	memset(hash, 0, sizeof(BtHash));
	hash->page_no = (page_no & ~bt->hashmask);
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
		FlushViewOfFile(hash->page, 0);
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
	if ( pread(bt->idx, *page, bt->page_size, off) < bt->page_size )
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

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
uid new_page;
char *pmap;
int reuse;

	// lock page zero

	if ( bt_lockpage(bt, ALLOC_page, BtLockWrite) )
		return 0;

	if( bt_mappage (bt, &bt->alloc, ALLOC_page) )
		return 0;

	// use empty chain first
	// else allocate empty page

	if( new_page = bt_getid(bt->alloc[1].right) ) {
		if( bt_mappage (bt, &bt->temp, new_page) )
			return 0;	// don't unlock on error
		memcpy(bt->alloc[1].right, bt->temp->right, BtId);
		reuse = 1;
	} else {
		new_page = bt_getid(bt->alloc->right);
		bt_putid(bt->alloc->right, new_page+1);
		reuse = 0;
	}

	if( bt_update(bt, bt->alloc, ALLOC_page) )
		return 0;	// don't unlock on error

	if( !bt->mapped_io ) {
		if( bt_update(bt, page, new_page) )
			return 0;	//don't unlock on error

		// unlock page zero 

		if ( bt_unlockpage(bt, ALLOC_page, BtLockWrite) )
			return 0;

		return new_page;
	}

#ifdef unix
	if ( pwrite(bt->idx, page, bt->page_size, new_page << bt->page_bits) < bt->page_size )
		return bt->err = BTERR_wrt, 0;

	// if writing first page of hash block, zero last page in the block

	if ( !reuse && bt->hashmask > 0 && (new_page & bt->hashmask) == 0 )
	{
		// use temp buffer to write zeros
		memset(bt->zero, 0, bt->page_size);
		if ( pwrite(bt->idx,bt->zero, bt->page_size, (new_page | bt->hashmask) << bt->page_bits) < bt->page_size )
			return bt->err = BTERR_wrt, 0;
	}
#else
	//	bring new page into page-cache and copy page.
	//	this will extend the file into the new pages.

	if( !(pmap = (char*)bt_hashpage(bt, new_page & ~bt->hashmask)) )
		return 0;

	memcpy(pmap+((new_page & bt->hashmask) << bt->page_bits), page, bt->page_size);
#endif

	// unlock page zero 

	if ( bt_unlockpage(bt, ALLOC_page, BtLockWrite) )
		return 0;

	return new_page;
}

//  find slot in page for given key at a given level
//	return 0 if beyond fence value

int bt_findslot (BtPageSet *set, unsigned char *key, uint len)
{
uint diff, higher = set->page->cnt, low = 1, slot;

	//	is page being deleted?  if so,
	//	tell caller to follow right link

	if( !set->page->act )
		return 0;

	//	make stopper key an infinite fence value

	if( bt_getid (set->page->right) )
		higher++;

	//	low is the next candidate, higher is already
	//	tested as .ge. the given key, loop ends when they meet

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
uint mode, prevmode;
int posted = 1;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (lock == BtLockWrite) && (drill == lvl) ? BtLockWrite : BtLockRead; 

	set->page_no = page_no;

 	// obtain access lock using lock chaining

	if( page_no > ROOT_page )
	  if( bt_lockpage(bt, page_no, BtLockAccess) )
		return 0;									

	if( prevpage )
	  if( bt_unlockpage(bt, prevpage, prevmode) )
		return 0;

 	// obtain read lock using lock chaining

	if( bt_lockpage(bt, page_no, mode) )
		return 0;									

	if( page_no > ROOT_page )
	  if( bt_unlockpage(bt, page_no, BtLockAccess) )
		return 0;									

	//	map/obtain page contents

	if( bt_mappage (bt, &set->page, page_no) )
		return 0;

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if ( page_no != ROOT_page )
			return bt->err = BTERR_struct, 0;
			
		drill = set->page->lvl;

		if( lock == BtLockWrite && drill == lvl )
		  if( bt_unlockpage(bt, page_no, mode) )
			return 0;
		  else
			continue;
	}

	prevpage = page_no;
	prevmode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( slot = bt_findslot (set, key, len) ) {
	  if( drill == lvl )
		return bt->posted = posted, slot;

	  if( slot > set->page->cnt )
		return bt->err = BTERR_struct, 0;

	//  if drilling down, find next active key

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
			continue;
		else
			return bt->err = BTERR_struct, 0;

	  page_no = bt_getid(slotptr(set->page, slot)->id);
	  posted = 1;
	  drill--;
	  continue;
	}

	//  or slide right into next page
	//  (slide left from deleted page)

	page_no = bt_getid(set->page->right);
	posted = 0;

  } while( page_no );

  // return error on end of right chain

  bt->err = BTERR_struct;
  return 0;	// return error
}

// drill down fixing fence values for left sibling tree
//	starting with current left page in bt->temp
//  call with bt->temp write locked
//	return with all pages unlocked.

BTERR bt_fixfences (BtDb *bt, uid page_no, unsigned char *newfence)
{
unsigned char oldfence[256];
uid prevpage = 0;
int chk;

  memcpy (oldfence, bt->temp->fence, 256);

  //  start at left page of btree and drill down
  //  to update their fence values

  do {
 	// obtain access lock using lock chaining

	if( prevpage ) {
	  if( bt_unlockpage(bt, prevpage, BtLockWrite) )
		return bt->err;
	  if( bt_unlockpage(bt, prevpage, BtLockParent) )
		return bt->err;

	  // obtain parent/fence key maintenance lock

	  if( bt_lockpage(bt, page_no, BtLockParent) )
		return bt->err;									

	  if( bt_lockpage(bt, page_no, BtLockAccess) )
		return bt->err;									

	  if( bt_unlockpage(bt, prevpage, BtLockWrite) )
		return bt->err;

 	  // obtain write lock using lock chaining

	  if( bt_lockpage(bt, page_no, BtLockWrite) )
		return bt->err;									

	  if( bt_unlockpage(bt, page_no, BtLockAccess) )
		return bt->err;									

	  //  map/obtain page contents

	  if( bt_mappage (bt, &bt->temp, page_no) )
		return bt->err;
	}

	chk = keycmp ((BtKey)bt->temp->fence, oldfence + 1, *oldfence);
    prevpage = page_no;

	if( chk < 0 ) {
		page_no = bt_getid (bt->temp->right);
		continue;
	}

	if( chk > 0 )
		return bt->err = BTERR_struct;

	memcpy (bt->temp->fence, newfence, 256);

	if( bt_update (bt, bt->temp, page_no) )
		return bt->err;

	//  return when we reach a leaf page

	if( !bt->temp->lvl ) {
		if( bt_unlockpage (bt, page_no, BtLockWrite) )
			return bt->err;
		return bt_unlockpage (bt, page_no, BtLockParent);
	}

	page_no = bt_getid(slotptr(bt->temp, bt->temp->cnt)->id);

  } while( page_no );

  // return error on end of right chain

  return bt->err = BTERR_struct;
}

//	return page to free list
//	page must be delete & write locked

BTERR bt_freepage (BtDb *bt, BtPage page, uid page_no)
{
	//	lock & map allocation page

	if( bt_lockpage (bt, ALLOC_page, BtLockWrite) )
		return bt->err;

	if( bt_mappage (bt, &bt->alloc, ALLOC_page) )
		return bt->err;

	//	store chain in second right
	bt_putid(page->right, bt_getid(bt->alloc[1].right));
	bt_putid(bt->alloc[1].right, page_no);

	if( bt_update(bt, bt->alloc, ALLOC_page) )
		return bt->err;
	if( bt_update(bt, page, page_no) )
		return bt->err;

	// unlock page zero 

	if( bt_unlockpage(bt, ALLOC_page, BtLockWrite) )
		return bt->err;

	//  remove write lock on deleted node

	if( bt_unlockpage(bt, page_no, BtLockWrite) )
		return bt->err;

	return bt_unlockpage (bt, page_no, BtLockDelete);
}

//	remove the root level by promoting its only child

BTERR bt_removeroot (BtDb *bt, BtPage root, BtPage child, uid page_no)
{
uid next = 0;

  do {
	if( next ) {
	  if( bt_lockpage (bt, next, BtLockDelete) )
		return bt->err;
	  if( bt_lockpage (bt, next, BtLockWrite) )
		return bt->err;

	  if( bt_mappage (bt, &child, next) )
		return bt->err;

	  page_no = next;
	}

	memcpy (root, child, bt->page_size);
	next = bt_getid (slotptr(child, child->cnt)->id);

	if( bt_freepage (bt, child, page_no) )
		return bt->err;
  } while( root->lvl > 1 && root->cnt == 1 );

  if( bt_update (bt, root, ROOT_page) )
	return bt->err;

  return bt_unlockpage (bt, ROOT_page, BtLockWrite);
}

//  pull right page over ourselves in simple merge

BTERR bt_mergeright (BtDb *bt, uid page_no, BtPageSet *parent, uint slot)
{
uid right;
uint idx;

	// find our right neighbor
	//	right must exist because the stopper prevents
	//	the rightmost page from deleting

	for( idx = slot; idx++ < parent->page->cnt; )
	  if( !slotptr(parent->page, idx)->dead )
		break;

	right = bt_getid (slotptr (parent->page, idx)->id);

	if( right != bt_getid (bt->page->right) )
		return bt->err = BTERR_struct;

	if( bt_lockpage (bt, right, BtLockDelete) )
		return bt->err;

	if( bt_lockpage (bt, right, BtLockWrite) )
		return bt->err;

	if( bt_mappage (bt, &bt->temp, right) )
		return bt->err;

	memcpy (bt->page, bt->temp, bt->page_size);

	if( bt_update(bt, bt->page, page_no) )
		return bt->err;

	//  install ourselves as child page
	//	and delete ourselves from parent

	bt_putid (slotptr(parent->page, idx)->id, page_no);
	slotptr(parent->page, slot)->dead = 1;
	parent->page->act--;

	//	collapse any empty slots

	while( idx = parent->page->cnt - 1 )
	  if( slotptr(parent->page, idx)->dead ) {
		*slotptr(parent->page, idx) = *slotptr(parent->page, idx + 1);
		memset (slotptr(parent->page, parent->page->cnt--), 0, sizeof(BtSlot));
	  } else
		  break;

	if( bt_freepage (bt, bt->temp, right) )
		return bt->err;

	//	do we need to remove a btree level?
	//	(leave the first page of leaves alone)

	if( parent->page_no == ROOT_page && parent->page->cnt == 1 )
	  if( bt->page->lvl )
		return bt_removeroot (bt, parent->page, bt->page, page_no);

	if( bt_update (bt, parent->page, parent->page_no) )
		return bt->err;

	if( bt_unlockpage (bt, parent->page_no, BtLockWrite) )
		return bt->err;

	if( bt_unlockpage (bt, page_no, BtLockWrite) )
		return bt->err;

	if( bt_unlockpage (bt, page_no, BtLockDelete) )
		return bt->err;

	return 0;
}

//	remove both child and parent from the btree
//	from the fence position in the parent

BTERR bt_removeparent (BtDb *bt, uid page_no, BtPageSet *parent, uint lvl)
{
unsigned char rightfence[256], pagefence[256];
uid right, ppage_no;

	right = bt_getid (bt->page->right);

	if( bt_lockpage (bt, right, BtLockParent) )
		return bt->err;

	if( bt_lockpage (bt, right, BtLockAccess) )
		return bt->err;

	if( bt_lockpage (bt, right, BtLockWrite) )
		return bt->err;

	if( bt_unlockpage (bt, right, BtLockAccess) )
		return bt->err;

	if( bt_mappage (bt, &bt->temp, right) )
		return bt->err;

	//	save right page fence value and
	//	parent fence value

	memcpy (rightfence, bt->temp->fence, 256);
	memcpy (pagefence, parent->page->fence, 256);
	ppage_no = parent->page_no;

	//  pull right sibling over ourselves and unlock

	memcpy (bt->page, bt->temp, bt->page_size);

	if( bt_update(bt, bt->page, page_no) )
		return bt->err;

	if( bt_unlockpage (bt, page_no, BtLockDelete) )
		return bt->err;

	if( bt_unlockpage (bt, page_no, BtLockWrite) )
		return bt->err;

	//  install ourselves into right link from old right page

	bt->temp->act = 0;		// tell bt_findslot to go right (left)
	bt_putid (bt->temp->right, page_no);

	if( bt_update (bt, bt->temp, right) )
		return bt->err;

	if( bt_unlockpage (bt, right, BtLockWrite) )
		return bt->err;

	//	remove our slot from our parent
	//	clear act to signal bt_findslot to move right

	slotptr(parent->page, parent->page->cnt)->dead = 1;
	parent->page->act = 0;

	if( bt_update (bt, parent->page, ppage_no) )
		return bt->err;
	if( bt_unlockpage (bt, ppage_no, BtLockWrite) )
		return bt->err;

	//	redirect right page pointer in its parent to our left
	//	and free the right page

	if( bt_insertkey (bt, rightfence+1, *rightfence, lvl, page_no, time(NULL)) )
		return bt->err;

	if( bt_removepage (bt, ppage_no, lvl, pagefence) )
		return bt->err;

	if( bt_unlockpage (bt, right, BtLockParent) )
		return bt->err;

	//	wait for others to drain away

	if( bt_lockpage (bt, right, BtLockDelete) )
		return bt->err;

	if( bt_lockpage (bt, right, BtLockWrite) )
		return bt->err;

	if( bt_mappage (bt, &bt->temp, right) )
		return bt->err;

	return bt_freepage (bt, bt->temp, right);
}

//	remove page from btree
//	call with page unlocked
//	returns with page on free list

BTERR bt_removepage (BtDb *bt, uid page_no, uint lvl, unsigned char *pagefence)
{
BtPageSet parent[1];
uint slot, idx;
BtKey ptr;
uid left;

	parent->page = bt->parent;

	//	load and lock our parent

retry:
	if( !(slot = bt_loadpage (bt, parent, pagefence+1, *pagefence, lvl+1, BtLockWrite)) )
		return bt->err;

	//	wait until we are posted in our parent

	if( !bt->posted ) {
		if( bt_unlockpage (bt, parent->page_no, BtLockWrite) )
			return bt->err;
#ifdef unix
		sched_yield();
#else
		SwitchToThread();
#endif
		goto retry;
	}

	//	wait for others to finish
	//	and obtain final WriteLock

	if( bt_lockpage (bt, page_no, BtLockDelete) )
		return bt->err;

	if( bt_lockpage (bt, page_no, BtLockWrite) )
		return bt->err;

	if( bt_mappage (bt, &bt->page, page_no) )
		return bt->err;

	//  was page re-established?

	if( bt->page->act ) {
		if( bt_unlockpage (bt, page_no, BtLockWrite) )
			return bt->err;
		if( bt_unlockpage (bt, page_no, BtLockDelete) )
			return bt->err;

		return bt_unlockpage (bt, parent->page_no, BtLockWrite);
	}
		
	//	can we do a simple merge entirely
	//	between siblings on the parent page?

	if( slot < parent->page->cnt )
		return bt_mergeright(bt, page_no, parent, slot);

	//  find our left neighbor in our parent page

	for( idx = slot; --idx; )
	  if( !slotptr(parent->page, idx)->dead )
		break;

	//	if no left neighbor, delete ourselves and our parent

	if( !idx )
		return bt_removeparent (bt, page_no, parent, lvl+1);

	// lock and map our left neighbor's page

	left = bt_getid (slotptr(parent->page, idx)->id);

	//	wait our turn on fence key maintenance

	if( bt_lockpage(bt, left, BtLockParent) )
		return bt->err;

	if( bt_lockpage(bt, left, BtLockAccess) )
		return bt->err;

	if( bt_lockpage(bt, left, BtLockWrite) )
		return bt->err;

	if( bt_unlockpage(bt, left, BtLockAccess) )
		return bt->err;

	if( bt_mappage (bt, &bt->temp, left) )
		return bt->err;

	//  wait until sibling is in our parent

	if( bt_getid (bt->temp->right) != page_no ) {
		if( bt_unlockpage (bt, parent->page_no, BtLockWrite) )
			return bt->err;
		if( bt_unlockpage (bt, left, BtLockWrite) )
			return bt->err;
		if( bt_unlockpage (bt, left, BtLockParent) )
			return bt->err;
		if( bt_unlockpage (bt, page_no, BtLockWrite) )
			return bt->err;
		if( bt_unlockpage (bt, page_no, BtLockDelete) )
			return bt->err;
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

	bt_putid (slotptr(parent->page, slot)->id, left);

	//	update left page with our fence key

	memcpy (bt->temp->right, bt->page->right, BtId);

	//	collapse dead slots from parent

	while( idx = parent->page->cnt - 1 )
	  if( slotptr(parent->page, idx)->dead ) {
		*slotptr(parent->page, idx) = *slotptr(parent->page, parent->page->cnt);
		memset (slotptr(parent->page, parent->page->cnt--), 0, sizeof(BtSlot));
	  } else
		  break;

	//  update parent page

	if( bt_update (bt, parent->page, parent->page_no) )
		return bt->err;

	//	go down the left node's fence keys to the leaf level
	//	and update the fence keys in each page

	if( bt_fixfences (bt, left, pagefence) )
		return bt->err;

	if( bt_unlockpage (bt, parent->page_no, BtLockWrite) )
		return bt->err;

	//  free our original page

	if( bt_mappage (bt, &bt->temp, page_no) )
		return bt->err;

	return bt_freepage (bt, bt->temp,page_no);
}

//  find and delete key on page by marking delete flag bit
//  when page becomes empty, delete it

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len)
{
unsigned char pagefence[256];
uint slot, found, act, idx;
BtPageSet set[1];
BtKey ptr;

	set->page = bt->page;

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

		  //	collapse empty slots

		  while( idx = set->page->cnt - 1 )
			if( slotptr(set->page, idx)->dead ) {
			  *slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			  memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
			} else
				break;

		  if( bt_update(bt, set->page, set->page_no) )
			return bt->err;
		}

	// delete page when empty

	memcpy (pagefence, set->page->fence, 256);
	act = set->page->act;

	if( bt_unlockpage(bt, set->page_no, BtLockWrite) )
		return bt->err;

	if( !act )
	  if( bt_removepage (bt, set->page_no, 0, pagefence) )
		return bt->err;

	bt->found = found;
	return 0;
}

//	find key in leaf level and return row-id

uid bt_findkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint  slot;
uid id = 0;
BtKey ptr;

	set->page = bt->page;

	if( slot = bt_loadpage (bt, set, key, len, 0, BtLockRead) )
		ptr = keyptr(set->page, slot);
	else
		return 0;

	// if key exists, return row-id
	//	otherwise return 0

	if( slot <= set->page->cnt )
	  if( !keycmp (ptr, key, len) )
		id = bt_getid(slotptr(set->page,slot)->id);

	if ( bt_unlockpage(bt, set->page_no, BtLockRead) )
		return 0;

	return id;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0 - new slot value
 
uint bt_cleanpage(BtDb *bt, BtPage page, uint amt, uint slot)
{
uint nxt = bt->page_size, off;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey key;

	if( page->min >= (max+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return slot;

	//	skip cleanup if nothing to reclaim

	if( !page->dirty )
		return 0;

	memcpy (bt->frame, page, bt->page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, bt->page_size - sizeof(*page));
	page->dirty = 0;
	page->act = 0;

	while( cnt++ < max ) {
		if( cnt == slot )
			newslot = idx + 1;
		if( slotptr(bt->frame,cnt)->dead )
			continue;

		// copy key
		if( !page->lvl || cnt < max ) {
			key = keyptr(bt->frame, cnt);
			off = nxt -= key->len + 1;
			memcpy ((unsigned char *)page + nxt, key, key->len + 1);
		} else
			off = offsetof(struct BtPage_, fence);

		// copy slot
		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = off;
		page->act++;
	}
	page->min = nxt;
	page->cnt = idx;

	if( page->min >= (idx+1) * sizeof(BtSlot) + sizeof(*page) + amt + 1 )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtDb *bt, BtPageSet *root, uid page_no2)
{
unsigned char leftkey[256];
uint nxt = bt->page_size;
uid new_page;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	memcpy (leftkey, root->page->fence, 256);
	root->page->posted = 1;

	if( !(new_page = bt_newpage(bt, root->page)) )
		return bt->err;

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, bt->page_size - sizeof(*root->page));
	memset(root->page->fence, 0, 256);
	root->page->fence[0] = 2;
	root->page->fence[1] = 0xff;
	root->page->fence[2] = 0xff;

	// insert new page fence key on newroot page

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

	// update and release root

	if( bt_update(bt, root->page, root->page_no) )
		return bt->err;

	return bt_unlockpage(bt, root->page_no, BtLockWrite);
}

//  split already locked full node
//	return unlocked.

BTERR bt_splitpage (BtDb *bt, BtPageSet *set)
{
uint cnt = 0, idx = 0, max, nxt = bt->page_size, off;
uid right, page_no = set->page_no;
unsigned char fencekey[256];
uint lvl = set->page->lvl;
BtKey key;

	//  split higher half of keys to bt->frame

	memset (bt->frame, 0, bt->page_size);
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

	if( page_no == ROOT_page )
		bt->frame->posted = 1;

	memcpy (bt->frame->fence, set->page->fence, 256);
	bt->frame->bits = bt->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	// link right node

	if( page_no > ROOT_page )
		memcpy (bt->frame->right, set->page->right, BtId);

	//	get new free page and write higher keys to it.

	if( !(right = bt_newpage(bt, bt->frame)) )
		return bt->err;

	//	update lower keys to continue in old page

	memcpy (bt->frame, set->page, bt->page_size);
	memset (set->page+1, 0, bt->page_size - sizeof(*set->page));
	nxt = bt->page_size;
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

	if( page_no == ROOT_page )
		return bt_splitroot (bt, set, right);

	if( bt_update (bt, set->page, page_no) )
		return bt->err; 

	if( bt_unlockpage (bt, page_no, BtLockWrite) )
		return bt->err;

	// insert new fences in their parent pages

	while( 1 ) {
		if( bt_lockpage (bt, page_no, BtLockParent) )
			return bt->err;

		if( bt_lockpage (bt, page_no, BtLockRead) )
			return bt->err;

		if( bt_mappage (bt, &set->page, page_no) )
			return bt->err;

		memcpy (fencekey, set->page->fence, 256);

		if( set->page->posted ) {
		  if( bt_unlockpage (bt, page_no, BtLockParent) )
			return bt->err;
			
		  return bt_unlockpage (bt, page_no, BtLockRead);
		}

		if( bt_unlockpage (bt, page_no, BtLockRead) )
			return bt->err;

		if( bt_insertkey (bt, fencekey+1, *fencekey, lvl+1, page_no, time(NULL)) )
			return bt->err;

		if( bt_lockpage (bt, page_no, BtLockWrite) )
			return bt->err;

		if( bt_mappage (bt, &set->page, page_no) )
			return bt->err;

		right = bt_getid (set->page->right);
		set->page->posted = 1;

		if( bt_update (bt, set->page, page_no) )
			return bt->err; 

		if( bt_unlockpage (bt, page_no, BtLockWrite) )
			return bt->err;

		if( bt_unlockpage (bt, page_no, BtLockParent) )
			return bt->err;

		if( !(page_no = right) )
			break;

		if( bt_mappage (bt, &set->page, page_no) )
			return bt->err;
	}

	return 0;
}

//  Insert new key into the btree at requested level.

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, uid id, uint tod)
{
BtPageSet set[1];
uint slot, idx;
BtKey ptr;

  set->page = bt->page;

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

		if ( bt_update(bt, set->page, set->page_no) )
			return bt->err;

		return bt_unlockpage(bt, set->page_no, BtLockWrite);
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

  if( bt_update(bt, set->page, set->page_no) )
	return bt->err;

  return bt_unlockpage(bt, set->page_no, BtLockWrite);
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint slot;

	set->page = bt->page;

	// cache page for retrieval
	if( slot = bt_loadpage (bt, set, key, len, 0, BtLockRead) )
		memcpy (bt->cursor, set->page, bt->page_size);
	bt->cursor_page = set->page_no;
	if ( bt_unlockpage(bt, set->page_no, BtLockRead) )
		return 0;

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
	  else if( right || (slot < bt->cursor->cnt)) // skip infinite stopper
		return slot;
	  else
		break;

	if( !right )
		break;

	bt->cursor_page = right;
	set->page = bt->page;

    if( bt_lockpage(bt, right, BtLockRead) )
		return 0;

	if( bt_mappage (bt, &set->page, right) )
		break;

	memcpy (bt->cursor, set->page, bt->page_size);

	if( bt_unlockpage(bt, right, BtLockRead) )
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
//  standalone program to index file of keys
//  then list them onto std-out

int main (int argc, char **argv)
{
uint slot, line = 0, off = 0, found = 0;
int dead, ch, cnt = 0, bits = 12;
unsigned char key[256];
clock_t done, start;
uint pgblk = 0;
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

	start = clock();
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

	bt = bt_open ((argv[1]), BT_rw, bits, map, pgblk);

	if( !bt ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	}

	switch(argv[3][0]| 0x20)
	{
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
		fprintf(stderr, "finished adding keys, %d \n", line);
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
			  if( bt_deletekey (bt, key, len) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < 245 )
				key[len++] = ch;
		fprintf(stderr, "finished deleting keys, %d \n", line);
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
		fprintf(stderr, "finished search of %d keys, found %d\n", line, found);
		break;

	case 's':
		scan++;
		break;

	}

	done = clock();
	fprintf(stderr, " Time to complete: %.2f seconds\n", (float)(done - start) / CLOCKS_PER_SEC);

	dead = cnt = 0;
	len = key[0] = 0;

	fprintf(stderr, "started reading\n");

	if( slot = bt_startkey (bt, key, len) )
	  slot--;
	else
	  fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

	while( slot = bt_nextkey (bt, slot) )
	  if( cnt++, scan ) {
			ptr = bt_key(bt, slot);
			fwrite (ptr->key, ptr->len, 1, stdout);
			fputc ('\n', stdout);
	  }

	fprintf(stderr, " Total keys read %d\n", cnt);
	return 0;
}

#endif	//STANDALONE
