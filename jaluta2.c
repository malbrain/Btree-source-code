// jaluta's balanced B-Link tree algorithms
// 26 NOV 2013

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
// http://code.google.com/p/high-concurrency-btree

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

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw
#define BT_fl 0x6c66	// fl

#define BT_maxbits		24					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size

#define BT_hashsize		512		// size of hash index for page cache
#define BT_hashprime	8191	// prime number for hashing

typedef enum{
	BtLockShared	= 1,
	BtLockUpdate	= 2,
	BtLockXclusive	= 3,
	BtLockUpgrade	= 4,
}BtLock;

//	Define the length of the page and key pointers

#define BtId 6

//	Page key slot definition.

//	If BT_maxbits is 16 or less, you can save 4 bytes
//	for each key stored by making the first two uints
//	into ushorts.  You can also save 4 bytes by removing
//	the tod field from the key.

typedef struct {
	uint off;				// page offset for key start
	uint tod;				// time-stamp for key
	unsigned char id[BtId];	// id associated with key
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by up to
//	255 value bytes.

typedef struct {
	unsigned char len;
	unsigned char key[255];
} *BtKey;

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct {
	uint cnt;					// count of keys in page
	uint min;					// next key offset
	unsigned char lvl:3;		// level of page
	unsigned char bits:5;		// page size in bits
	unsigned char fence;		// len of fence key at top of page
	unsigned char right[BtId];	// page number to right
	BtSlot slots[0];			// page slots
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

//	The object structure for Btree access

typedef struct {
	uint page_size;		// each page size	
	uint page_bits;		// each page size in bits	
	uid parentpage;		// current parent page number	
	uid cursorpage;		// current cursor page number	
	uid childpage;		// current child page number	
	int  err;
	uint mode;			// read-write mode
	uint mapped_io;		// use memory mapping
	BtPage temp;		// temporary frame buffer (memory mapped/file IO)
	BtPage alloc;		// frame buffer for alloc page ( page 0 )
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	BtPage parent;		// current parent page
	BtPage child;		// current child page
	BtPage sibling;		// current sibling page
	BtPage sibling2;	// current sibling2 page
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	unsigned char *mem;			// frame, cursor, page memory buffer
	int nodecnt;				// highest page cache node in use
	int nodemax;				// highest page cache node allocated
	int hashmask;				// number of hash headers in cache - 1
	BtHash *lrufirst;			// lru list head
	BtHash *lrulast;			// lru list tail
	ushort cache[BT_hashsize];	// hash index for cache
	BtHash nodes[1];			// page cache follows
} BtDb;

typedef enum {
BTERR_ok = 0,
BTERR_struct,
BTERR_ovflw,
BTERR_lock,
BTERR_map,
BTERR_wrt,
BTERR_hash,
BTERR_restart
} BTERR;

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (char *name, uint mode, uint bits, uint cacheblk);
extern BTERR  bt_insertkey (BtDb *bt, unsigned char *key, uint len, uid id, uint tod);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len);
extern uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//  Helper functions to return slot values

extern BtKey bt_key (BtDb *bt, uint slot);
extern uid bt_uid (BtDb *bt, uint slot);
extern uint bt_tod (BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0
#define ROOT_page		1

//  The page is allocated from low and hi ends.
//  The key offsets and row-id's are allocated
//  from the bottom, while the text of the key
//  is allocated from the top.  When the two
//  areas meet, the page overflowns.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65535), and up to 253 bytes
//  of key value.  Duplicate keys are discarded.
//  Associated with each key is a 48 bit row-id.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	The b-tree pages at each level are linked
//	with next page to right to facilitate
//	cursors and provide for concurrency.

//	When to root page overflows, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//  Groups of pages from the btree are optionally
//  cached with memory mapping. A hash table is used to keep
//  track of the cached pages.  This behaviour is controlled
//  by the cache block size parameter to bt_open.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse.

//	Empty nodes are chained together through the ALLOC page and reused.

//	A special open mode of BT_fl is provided to safely access files on
//	WIN32 networks. WIN32 network operations should not use memory mapping.
//	This WIN32 mode sets FILE_FLAG_NOBUFFERING and FILE_FLAG_WRITETHROUGH
//	to prevent local caching of network file contents.

//	Access macros to address slot and key values from the page

#define slotptr(page, slot) ((page)->slots + slot - 1)
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

// place requested latch on requested page_no.
//		the Shared latch is a read lock over segment 0
//		the Update latch is a write lock over segment 1
//		the Xclusive latch is a write lock over segment 0 & 1
//		the Upgrade latch upgrades Update to Xclusive

BTERR bt_lockpage(BtDb *bt, uid page_no, BtLock mode)
{
off64_t off = page_no << bt->page_bits;
uint len = sizeof(*bt->parent);
uint type;
#ifdef unix
int flag = PROT_READ | ( bt->mode == BT_ro ? 0 : PROT_WRITE );
struct flock lock[1];
#else
uint flags = 0;
OVERLAPPED ovl[1];
#endif

	switch( mode ) {
	case BtLockShared:	// lock segment 0 w/read lock
		type = 0;
		break;

	case BtLockUpdate:	// lock segment 1 w/write lock
		off += sizeof(*bt->parent);
		type = 1;
		break;

	case BtLockXclusive:// lock both segments w/write lock
		len +=  sizeof(*bt->parent);
		type = 1;
		break;

	case BtLockUpgrade:	// lock segment 0 w/write lock
		type = 1;
		break;
	}

#ifdef unix
	memset (lock, 0, sizeof(lock));

	lock->l_start = off;
	lock->l_type = type ? F_WRLCK : F_RDLCK;
	lock->l_len = len;
	lock->l_whence = 0;

	if( fcntl (bt->idx, F_SETLKW, lock) < 0 )
		return bt->err = BTERR_lock;

	return 0;
#else
	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = (uint)(off >> 32);
	ovl->Offset = (uint)off;

	//	use large offsets to
	//	simulate advisory locking

	ovl->OffsetHigh |= 0x80000000;

	if( type = 1 )
		flags |= LOCKFILE_EXCLUSIVE_LOCK;

	if( LockFileEx (bt->idx, flags, 0, len, 0L, ovl) )
		return bt->err = 0;

	return bt->err = BTERR_lock;
#endif 
}

// remove lock on requested page_no.

BTERR bt_unlockpage(BtDb *bt, uid page_no, BtLock mode)
{
off64_t off = page_no << bt->page_bits;
uint len = sizeof(*bt->parent);
#ifdef unix
struct flock lock[1];
#else
OVERLAPPED ovl[1];
#endif

	switch( mode ) {
	case BtLockShared:	// unlock segment 0
		break;

	case BtLockUpdate:	// unlock segment 1
		off += sizeof(*bt->parent);
		break;

	case BtLockXclusive:// unlock both segments
		len += sizeof(*bt->parent);
		break;

	case BtLockUpgrade:	// unlock segment 0
		break;
	}

#ifdef unix
	memset (lock, 0, sizeof(lock));

	lock->l_start = off;
	lock->l_type = F_UNLCK;
	lock->l_len = len;
	lock->l_whence = 0;

	if( fcntl (bt->idx, F_SETLK, lock) < 0 )
		return bt->err = BTERR_lock;
#else
	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = (uint)(off >> 32);
	ovl->Offset = (uint)off;

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
	GlobalFree (bt);
#endif
}

//  open/create new btree
//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of mapped page cache (e.g. 8192) or zero for no mapping.

BtDb *bt_open (char *name, uint mode, uint bits, uint nodemax)
{
BtLock lockmode = BtLockXclusive;
uint lvl, attr, cacheblk;
BtPage alloc;
off64_t size;
uint amt[1];
BtKey key;
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
		lockmode = BtLockShared;
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
		lockmode = BtLockShared;
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

		bt->hashmask = (cacheblk >> bits) - 1;
		bt->mapped_io = 1;
	}

#ifdef unix
	bt->mem = malloc (8 *bt->page_size);
#else
	bt->mem = VirtualAlloc(NULL, 8 * bt->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
	bt->frame = (BtPage)bt->mem;
	bt->cursor = (BtPage)(bt->mem + bt->page_size);
	bt->alloc = (BtPage)(bt->mem + 2 * bt->page_size);
	bt->parent = (BtPage)(bt->mem + 3 * bt->page_size);
	bt->child = (BtPage)(bt->mem + 4 * bt->page_size);
	bt->temp = (BtPage)(bt->mem + 5 * bt->page_size);
	bt->sibling = (BtPage)(bt->mem + 6 * bt->page_size);
	bt->sibling2 = (BtPage)(bt->mem + 7 * bt->page_size);

	if( size || *amt ) {
		if ( bt_unlockpage(bt, ALLOC_page, lockmode) )
			return bt_close (bt), NULL;

		return bt;
	}

	// initialize an empty b-tree with alloc page & root page

	memset (bt->alloc, 0, bt->page_size);
	bt_putid(bt->alloc->right, ROOT_page + 1);
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

	//	write root page

	memset (bt->frame, 0, bt->page_size);
	bt->frame->bits = bt->page_bits;

	bt->frame->min = bt->page_size;
#ifdef unix
	if( write (bt->idx, bt->frame, bt->page_size) < bt->page_size )
		return bt_close (bt), NULL;
#else
	if( !WriteFile (bt->idx, (char *)bt->frame, bt->page_size, amt, NULL) )
		return bt_close (bt), NULL;

	if( *amt < bt->page_size )
		return bt_close (bt), NULL;
#endif

	// create initial empty page area by writing last page of first
	// cache area (other pages are zeroed by O/S)

	if( bt->mapped_io && bt->hashmask > 2 ) {
		memset(bt->frame, 0, bt->page_size);

#ifdef unix
		pwrite(bt->idx, bt->frame, bt->page_size, bt->hashmask << bt->page_bits);
#else
		SetFilePointer (bt->idx, bt->hashmask << bt->page_bits, NULL, FILE_BEGIN);
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

//  reset parent/child page pointers

void bt_resetpages (BtDb *bt)
{
	if( bt->mapped_io )
		return;

	bt->frame = (BtPage)bt->mem;
	bt->cursor = (BtPage)(bt->mem + bt->page_size);
	bt->alloc = (BtPage)(bt->mem + 2 * bt->page_size);
	bt->parent = (BtPage)(bt->mem + 3 * bt->page_size);
	bt->child = (BtPage)(bt->mem + 4 * bt->page_size);
	bt->temp = (BtPage)(bt->mem + 5 * bt->page_size);
	bt->sibling = (BtPage)(bt->mem + 6 * bt->page_size);
	bt->sibling2 = (BtPage)(bt->mem + 7 * bt->page_size);
}

//	return pointer to high key
//	or NULL if infinite value

BtKey bt_highkey (BtDb *bt, BtPage page)
{
	if( page->lvl )
	  if( bt_getid (page->right) )
		return keyptr(page, page->cnt);
	  else
		return NULL;

	if( bt_getid (page->right) )
		return ((BtKey)((unsigned char*)(page) + bt->page_size - page->fence));

	return NULL;
}

//	return pointer to slot key in index page
//	or NULL if infinite value

BtKey bt_slotkey (BtPage page, uint slot)
{
	if( slot < page->cnt || bt_getid (page->right) )
		return keyptr(page, slot);
	else
		return NULL;
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
	idx = (uint)(page_no * BT_hashprime % BT_hashsize);

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
uint idx = (uint)((page_no & ~bt->hashmask) * BT_hashprime % BT_hashsize);
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
uint idx = (uint)((node->page_no & ~bt->hashmask) * BT_hashprime % BT_hashsize);
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
	if( (long long int)hash->page == -1LL )
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

//	deallocate a deleted page 
//	place on free chain out of allocator page
//	page must already be BtLockXclusive and mapped

BTERR bt_freepage (BtDb *bt, BtPage page, uid page_no)
{
	//	lock allocation page

	if ( bt_lockpage(bt, ALLOC_page, BtLockUpdate) )
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

	if( bt_unlockpage(bt, ALLOC_page, BtLockUpdate) )
		return bt->err;

	return 0;
}

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
uid new_page;
char *pmap;
int reuse;

	// lock page zero

	if ( bt_lockpage(bt, ALLOC_page, BtLockUpdate) )
		return 0;

	if( bt_mappage (bt, &bt->alloc, ALLOC_page) )
		return 0;

	// use empty chain first
	// else allocate empty page

	if( new_page = bt_getid(bt->alloc[1].right) ) {
		if( bt_mappage (bt, &bt->temp, new_page) )
			return 0;	// don't unlock on error
		bt_putid(bt->alloc[1].right, bt_getid(bt->temp->right));
		reuse = 1;
	} else {
		new_page = bt_getid(bt->alloc->right);
		bt_putid(bt->alloc->right, new_page+1);
		reuse = 0;
	}

	if( bt_update(bt, bt->alloc, ALLOC_page) )
		return 0;	// don't unlock on error

	// unlock page zero 

	if ( bt_unlockpage(bt, ALLOC_page, BtLockUpdate) )
		return 0;

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
		memset(bt->temp, 0, bt->page_size);
		if ( pwrite(bt->idx,bt->temp, bt->page_size, (new_page | bt->hashmask) << bt->page_bits) < bt->page_size )
			return bt->err = BTERR_wrt, 0;
	}
#else
	//	bring new page into page-cache and copy page.
	//	this will extend the file into the new pages.

	if( !(pmap = (char*)bt_hashpage(bt, new_page & ~bt->hashmask)) )
		return 0;

	memcpy(pmap+((new_page & bt->hashmask) << bt->page_bits), page, bt->page_size);
#endif

	return new_page;
}

//  find slot in given page for given key

int bt_findslot (BtPage page, unsigned char *key, uint len)
{
uint diff, higher = page->cnt, low = 1, slot;
uint good = 0;

	//	make last key an infinite fence value

	if( !page->lvl || bt_getid (page->right) )
		higher++;
	else
		good++;

	//	low is the next candidate, higher is already
	//	tested as .ge. the given key, loop ends when they meet

	if( higher )
	  while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	  }

	//	return zero if key is beyond highkey value
	//	or page is empty

 	return good ? higher : 0;
}

//  split full parent node

BTERR bt_splitparent (BtDb *bt, unsigned char *key, uint len)
{
uint cnt = 0, idx = 0, max, nxt = bt->page_size;
uid parentpage = bt->parentpage, right;
BtPage page = bt->parent;
uid new_page;
BtKey ptr;

	//	upgrade parent latch to Xclusive

	if( bt_lockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	//  split higher half of keys to bt->frame

	memset (bt->frame, 0, bt->page_size);
	max = (int)page->cnt;
	cnt = max / 2;
	idx = 0;

	// link right sibling node into new right page

	right = bt_getid (page->right);
	bt_putid(bt->frame->right, right);

	//	record higher fence key in new right leaf page

	if( bt->frame->fence = page->fence ) {
		memcpy ((unsigned char *)bt->frame + bt->page_size - bt->frame->fence, (unsigned char *)(page) + bt->page_size - bt->frame->fence, bt->frame->fence);
		nxt -= page->fence;
	}

	while( cnt++ < max ) {

		// copy key, but not infinite values

		if( cnt < max || !page->lvl || right ) {
			ptr = keyptr(page, cnt);
			nxt -= ptr->len + 1;
			memcpy ((unsigned char *)bt->frame + nxt, ptr, ptr->len + 1);
		}

		// copy slot

		memcpy(slotptr(bt->frame,++idx)->id, slotptr(page,cnt)->id, BtId);
		slotptr(bt->frame, idx)->tod = slotptr(page, cnt)->tod;
		slotptr(bt->frame, idx)->off = nxt;
	}

	bt->frame->bits = bt->page_bits;
	bt->frame->lvl = page->lvl;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;

	//	get new free page and write right frame to it.

	if( !(new_page = bt_newpage(bt, bt->frame)) )
		return bt->err;

	//	update lower keys to continue in old page

	memcpy (bt->frame, page, bt->page_size);
	memset (page+1, 0, bt->page_size - sizeof(*page));
	nxt = bt->page_size;
	max /= 2;
	cnt = 0;
	idx = 0;

	//	record fence key in left leaf page

	if( !page->lvl ) {
		ptr = keyptr(bt->frame, max);
		nxt -= ptr->len + 1;
		memcpy ((unsigned char *)page + nxt, ptr, ptr->len + 1);
		page->fence = ptr->len + 1;
	}

	//  assemble page of smaller keys
	//	no infinite value to deal with

	while( cnt++ < max ) {
		ptr = keyptr(bt->frame, cnt);
		nxt -= ptr->len + 1;
		memcpy ((unsigned char *)page + nxt, ptr, ptr->len + 1);
		memcpy(slotptr(page,++idx)->id, slotptr(bt->frame,cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
	}

	bt_putid(page->right, new_page);
	page->min = nxt;
	page->cnt = idx;

	// update left node

	if( bt_update(bt, page, parentpage) )
		return bt->err;

	//	decide to move to new right
	//	node or stay on left node

	ptr = bt_highkey (bt, page);

	if( keycmp (ptr, key, len) >= 0 )
		return bt_unlockpage (bt, parentpage, BtLockUpgrade);

	bt->parentpage = new_page;

	if( bt_mappage (bt, &bt->parent, new_page) )
		return bt->err;
	if( bt_lockpage (bt, new_page, BtLockUpdate) )
		return bt->err;
	if( bt_unlockpage (bt, parentpage, BtLockUpgrade) )
		return bt->err;

	return bt_unlockpage (bt, parentpage, BtLockUpdate);
}

//  add unlinked node key into parent
//  childpage is existing record
//	siblingpage is right record 

BTERR bt_parentlink (BtDb *bt, BtPage left, uid leftpage, BtKey rightkey, uid rightpage)
{
BtKey leftkey = bt_highkey (bt, left);
BtPage page = bt->parent;
uint slot, idx;

	// upgrade parent latch to exclusive

	if( bt_lockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	// find the existing right high key in the parent
	// and fix the downlink to point to right page

	if( rightkey ) {
	  if( !(slot = bt_findslot (page, rightkey->key, rightkey->len)) )
		return bt->err = BTERR_struct;
	} else
	  slot = page->cnt;

	bt_putid(slotptr(page,slot)->id, rightpage);

	// calculate next available slot and copy left key onto page

	page->min -= leftkey->len + 1; // reset lowest used offset
	memcpy ((unsigned char *)page + page->min, leftkey, leftkey->len + 1);

	// now insert key into array before slot

	idx = ++page->cnt;

	while( idx > slot )
	  *slotptr(page, idx) = *slotptr(page, idx -1), idx--;

	bt_putid(slotptr(page,slot)->id, leftpage);
	slotptr(page, slot)->off = page->min;

	if ( bt_update(bt, page, bt->parentpage) )
	  return bt->err;

	// downgrade parent page lock to BtLockUpdate

	if( bt_unlockpage(bt, bt->parentpage, BtLockUpgrade) )
	  return bt->err;

	return 0;
}

//	remove slot from parent

void bt_removeslot(BtDb *bt, uint slot)
{
uint nxt = bt->page_size, amt;
BtPage page = bt->parent;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uid right;
BtKey key;

	memcpy (bt->frame, page, bt->page_size);

	// skip page info and set rest of page to zero
	memset (page+1, 0, bt->page_size - sizeof(*page));

	// copy fence key onto new page
	if( amt = page->fence ) {
		nxt -= amt;
		memcpy ((unsigned char *)page + nxt, (unsigned char *)(bt->frame) + nxt, amt);
	}

	right = bt_getid (page->right);

	while( cnt++ < max ) {

		// skip key to delete

		if( cnt == slot )
			continue;

		// copy key, but not infinite value

		if( cnt < max || !page->lvl || right ) {
			key = keyptr(bt->frame, cnt);
			nxt -= key->len + 1;
			memcpy ((unsigned char *)page + nxt, key, key->len + 1);
		}

		// copy slot
		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
	}
	page->min = nxt;
	page->cnt = idx;
}

//	unlink sibling node

BTERR bt_parentunlink (BtDb *bt, BtPage child, uid childpage)
{
BtKey parentkey = bt_highkey (bt, child);
uint slot;

	if( bt_lockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	// delete child's slot from parent

	if( slot = bt_findslot (bt->parent, parentkey->key, parentkey->len) )
		bt_removeslot (bt, slot);
	else
		return bt->err + BTERR_struct;

	//	sibling now in the slot

	bt_putid(slotptr(bt->parent,slot)->id, childpage);

	if ( bt_update(bt, bt->parent, bt->parentpage) )
	  return bt->err;

	// unlock parent page completely

	if( bt_unlockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	return bt_unlockpage(bt, bt->parentpage, BtLockUpdate);
}

//	merge right sibling page into child page

BTERR bt_mergepages (BtDb *bt, BtPage *right, uid rightpage)
{
BtPage *left = &bt->child;
uint idx, amt;
BtKey ptr;

	if( bt_lockpage (bt, bt->childpage, BtLockUpgrade) )
		return bt->err;
	if( bt_lockpage (bt, rightpage, BtLockUpgrade) )
		return bt->err;

	//	initialize empty frame

	memset (bt->frame, 0, bt->page_size);
	*bt->frame = **right;

	//	copy right fence key

	if( amt = (*right)->fence )
		memcpy ((unsigned char *)bt->frame + bt->page_size - amt, (unsigned char *)(*right) + bt->page_size - amt, amt);

	bt->frame->min = bt->page_size - amt;

	//	copy lowerkey key/values from left page

	for( idx = 1; idx <= (*left)->cnt; idx++ ) {
		ptr = keyptr(*left, idx);
		bt->frame->min -= ptr->len + 1;
		memcpy ((unsigned char *)bt->frame + bt->frame->min, ptr, ptr->len + 1);
		memcpy (slotptr(bt->frame, idx)->id, slotptr(*left, idx)->id, BtId);
		slotptr(bt->frame, idx)->tod = slotptr(*left, idx)->tod;
		slotptr(bt->frame, idx)->off = bt->frame->min;
	}

	bt->frame->cnt = (*left)->cnt;

	//	copy higherkey key/values from right page

	for( idx = 1; idx <= (*right)->cnt; idx++ ) {

		// copy key but not infinite value

		if( idx < (*right)->cnt || !(*right)->lvl || right ) {
			ptr = keyptr(*right, idx);
			bt->frame->min -= ptr->len + 1;
			memcpy ((unsigned char *)bt->frame + bt->frame->min, ptr, ptr->len + 1);
		}

		// copy slot
		memcpy (slotptr(bt->frame, bt->frame->cnt + idx)->id, slotptr(*right, idx)->id, BtId);
		slotptr(bt->frame, bt->frame->cnt + idx)->tod = slotptr(*right, idx)->tod;
		slotptr(bt->frame, bt->frame->cnt + idx)->off = bt->frame->min;
	}

	bt->frame->cnt += (*right)->cnt;
	memcpy (*left, bt->frame, bt->page_size);

	if( bt_update (bt, *left, bt->childpage) )
		return bt->err;
	if( bt_freepage (bt, *right, rightpage) )
		return bt->err;
	if( bt_unlockpage (bt, rightpage, BtLockUpgrade) )
		return bt->err;
	if( bt_unlockpage (bt, rightpage, BtLockUpdate) )
		return bt->err;

	return bt_unlockpage (bt, bt->childpage, BtLockUpgrade);
}

//	redistribute right sibling page with child page
//	switch child page to containing page

BTERR bt_redistribute (BtDb *bt, BtPage *right, uid rightpage, unsigned char *key, uint len)
{
uid siblingpage = bt_getid((*right)->right);
BtPage *left = &bt->child;
uint idx, cnt = 0, amt = 0;
uint leftmax, rightmin;
BtPage swap;
BtKey ptr;

	if( bt_lockpage (bt, bt->childpage, BtLockUpgrade) )
		return bt->err;
	if( bt_lockpage (bt, rightpage, BtLockUpgrade) )
		return bt->err;

	//	initialize empty frames to contain redistributed pages

	memset (bt->frame, 0, bt->page_size);	// new left page
	memset (bt->temp, 0, bt->page_size);	// new right page

	*bt->frame = **left;
	*bt->temp = **right;

	bt->frame->min = bt->page_size;
	bt->temp->min = bt->page_size;
	bt->frame->cnt = 0;
	bt->temp->cnt = 0;

	//	find new left fence index
	//	and copy left fence key

	if( (*left)->cnt > (*right)->cnt ) {
		rightmin = 0;
		leftmax = (*left)->cnt / 2;
		if( !bt->frame->lvl ) {
			ptr = keyptr(*left, leftmax);
			bt->frame->fence = ptr->len + 1;
			bt->frame->min -= ptr->len + 1;
			memcpy ((unsigned char *)bt->frame + bt->frame->min, ptr, ptr->len + 1);
		}
	} else {
		leftmax = (*left)->cnt;
		rightmin = (*right)->cnt / 2;
		if( !bt->frame->lvl ) {
			ptr = keyptr(*right, rightmin);
			bt->frame->fence = ptr->len + 1;
			bt->frame->min -= ptr->len + 1;
			memcpy ((unsigned char *)bt->frame + bt->frame->min, ptr, ptr->len + 1);
		}
	}

	//	right fence stays the same, if any

	if( amt = (*right)->fence ) {
		bt->temp->min -= amt;
		memcpy ((unsigned char *)bt->temp + bt->temp->min, (unsigned char *)(*right) + bt->temp->min, amt);
	}

	//	copy first set of lowerkey key/values from left page

	for( idx = 1; idx <= leftmax; idx++ ) {
		bt->frame->cnt++;
		ptr = keyptr(*left, idx);
		bt->frame->min -= ptr->len + 1;
		memcpy ((unsigned char *)bt->frame + bt->frame->min, ptr, ptr->len + 1);
		memcpy (slotptr(bt->frame, idx)->id, slotptr(*left, idx)->id, BtId);
		slotptr(bt->frame, idx)->tod = slotptr(*left, idx)->tod;
		slotptr(bt->frame, idx)->off = bt->frame->min;
	}

	//	copy remaining left page key/values from right page, if any

	for( idx = 1; idx <= rightmin; idx++ ) {
		bt->frame->cnt++;
		ptr = keyptr(*right, idx);
		bt->frame->min -= ptr->len + 1;
		memcpy ((unsigned char *)bt->frame + bt->frame->min, ptr, ptr->len + 1);
		memcpy (slotptr(bt->frame, bt->frame->cnt)->id, slotptr(*right, idx)->id, BtId);
		slotptr(bt->frame, bt->frame->cnt)->tod = slotptr(*right, idx)->tod;
		slotptr(bt->frame, bt->frame->cnt)->off = bt->frame->min;
	}

	//	copy remaining left page key/values into new right page, if any

	for( idx = leftmax; idx <= (*left)->cnt; idx++ ) {
		bt->temp->cnt++;
		ptr = keyptr(*left, idx);
		bt->temp->min -= ptr->len + 1;
		memcpy ((unsigned char *)bt->temp + bt->temp->min, ptr, ptr->len + 1);
		memcpy (slotptr(bt->temp, bt->temp->cnt)->id, slotptr(*left, idx)->id, BtId);
		slotptr(bt->temp, bt->temp->cnt)->tod = slotptr(*left, idx)->tod;
		slotptr(bt->temp, bt->temp->cnt)->off = bt->temp->min;
	}

	//	copy rest of higherkey key/values from right page

	for( idx = rightmin; idx <= (*right)->cnt; idx++ ) {

		// copy key, but not infinite value

		if( idx < (*right)->cnt || !(*right)->lvl || siblingpage ) {
			ptr = keyptr(*right, idx);
			bt->temp->min -= ptr->len + 1;
			memcpy ((unsigned char *)bt->temp + bt->temp->min, ptr, ptr->len + 1);
		}

		//	copy slot

		bt->temp->cnt++;
		memcpy (slotptr(bt->temp, bt->temp->cnt)->id, slotptr(*right, idx)->id, BtId);
		slotptr(bt->temp, bt->temp->cnt)->tod = slotptr(*right, idx)->tod;
		slotptr(bt->temp, bt->temp->cnt)->off = bt->temp->min;
	}

	memcpy (*left, bt->frame, bt->page_size);
	memcpy (*right, bt->temp, bt->page_size);

	if( bt_update (bt, *left, bt->childpage) )
		return bt->err;
	if( bt_update (bt, *right, rightpage) )
		return bt->err;

	if( bt_unlockpage (bt, rightpage, BtLockUpgrade) )
		return bt->err;
	if( bt_unlockpage (bt, rightpage, BtLockUpdate) )
		return bt->err;

	ptr = bt_highkey (bt, *left);

	//	decide which page is the child page
	//	if leftkey >= our key, go with left

	if( keycmp (ptr, key, len) >= 0 ) {
		if( bt_unlockpage (bt, bt->childpage, BtLockUpgrade) )
			return bt->err;
		if( bt_unlockpage (bt, rightpage, BtLockUpgrade) )
			return bt->err;
		if( bt_unlockpage (bt, rightpage, BtLockUpdate) )
			return bt->err;
	} else {
		if( bt_unlockpage (bt, rightpage, BtLockUpgrade) )
			return bt->err;
		if( bt_unlockpage (bt, bt->childpage, BtLockUpgrade) )
			return bt->err;
		if( bt_unlockpage (bt, bt->childpage, BtLockUpdate) )
			return bt->err;
		swap = bt->child;
		bt->child = *right;
		*left = swap;
		bt->childpage = rightpage;
	}

	return 0;
}

//	lower the root level by removing the child node

BTERR bt_lowerroot(BtDb *bt)
{
	if( bt_lockpage (bt, ROOT_page, BtLockUpgrade) )
		return bt->err;

	if( bt_lockpage (bt, bt->childpage, BtLockUpgrade) )
		return bt->err;

	memcpy (bt->parent, bt->child, bt->page_size);

	if( bt_update(bt, bt->parent, ROOT_page) )
		return bt->err;

	if( bt_freepage(bt, bt->child, bt->childpage) )
		return bt->err;

	if( bt_unlockpage (bt, bt->childpage, BtLockUpgrade) )
		return bt->err;

	if( bt_unlockpage (bt, bt->childpage, BtLockUpdate) )
		return bt->err;

	return bt_unlockpage (bt, ROOT_page, BtLockUpgrade);
}

// split the root and raise the height of the btree
//	return with parent page set to appropriate sibling

BTERR bt_raiseroot(BtDb *bt, uid sibling, unsigned char *key, uint len)
{
unsigned char lowerkey[256];
uint nxt = bt->page_size;
BtPage root = bt->parent;
uid new_page;
BtKey ptr;

	//	upgrade root page lock to exclusive

	if( bt_lockpage (bt, ROOT_page, BtLockUpgrade) )
		return bt->err;

	//  Obtain an empty page to use, and copy the current
	//  root (lower half) contents into it

	if( !(new_page = bt_newpage(bt, root)) )
		return bt->err;

	if( bt_lockpage (bt, new_page, BtLockUpdate) )
		return bt->err;

	//	save high fence key for left page

	ptr = bt_highkey(bt, root);
	memcpy (lowerkey, ptr, ptr->len + 1);

	// preserve the page info at the bottom
	// and set rest to zero to initialize new root page

	memset(root+1, 0, bt->page_size - sizeof(*root));

	// insert left key in newroot page

	nxt -= *lowerkey + 1;
	memcpy ((unsigned char *)root + nxt, lowerkey, *lowerkey + 1);
	bt_putid(slotptr(root, 1)->id, new_page);
	slotptr(root, 1)->off = nxt;
	
	// insert second (infinite) key on newroot page that's never examined
	// and increase the root level

	bt_putid(slotptr(root, 2)->id, sibling);
	bt_putid(root->right, 0);

	root->min = nxt;		// reset lowest used offset and key count
	root->fence = 0;
	root->cnt = 2;
	root->lvl++;

	if( bt_update(bt, root, bt->parentpage) )
		return bt->err;

	if( bt_unlockpage(bt, ROOT_page, BtLockUpgrade) )
		return bt->err;

	if( bt_unlockpage(bt, ROOT_page, BtLockUpdate) )
		return bt->err;

	//	decide which root node to continue with
	//		sibling has upper keys, newpage the lower ones

	if( keycmp((BtKey)lowerkey, key, len) < 0 ) {
		bt->parentpage = sibling;		// go with the upper ones

		if( bt_unlockpage (bt, new_page, BtLockUpdate) )
			return bt->err;

		return bt_mappage (bt, &bt->parent, sibling);
	}

	bt->parentpage = new_page;			// go with the lower ones

	if( bt_unlockpage (bt, sibling, BtLockUpdate) )
		return bt->err;

	return bt_mappage (bt, &bt->parent, new_page);
}

//	handle underflowing child node

BTERR bt_repairchild (BtDb *bt, uint parentslot, unsigned char *key, uint len)
{
BtKey parentkey = bt_slotkey(bt->parent, parentslot);
BtKey fencekey = bt_highkey (bt, bt->child);
BtKey highkey = bt_highkey(bt, bt->parent);
BtKey siblingkey, siblingkey2;
uid siblingpage, siblingpage2;
uid swappage;
BtPage swap;
uint slot;

  // high key is never NULL
  // fence key is null on right end

  if( fencekey )
   if( !highkey || keycmp (fencekey, highkey->key, highkey->len) < 0 ) {

	//  childpage is not rightmost child of parent page

	siblingpage = bt_getid(bt->child->right);

	if( bt_lockpage (bt, siblingpage, BtLockUpdate) )
	  return bt->err;

	if( bt_mappage (bt, &bt->sibling, siblingpage) )
	  return bt->err;

	if( !parentkey || keycmp (fencekey, parentkey->key, parentkey->len) < 0 ) {

	  // sibling is not linked in parent, so we can merge it

	  if( bt_unlockpage (bt, bt->parentpage, BtLockUpdate) )
		return bt->err;

	  // calculate size of merged page by adding single child key to sibling

	  fencekey = keyptr(bt->child, 1);

	  if( bt->sibling->min < (bt->sibling->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->sibling) + fencekey->len + 1)
	    return bt_redistribute (bt, &bt->sibling, siblingpage, key, len);
	  else
	    return bt_mergepages (bt, &bt->sibling, siblingpage);
    }

	//	sibling has a key in the parent node
	//	find its slot in parent

	if( siblingkey = bt_highkey (bt, bt->sibling) )
	  slot = bt_findslot (bt->parent, siblingkey->key, siblingkey->len);
	else
	  slot = bt->parent->cnt;

	parentkey = bt_slotkey (bt->parent, slot);
	siblingpage2 = bt_getid(bt->sibling->right);

	if( siblingkey && parentkey && keycmp (siblingkey, parentkey->key, parentkey->len) < 0 ) {

	  //  sibling2 is not linked in P, its key is parentkey
	  //  can parent support its insertion?
	  //  if not, split parent node first.

	  if( bt->parent->min < (bt->parent->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->parent) + parentkey->len + 1) {
		if( bt_splitparent (bt, key, len) )
		  return bt->err;

		//  are we in the correct half of new parent nodes?
		//  if not, restart the function.

		if( slot = bt_findslot (bt->parent, siblingkey->key, siblingkey->len) )
		  parentkey = keyptr(bt->parent, slot);
		else
		  return BTERR_restart;
	  }

	  //  link right of sibling into parent

	  if( bt_parentlink (bt, bt->sibling, siblingpage, parentkey, siblingpage2) )
		return bt->err;

	  //  unlink sibling from parent

	  if( bt_parentunlink (bt, bt->child, bt->childpage) )
		return bt->err;

	  fencekey = keyptr(bt->child, 1);

	  if( bt->sibling->min < (bt->sibling->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->sibling) + fencekey->len + 1)
	    return bt_redistribute (bt, &bt->sibling, siblingpage, key, len);
	  else
	    return bt_mergepages (bt, &bt->sibling, siblingpage);
	} else {

	  // unlink sibling from parent and merge

	  if( bt_parentunlink (bt, bt->child, bt->childpage) )
		return bt->err;

	  fencekey = keyptr(bt->child, 1);

	  if( bt->sibling->min < (bt->sibling->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->sibling) + fencekey->len + 1)
	    return bt_redistribute (bt, &bt->sibling, siblingpage, key, len);
	  else
	    return bt_mergepages (bt, &bt->sibling, siblingpage);
	}
  }

  //  child is rightmost key in the parent,
  //  work with nodes to left.

  siblingpage = bt_getid(slotptr(bt->parent, parentslot - 1)->id);
  siblingkey = keyptr(bt->parent, parentslot - 1);

  if( bt_unlockpage (bt, bt->childpage, BtLockUpdate) )
	return bt->err;
  if( bt_lockpage (bt, siblingpage, BtLockUpdate) )
	return bt->err;
  if( bt_mappage (bt, &bt->sibling, siblingpage) )
	return bt->err;

  siblingpage2 = bt_getid(bt->sibling->right);
  siblingkey2 = bt_highkey (bt, bt->sibling);

  if( bt_lockpage (bt, siblingpage2, BtLockUpdate) )
	return bt->err;
  if( bt_mappage (bt, &bt->sibling2, siblingpage2) )
	return bt->err;

  if( keycmp (siblingkey, siblingkey2->key, siblingkey2->len) == 0 ) {

	//	left sibling right (mapped into sibling2) is our child node

	swap = bt->sibling2;
	bt->sibling2 = bt->child;
	bt->child = swap;

	//  does child still need to merge/redistribute?

	if( bt->child->cnt > 1 ) {
	  if( bt_unlockpage (bt, bt->parentpage, BtLockUpdate) )
		return bt->err;

	  return bt_unlockpage (bt, siblingpage, BtLockUpdate);
	}
	
	swap = bt->sibling;
	bt->sibling = bt->child;
	bt->child = swap;

	bt->childpage = siblingpage;
	siblingpage = siblingpage2;

	// unlink sibling node from parent and merge with child

	if( bt_parentunlink (bt, bt->child, bt->childpage) )
		return bt->err;

	fencekey = keyptr(bt->sibling, 1);

	if( bt->child->min < (bt->child->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->sibling) + fencekey->len + 1)
	  return bt_redistribute (bt, &bt->sibling, siblingpage, key, len);
	else
	  return bt_mergepages (bt, &bt->sibling, siblingpage);
  }

  //  currently unlinked sibling2 merges with child

  fencekey = bt_highkey (bt, bt->sibling2);

  if( bt->parent->min < (bt->parent->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->parent) + fencekey->len + 1)
	if( bt_splitparent (bt, key, len) )
	  return bt->err;

  if( bt_parentlink (bt, bt->sibling, siblingpage, fencekey, siblingpage2) )
	return bt->err;

  if( bt_unlockpage (bt, siblingpage, BtLockUpdate) )
	  return bt->err;
  if( bt_lockpage (bt, bt->childpage, BtLockUpdate) )
	  return bt->err;
  if( bt_mappage (bt, &bt->child, bt->childpage) )
	return bt->err;

  if( bt->child->cnt > 1 ) {
    if( bt_unlockpage (bt, bt->parentpage, BtLockUpdate) )
	  return bt->err;
    return bt_unlockpage (bt, siblingpage2, BtLockUpdate);
  }

  // unlink child from parent node leaving immediate
  // left sibling2 to accept merge/redistribution

  if( bt_parentunlink (bt, bt->sibling2, siblingpage2) )
	return bt->err;

  swap = bt->sibling2;
  bt->sibling2 = bt->child;
  bt->child = swap;
  bt->childpage = siblingpage2;

  fencekey = keyptr(bt->sibling2, 1);

  if( bt->child->min < (bt->child->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->child) + fencekey->len + 1) 
	return bt_redistribute (bt, &bt->sibling2, siblingpage2, key, len);
  else
	return bt_mergepages (bt, &bt->sibling2, siblingpage2);
}

//  find and load leaf page for given key
//	leave page BtLockShared, return with key's slot

int bt_loadpageread (BtDb *bt, unsigned char *key, uint len)
{
uid page_no = ROOT_page, prevpage = 0;
uint slot, mode;

  //  start at root of btree and drill down

  do {
	bt->parentpage = page_no;

	if( bt_lockpage(bt, bt->parentpage, BtLockShared) )
		return 0;									

	if( prevpage )
	  if( bt_unlockpage(bt, prevpage, BtLockShared) )
		return 0;

	//	map/obtain page contents

	if( bt_mappage (bt, &bt->parent, page_no) )
		return 0;

	//  find key on page at this level
	//	return if leaf page

	if( (slot = bt_findslot (bt->parent, key, len)) ) {
	  if( !bt->parent->lvl )
		return slot;

	  // continue down to next level

	  page_no = bt_getid(slotptr(bt->parent, slot)->id);
	}

	//  or slide right into next page

	else
		page_no = bt_getid(bt->parent->right);

	prevpage = bt->parentpage;
  } while( page_no );

  // return EOF on end of right chain

  if( bt_unlockpage(bt, bt->parentpage, BtLockShared) )
	return 0;									

  return 0;	// return EOF
}

//  find and load leaf page for given key
//	return w/slot # on leaf page
//	leave page BtLockUpdate

int bt_loadpageupdate (BtDb *bt, unsigned char *key, uint len)
{
uid parentpage = 0, nextpage;
BtKey fencekey, parentkey;
uint slot, mode;
BtPage swap;

  //  start at root of btree and drill down

  if( bt_lockpage(bt, ROOT_page, BtLockUpdate) )
	return 0;									

  //	map/obtain page contents

  if( bt_mappage (bt, &bt->parent, ROOT_page) )
	return 0;

  bt->parentpage = ROOT_page;

  do {
	// if root page, check for tree level growth
	// by existence of right pointer

	if( bt->parentpage == ROOT_page )
 	  if( nextpage = bt_getid(bt->parent->right) ) {
		if( bt_lockpage (bt, nextpage, BtLockUpdate) )
		  return 0;

		if( bt_raiseroot (bt, nextpage, key, len) )
		  return 0;
	  }

	//  find key on page at this level
	//	return if leaf page

	slot = bt_findslot (bt->parent, key, len);

	if( !bt->parent->lvl )
		return slot;

	// lock & map the child

	bt->childpage = bt_getid(slotptr(bt->parent, slot)->id);

	if( bt_lockpage(bt, bt->childpage, BtLockUpdate) )
		return 0;									

	if( bt_mappage (bt, &bt->child, bt->childpage) )
		return 0;

	//  check child for underflow

	if( bt->parentpage == ROOT_page )
	  if( bt->parent->cnt == 1 )
	   if( !bt_getid(bt->child->right) ) {
		if( bt_lowerroot (bt) )
			return 0;
		nextpage = ROOT_page;
		continue;
	   }

	if( bt->child->cnt == 1 ) {
		while( bt_repairchild (bt, slot, key, len) == BTERR_restart );
	 	if( bt->err )
			return 0;

	    swap = bt->child;
	    bt->child = bt->parent;
	    bt->parent = swap;
	    nextpage = bt->childpage;
		continue;
	}

	fencekey = bt_highkey (bt, bt->child);
	parentkey = bt_slotkey(bt->parent, slot);

	//  if right sibling is not linked,
	//	fix links in parent node

	if( fencekey )
	 if( !parentkey || keycmp (fencekey, parentkey->key, parentkey->len) < 0 ) {

	  if( bt->parent->min < (bt->parent->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->parent) + fencekey->len + 1)
		if( bt_splitparent (bt, key, len) ) {
			return 0;
		} else {
			slot = bt_findslot (bt->parent, key, len);
			parentkey = bt_slotkey(bt->parent, slot);
		}

	  //  add key to parent for child page
	  //	and fix downlink for childpage

 	  nextpage = bt_getid(bt->child->right);

	  if( bt_parentlink (bt, bt->child, bt->childpage, parentkey, nextpage) )
		return 0;
	 }

	//  unlock the parent page

	if( bt_unlockpage (bt, bt->parentpage, BtLockUpdate) )
		return 0;

	//	is our key on this child page?
	//	is fencekey infinite, or .ge. our key

	if( !fencekey || keycmp (fencekey, key, len) >= 0 ) {
	  swap = bt->child;
	  bt->child = bt->parent;
	  bt->parent = swap;
	  nextpage = bt->childpage;
	  continue;
	}

	//  otherwise slide right into next page

 	nextpage = bt_getid(bt->child->right);

	if( bt_lockpage (bt, nextpage, BtLockUpdate) )
		return 0;

	if( bt_unlockpage (bt, bt->childpage, BtLockUpdate) )
		return 0;

	if( bt_mappage (bt, &bt->parent, nextpage) )
		return 0;

  } while( bt->parentpage = nextpage );

  // return error on end of right chain

  bt->err = BTERR_struct;
  return 0;	// return error
}

//  find and delete key on leaf page

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len)
{
uid page_no, right;
uint slot, tod;
BtKey ptr;

	bt_resetpages (bt);

	if( slot = bt_loadpageupdate (bt, key, len) )
		ptr = keyptr(bt->parent, slot);
	else if( bt->err )
		return bt->err;

	// if key is found delete it, otherwise ignore request

	if( slot && !keycmp (ptr, key, len) ) {
	  if( bt_lockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	  bt_removeslot (bt, slot);

	  if( bt_update(bt, bt->parent, bt->parentpage) )
		return bt->err;

	  if( bt_unlockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;
	}

	return bt_unlockpage (bt, bt->parentpage, BtLockUpdate);
}

//	find key in leaf page and return row-id
//	or zero if key is not found.

uid bt_findkey (BtDb *bt, unsigned char *key, uint len)
{
uint  slot;
BtKey ptr;
uid id;

	bt_resetpages (bt);

	if( slot = bt_loadpageread (bt, key, len) )
		ptr = keyptr(bt->parent, slot);
	else
		return 0;

	// if key exists, return row-id
	//	otherwise return 0

	if( ptr->len == len && !memcmp (ptr->key, key, len) )
		id = bt_getid(slotptr(bt->parent,slot)->id);
	else
		id = 0;

	if ( bt_unlockpage(bt, bt->parentpage, BtLockShared) )
		return 0;

	return id;
}

//  Insert new key into the btree leaf page

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uid id, uint tod)
{
uint slot, idx;
BtKey ptr;

	if( slot = bt_loadpageupdate (bt, key, len) )
		ptr = keyptr(bt->parent, slot);
	else if( bt->err )
		return bt->err;

	if( bt->parent->lvl )
		abort();
	if( bt->parent->lvl )
		abort();


	// if key already exists, update id and return

	if( slot )
	  if( !keycmp (ptr, key, len) ) {
		if( bt_lockpage (bt, bt->parentpage, BtLockUpgrade) )
			return bt->err;
		slotptr(bt->parent, slot)->tod = tod;
		bt_putid(slotptr(bt->parent,slot)->id, id);
		if ( bt_update(bt, bt->parent, bt->parentpage) )
			return bt->err;
		if( bt_unlockpage (bt, bt->parentpage, BtLockUpgrade) )
			return bt->err;
		return bt_unlockpage(bt, bt->parentpage, BtLockUpdate);
	  }

	// check if leaf page has enough space

	if( bt->parent->min < (bt->parent->cnt + 1) * sizeof(BtSlot) + sizeof(*bt->parent) + len + 1) {
	  if( bt_splitparent (bt, key, len) )
		return bt->err;

	  slot = bt_findslot (bt->parent, key, len);
	}

	// calculate next available slot and copy key into page

	if( bt_lockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	bt->parent->min -= len + 1; // reset lowest used offset
	((unsigned char *)bt->parent)[bt->parent->min] = len;
	memcpy ((unsigned char *)bt->parent + bt->parent->min +1, key, len );

	// now insert key into array before slot

	idx = ++bt->parent->cnt;

	if( slot )
	 while( idx > slot )
	  *slotptr(bt->parent, idx) = *slotptr(bt->parent, idx -1), idx--;

	bt_putid(slotptr(bt->parent,idx)->id, id);
	slotptr(bt->parent, idx)->off = bt->parent->min;
	slotptr(bt->parent, idx)->tod = tod;

	if ( bt_update(bt, bt->parent, bt->parentpage) )
	  return bt->err;

	if( bt_unlockpage (bt, bt->parentpage, BtLockUpgrade) )
		return bt->err;

	return bt_unlockpage(bt, bt->parentpage, BtLockUpdate);
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
uint slot;

	// cache page for retrieval
	if( slot = bt_loadpageread (bt, key, len) )
		memcpy (bt->cursor, bt->parent, bt->page_size);
	bt->cursorpage = bt->parentpage;
	if ( bt_unlockpage(bt, bt->parentpage, BtLockShared) )
		return 0;

	return slot;
}

//  return next slot for cursor page
//  or slide cursor right into next page

uint bt_nextkey (BtDb *bt, uint slot)
{
off64_t right;

  do {
	if( slot++ < bt->cursor->cnt )
		return slot;

	right = bt_getid(bt->cursor->right);

	if( !right )
		break;

	bt->cursorpage = right;

    if( bt_lockpage(bt, right,BtLockShared) )
		return 0;

	if( bt_mappage (bt, &bt->parent, right) )
		break;

	memcpy (bt->cursor, bt->parent, bt->page_size);
	if ( bt_unlockpage(bt, right, BtLockShared) )
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
uint slot, found = 0, line = 0, off = 0;
int ch, cnt = 0, bits = 12;
unsigned char key[256];
clock_t done, start;
time_t tod[1];
uint scan = 0;
uint len = 0;
uint map = 0;
BtKey ptr;
BtDb *bt;
FILE *in;

	if( argc < 4 ) {
		fprintf (stderr, "Usage: %s idx_file src_file Read/Write/Scan/Delete/Find [page_bits mapped_pool_pages start_line_number]", argv[0]);
		exit(0);
	}

	start = clock();
	time (tod);

	if( argc > 4 )
		bits = atoi(argv[4]);

	if( argc > 5 )
		map = atoi(argv[5]);

	if( map > 65536 )
		fprintf (stderr, "Warning: mapped_pool > 65536 pages\n");

	if( argc > 6 )
		off = atoi(argv[6]);

	bt = bt_open ((argv[1]), BT_rw, bits, map);

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

			  if( bt_insertkey (bt, key, len, ++line, *tod) )
				fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line), exit(0);
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
		fprintf(stderr, "finished deleting keys, %d\n", line);
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

	cnt = 0;
	len = key[0] = 0;

	fprintf(stderr, "started reading\n");

	slot = bt_startkey (bt, key, len);

	if( bt->err )
	  fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

	if( slot-- )
	 while( slot = bt_nextkey (bt, slot) )
	  if( cnt++, scan ) {
			ptr = bt_key(bt, slot);
			fwrite (ptr->key, ptr->len, 1, stdout);
			fputc ('\n', stdout);
	  }

	fprintf(stderr, " Total keys read %d\n", cnt);
}

#endif	//STANDALONE
