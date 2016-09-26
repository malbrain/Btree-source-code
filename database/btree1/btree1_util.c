#include "../db.h"
#include "../db_object.h"
#include "../db_arena.h"
#include "../db_index.h"
#include "../db_map.h"
#include "btree1.h"

//	debug slot function

#ifdef DEBUG
Btree1Slot *btree1Slot(Btree1Page *page, uint32_t idx)
{
	return slotptr(page, idx);
}

uint8_t *btree1Key(Btree1Page *page, uint32_t idx)
{
	return keyptr(page, idx);
}

uint8_t *btree1Addr(Btree1Page *page, uint32_t off)
{
	return keyaddr(page, off);
}

#undef keyptr
#undef keyaddr
#undef slotptr
#define keyptr(p,x) btree1Key(p,x)
#define keyaddr(p,o) btree1Addr(p,o)
#define slotptr(p,x) btree1Slot(p,x)
#endif

uint32_t Splits;

// split the root and raise the height of the btree1
// call with key for smaller half and right page addr.

Status btree1SplitRoot(Handle *hndl, Btree1Set *root, DbAddr right, uint8_t *leftKey) {
Btree1Index *btree1 = btree1index(hndl->map);
uint32_t keyLen, nxt = btree1->pageSize;
Btree1Page *leftPage, *rightPage;
Btree1Slot *slot;
uint8_t *ptr;
uint32_t off;
Status stat;
DbAddr left;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( (left.bits = btree1NewPage(hndl, root->page->lvl)) )
		leftPage = getObj(hndl->map, left);
	else
		return ERROR_outofmemory;

	//	copy in new smaller keys into left page
	//	(clear the latches)

	memcpy (leftPage->latch + 1, root->page->latch + 1, btree1->pageSize - sizeof(*leftPage->latch));
	rightPage = getObj(hndl->map, right);
	rightPage->left.bits = left.bits;

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, btree1->pageSize - sizeof(*root->page));

	// insert stopper key on root page
	// pointing to right half page 
	// and increase the root height

	nxt -= 1 + sizeof(uint64_t);
	slot = slotptr(root->page, 2);
	slot->type = Btree1_stopper;
	slot->off = nxt;

	ptr = keyaddr(root->page, nxt);
	btree1PutPageNo(ptr + 1, 0, right.bits);
	ptr[0] = sizeof(uint64_t);

	// next insert lower keys (left) fence key on newroot page as
	// first key and reserve space for the key.

	keyLen = keylen(leftKey);

	if (keyLen < 128)
		off = 1;
	else
		off = 2;

	nxt -= keyLen + off;
	slot = slotptr(root->page, 1);
	slot->type = Btree1_indexed;
	slot->off = nxt;

	//	construct lower (left) page key

	ptr = keyaddr(root->page, nxt);
	memcpy (ptr + off, leftKey + keypre(leftKey), keyLen - sizeof(uint64_t));
	btree1PutPageNo(ptr + off, keyLen - sizeof(uint64_t), left.bits);

	if (off == 1)
		ptr[0] = keyLen;
	else
		ptr[0] = keyLen / 256 | 0x80, ptr[1] = keyLen;
	
	root->page->right.bits = 0;
	root->page->min = nxt;
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	// release root page

	btree1UnlockPage(root->page, Btree1_lockWrite);
	return OK;
}

//  split already locked full node
//	return unlocked.

Status btree1SplitPage (Handle *hndl, Btree1Set *set) {
uint8_t leftKey[Btree1_maxkey], rightKey[Btree1_maxkey];
Btree1Index *btree1 = btree1index(hndl->map);
uint32_t cnt = 0, idx = 0, max, nxt, off;
Btree1Slot librarian, *source, *dest;
Btree1PageType type = Btree1_leafPage;
uint32_t size = btree1->pageSize;
Btree1Page *frame, *rightPage;
uint8_t lvl = set->page->lvl;
uint32_t totLen, keyLen;
DbAddr right, addr;
uint8_t *key;
bool stopper;
Status stat;

#ifdef DEBUG
	atomicAdd32(&Splits, 1);
#endif

	librarian.bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	if( !set->page->lvl )
		size <<= btree1->leafXtra;
	else
		type = Btree1_interior;

	//	get new page and write higher keys to it.

	if( (right.bits = btree1NewPage(hndl, lvl)) )
		rightPage = getObj(hndl->map, right);
	else
		return ERROR_outofmemory;

	max = set->page->cnt;
	cnt = max / 2;
	nxt = size;
	idx = 0;

	source = slotptr(set->page, cnt);
	dest = slotptr(rightPage, 0);

	while( source++, cnt++ < max ) {
		if( source->dead )
			continue;

		key = keyaddr(set->page, source->off);
		totLen = keylen(key) + keypre(key);
		nxt -= totLen;

		memcpy (keyaddr(rightPage, nxt), key, totLen);
		rightPage->act++;

		//	add librarian slot

		if (cnt < max) {
			(++dest)->bits = librarian.bits;
			dest->off = nxt;
			idx++;
		}

		//  add actual slot

		(++dest)->bits = source->bits;
		dest->off = nxt;
		idx++;
	}

	//	remember right fence key for larger page
	//	extend right leaf fence key with
	//	the right page number on leaf page.

	stopper = dest->type == Btree1_stopper;
	keyLen = keylen(key);

	if( set->page->lvl)
		keyLen -= sizeof(uint64_t);		// strip off pageNo

	if( keyLen + sizeof(uint64_t) < 128 )
		off = 1;
	else
		off = 2;

	//	copy key and add pageNo

	memcpy (rightKey + off, key + keypre(key), keyLen);
	btree1PutPageNo(rightKey + off, keyLen, right.bits);
	keyLen += sizeof(uint64_t);

	if (off == 1)
		rightKey[0] = keyLen;
	else
		rightKey[0] = keyLen / 256 | 0x80, rightKey[1] = keyLen;

	rightPage->min = nxt;
	rightPage->cnt = idx;
	rightPage->lvl = lvl;

	// link right node

	if( set->pageNo.type != Btree1_rootPage ) {
		rightPage->right.bits = set->page->right.bits;
		rightPage->left.bits = set->pageNo.bits;

		if( !lvl && rightPage->right.bits ) {
			Btree1Page *farRight = getObj(hndl->map, rightPage->right);
			btree1LockPage (farRight, Btree1_lockLink);
			farRight->left.bits = right.bits;
			btree1UnlockPage (farRight, Btree1_lockLink);
		}
	}

	//	copy lower keys from temporary frame back into old page

	if( (addr.bits = btree1NewPage(hndl, lvl)) )
		frame = getObj(hndl->map, addr);
	else
		return ERROR_outofmemory;

	memcpy (frame, set->page, size);
	memset (set->page+1, 0, size - sizeof(*set->page));

	set->page->garbage = 0;
	set->page->act = 0;
	nxt = size;
	max /= 2;
	cnt = 0;
	idx = 0;

	//  ignore librarian max key

	if( slotptr(frame, max)->type == Btree1_librarian )
		max--;

	source = slotptr(frame, 0);
	dest = slotptr(set->page, 0);

#ifdef DEBUG
	key = keyaddr(frame, source[2].off);
	assert(keylen(key) > 0);
#endif
	//  assemble page of smaller keys from temporary frame copy

	while( source++, cnt++ < max ) {
		if( source->dead )
			continue;

		key = keyaddr(frame, source->off);
		totLen = keylen(key) + keypre(key);
		nxt -= totLen;

		memcpy (keyaddr(set->page, nxt), key, totLen);

		//	add librarian slot, except before fence key

		if (cnt < max) {
			(++dest)->bits = librarian.bits;
			dest->off = nxt;
			idx++;
		}

		//	add actual slot

		(++dest)->bits = source->bits;
		dest->off = nxt;
		idx++;

		set->page->act++;
	}

	set->page->right.bits = right.bits;
	set->page->min = nxt;
	set->page->cnt = idx;

	//	remember left fence key for smaller page
	//	extend left leaf fence key with
	//	the left page number.

	keyLen = keylen(key);

	if( set->page->lvl)
		keyLen -= sizeof(uint64_t);		// strip off pageNo

	if( keyLen + sizeof(uint64_t) < 128 )
		off = 1;
	else
		off = 2;

	//	copy key and add pageNo

	memcpy (leftKey + off, key + keypre(key), keyLen);
	btree1PutPageNo(leftKey + off, keyLen, set->pageNo.bits);
	keyLen += sizeof(uint64_t);

	if (off == 1)
		leftKey[0] = keyLen;
	else
		leftKey[0] = keyLen / 256 | 0x80, leftKey[1] = keyLen;

	//  return temporary frame

	freeNode(hndl->map, hndl->list, addr);

	// if current page is the root page, split it

	if( set->pageNo.type == Btree1_rootPage )
		return btree1SplitRoot (hndl, set, right, leftKey);

	// insert new fences in their parent pages

	btree1LockPage (rightPage, Btree1_lockParent);
	btree1LockPage (set->page, Btree1_lockParent);
	btree1UnlockPage (set->page, Btree1_lockWrite);

	// insert new fence for reformulated left block of smaller keys

	if( (stat = btree1InsertKey(hndl, leftKey + keypre(leftKey), keylen(leftKey), lvl+1, Btree1_indexed) ))
		return stat;

	// switch fence for right block of larger keys to new right page

	if( (stat = btree1FixKey(hndl, rightKey, lvl+1, stopper) ))
		return stat;

	btree1UnlockPage (set->page, Btree1_lockParent);
	btree1UnlockPage (rightPage, Btree1_lockParent);
	return OK;
}

//	check page for space available,
//	clean if necessary and return
//	false - page needs splitting
//	true  - ok to insert

Status btree1CleanPage(Handle *hndl, Btree1Set *set, uint32_t totKeyLen) {
Btree1Index *btree1 = btree1index(hndl->map);
Btree1Slot librarian, *source, *dest;
uint32_t size = btree1->pageSize;
Btree1Page *page = set->page;
uint32_t max = page->cnt;
uint32_t len, cnt, idx;
uint32_t newslot = max;
Btree1PageType type;
Btree1Page *frame;
uint8_t *key;
DbAddr addr;

	librarian.bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	if( !page->lvl ) {
		size <<= btree1->leafXtra;
		type = Btree1_leafPage;
	} else {
		type = Btree1_interior;
	}

	if( page->min >= (max+1) * sizeof(Btree1Slot) + sizeof(*page) + totKeyLen )
		return OK;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < size / 5 )
		return BTREE_needssplit;

	if( (addr.bits = allocNode(hndl->map, hndl->list, type, size, false)) )
		frame = getObj(hndl->map, addr);
	else
		return ERROR_outofmemory;

	memcpy (frame, page, size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, size - sizeof(*page));
	page->garbage = 0;
	page->act = 0;

	cnt = 0;
	idx = 0;

	source = slotptr(frame, cnt);
	dest = slotptr(page, idx);

	// clean up page first by
	// removing deleted keys

	while( source++, cnt++ < max ) {
		if( cnt == set->slotIdx )
			newslot = idx + 2;

		if( source->dead )
			continue;

		// copy the active key across

		key = keyaddr(frame, source->off);
		len = keylen(key) + keypre(key);
		size -= len;

		memcpy ((uint8_t *)page + size, key, len);

		// make a librarian slot

		if (cnt < max) {
			(++dest)->bits = librarian.bits;
			++idx;
		}

		// set up the slot

		(++dest)->bits = source->bits;
		dest->off = size;
		idx++;

		page->act++;
	}

	page->min = size;
	page->cnt = idx;

	//  return temporary frame

	freeNode(hndl->map, hndl->list, addr);

	//	see if page has enough space now, or does it still need splitting?

	if( page->min >= (idx+1) * sizeof(Btree1Slot) + sizeof(*page) + totKeyLen )
		return OK;

	return BTREE_needssplit;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree1KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2)
{
uint32_t len1 = keylen(key1);
int ans;

	key1 += keypre(key1);

	if( ans = memcmp (key1, key2, len1 > len2 ? len2 : len1) )
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find slot in page for given key at a given level

uint32_t btree1FindSlot (Btree1Page *page, uint8_t *key, uint32_t keyLen, bool stopper)
{
uint32_t diff, higher = page->cnt, low = 1, slot;
uint32_t good = 0;

	assert(higher > 0);

	//	are we being asked for the stopper(fence) key?

	if (stopper)
		return higher;

	//	  make stopper key an infinite fence value

	if( page->right.bits )
		higher++;
	else
		good++;

	//	low is the lowest candidate.
	//  loop ends when they meet

	//  higher is already
	//	tested as .ge. the passed key.

	while( (diff = higher - low) ) {
		slot = low + diff / 2;
		if( btree1KeyCmp (keyptr(page, slot), key, keyLen) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on next right page

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

Status btree1LoadPage(Handle *hndl, Btree1Set *set, uint8_t *key, uint32_t keyLen, uint8_t lvl, Btree1Lock lock, bool stopper) {
Btree1Index *btree1 = btree1index(hndl->map);
uint8_t drill = 0xff, *ptr;
Btree1Page *prevPage = NULL;
Btree1Lock mode, prevMode;
DbAddr prevPageNo;

  set->pageNo.bits = btree1->root.bits;
  prevPageNo.bits = 0;

  //  start at our idea of the root level of the btree1 and drill down

  do {
	// determine lock mode of drill level

	mode = (drill == lvl) ? lock : Btree1_lockRead; 
	set->page = getObj(hndl->map, set->pageNo);

	//	release parent or left sibling page

	if( prevPageNo.bits ) {
	  btree1UnlockPage(prevPage, prevMode);
	  prevPageNo.bits = 0;
	}

 	// obtain mode lock

	btree1LockPage(set->page, mode);

	if( set->page->free )
		return ERROR_btreestruct;

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		assert(drill == 0xff);
		drill = set->page->lvl;

		if( lock != Btree1_lockRead && drill == lvl ) {
		  btree1UnlockPage(set->page, mode);
		  continue;
		}
	}

	assert(lvl <= set->page->lvl);

	prevPageNo.bits = set->pageNo.bits;
	prevPage = set->page;
	prevMode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( !set->page->kill )
	 if( (set->slotIdx = btree1FindSlot (set->page, key, keyLen, stopper)) ) {
	  if( drill == lvl )
		return OK;

	  // find next non-dead slot -- the fence key if nothing else

	  while( slotptr(set->page, set->slotIdx)->dead )
		if( set->slotIdx++ < set->page->cnt )
		  continue;
		else
  		  return ERROR_btreestruct;

	  // get next page down

	  ptr = keyptr(set->page, set->slotIdx);
	  set->pageNo.bits = btree1GetPageNo(ptr + keypre(ptr), keylen(ptr));

	  assert(drill > 0);
	  drill--;
	  continue;
	 }

	//  or slide right into next page

	set->pageNo.bits = set->page->right.bits;
  } while( set->pageNo.bits );

  // return error on end of right chain

  return ERROR_btreestruct;
}
