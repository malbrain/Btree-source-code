#include "../db.h"
#include "../db_object.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "btree1.h"

uint64_t btreeObjId(BtreeCursor *cursor) {
uint8_t *ptr = keyptr(cursor->page, cursor->slotIdx);
uint64_t result;

	get64(ptr + keypre(ptr), keylen(ptr), &result);
	return result;
}

BtreeCursor *btreeCursor(Handle *index) {
BtreeCursor *cursor;
BtreeIndex *btree;
BtreePage *first;

    btree = btreeIndex(index->map);

	cursor = db_malloc(sizeof(BtreeCursor), true);
	cursor->pageAddr.bits = btreeNewPage(index, 0);
	cursor->page = getObj(index->map, cursor->pageAddr);

	first = getObj(index->map, btree->leaf);
	btreeLockPage (first, Btree_lockRead);
	memcpy(cursor->page, first, btree->pageSize);
	btreeUnlockPage (first, Btree_lockRead);

	*cursor->idx = index;
	cursor->slotIdx = 0;
	return cursor;
}

int btreeReturnCursor(BtreeCursor *cursor) {
Handle *index;

	// return cursor page buffer

	if ((index = bindHandle(cursor->idx)))
		freeNode(index->map, index->list, cursor->pageAddr);
	else
		return ERROR_arenadropped;

	releaseHandle(index);
	db_free(cursor);
	return OK;
}

uint8_t *btreeCursorKey(BtreeCursor *cursor, uint32_t *len) {
uint8_t *key = keyptr(cursor->page, cursor->slotIdx);

	*len = keylen(key);
	return key;
}

bool btreeSeekKey (BtreeCursor *cursor, uint8_t *key, uint32_t keylen) {
	return true;
}

uint64_t btreeNextKey (BtreeCursor *cursor) {
BtreeIndex *btree;
Handle *index;

	if ((index = bindHandle(cursor->idx)))
		btree = btreeIndex(index->map);
	else
		return 0;

	while (true) {
	  uint32_t max = cursor->page->cnt;

	  if (!cursor->page->right.bits)
		max--;

	  while (cursor->slotIdx < max)
		if (slotptr(cursor->page, ++cursor->slotIdx)->dead)
		  continue;
		else
		  return btreeObjId(cursor);

	  if (cursor->page->right.bits)
		cursor->page = getObj(index->map, cursor->page->right);
	  else
		return 0;

	  cursor->slotIdx = 0;
	}
}

uint64_t btreePrevKey (BtreeCursor *cursor) {
BtreeIndex *btree;
Handle *index;

	if ((index = bindHandle(cursor->idx)))
		btree = btreeIndex(index->map);
	else
		return 0;

	while (true) {
	  if (cursor->slotIdx) {
		if (slotptr(cursor->page, --cursor->slotIdx)->dead)
		  continue;
		else
		  return btreeObjId(cursor);
	  }

	  if (cursor->page->left.bits)
		cursor->page = getObj(index->map, cursor->page->left);
	  else
		return 0;

	  cursor->slotIdx = cursor->page->cnt;
	}
}
