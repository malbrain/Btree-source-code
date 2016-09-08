#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "btree1.h"

uint64_t btreeObjId(BtreeCursor *cursor) {
uint8_t *ptr = keyptr(cursor->page, cursor->slotIdx);
uint8_t *suffix = (ptr + keypre(ptr) + keylen(ptr) - sizeof(ObjId));

	return get64(suffix);
}

BtreeCursor *btreeCursor(Handle *hndl) {
BtreeCursor *cursor;
BtreeIndex *btree;

	if (bindHandle(hndl))
    	btree = btreeIndex(hndl->map);
	else
		return NULL;

	cursor = db_malloc(sizeof(BtreeCursor), true);
	cursor->page = getObj(hndl->map, btree->leaf);
	cursor->slotIdx = 0;
	cursor->hndl = hndl;
	return cursor;
}

void btreeCloseCursor(BtreeCursor *cursor) {
	releaseHandle(cursor->hndl);
	db_free(cursor);
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
BtreeIndex *btree = btreeIndex(cursor->hndl->map);

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
		cursor->page = getObj(cursor->hndl->map, cursor->page->right);
	  else
		return 0;

	  cursor->slotIdx = 0;
	}
}

uint64_t btreePrevKey (BtreeCursor *cursor) {
BtreeIndex *btree = btreeIndex(cursor->hndl->map);

	while (true) {
	  if (cursor->slotIdx) {
		if (slotptr(cursor->page, --cursor->slotIdx)->dead)
		  continue;
		else
		  return btreeObjId(cursor);
	  }

	  if (cursor->page->left.bits)
		cursor->page = getObj(cursor->hndl->map, cursor->page->left);
	  else
		return 0;

	  cursor->slotIdx = cursor->page->cnt;
	}
}
