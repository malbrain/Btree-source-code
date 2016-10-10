#include "../db.h"
#include "../db_object.h"
#include "../db_arena.h"
#include "../db_index.h"
#include "../db_map.h"
#include "btree1.h"

uint64_t btree1ObjId(Btree1Cursor *cursor) {
uint8_t *ptr = keyptr(cursor->page, cursor->slotIdx);
uint64_t result;

	get64(ptr + keypre(ptr), keylen(ptr), &result);
	return result;
}

DbCursor *btree1NewCursor(Handle *index, uint64_t timestamp, ObjId txnId, char type) {
Btree1Cursor *cursor;
Btree1Index *btree1;
Btree1Page *first;

    btree1 = btree1index(index->map);

	cursor = db_malloc(sizeof(Btree1Cursor), true);
	cursor->pageAddr.bits = btree1NewPage(index, 0);
	cursor->page = getObj(index->map, cursor->pageAddr);

	//  TODO: reverse cursor

	first = getObj(index->map, btree1->leaf);
	btree1LockPage (first, Btree1_lockRead);
	memcpy(cursor->page, first, btree1->pageSize);
	btree1UnlockPage (first, Btree1_lockRead);

	cursor->base->txnId.bits = txnId.bits;
	cursor->base->ts = timestamp;
	*cursor->base->idx = index;
	cursor->slotIdx = 0;
	return cursor->base;
}

Status btree1ReturnCursor(DbCursor *dbCursor) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Handle *index;

	// return cursor page buffer

	if ((index = bindHandle(cursor->base->idx)))
		addSlotToFrame(index->map, &index->list[cursor->pageAddr.type], cursor->pageAddr);
	else
		return ERROR_arenadropped;

	releaseHandle(index);
	db_free(cursor);
	return OK;
}

uint8_t *btree1CursorKey(DbCursor *dbCursor, uint32_t *len) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
uint8_t *key = keyptr(cursor->page, cursor->slotIdx);

	*len = keylen(key);
	return key;
}

bool btree1SeekKey (DbCursor *dbCursor, uint8_t *key, uint32_t keylen) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
	return true;
}

Status btree1NextKey (DbCursor *dbCursor, DbMap *index) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
uint8_t *key;

	while (true) {
	  uint32_t max = cursor->page->cnt;

	  if (!cursor->page->right.bits)
		max--;

	  while (cursor->slotIdx < max) {
		Btree1Slot *slot = slotptr(cursor->page, ++cursor->slotIdx);

		if (slot->dead)
		  continue;

		key = keyaddr(cursor->page, slot->off);
		cursor->base->key = key + keypre(key);
		cursor->base->keyLen = keylen(key);
		return OK;
	  }

	  if (cursor->page->right.bits)
		cursor->page = getObj(index, cursor->page->right);
	  else
		return ERROR_endoffile;

	  cursor->slotIdx = 0;
	}
}

Status btree1PrevKey (DbCursor *dbCursor, DbMap *index) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
uint8_t *key;

	while (true) {
	  if (cursor->slotIdx) {
		Btree1Slot *slot = slotptr(cursor->page, --cursor->slotIdx);

		if (slot->dead)
		  continue;

		key = keyaddr(cursor->page, slot->off);
		cursor->base->key = key + keypre(key);
		cursor->base->keyLen = keylen(key);
		return OK;
	  }

	  if (cursor->page->left.bits)
		cursor->page = getObj(index, cursor->page->left);
	  else
		return ERROR_endoffile;

	  cursor->slotIdx = cursor->page->cnt;
	}
}
