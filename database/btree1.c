#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "btree1.h"

//	create an empty page

uint64_t btreeNewPage (Handle *hndl, uint8_t lvl) {
BtreeIndex *btree = btreeIndex(hndl->map);
BtreePageType type;
BtreePage *page;
uint32_t size;
DbAddr addr;

	size = btree->pageSize;

	if (lvl)
		type = Btree_interior;
	else {
		type = Btree_leafPage;
		size <<= btree->leafXtra;
	}

	if ((addr.bits = allocNode(hndl->map, hndl->list, type, size, true)))
		page = getObj(hndl->map, addr);
	else
		return 0;

	page->lvl = lvl;
	page->min = size;
	return addr.bits;
}

//	initialize btree root page

Status btreeInit(Handle *hndl) {
BtreeIndex *btree = btreeIndex(hndl->map);
BtreePage *page;
BtreeSlot *slot;
uint8_t *buff;

	//	initial btree root & leaf pages

	if ((btree->leaf.bits = btreeNewPage(hndl, 0)))
		page = getObj(hndl->map, btree->leaf);
	else
		return ERROR_outofmemory;

	//  set up new leaf page with stopper key

	btree->leaf.type = Btree_leafPage;
	page->min -= 1;
	page->cnt = 1;
	page->act = 1;

	buff = keyaddr(page, page->min);
	buff[0] = 0;

	//  set up stopper slot

	slot = slotptr(page, 1);
	slot->type = Btree_stopper;
	slot->off = page->min;

	//	set  up the tree root page with stopper key

	if ((btree->root.bits = btreeNewPage(hndl, 1)))
		page = getObj(hndl->map, btree->root);
	else
		return ERROR_outofmemory;

	//  set up new root page with stopper key

	btree->root.type = Btree_rootPage;
	page->min -= 1 + sizeof(uint64_t);
	page->cnt = 1;
	page->act = 1;

	//  set up stopper key

	buff = keyaddr(page, page->min);
	btreePutPageNo(buff + 1, 0, btree->leaf.bits);
	buff[0] = sizeof(uint64_t);

	//  set up slot

	slot = slotptr(page, 1);
	slot->type = Btree_stopper;
	slot->off = page->min;

	hndl->map->arena->type[0] = BtreeIndexType;
	return OK;
}

// place write, read, or parent lock on requested page_no.

void btreeLockPage(BtreePage *page, BtreeLock mode) {
	switch( mode ) {
	case Btree_lockRead:
		readLock2 (page->latch->readwr);
		break;
	case Btree_lockWrite:
		writeLock2 (page->latch->readwr);
		break;
	case Btree_lockParent:
		writeLock2 (page->latch->parent);
		break;
	case Btree_lockLink:
		writeLock2 (page->latch->link);
		break;
	}
}

void btreeUnlockPage(BtreePage *page, BtreeLock mode)
{
	switch( mode ) {
	case Btree_lockWrite:
		writeUnlock2 (page->latch->readwr);
		break;
	case Btree_lockRead:
		readUnlock2 (page->latch->readwr);
		break;
	case Btree_lockParent:
		writeUnlock2 (page->latch->parent);
		break;
	case Btree_lockLink:
		writeUnlock2 (page->latch->link);
		break;
	}
}

void btreePutPageNo(uint8_t *key, uint32_t len, uint64_t bits) {
int idx = sizeof(uint64_t);

	while (idx--)
		key[len + idx] = bits, bits >>= 8;
}

uint64_t btreeGetPageNo(uint8_t *key, uint32_t len) {
uint64_t result = 0;
int idx = 0;

	len -= sizeof(uint64_t);

	do result <<= 8, result |= key[len + idx];
	while (++idx < sizeof(uint64_t));

	return result;
}
