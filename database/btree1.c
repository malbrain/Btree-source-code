#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "btree1.h"

#define pagebits 13		// btree interior page size
#define leafxtra 0		// btree leaf extra bits

#if (pagebits > Btree_maxbits)
#error btree interior pages too large
#endif

#if (pagebits + leafxtra > Btree_maxbits)
#error btree leaf pages too large
#endif

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

	if ((addr.bits = allocNode(hndl->map, hndl->freeList, type, size, true)))
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

	btree->pageSize = 1 << pagebits;
	btree->pageBits = pagebits;
	btree->leafXtra = leafxtra;

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
	buff[0] = sizeof(uint64_t);
	store64(buff + 1, btree->leaf.bits);

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
		ReadLock3 (page->latch->readwr);
		break;
	case Btree_lockWrite:
		WriteLock3 (page->latch->readwr);
		break;
	case Btree_lockAccess:
		ReadLock3 (page->latch->access);
		break;
	case Btree_lockDelete:
		WriteLock3 (page->latch->access);
		break;
	case Btree_lockParent:
		WriteLock3 (page->latch->parent);
		break;
	case Btree_lockLink:
		WriteLock3 (page->latch->link);
		break;
	}
}

void btreeUnlockPage(BtreePage *page, BtreeLock mode)
{
	switch( mode ) {
	case Btree_lockWrite:
		WriteUnlock3 (page->latch->readwr);
		break;
	case Btree_lockRead:
		ReadUnlock3 (page->latch->readwr);
		break;
	case Btree_lockAccess:
		ReadUnlock3 (page->latch->access);
		break;
	case Btree_lockDelete:
		WriteUnlock3 (page->latch->access);
		break;
	case Btree_lockParent:
		WriteUnlock3 (page->latch->parent);
		break;
	case Btree_lockLink:
		WriteUnlock3 (page->latch->link);
		break;
	}
}

