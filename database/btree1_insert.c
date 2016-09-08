#include "db.h"
#include "db_object.h"
#include "db_map.h"
#include "btree1.h"

Status btreeInsertSlot (Handle *hndl, BtreeSet *set, uint8_t *key, uint32_t keyLen, BtreeSlotType type);

Status btreeInsertKey(Handle *hndl, uint8_t *key, uint32_t keyLen, uint8_t lvl, BtreeSlotType type) {
uint32_t totKeyLen = keyLen;
BtreeSet set[1];
Status stat;

	if (keyLen < 128)
		totKeyLen += 1;
	else
		totKeyLen += 2;

	while (true) {
	  if ((stat = btreeLoadPage(hndl, set, key, keyLen, lvl, Btree_lockWrite, false)))
		return stat;

	  if ((stat = btreeCleanPage(hndl, set, totKeyLen))) {
		if (stat == BTREE_needssplit) {
		  if ((stat = btreeSplitPage(hndl, set)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  }

	  // add the key to the page

	  return btreeInsertSlot (hndl, set, key, keyLen, type);
	}

	return OK;
}

//	update page's fence key in its parent

Status btreeFixKey (Handle *hndl, uint8_t *fenceKey, uint8_t lvl, bool stopper) {
uint32_t keyLen = keylen(fenceKey);
BtreeSet set[1];
BtreeSlot *slot;
uint8_t *ptr;
Status stat;

	if ((stat = btreeLoadPage(hndl, set, fenceKey + keypre(fenceKey), keyLen - sizeof(uint64_t), lvl, Btree_lockWrite, stopper)))
		return stat;

	slot = slotptr(set->page, set->slotIdx);
	ptr = keyptr(set->page, set->slotIdx);

	// if librarian slot

	if (slot->type == Btree_librarian) {
		slot = slotptr(set->page, ++set->slotIdx);
		ptr = keyptr(set->page, set->slotIdx);
	}

	// update child pointer value

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keylen(fenceKey) - sizeof(uint64_t), sizeof(uint64_t));

	// release write lock

	btreeUnlockPage (set->page, Btree_lockWrite);
	return OK;
}

//	install new key onto page
//	page must already be checked for
//	adequate space

Status btreeInsertSlot (Handle *hndl, BtreeSet *set, uint8_t *key, uint32_t keyLen, BtreeSlotType type) {
uint32_t idx, prefixLen;
BtreeSlot *slot;
uint8_t *ptr;

	//	if found slot > desired slot and previous slot
	//	is a librarian slot, use it

	if( set->slotIdx > 1 )
	  if( slotptr(set->page, set->slotIdx-1)->type == Btree_librarian )
		set->slotIdx--;

	//	calculate key length

	prefixLen = keyLen < 128 ? 1 : 2;

	// copy key onto page

	set->page->min -= prefixLen + keyLen;
	ptr = keyaddr(set->page, set->page->min);

	if( keyLen < 128 )	
		*ptr++ = keyLen;
	else
		*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	memcpy (ptr, key, keyLen);
	slot = slotptr(set->page, set->slotIdx);
	
	//	find first empty slot

	for( idx = set->slotIdx; idx < set->page->cnt; slot++, idx++ )
		if( slot->dead )
			break;

	if( idx == set->page->cnt )
		idx++, set->page->cnt++, slot++;

	set->page->act++;

	while( idx-- > set->slotIdx )
		slot->bits = slot[-1].bits, slot--;

	//	fill in new slot

	slot->bits = set->page->min;
	slot->type = type;

	btreeUnlockPage (set->page, Btree_lockWrite);
	return OK;
}

