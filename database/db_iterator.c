#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_iterator.h"

//
// create and position the start of an iterator
//

void *createIterator(Handle *hndl, bool fromMin) {
Iterator *it = db_malloc(sizeof(Iterator), true);

	it->objMap = hndl->map;

	if (fromMin) {
		it->objId.bits = 0;
	} else {
		it->objId = hndl->map->arena->segs[hndl->map->arena->currSeg].nextObj;
		it->objId.index++;
	}

	return it;
}

void destroyIterator(Iterator *it) {
	db_free(it);
}

//
// increment a segmented ObjId
//

bool incrObjId(Iterator *it) {
ObjId start = it->objId;

	while (it->objId.segment <= it->objMap->arena->currSeg) {
		if (++it->objId.index <= it->objMap->arena->segs[it->objId.segment].nextObj.index)
			return true;

		it->objId.index = 0;
		it->objId.segment++;
	}

	it->objId = start;
	return false;
}

//
// decrement a segmented recordId
//

bool decrObjId(Iterator *it) {
ObjId start = it->objId;

	while (it->objId.index) {
		if (--it->objId.index)
			return true;
		if (!it->objId.segment)
			break;

		it->objId.segment--;
		it->objId.index = it->objMap->arena->segs[it->objId.segment].nextObj.index + 1;
	}

	it->objId = start;
	return false;
}

//
//  advance iterator forward
//

//  TODO:  lock the record

void *iteratorNext(Iterator *it) {
DbAddr *addr;

	while (incrObjId(it)) {
		addr = fetchObjSlot(it->objMap, it->objId);
		if (addr->bits)
			return getObj(it->objMap, *addr);
	}

	return NULL;
}

//
//  advance iterator backward
//

//  TODO:  lock the record

void *iteratorPrev(Iterator *it) {
DbAddr *addr;

	while (decrObjId(it)) {
		addr = fetchObjSlot(it->objMap, it->objId);
		if (addr->bits)
			return getObj(it->objMap, *addr);
	}

	return NULL;
}

//
//  set iterator to specific objectId
//

//  TODO:  lock the record

void *iteratorSeek(Iterator *it, uint64_t objBits) {
DbAddr *addr;
ObjId objId;

	objId.bits = objBits;

	addr = fetchObjSlot(it->objMap, objId);

	if (addr->bits)
		return getObj(it->objMap, *addr);

	return NULL;
}
