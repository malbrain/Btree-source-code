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
		it->objId = hndl->map->arena->segs[hndl->map->arena->objSeg].nextId;
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

	while (it->objId.seg <= it->objMap->arena->objSeg) {
		if (++it->objId.index <= it->objMap->arena->segs[it->objId.seg].nextId.index)
			return true;

		it->objId.index = 0;
		it->objId.seg++;
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
		if (!it->objId.seg)
			break;

		it->objId.seg--;
		it->objId.index = it->objMap->arena->segs[it->objId.seg].nextId.index + 1;
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
		addr = fetchIdSlot(it->objMap, it->objId);
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
		addr = fetchIdSlot(it->objMap, it->objId);
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

	addr = fetchIdSlot(it->objMap, objId);

	if (addr->bits)
		return getObj(it->objMap, *addr);

	return NULL;
}
