#pragma once

//	iterator object

typedef struct {
	DbMap *objMap;		// Object map
	ObjId objId;		// current ObjID
} Iterator;

void *createIterator(Handle *hndl, bool atEnd);

void *iteratorSeek(Iterator *it, uint64_t objBits);
void *iteratorNext(Iterator *it);
void *iteratorPrev(Iterator *it);
