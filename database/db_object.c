#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

Handle *makeHandle(DbMap *map) {
HandleArray *element;
Handle *hndl;

	hndl = db_malloc(sizeof(Handle), true);
	hndl->idx = arrayAlloc(map, map->arena->handleArray, sizeof(HandleArray));
	hndl->map = map;

	element = arrayElement(map, map->arena->handleArray, hndl->idx, sizeof(HandleArray));

	// allocate freeList array

	if (!element->freeList.bits)
		element->freeList.bits = allocObj(map, map->arena->freeBlk, NULL, -1, sizeof(FreeList) * MaxObjType, true);

	hndl->freeList = getObj(map, element->freeList);
	return hndl;
}

void *arrayElement(DbMap *map, DbAddr *array, uint32_t idx, size_t size) {
uint8_t *base = getObj(map, *array);

	base += array->nslot * sizeof(uint64_t);
	return (void *)(base + size * idx);
}

uint32_t arrayAlloc(DbMap *map, DbAddr *array, size_t size) {
uint64_t *inUse, *newArray;
unsigned long bits[1];
DbAddr next[1];
int idx, max;

	lockLatch(array->latch);

	if (array->nslot)
		inUse = getObj(map, *array);

	//  find unused array entry

	for (idx = 0; idx < array->nslot; idx++) {
	  if (!~inUse[idx])
		continue;

#	  ifdef _WIN32
		_BitScanForward64(bits, ~inUse[idx]);
#	  else
		*bits = (__builtin_ffs (~inUse[idx])) - 1;
#	  endif

	  inUse[idx] |= 1ULL << *bits;
	  unlockLatch(array->latch);
	  return *bits + idx * 64;
	}

	//	increase array size

	if (array->nslot == 255)
		fprintf(stderr, "Array overflow\n"), exit(1);

	if ((max = array->nslot))
		max += max / 2;
	else
		max = 1;

	if (max > 255)
		max = 255;

	next->bits = allocObj(map, map->arena->freeBlk, NULL, -1, size * (sizeof(uint64_t) + size) * max, true);

	next->nslot = max;

	// allocate new array

	newArray = getObj(map, *next);
	memcpy (newArray, inUse, array->nslot);
	memcpy (newArray + next->nslot, inUse + array->nslot, array->nslot * size);

	// assign first new entry

	idx = array->nslot * 64;
	newArray[array->nslot] = 1;

	// release old array

	addSlotToFrame(map, &map->arena->freeBlk[array->type], array->bits);

	// point to new array, release lock in the process

	array->bits = next->bits;

	return idx;
}

void closeHandle(Handle  *hndl) {
DbAddr *array = hndl->map->arena->handleArray;
uint64_t *inUse;

	lockLatch(array->latch);
	inUse = getObj(hndl->map, *array);
	inUse[hndl->idx / 64] &= ~(1ULL << (hndl->idx % 64));
	unlockLatch(array->latch);

	db_free(hndl);
}

//	handle being used in API call
//	return false if map being dropped

bool bindHandle(Handle *hndl) {
DbAddr *array = hndl->map->arena->handleArray;
HandleArray *element;

	element = arrayElement(hndl->map, array, hndl->idx, sizeof(HandleArray));

	if (atomicAdd32(hndl->actve, 1) - 1)
		waitNonZero64(&element->objTs);
	else
		element->objTs = hndl->map->arena->delTs;

	return ~hndl->map->mutex[0] & DEAD_BIT;
}

void releaseHandle(Handle *hndl) {
DbAddr *array = hndl->map->arena->handleArray;
HandleArray *element;

	if (atomicAdd32(hndl->actve, -1))
		return;

	element = arrayElement(hndl->map, array, hndl->idx, sizeof(HandleArray));
	waitNonZero64(&element->objTs);
	element->objTs = 0;
}

//	get 64 bit suffix value

uint64_t get64(uint8_t *from) {
uint64_t result = 0;
int idx;

	for (idx = 0; idx < sizeof(uint64_t); idx++) {
		result <<= 8;
		result |= from[idx];
	}
	return result;
}

//  fill in 64 bit suffix value

void store64(uint8_t *where, uint64_t what) {
int idx = sizeof(uint64_t);

	while (idx--) {
		where[idx] = what & 0xff;
		what >>= 8;
	}
}

