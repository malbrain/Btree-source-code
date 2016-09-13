#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

extern DbMap memMap[1];

//	return a handle for an arena

Handle *makeHandle(DbMap *map) {
uint16_t localIdx;
Handle *hndl;

	localIdx = arrayAlloc(memMap, map->hndlArray, sizeof(Handle));
	hndl = arrayElement(memMap, map->hndlArray, localIdx, sizeof(Handle));

	hndl->arenaIdx = arrayAlloc(map, map->arena->handleArray, sizeof(HandleArray));
	hndl->localIdx = localIdx;
	hndl->map = map;

	hndl->array = arrayElement(map, map->arena->handleArray, hndl->arenaIdx, sizeof(HandleArray));

	return hndl;
}

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size) {
uint8_t *base = getObj(map, *array);

	base += array->nslot * sizeof(uint64_t);
	return (void *)(base + size * idx);
}

//	assign an array element
//	return payload address

void *arrayAssign(DbMap *map, DbAddr *array, uint16_t idx, size_t size) {
uint8_t *base;

	if (array->nslot * 64 <= idx)
		arrayExpand(map, array, size, idx);

	base = getObj(map, *array);
	base += array->nslot * sizeof(uint64_t);
	return (void *)(base + size * idx);
}

//	allocate an array element

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size) {
uint64_t *inUse, *newArray;
unsigned long bits[1];
int idx, max;

  lockLatch(array->latch);

  while (true) {
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

	// table is full

	arrayExpand(map, array, size, idx * 64);
  }
}

//	increase array size

void arrayExpand(DbMap *map, DbAddr *array, size_t size, uint16_t max) {
uint64_t *newArray, *inUse;
DbAddr next[1];

	if (array->nslot)
		inUse = getObj(map, *array);

	// calculate number of slots

	max += 63;
	max &= -8;
	max /= 64;

	if (max)
		max += max / 2;
	else
		max = 1;

	if (max > 255)
		max = 255;

	if (max <= array->nslot)
		fprintf(stderr, "Array overflow: %s\n", map->path), exit(1);

	next->bits = allocBlk(map, max * sizeof(uint64_t) + (max * 64) * size, true);

	next->nslot = max;

	// allocate new array

	newArray = getObj(map, *next);
	memcpy (newArray, inUse, array->nslot);
	memcpy (newArray + next->nslot, inUse + array->nslot, array->nslot * size);

	// release old array

	if (array->addr)
		freeBlk(map, array);

	// point to new array, keeping lock in the process

	next->mutex = 1;
	array->bits = next->bits;
}

//	add entry to redblack tree

void arraySort (DbMap *map, DbAddr *array, char *name, int idx) {
}

//  check if all handles are dead/closed

void checkHandles(Handle *hndl) {
DbAddr *array = hndl->map->hndlArray;
uint64_t *inUse = getObj(hndl->map, *array);
Handle *hndlArray;
int idx, jdx;

	hndlArray = (Handle *)(inUse + array->nslot);

	for (idx = 0; idx < array->nslot; hndlArray += 64, idx++)
	  for (jdx = 0; jdx < 64; jdx++)
		if (inUse[idx] & 1ULL << jdx)
		  if (hndlArray[jdx].status[0] != HANDLE_dead)
			return;

	lockLatch(array->latch);

	if (hndl->map->arena)
		closeMap(hndl->map);

	unlockLatch(array->latch);
}

//	delete handle

void deleteHandle(Handle  *hndl) {
DbAddr *array = hndl->map->arena->handleArray;
uint64_t *inUse;

	atomicOr32(hndl->status, HANDLE_dead);

	// return permanent handle
 
	lockLatch(array->latch);
	inUse = getObj(hndl->map, *array);

	// clear permanent in-use bit

	inUse[hndl->arenaIdx / 64] &= ~(1ULL << (hndl->arenaIdx % 64));
	unlockLatch(array->latch);

	// return local handle

	array = memMap->arena->handleArray;

	lockLatch(array->latch);
	inUse = getObj(memMap, *array);

	// clear local in-use bit

	inUse[hndl->localIdx / 64] &= ~(1ULL << (hndl->localIdx % 64));
	unlockLatch(array->latch);
}

//	API entry to close handle

void closeHandle(Handle  *hndl) {

	if (bindHandle(hndl)) {
		deleteHandle(hndl);
		releaseHandle(hndl);
	}
}

//	bind handle for use in API call
//	return false if arena dropped

bool bindHandle(Handle *hndl) {
DbAddr *array = hndl->map->arena->handleArray;
uint32_t actve;
bool first;

	if (hndl->status[0] & HANDLE_dead)
		return false;

	//	increment count of active api entries

	actve = atomicAdd32(hndl->status, HANDLE_incr);

	if (actve & HANDLE_dead)
		return releaseHandle(hndl), false;

	//	is there a DROP request active?

	if (hndl->map->arena->mutex[0] & DEAD_BIT) {
		atomicOr32(hndl->status, HANDLE_dead);
		releaseHandle(hndl);
		return false;
	}

	//  is this the first bind?

	if ((actve >> 1) > 1)
		waitNonZero64(&hndl->array->objTs);
	else
		hndl->array->objTs = hndl->map->arena->delTs;

	return true;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
uint32_t actve;

	actve = atomicAdd32(hndl->status, -HANDLE_incr);

	//	decrement active arena pointer count
	//	if we are the last active api entry

	if (actve == HANDLE_dead) {
		checkHandles(hndl);
		return;
	}

	waitNonZero64(&hndl->array->objTs);
	hndl->array->objTs = 0;
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

