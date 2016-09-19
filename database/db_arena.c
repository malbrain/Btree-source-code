#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#endif

#include "db.h"
#include "db_map.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_frame.h"

extern DbMap memMap[1];

DbMap *initMap (DbMap *map, ArenaDef *arenaDef);
bool mapSeg (DbMap *map, uint32_t currSeg);
void mapZero(DbMap *map, uint64_t size);
void mapAll (DbMap *map);

//	open map given red/black entry

DbMap *arenaRbMap(Handle *hndl, RedBlack *entry) {
DbMap *parent = hndl->map, **catalog, *map;
ArenaDef *arenaDef;

	arenaDef = rbPayload(entry);

	writeLock2(parent->childMaps->lock);
	catalog = skipAdd(hndl, parent->childMaps->head, arenaDef->id);

	//  open it

	if (!*catalog)
		*catalog = openMap(parent, entry->key, entry->keyLen, arenaDef);

	map = *catalog;
	writeUnlock2(parent->childMaps->lock);
	return map;
}

// return existing arena from parent's child list or NULL
//	call with parent nameTree r/b tree locked

DbMap *arenaMap(Handle *hndl, char *name, uint32_t nameLen, PathStk *path) {
DbMap *parent = hndl->map;
DbMap *db = parent->db;
RedBlack *entry;

	//	see if this arena map has already been opened in our process

	if ((entry = rbFind(db, parent->arenaDef->nameTree, name, nameLen, path)))
	  return arenaRbMap(hndl, entry);

	return NULL;
}

//  open/create arena
//	call with parent's nameTree r/b tree locked

DbMap *createMap(Handle *hndl, char *name, uint32_t nameLen, uint32_t localSize, uint32_t baseSize, uint32_t objSize, uint64_t initSize, bool onDisk) {
DbMap *map, *db = hndl->map->db;
DbMap *parent = hndl->map;
ArenaDef *arenaDef;
DbMap **catalog;
PathStk path[1];
RedBlack *entry;
DbAddr *rbAddr;

	//	see if this arena ArenaDef already exists

	if ((entry = rbFind(db, parent->arenaDef->nameTree, name, nameLen, path)))
		return arenaRbMap(hndl, entry);

	// otherwise, create new database ArenaDef entry

	if ((entry = rbNew(db, name, nameLen, sizeof(ArenaDef))))
		arenaDef = rbPayload(entry);
	else
		return NULL;

	arenaDef->id = atomicAdd64(&parent->arenaDef->childId, CHILDID_INCR);
	arenaDef->node.bits = entry->addr.bits;
	arenaDef->localSize = localSize;
	arenaDef->initSize = initSize;
	arenaDef->baseSize = baseSize;
	arenaDef->objSize = objSize;
	arenaDef->onDisk = onDisk;

	map = openMap(parent, name, nameLen, arenaDef);

	writeLock2(parent->childMaps->lock);
	catalog = skipAdd(hndl, parent->childMaps->head, arenaDef->id);
	*catalog = map;

	writeUnlock2(parent->childMaps->lock);

	//	add arena to parent child arenas

	rbAdd(parent, parent->arenaDef->nameTree, entry, path);

	//	add arenaId to child idList

	writeLock2(parent->arenaDef->idList->lock);
	rbAddr = skipAdd (hndl, parent->arenaDef->idList->head, arenaDef->id);
	rbAddr->bits = entry->addr.bits;
	writeUnlock2(parent->arenaDef->idList->lock);
	return map;
}

//  open/create an Object database/store/index arena file
//	call with parent's nameTree r/b tree locked

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arenaDef) {
DbArena *segZero = NULL;
DataBase *db;
DbMap *map;

#ifdef _WIN32
DWORD amt = 0;
#else
int32_t amt = 0;
#endif

	map = db_malloc(sizeof(DbMap) + arenaDef->localSize, true);
	map->pathLen = getPath(map->path, MAX_path, name, nameLen, parent);

	if ((map->parent = parent))
		map->db = parent->db;
	else
		map->db = map;

	if (!(map->onDisk = arenaDef->onDisk)) {
#ifdef _WIN32
		map->hndl = INVALID_HANDLE_VALUE;
#else
		map->hndl = -1;
#endif
		initMap(map, arenaDef);
		return map;
	}

	//	open the onDisk arena file

#ifdef _WIN32
	map->hndl = openPath (map->path);

	if (map->hndl == INVALID_HANDLE_VALUE) {
		db_free(map);
		return NULL;
	}

	lockArena(map);

	segZero = VirtualAlloc(NULL, sizeof(DbArena), MEM_COMMIT, PAGE_READWRITE);

	if (!ReadFile(map->hndl, segZero, sizeof(DbArena), &amt, NULL)) {
		fprintf (stderr, "Unable to read %lld bytes from %s, error = %d", sizeof(DbArena), map->path, errno);
		VirtualFree(segZero, 0, MEM_RELEASE);
		CloseHandle(map->hndl);
		db_free(map);
		return NULL;
	}
#else
	map->hndl = openPath (map->path);

	if (map->hndl == -1) {
		db_free(map);
		return NULL;
	}

	lockArena(map);

	// read first part of segment zero if it exists

	segZero = valloc(sizeof(DbArena));

	amt = pread(map->hndl, segZero, sizeof(DbArena), 0LL);

	if (amt < 0) {
		fprintf (stderr, "Unable to read %d bytes from %s, error = %d", (int)sizeof(DbArena), map->path, errno);
		free(segZero);
		close(map->hndl);
		db_free(map);
		return NULL;
	}
#endif
	if (amt < sizeof(DbArena)) {
		initMap(map, arenaDef);
		unlockArena(map);
		return map;
	}

	//  since segment zero exists, map the arena

	mapZero(map, segZero->segs->size);
#ifdef _WIN32
	VirtualFree(segZero, 0, MEM_RELEASE);
#else
	free(segZero);
#endif

	map->arenaDef = arenaDef;
	unlockArena(map);

	// wait for initialization to finish

	waitNonZero(map->arena->type);
	return map;
}

//	finish creating new arena
//	call with arena locked

DbMap *initMap (DbMap *map, ArenaDef *arenaDef) {
uint64_t initSize = arenaDef->initSize;
uint32_t segOffset;
uint32_t bits;
DataBase *db;

	segOffset = sizeof(DbArena) + arenaDef->baseSize;
	segOffset += 7;
	segOffset &= -8;

	if (initSize < segOffset)
		initSize = segOffset;

	if (initSize < MIN_segsize)
		initSize = MIN_segsize;

	initSize += 65535;
	initSize &= -65536;

#ifdef _WIN32
	_BitScanReverse((unsigned long *)&bits, initSize - 1);
	bits++;
#else
	bits = 32 - (__builtin_clz (initSize - 1));
#endif
	//  create initial segment on unix, windows will automatically do it

	initSize = 1ULL << bits;

#ifndef _WIN32
	if (ftruncate(map->hndl, initSize)) {
		fprintf (stderr, "Unable to initialize file %s, error = %d", map->path, errno);
		close(map->hndl);
		db_free(map);
		return NULL;
	}
#endif

	//  initialize new arena segment zero

	mapZero(map, initSize);
	map->arena->segs[map->arena->currSeg].nextObject.offset = segOffset >> 3;
	map->arena->objSize = arenaDef->objSize;
	map->arena->segs->size = initSize;
	map->arena->segBits = bits;
	map->arena->delTs = 1;

	//	are we a database?

	if (map->parent) {
		map->arenaDef = arenaDef;
		return map;
	}

	db = database(map);
	map->arenaDef = db->arenaDef;
	memcpy(db->arenaDef, arenaDef, sizeof(ArenaDef));
	return map;
}

//  initialize arena segment zero

void mapZero(DbMap *map, uint64_t size) {

	map->arena = mapMemory (map, 0, size, 0);
	map->base[0] = (char *)map->arena;

	mapAll(map);
}

//  extend arena into new segment
//  return FALSE if out of memory

bool newSeg(DbMap *map, uint32_t minSize) {
uint64_t size = map->arena->segs[map->arena->currSeg].size;
uint64_t off = map->arena->segs[map->arena->currSeg].off;
uint32_t nextSeg = map->arena->currSeg + 1;
uint64_t nextSize;

	off += size;

	// bootstrapping new inMem arena?

	if (map->arena->segBits == 0) {
		map->arena->segBits = MIN_segbits - 1;
		nextSize = MIN_segsize;
		nextSeg = 0;
	} else
		nextSize = 1ULL << map->arena->segBits;

	while (nextSize < minSize)
	 	if (map->arena->segBits++ < MAX_segbits)
			nextSize += nextSeg ? 1ULL << map->arena->segBits : nextSize;
		else
			fprintf(stderr, "newSeg segment overrun: %d\n", minSize), exit(1);

	if (map->arena->segBits < MAX_segbits)
		map->arena->segBits++;

	if (nextSize > 1ULL << MAX_segbits)
		nextSize = 1ULL << MAX_segbits;

#ifdef _WIN32
	assert(__popcnt64(off + nextSize) == 1);
#else
	assert(__builtin_popcountll(off + nextSize) == 1);
#endif

	map->arena->segs[nextSeg].off = off;
	map->arena->segs[nextSeg].size = nextSize;
	map->arena->segs[nextSeg].nextId.seg = nextSeg;
	map->arena->segs[nextSeg].nextObject.segment = nextSeg;
	map->arena->segs[nextSeg].nextObject.offset = nextSeg ? 0 : 1;

	//  extend the disk file, windows does this automatically

#ifndef _WIN32
	if (map->hndl >= 0)
	  if (ftruncate(map->hndl, (off + nextSize))) {
		fprintf (stderr, "Unable to initialize file %s, error = %d", map->path, errno);
		return false;
	  }
#endif

	if (!mapSeg(map, nextSeg))
		return false;

	map->arena->currSeg = nextSeg;
	map->maxSeg = nextSeg;
	return true;
}

//  allocate an object from non-wait frame list
//  return 0 if out of memory.

uint64_t allocObj(DbMap* map, DbAddr *free, int type, uint32_t size, bool zeroit ) {
uint32_t bits;
DbAddr slot;

	size += 7;
	size &= -8;
#ifdef _WIN32
	_BitScanReverse((unsigned long *)&bits, size - 1);
	bits++;
#else
	bits = 32 - (__builtin_clz (size - 1));
#endif
	if (type < 0) {
		type = bits;
		free += type;
	}

	lockLatch(free->latch);

	while (!(slot.bits = getNodeFromFrame(map, free))) {
	  if (!getNodeWait(map, free, NULL))
		if (!initObjFrame(map, free, type, size)) {
			unlockLatch(free->latch);
			return 0;
		}
	}

	unlockLatch(free->latch);

	if (zeroit)
		memset (getObj(map, slot), 0, size);

	{
		uint64_t max = map->arena->segs[slot.segment].size
		  - map->arena->segs[slot.segment].nextId.index * sizeof(DbAddr);

		if (slot.offset * 8ULL + size > max)
			fprintf(stderr, "allocObj segment overrun\n"), exit(1);
	}

	slot.type = type;
	return slot.bits;
}

void freeBlk(DbMap *map, DbAddr *addr) {
	addSlotToFrame(map, &map->arena->freeBlk[addr->type], addr->bits);
}

uint64_t allocBlk(DbMap *map, uint32_t size, bool zeroit) {
	return allocObj(map, map->arena->freeBlk, -1, size, zeroit);
}

void mapAll (DbMap *map) {
	lockLatch(map->mapMutex);

	while (map->maxSeg < map->arena->currSeg)
		if (mapSeg (map, map->maxSeg + 1))
			map->maxSeg++;
		else
			fprintf(stderr, "Unable to map segment %d on map %s\n", map->maxSeg + 1, map->path), exit(1);

	unlockLatch(map->mapMutex);
}

void* getObj(DbMap *map, DbAddr slot) {
	if (!slot.addr) {
		fprintf (stderr, "Invalid zero DbAddr: %s\n", map->path);
		exit(1);
	}

	//  catch up segment mappings

	if (slot.segment > map->maxSeg)
		mapAll(map);

	return map->base[slot.segment] + slot.offset * 8ULL;
}

//	close the arena

void closeMap(DbMap *map) {
	while (map->maxSeg)
		unmapSeg(map, map->maxSeg--);

	map->arena = NULL;
}

//  allocate raw space in the current segment
//  or return 0 if out of memory.

uint64_t allocMap(DbMap *map, uint32_t size) {
uint64_t max, addr;

	lockLatch(map->arena->mutex);

	max = map->arena->segs[map->arena->currSeg].size
		  - map->arena->segs[map->arena->objSeg].nextId.index * map->arena->objSize;

	size += 7;
	size &= -8;

	// see if existing segment has space
	// otherwise allocate a new segment.

	if (map->arena->segs[map->arena->currSeg].nextObject.offset * 8ULL + size > max) {
		if (!newSeg(map, size)) {
			unlockLatch (map->arena->mutex);
			return 0;
		}
	}

	addr = map->arena->segs[map->arena->currSeg].nextObject.bits;
	map->arena->segs[map->arena->currSeg].nextObject.offset += size >> 3;
	unlockLatch(map->arena->mutex);
	return addr;
}

bool mapSeg (DbMap *map, uint32_t currSeg) {
uint64_t size = map->arena->segs[currSeg].size;
uint64_t off = map->arena->segs[currSeg].off;

	if ((map->base[currSeg] = mapMemory (map, off, size, currSeg)))
		return true;

	return false;
}

//	return pointer to Obj slot

void *fetchIdSlot (DbMap *map, ObjId objId) {
	if (!objId.index) {
		fprintf (stderr, "Invalid zero document index: %s\n", map->path);
		exit(1);
	}

	return map->base[objId.seg] + map->arena->segs[objId.seg].size - objId.index * map->arena->objSize;
}

uint32_t Used, Alloc;

uint64_t allocNode(DbMap *map, FreeList *list, int type, uint32_t size, bool zeroit) {
unsigned long bits = 3;
uint32_t dup;
DbAddr slot;

	size += 7;
	size &= -8;

#ifdef _WIN32
	_BitScanReverse(&bits, size - 1);
	bits++;
#else
	bits = 32 - (__builtin_clz (size - 1));
#endif

	if (type < 0)
		type = bits;

	lockLatch(list[type].free->latch);

	while (!(slot.bits = getNodeFromFrame(map, list[type].free)))
	  if (!getNodeWait(map, list[type].free, list[type].tail))
		if (!(dup = initObjFrame(map, list[type].free, type, size)))
		  return unlockLatch(list[type].free->latch), 0;
#ifdef DEBUG
		else
		  atomicAdd32(&Alloc, dup);
#endif

	unlockLatch(list[type].free->latch);

	if (zeroit)
		memset (getObj(map, slot), 0, size);

#ifdef DEBUG
	atomicAdd32(&Used, 1);
#endif
	slot.type = type;
	return slot.bits;
}

void freeNode(DbMap *map, FreeList *list, DbAddr slot) {
	addSlotToFrame(map, list[slot.type].free, slot.bits);
	atomicAdd32(&Used, -1);
}

//
// allocate next available object id
//

uint64_t allocObjId(DbMap *map, FreeList *list, uint16_t idx) {
ObjId objId;

	lockLatch(list[ObjIdType].free->latch);

	// see if there is a free object in the free queue
	// otherwise create a new frame of new objects

	while (!(objId.bits = getNodeFromFrame(map, list[ObjIdType].free))) {
		if (!getNodeWait(map, list[ObjIdType].free, list[ObjIdType].tail))
			if (!initObjIdFrame(map, list[ObjIdType].free)) {
				unlockLatch(list[ObjIdType].free->latch);
				return 0;
			}
	}

	objId.idx = idx;
	unlockLatch(list[ObjIdType].free->latch);
	return objId.bits;
}
