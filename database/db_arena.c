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
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

extern DbMap memMap[1];

bool mapSeg (DbMap *map, uint32_t currSeg);
void mapZero(DbMap *map, uint64_t size);
void mapAll (DbMap *map);

//  create/open a Object store/index arena file

void *createMap(char *path, uint32_t baseSize, uint32_t objSize, uint64_t initSize, bool onDisk) {

#ifdef _WIN32
HANDLE fileHndl;
DWORD amt = 0;
#else
int fileHndl;
int32_t amt = 0;
#endif

DbArena *segZero = NULL;
uint32_t segOffset;
Handle *hndl;
DbMap *map;

	map = db_malloc(sizeof(DbMap), true);
	map->onDisk = onDisk;
	strncpy(map->path, path, sizeof(map->path));

	if (onDisk) {
#ifdef _WIN32
		fileHndl = openPath (map->path, 0);

		if (fileHndl == INVALID_HANDLE_VALUE) {
			db_free(map);
			return NULL;
		}

		segZero = VirtualAlloc(NULL, sizeof(DbArena), MEM_COMMIT, PAGE_READWRITE);
		lockArena(fileHndl, path);

		if (!ReadFile(fileHndl, segZero, sizeof(DbArena), &amt, NULL)) {
			fprintf (stderr, "Unable to read %lld bytes from %s, error = %d", sizeof(DbArena), path, errno);
			VirtualFree(segZero, 0, MEM_RELEASE);
			unlockArena(fileHndl, path);
			CloseHandle(fileHndl);
			db_free(map);
			return NULL;
		}
#else
		fileHndl = openPath (map->path, 0);

		if (fileHndl == -1) {
			db_free(map);
			return NULL;
		}

		// read first part of segment zero if it exists

		lockArena(fileHndl, path);
		segZero = valloc(sizeof(DbArena));

		if ((amt = pread(fileHndl, segZero, sizeof(DbArena), 0))) {
			if (amt < sizeof(DbArena)) {
				fprintf (stderr, "Unable to read %d bytes from %s, error = %d", (int)sizeof(DbArena), path, errno);
				unlockArena(fileHndl, path);
				free(segZero);
				close(fileHndl);
				db_free(map);
				return NULL;
			}
		}
#endif
	} else
#ifdef _WIN32
		fileHndl = INVALID_HANDLE_VALUE;
#else
		fileHndl = -1;
#endif

	map->hndl[0] = fileHndl;

	//  if segment zero exists, map the arena

	if (amt) {
		unlockArena(fileHndl, path);
		mapZero(map, segZero->segs->size);
#ifdef _WIN32
		VirtualFree(segZero, 0, MEM_RELEASE);
		CloseHandle(fileHndl);
#else
		free(segZero);
#endif
		// wait for initialization to finish

		waitNonZero(map->arena->type);
		return makeHandle(map);
	}

	if (segZero) {
#ifdef _WIN32
		VirtualFree(segZero, 0, MEM_RELEASE);
#else
		free(segZero);
#endif
	}

	segOffset = sizeof(DbArena) + baseSize;
	segOffset += 7;
	segOffset &= -8;

	if (initSize < segOffset)
		initSize = segOffset;

	if (initSize < MIN_segsize)
		initSize = MIN_segsize;

	initSize += 65535;
	initSize &= -65536;

	//  create initial segment on unix, windows will automatically do it

#ifndef _WIN32
	if (onDisk && ftruncate(fileHndl, initSize)) {
		fprintf (stderr, "Unable to initialize file %s, error = %d", path, errno);
		close(fileHndl);
		db_free(map);
		return NULL;
	}
#endif

	//  initialize new arena segment zero

	mapZero(map, initSize);
	map->arena->nextObject.offset = segOffset >> 3;
	map->arena->segs->size = initSize;
	map->arena->objSize = objSize;
	map->arena->delTs = 1;
	map->created = true;

	if (onDisk)
		unlockArena(fileHndl, path);
#ifdef _WIN32
	CloseHandle(fileHndl);
#endif
	return makeHandle(map);
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
uint64_t off = map->arena->segs[map->arena->currSeg].off;
uint64_t size = map->arena->segs[map->arena->currSeg].size;
uint32_t nextSeg = map->arena->currSeg + 1;

	minSize += sizeof(DbSeg);

	// bootstrapping new inMem arena?

	if (size)
		off += size;
	else
		nextSeg = 0;

	if (size < MIN_segsize / 2)
		size = MIN_segsize / 2;

	// double the current size up to 32GB

	do size <<= 1;
	while (size < minSize);

	if (size > 32ULL * 1024 * 1024 * 1024)
		size = 32ULL * 1024 * 1024 * 1024;

	map->arena->segs[nextSeg].off = off;
	map->arena->segs[nextSeg].size = size;
	map->arena->segs[nextSeg].nextObj.segment = nextSeg;

	//  extend the disk file, windows does this automatically

#ifndef _WIN32
	if (map->hndl[0] >= 0)
	  if (ftruncate(map->hndl[0], off + size)) {
		fprintf (stderr, "Unable to initialize file %s, error = %d", map->path, errno);
		return false;
	  }
#endif

	if (!mapSeg(map, nextSeg))
		return false;

	map->maxSeg = nextSeg;

	map->arena->nextObject.offset = nextSeg ? 0 : 1;
	map->arena->nextObject.segment = nextSeg;
	map->arena->currSeg = nextSeg;
	return true;
}

//  allocate an object from non-wait frame list
//  return 0 if out of memory.

uint64_t allocObj( DbMap* map, DbAddr *free, DbAddr *tail, int type, uint32_t size, bool zeroit ) {
uint32_t bits = 3;
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
		if (tail)
			tail += type;
	}

	lockLatch(free->latch);

	while (!(slot.bits = getNodeFromFrame(map, free))) {
	  if (!getNodeWait(map, free, tail))
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
		  - map->arena->segs[slot.segment].nextObj.index * sizeof(DbAddr);

		if (slot.offset * 8ULL + size > max)
			fprintf(stderr, "allocObj segment overrun\n"), exit(1);
	}

	slot.type = type;
	return slot.bits;
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
		  - map->arena->segs[map->arena->currSeg].nextObj.index * map->arena->objSize;

	size += 7;
	size &= -8;

	// see if existing segment has space
	// otherwise allocate a new segment.

	if (map->arena->nextObject.offset * 8ULL + size > max) {
		if (!newSeg(map, size)) {
			unlockLatch (map->arena->mutex);
			return 0;
		}
	}

	addr = map->arena->nextObject.bits;
	map->arena->nextObject.offset += size >> 3;
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

void *fetchObjSlot (DbMap *map, ObjId objId) {
	if (!objId.index) {
		fprintf (stderr, "Invalid zero document index: %s\n", map->path);
		exit(1);
	}

	return map->base[objId.segment] + map->arena->segs[objId.segment].size - objId.index * map->arena->objSize;
}

uint64_t allocNode(DbMap *map, FreeList *list, int type, uint32_t size, bool zeroit) {
unsigned long bits = 3;
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
		if (!initObjFrame(map, list[type].free, type, size))
		  return unlockLatch(list[type].free->latch), 0;

	unlockLatch(list[type].free->latch);

	if (zeroit)
		memset (getObj(map, slot), 0, size);

	slot.type = type;
	return slot.bits;
}
