#include <errno.h>

#ifndef __APPLE__
#include <malloc.h>
#endif

#include "db.h"
#include "db_map.h"
#include "db_arena.h"
#include "db_malloc.h"

//
// Raw object wrapper
//

typedef struct RawObj {
	DbAddr addr[1];
} rawobj_t;

DbArena memArena[1];
DbMap memMap[1];

void memInit() {
ArenaDef arenaDef[1];

	memMap->arena = memArena;
	memMap->db = memMap;

#ifdef _WIN32
	memMap->hndl = INVALID_HANDLE_VALUE;
#else
	memMap->hndl = -1;
#endif

	memset (arenaDef, 0, sizeof(arenaDef));
	initMap(memMap, arenaDef);
}

void db_free (void *obj) {
rawobj_t *raw = obj;

	if (!raw[-1].addr->alive) {
		fprintf(stderr, "Duplicate db_free\n");
		exit (1);
	}

	raw[-1].addr->alive = 0;
	freeBlk(memMap, raw[-1].addr);
}

//	raw memory allocator

uint64_t db_rawalloc(uint32_t amt, bool zeroit) {
uint64_t addr;

	if ((addr = allocBlk(memMap, amt, zeroit)))
		return addr;

	fprintf (stderr, "out of memory!\n");
	exit(1);
}

//	allocate object

void *db_malloc(uint32_t len, bool zeroit) {
rawobj_t *mem;
DbAddr addr;

	addr.bits = db_rawalloc(len + sizeof(rawobj_t), zeroit);
	mem = getObj(memMap, addr);
	mem->addr->bits = addr.bits;
	return mem + 1;
}

uint32_t db_size (void *obj) {
rawobj_t *raw = obj;

	return (1 << raw[-1].addr->type) - sizeof(rawobj_t);
}

void *db_realloc(void *old, uint32_t size, bool zeroit) {
uint32_t amt = size + sizeof(rawobj_t);
rawobj_t *raw = old, *mem;
uint32_t oldSize, newSize;
DbAddr addr[1];

	if (!raw[-1].addr->alive) {
		fprintf(stderr, "Duplicate db_realloc\n");
		exit (1);
	}

	//  is the new size within the same power of two?

	oldSize = 1ULL << raw[-1].addr->type;

	if (oldSize >= amt)
		return old;

	if ((addr->bits = allocBlk(memMap, amt, zeroit)))
		mem = getObj(memMap, *addr);
	else {
		fprintf (stderr, "out of memory!\n");
		exit(1);
	}

	//  copy contents and release old allocation

	newSize = 1UL << addr->type;

	memcpy(mem, raw - 1, oldSize);

	if (zeroit)
		memset((char *)mem + oldSize, 0, newSize - oldSize);

	freeBlk (memMap, raw[-1].addr);
	mem->addr->bits = addr->bits;
	return mem + 1;
}

