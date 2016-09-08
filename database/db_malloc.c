#include <errno.h>

#ifndef __APPLE__
#include <malloc.h>
#endif

#include "db.h"
#include "db_map.h"
#include "db_arena.h"
#include "db_malloc.h"

#define MAX_blk 24

//
// Raw object wrapper
//

typedef struct RawObj {
	DbAddr addr[1];
} rawobj_t;

DbArena memArena[1];
DbMap memMap[1];

void memInit() {
	memMap->arena = memArena;
#ifdef _WIN32
	memMap->hndl[0] = INVALID_HANDLE_VALUE;
#else
	memMap->hndl[0] = -1;
#endif
}

void db_free (void *obj) {
rawobj_t *raw = obj;

	if (raw[-1].addr->dead) {
		fprintf(stderr, "Duplicate db_free\n");
		exit (1);
	}

	addSlotToFrame(memMap, &memArena->freeBlk[raw[-1].addr->type], raw[-1].addr->bits);
	raw[-1].addr->dead = 1;
}

//	raw memory allocator

uint64_t db_rawalloc(uint32_t amt, bool zeroit) {
uint32_t bits = 3;
uint64_t addr;

#ifdef _WIN32
	_BitScanReverse((unsigned long *)&bits, amt - 1);
	bits++;
#else
	bits = 32 - (__builtin_clz (amt - 1));
#endif
	if ((addr = allocObj(memMap, &memArena->freeBlk[bits], NULL, bits, 1UL << bits, zeroit)))
		return addr;

	fprintf (stderr, "out of memory!\n");
	exit(1);
}

void *db_rawaddr(uint64_t rawAddr) {
DbAddr addr;

	addr.bits = rawAddr;
	return getObj(memMap, addr);
}

void db_rawfree(uint64_t rawAddr) {
DbAddr addr;

	addr.bits = rawAddr;
	addSlotToFrame(memMap, &memArena->freeBlk[addr.type], rawAddr);
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
uint32_t amt = size + sizeof(rawobj_t), bits;
rawobj_t *raw = old, *mem;
uint32_t oldSize, newSize;
DbAddr addr[1];
int oldBits;

#ifdef _WIN32
	_BitScanReverse((unsigned long *)&bits, amt - 1);
	bits++;
#else
	bits = 32 - (__builtin_clz (amt - 1));
#endif
	if (raw[-1].addr->dead) {
		fprintf(stderr, "Duplicate db_realloc\n");
		exit (1);
	}

	//  is the new size within the same power of two?

	oldBits = raw[-1].addr->type;
	oldSize = 1UL << oldBits;
	newSize = 1UL << bits;

	if (oldBits == bits)
		return old;

	if ((addr->bits = allocObj(memMap, &memArena->freeBlk[bits], NULL, bits, newSize, zeroit)))
		mem = getObj(memMap, *addr);
	else {
		fprintf (stderr, "out of memory!\n");
		exit(1);
	}

	//  copy contents and release old allocation

	memcpy(mem, raw - 1, oldSize);

	if (zeroit)
		memset((char *)mem + oldSize, 0, newSize - oldSize);

	addSlotToFrame(memMap, &memArena->freeBlk[oldBits], raw[-1].addr->bits);
	raw[-1].addr->dead = 1;

	mem->addr->bits = addr->bits;
	return mem + 1;
}

