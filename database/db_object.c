#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

//	return payload address for an array element idx

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size) {
uint64_t *inUse;
uint8_t *base;
DbAddr *addr;

	lockLatch(array->latch);

	if (!array->addr) {
		array->bits = allocBlk(map, sizeof(DbAddr) * 256, true) | ADDR_MUTEX_SET;
		addr = getObj(map, *array);
		addr->bits = allocBlk(map, sizeof(uint64_t) + size * 64, true);
	} else
		addr = getObj(map, *array);

	while (idx / 64 > array->maxidx)
	  if (array->maxidx == 255) {
#ifdef DEBUG
		fprintf(stderr, "Array Overflow file: %s\n", map->path);
#endif
		return NULL;
	  } else
		addr[++array->maxidx].bits = allocBlk(map, sizeof(uint64_t) + size * 64, true);

	inUse = getObj(map, addr[idx / 64]);
	*inUse |= 1ULL << idx % 64;

	base = (uint8_t *)(inUse + 1);
	base += size * (idx % 64);
	unlockLatch(array->latch);

	return (void *)base;
}

//	allocate an array element

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size) {
unsigned long bits[1];
uint64_t *inUse;
DbAddr *addr;
int idx, max;

	lockLatch(array->latch);

	if (!array->addr) {
		array->bits = allocBlk(map, sizeof(DbAddr) * 256, true) | ADDR_MUTEX_SET;
		addr = getObj(map, *array);
		addr->bits = allocBlk(map, sizeof(uint64_t) + size * 64, true);
	} else
		addr = getObj(map, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
		inUse = getObj(map, addr[idx]);

		//  skip completely used array entry

		if (inUse[0] == ULLONG_MAX)
			continue;

#		ifdef _WIN32
		  _BitScanForward64(bits, ~inUse[0]);
#		else
		  *bits = (__builtin_ffs (~inUse[0])) - 1;
#		endif

		*inUse |= 1ULL << *bits;
		unlockLatch(array->latch);
		return *bits + idx * 64;
	}

	// current array is full
	//	allocate a new segment

	if (array->maxidx == 255) {
		fprintf(stderr, "Array Overflow file: %s\n", map->path);
		exit(1);
	 }

	addr[++array->maxidx].bits = allocBlk(map, sizeof(uint64_t) + size * 64, true);
	inUse = getObj(map, addr[idx]);
	*inUse = 1ULL;

	unlockLatch(array->latch);
	return array->maxidx * 64;
}

Handle *makeHandle(DbMap *map) {
DbAddr *array = map->arena->handleArray;
Handle *hndl;
uint16_t idx;

	idx = arrayAlloc(map, array, sizeof(Handle));

	hndl = arrayElement(map, array, idx, sizeof(Handle));
	hndl->hndlType = *map->arena->type;
	hndl->arenaIdx = idx;
	hndl->map = map;
	return hndl;
}

//	return handle

void returnHandle(Handle  *hndl) {
DbAddr *array = hndl->map->arena->handleArray;
DbAddr *addr = getObj(hndl->map, *array);
uint64_t *inUse;

	lockLatch(array->latch);
	inUse = getObj(hndl->map, addr[hndl->arenaIdx / 64]);

	// clear handle in-use bit

	inUse[0] &= ~(1ULL << (hndl->arenaIdx % 64));
	unlockLatch(array->latch);
}

//	bind handle for use in API call
//	return false if arena dropped

Handle *bindHandle(void **hndlPtr) {
uint32_t actve;
Handle *hndl;

	if (!(hndl = *hndlPtr))
		return NULL;

	if (hndl->status[0] & HANDLE_dead)
		return NULL;

	//	increment count of active binds

	actve = atomicAdd32(hndl->status, HANDLE_incr);

	if (actve & HANDLE_dead)
		return releaseHandle(hndl), NULL;

	//	is there a DROP request active?

	if (~hndl->map->arena->mutex[0] & ALIVE_BIT) {
		atomicOr32(hndl->status, HANDLE_dead);
		return releaseHandle(hndl), NULL;
	}

	return hndl;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
	atomicAdd32(hndl->status, -HANDLE_incr);
}

//	peel off 64 bit suffix value from key
//	return number of key bytes remaining

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *where) {
uint32_t xtrabytes = key[len - 1] & 0x7;
uint64_t result;
int idx = 0;

	len -= xtrabytes + 2;
	result = key[len] & 0x1f;

	while (idx++ < xtrabytes) {
	  result <<= 8;
	  result |= key[len + idx];
	}

	result <<= 5;
	result |= key[len + idx] >> 3;

	if (where)
		*where = result;

	return len;
}

// concatenate key with 64 bit value
// returns length of concatenated key

uint32_t store64(uint8_t *key, uint32_t keylen, uint64_t recId) {
uint64_t tst64 = recId >> 10;
uint32_t xtrabytes = 0;
uint32_t idx;

	while (tst64)
		xtrabytes++, tst64 >>= 8;

    key[keylen + xtrabytes + 1] = (recId & 0x1f) << 3 | xtrabytes;

    recId >>= 5;

    for (idx = xtrabytes; idx; idx--) {
        key[keylen + idx] = (recId & 0xff);
        recId >>= 8;
    }

    key[keylen] = recId | (xtrabytes << 5);
    return keylen + xtrabytes + 2;
}

//	allocate a new timestamp

uint64_t allocateTimestamp(DbMap *map, enum ReaderWriterEnum e) {
DataBase *db = database(map->db);
uint64_t ts;

	ts = *db->timestamp;

	if (!ts)
		ts = atomicAdd64(db->timestamp, 1);

	switch (e) {
	case en_reader:
		while (!isReader(ts))
			ts = atomicAdd64(db->timestamp, 1);
		break;
	case en_writer:
		while (!isWriter(ts))
			ts = atomicAdd64(db->timestamp, 1);
		break;

	default: break;
	}

	return ts;
}

//	reader == even

bool isReader(uint64_t ts) {
	return !(ts & 1);
}

//	writer == odd

bool isWriter(uint64_t ts) {
	return (ts & 1);
}

//	committed == not reader

bool isCommitted(uint64_t ts) {
	return (ts & 1);
}

