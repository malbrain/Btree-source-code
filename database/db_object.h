#pragma once

#include "db_lock.h"

enum ObjType {
	FrameType,
	ObjIdType,			// ObjId value
	MinObjType = 3,		// minimum object size in bits
	MaxObjType = 24		// each power of two, 3 - 24
};

typedef struct {
	DbAddr head[1];		// earliest frame waiting to be recycled
	DbAddr tail[1];		// location of latest frame to be recycle
	DbAddr free[1];		// frames of free objects
} FreeList;

//	Local Handle for an arena

#define HANDLE_dead	0x1
#define HANDLE_incr	0x2

struct Handle_ {
	DbMap *map;			// pointer to map
	uint32_t status[1];	// active entry count/dead status
	uint16_t arenaIdx;	// arena handle table entry index
	uint16_t hndlType;	// type of arena map
	FreeList list[MaxObjType];
};

/**
 * even =>  reader timestamp
 * odd  =>  writer timestamp
 */

enum ReaderWriterEnum {
	en_reader,
	en_writer,
	en_current
};

//	skip list head

typedef struct {
	DbAddr head[1];		// list head
	RWLock2 lock[1];	// reader/writer lock
} SkipHead;

//	skip list entry

typedef struct {
	uint64_t key[1];	// entry key
	uint64_t val[1];	// entry value
} SkipEntry;

//	skip list entry array

#define SKIP_node 15

typedef struct {
	DbAddr next[1];				// next block of keys
	SkipEntry array[SKIP_node];	// array of key/value pairs
} SkipList;

//	regular list

typedef struct {
	DbAddr next[1];		// next in list
	SkipEntry node[1];	// this element
} DbList;

//	Identifier bits

#define CHILDID_DROP 0x1
#define CHILDID_INCR 0x2

bool isReader(uint64_t ts);
bool isWriter(uint64_t ts);
bool isCommitted(uint64_t ts);

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *result);
uint32_t store64(uint8_t *key, uint32_t keylen, uint64_t what);
void closeHandle(Handle  *hndl);
Handle *makeHandle(DbMap *map);

Handle *bindHandle(void **hndl);
void releaseHandle(Handle *hndl);

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void *arrayAssign(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void arrayExpand(DbMap *map, DbAddr *array, size_t size, uint16_t idx);
uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);

void *skipFind(DbMap *map, DbAddr *skip, uint64_t key);
void *skipPush(DbMap *map, DbAddr *skip, uint64_t key);
void *skipAdd(DbMap *map, DbAddr *skip, uint64_t key);
void skipDel(DbMap *map, DbAddr *skip, uint64_t key);

void *listAdd(DbMap *map, DbAddr *list, uint64_t key);
void *listFind(DbMap *map, DbAddr *list, uint64_t key);
