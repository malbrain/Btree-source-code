#pragma once

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

//	handle array elements

typedef struct {
	uint64_t objTs;		// object timestamp on current API
	DbAddr freeList;	// node free list by type
} HandleArray;

//	Handle returned for arena

typedef struct {
	FreeList *freeList;	// pointer to free list
	uint32_t actve[1];	// active entry count
	uint32_t idx;		// handle table entry index
	DbMap *map;			// pointer to map
} Handle;

uint64_t get64(uint8_t *from);
void store64(uint8_t *to, uint64_t what);
void closeHandle(Handle  *hndl);
Handle *makeHandle(DbMap *map);

bool bindHandle(Handle *hndl);
void releaseHandle(Handle *hndl);

void *arrayElement(DbMap *map, DbAddr *array, uint32_t idx, size_t size);
uint32_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);
