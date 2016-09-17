#pragma once

enum ObjType {
	FrameType,
	ObjIdType,			// ObjId value
	SkipType,			// skip list node
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
	FreeList list[MaxObjType];
};

typedef struct {
	Handle *hndl;		// docStore handle
	Handle *indexes[64];// handles to indexes
	uint16_t count;		// number of indexes
} DocStore;

typedef struct {
	DbAddr prevDoc[1];	// previous version of doc
	uint64_t version;	// version of the document
	ObjId docId;		// ObjId of the document
	ObjId txnId;		// insert/update txn ID
	ObjId delId;		// delete txn ID
	uint32_t size;		// object size
} Document;

typedef struct {
	uint32_t size;
} Object;

/**
 * even =>  reader timestamp
 * odd  =>  writer timestamp
 */

enum ReaderWriterEnum {
	en_reader,
	en_writer,
	en_current
};

bool isReader(uint64_t ts);
bool isWriter(uint64_t ts);
bool isCommitted(uint64_t ts);

uint64_t get64(uint8_t *from);
void store64(uint8_t *to, uint64_t what);
void closeHandle(Handle  *hndl);
Handle *makeHandle(DbMap *map);

bool bindHandle(Handle *hndl);
void releaseHandle(Handle *hndl);

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void *arrayAssign(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void arrayExpand(DbMap *map, DbAddr *array, size_t size, uint16_t idx);
uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);

uint64_t *skipFind(Handle *hndl, DbAddr *skip, uint64_t key);
void skipPush(Handle *hndl, DbAddr *skip, uint64_t key, uint64_t val);
void skipDel(Handle *hndl, DbAddr *skip, uint64_t key);
