#pragma once

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#define MAX_segs  1000
#define MIN_segsize  131072

#define MAX_path  4096
#define MAX_blk		24	// max arena blk size in bits

//	types of arenas

enum MapType {
	NotSetYet = 0,
	DatabaseType,
	DocStoreType,
	BtreeIndexType
};

//	Database transactions: ObjId
//	entries in the database arena

typedef struct {
	DbAddr txnFrame[1];	// head of frames containing txn steps
	uint64_t timestamp;	// txn committed timestamp
} Txn;

//	Transaction steps

typedef struct {
	ObjId objId;		// ObjId for the docStore
	uint16_t objIdx;	// arena entry idx for the docStore
	uint16_t subIdx;	// docStore arena entry idx for the index
	uint32_t size;		// transaction step object size
} TxnStep;

//  on disk arena segment

typedef struct {
	uint64_t off;		// file offset of segment
	uint64_t size;		// size of the segment
	ObjId nextObj;		// highest object ID in use
} DbSeg;

//  Child arena specifications

typedef struct ArenaDef_ {
	DbAddr node;				// database redblack node
	uint64_t id;				// arena id
	uint64_t arenaId;			// highest arenaId issued
	uint64_t initSize;			// initial arena size
	uint32_t localSize;			// extra space after DbMap
	uint32_t baseSize;			// extra space after DbArena
	uint32_t objSize;			// size of ObjectId array slot
	uint16_t cmd;				// 0 = add, 1 = delete
	uint16_t idx;				// arena handle array index
	uint8_t onDisk;				// arena onDisk/inMemory
	DbAddr next, prev;			// linked list
	DbAddr arenaHndlIdx[1];		// allocate local arena handles
	DbAddr arenaIdList[1];		// head of arena id list
	DbAddr arenaNames[1];		// arena name red/black tree
} ArenaDef;

//  on disk/mmap arena seg zero

struct DbArena_ {
	DbSeg segs[MAX_segs]; 		// segment meta-data
	uint64_t lowTs, delTs;		// low hndl ts, Incr on delete
	DbAddr freeBlk[MAX_blk];	// free blocks in frames
	DbAddr handleArray[1];		// arena handle array
	DbAddr freeFrame[1];		// free frames in frames
	DbAddr nextObject;			// next Object address
	uint64_t objCount;			// overall number of objects
	uint64_t objSpace;			// overall size of objects
	uint32_t objSize;			// size of object array element
	uint16_t currSeg;			// index of highest segment
	char mutex[1];				// arena allocation lock/drop flag
	char type[1];				// arena type
};

//	in memory arena map

struct DbMap_ {
	char *base[MAX_segs];	// pointers to mapped segment memory
#ifndef _WIN32
	int hndl;				// OS file handle
#else
	HANDLE hndl;
	HANDLE maphndl[MAX_segs];
#endif
	DbMap *parent;			// ptr to parent
	DbMap *database;		// ptr to database
	DbArena *arena;			// ptr to mapped seg zero
	char path[MAX_path];	// file database path
	DbAddr arenaMaps[1];	// array of DbMap pointers for open children
	DbAddr hndlArray[1];	// array of open handles issued for this arena
	ArenaDef *arenaDef;		// our arena definition
	uint64_t arenaId;		// last arena arenaId processsed
	uint16_t pathOff;		// start of path in buffer
	uint16_t maxSeg;		// maximum segment array index in use
	uint16_t myIdx;			// our index in parent's handle table
	char mapMutex[1];		// segment mapping mutex
	char created;			// set if map created
	char onDisk;			// on disk bool flag
};


/**
 *  memory mapping
 */

void* mapMemory(DbMap *map, uint64_t offset, uint64_t size, uint32_t segNo);
void unmapSeg(DbMap *map, uint32_t segNo);
bool mapSeg(DbMap *map, uint32_t segNo);

bool newSeg(DbMap *map, uint32_t minSize);
void mapSegs(DbMap *map);

int getPath(char *path, int off, char *name, int len, DbMap *parent);
#ifdef _WIN32
HANDLE openPath(char *name);
#else
int openPath(char *name);
#endif
