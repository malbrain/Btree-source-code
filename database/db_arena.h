#pragma once

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#define MAX_segs  1000
#define MIN_segbits  17
#define MIN_segsize  (1ULL << MIN_segbits)
#define MAX_segbits  (32 + 3)  // 32 bit offset and 3 bits multiplier

#define MAX_path  4096
#define MAX_blk		24	// max arena blk size in bits

//	types of arenas

enum MapType {
	NotSetYet = 0,
	DatabaseType,
	DocStoreType,
	BtreeIndexType
};

//	Database transactions: included ObjId frames

typedef struct {
	DbAddr txnFrame[1];	// frames containing Object Ids
	uint64_t timestamp;	// txn committed timestamp
	ObjId txnId;		// txn ID
} Txn;

//  on disk arena segment

typedef struct {
	uint64_t off;		// file offset of segment
	uint64_t size;		// size of the segment
	DbAddr nextObject;	// next Object address
	ObjId nextId;		// highest object ID in use
} DbSeg;

//  Child arena specifications

typedef struct ArenaDef_ {
	DbAddr node;				// database redblack node
	uint64_t id;				// our arena id in parent
	uint64_t arenaId;			// highest child arenaId issued
	uint64_t initSize;			// initial arena size
	uint32_t localSize;			// extra space after DbMap
	uint32_t baseSize;			// extra space after DbArena
	uint32_t objSize;			// size of ObjectId array slot
	uint16_t idx;				// arena handle array index
	uint8_t onDisk;				// arena onDisk/inMemory
	uint8_t cmd;				// 0 = add, 1 = delete
	DbAddr next, prev;			// linked list
	DbAddr arenaHndlIdx[1];		// allocate local child arena handles
	DbAddr arenaIdList[1];		// head of child arena id list
	DbAddr arenaNames[1];		// child arena name red/black tree
} ArenaDef;

//  on disk/mmap arena seg zero

struct DbArena_ {
	DbSeg segs[MAX_segs]; 		// segment meta-data
	uint64_t lowTs, delTs;		// low hndl ts, Incr on delete
	DbAddr freeBlk[MAX_blk];	// free blocks in frames
	DbAddr handleArray[1];		// handle array for our arena
	DbAddr freeFrame[1];		// free frames in frames
	uint64_t objCount;			// overall number of objects
	uint64_t objSpace;			// overall size of objects
	uint32_t objSize;			// size of object array element
	uint16_t currSeg;			// index of highest segment
	uint16_t objSeg;			// current segment index for ObjIds
	uint8_t segBits;			// segment size in bits
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
	DbMap *parent, *db;		// ptr to parent and database
	DbArena *arena;			// ptr to mapped seg zero
	char path[MAX_path];	// file database path
	DbAddr arenaMaps[1];	// array of DbMap pointers for open children
	ArenaDef *arenaDef;		// our arena definition
	uint64_t arenaId;		// last child arenaId opened
	uint16_t pathOff;		// start of path in buffer
	uint16_t maxSeg;		// maximum mapped segment array index
	char mapMutex[1];		// segment mapping mutex
	char created;			// set if map created
	char onDisk;			// on disk bool flag
};

//	database variables

typedef struct {
	uint64_t timestamp[1];
	ArenaDef arenaDef[1];
} DataBase;

#define database(db) ((DataBase *)(db->arena + 1))

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
