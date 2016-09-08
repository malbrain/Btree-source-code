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
	ObjStoreType,
	BtreeIndexType
};

//  on disk arena segment

typedef struct {
	uint64_t off;		// file offset of the segment
	uint64_t size;		// size of the segment
	ObjId nextObj;		// highest object ID in use
} DbSeg;

//  on disk/mmap arena seg zero

struct DbArena_ {
	DbSeg segs[MAX_segs]; 		// segment meta-data
	uint64_t lowTs, delTs;		// low hndl ts, Incr on delete
	DbAddr freeBlk[MAX_blk];	// list of free frames in frames
	DbAddr handleArray[1];		// handle array
	DbAddr freeFrame[1];		// list of free frames in frames
	DbAddr nextObject;			// next Object address
	uint64_t objCount;			// overall number of objects
	uint64_t objSpace;			// overall size of objects
	uint32_t objSize;			// size of object array element
	uint8_t currSeg;			// index of highest segment
	char newHndl[1];			// handle allocation lock
	char mutex[1];				// arena allocation lock/drop flag
	char type[1];				// arena type
};

//	in memory arena map

struct DbMap_ {
	char *base[MAX_segs];	// pointers to mapped segment memory
#ifndef _WIN32
	int hndl[1];			// OS file handle
#else
	HANDLE hndl[MAX_segs];
	HANDLE maphndl[MAX_segs];
#endif
	DbArena *arena;			// ptr to mapped seg zero
	char path[MAX_path];	// path to file
	uint32_t maxSeg;		// maximum segment array index in use
	char mutex[1];			// arena lock
	char onDisk;			// on disk bool flag
	char created;			// set if map created
};

void *createMap(char *path, uint32_t baseSize, uint32_t idSize, uint64_t initSize, bool onDisk);
void returnFreeFrame(DbMap *map, DbAddr slot);
#ifdef _WIN32
HANDLE openPath(char *name, uint32_t segNo);
#else
int openPath(char *name, uint32_t segNo);
#endif
