#pragma once

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#define MAX_segs  1000
#define MIN_segsize  131072

#define MAX_path  4096
#define MAX_blk		24	// max arena blk size in bits

#define HNDL_cnt	64	// number of arena Handles in a segment
#define HNDL_max	64	// max number of Handle segments

//	arena handle array

typedef struct {
	uint64_t inUse;				// handle in use bit array
	uint64_t docTs[HNDL_cnt];	// document timestamp
} HandleFrame;

//  on disk arena segment

typedef struct {
	uint64_t off;		// file offset of the segment
	uint64_t size;		// size of the segment
	DocId nextDoc;		// highest document ID in use
} DbSeg;

//  on disk/mmap arena seg zero

typedef struct {
	DbSeg segs[MAX_segs]; 	// segment meta-data
	DbAddr hndls[HNDL_max]; // segment handle frames
	DbAddr freeBlk[MAX_blk];// Arena free block frames
	DbAddr freeFrame[1];	// next free frame address
	DbAddr nextObject;		// next Object address
	uint64_t docTs;			// Incremented on delete doc
	uint32_t idSize;		// size of id array element
	uint8_t currSeg;		// index of highest segment
	uint8_t hndlSeg;		// index of highest handle seg
	char mutex;				// arena allocation lock/drop flag
	char type;				// arena type
} DbArena;

//	in memory arena map

typedef struct {
	char *base[MAX_segs];	// pointers to mapped segment memory
#ifndef _WIN32
	int hndl[1];			// OS file handle
#else
	HANDLE hndl[MAX_segs];
	HANDLE maphndl[MAX_segs];
#endif
	DbArena *arena;			// ptr to mapped seg zero
	uint32_t maxSeg;		// maximum segment array index in use
	char *path;				// path to file name
	char onDisk;			// on disk bool flag
	char mutex;				// arena lock
} DbMap;

DbMap *createMap(char *path, uint32_t baseSize, uint32_t idSize, uint64_t initSize, bool onDisk);
void returnFreeFrame(DbMap *map, DbAddr slot);
uint64_t allocBlk (DbMap *map, uint32_t size);
#ifdef _WIN32
HANDLE openPath(char *name, uint32_t segNo);
#else
int openPath(char *name, uint32_t segNo);
#endif
