#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

enum DocType {
	FrameType,
	DocIdType,		// DocId value
	MinDocType = 3,	// minimum document size in bits
	MaxDocType = 24	// each power of two, 3 - 24
};

typedef union {
	struct {
		uint32_t offset;	// offset in the segment
		uint16_t segment;	// arena segment number
		union {
			struct {
				uint8_t mutex:1;	// mutex bit;
				uint8_t dead:1;		// mutex bit, node type
				uint8_t type:6;		// object type
			};
			volatile char latch[1];
		};
		union {
			uint8_t nbyte;		// number of bytes in a span node
			uint8_t nslot;		// number of slots of frame in use
			uint8_t nbits;		// power of two for object size
			uint8_t ttype;		// index transaction type
			int8_t rbcmp;		// red/black comparison
		};
	};
	uint64_t bits;
	struct {
		uint64_t addr:48;
		uint64_t fill:16;
	};
} DbAddr;

typedef union {
	struct {
		uint32_t index;		// record ID in the segment
		uint16_t segment;	// arena segment number
		uint16_t filler;
	};
	uint64_t bits;
	struct {
		uint64_t addr:48;
		uint64_t fill:16;
	};
} DocId;

typedef struct {
	DbAddr head[1];		// earliest frame waiting to be recycled
	DbAddr tail[1];		// location of latest frame to be recycle
	DbAddr free[1];		// frames of free objects
} FreeList;

#include "db_arena.h"
#include "db_map.h"
#include "db_malloc.h"
// #include "db_docs.h"
// #include "db_index.h"
// #include "db_art.h"
// #include "db_btree.h"

#define FrameSlots 64

typedef struct {
	DbAddr next;			// next frame in queue
	DbAddr prev;			// prev frame in queue
	uint64_t timestamp;		// latest timestamp
	DbAddr slots[FrameSlots];// array of waiting/free slots
} Frame;

//	red-black tree descent stack

#define RB_bits		24

typedef struct {
	uint64_t lvl;			// height of the stack
	DbAddr entry[RB_bits];	// stacked tree nodes
} PathStk;

typedef struct {
	uint32_t keyLen;		// length of key
	DbAddr left, right;		// next nodes down
	DbAddr addr;			// entry addr in parent
	char red;				// is tree node red?
	char key[0];			// entry key
} RedBlack;

uint64_t allocMap(DbMap *map, uint32_t size);
uint64_t allocObj(DbMap *map, DbAddr *free, int type, uint32_t size, bool zeroit);
void *getObj(DbMap *map, DbAddr addr); 
void closeMap(DbMap *map);

bool newSeg(DbMap *map, uint32_t minSize);
void mapSegs(DbMap *map);

uint64_t getNodeFromFrame (DbMap *map, DbAddr *queue);
bool getNodeWait (DbMap *map, DbAddr *queue, DbAddr *tail);
bool initObjFrame (DbMap *map, DbAddr *queue, uint32_t type, uint32_t size);
bool addSlotToFrame(DbMap *map, DbAddr *head, uint64_t addr);

void *fetchIdSlot (DbMap *map, DocId docId);
uint64_t allocDocId(DbMap *map, DbAddr *free, DbAddr *tail);
uint64_t getFreeFrame(DbMap *map);
uint64_t allocFrame(DbMap *map);

// void *cursorNext(DbCursor *cursor, DbMap *index);
// void *cursorPrev(DbCursor *cursor, DbMap *index);
