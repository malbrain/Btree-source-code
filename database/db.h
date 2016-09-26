#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "db_error.h"

typedef union {
	struct {
		uint32_t offset;	// offset in the segment
		uint16_t segment;	// arena segment number
		union {
			struct {
				uint8_t mutex:1;	// mutex bit;
				uint8_t dead:1;		// entry dead
				uint8_t type:6;		// object type
			};
			volatile char latch[1];
		};
		union {
			uint8_t nbyte;		// number of bytes in a span node
			uint8_t nslot;		// number of frame slots in use
			uint8_t maxidx;		// maximum slot index in use
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

#define ADDR_MUTEX_SET 0x1000000000000ULL

typedef union {
	struct {
		uint32_t index;		// record ID in the segment
		uint16_t seg:10;	// arena segment number
		uint16_t cmd:6;		// for use in txn
		uint16_t idx;		// document store arena idx
	};
	uint64_t bits;
} ObjId;

typedef struct DbArena_ DbArena;
typedef struct Handle_ Handle;
typedef struct DbMap_ DbMap;

//	param slots

typedef enum {
	OnDisk = 0,			// base set
	InitSize,

	Btree1Bits = 2,		// Btree1 set
	Btree1Xtra,

	MaxParam = 4		// param array size
} ParamSlot;

typedef union {
	uint64_t int64Val;
	char *strVal;
	bool boolVal;
	int intVal;
} Params;

//	types of arenas

typedef enum {
	NotSetYet = 0,
	DatabaseType,
	DocStoreType,
	Btree1IndexType,
	Btree2IndexType,
	ARTreeType
} ArenaType;

typedef struct {
	DbAddr verKeys[1];	// skiplist of versions by index key
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

