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
		uint16_t txn:6;		// for use in txn
		uint16_t idx;		// document store arena idx
	};
	uint64_t bits;
} ObjId;

typedef struct DbArena_ DbArena;
typedef struct DbMap_ DbMap;
