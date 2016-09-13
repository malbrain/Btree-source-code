#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
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

#define ADDR_MUTEX_SET 0x0000000000001

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
} ObjId;

typedef struct DbArena_ DbArena;
typedef struct DbMap_ DbMap;
