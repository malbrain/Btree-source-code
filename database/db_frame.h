#pragma once

#define FrameSlots 64

typedef struct {
	DbAddr next;			// next frame in queue
	DbAddr prev;			// prev frame in queue
	uint64_t timestamp;		// latest timestamp
	DbAddr slots[FrameSlots];// array of waiting/free slots
} Frame;

void returnFreeFrame(DbMap *map, DbAddr slot);

