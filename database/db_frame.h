#pragma once

#define FrameSlots 64

typedef struct {
	DbAddr next;			// next frame in queue
	DbAddr prev;			// prev frame in queue
	uint64_t timestamp;		// latest timestamp
	DbAddr slots[FrameSlots];// array of waiting/free slots
} Frame;

void returnFreeFrame(DbMap *map, DbAddr slot);

uint64_t getNodeFromFrame (DbMap *map, DbAddr *queue);
bool getNodeWait (DbMap *map, DbAddr *queue, DbAddr *tail);
uint32_t initObjFrame (DbMap *map, DbAddr *queue, uint32_t type, uint32_t size);
bool addSlotToFrame(DbMap *map, DbAddr *head, uint64_t addr);


