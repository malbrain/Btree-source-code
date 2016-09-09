#include "db.h"
#include "db_map.h"
#include "db_arena.h"
#include "db_frame.h"

uint64_t getFreeFrame(DbMap *map);
uint64_t allocFrame( DbMap *map);

//	fill in new frame with new available objects
//	return false if out of memory

bool initObjFrame(DbMap *map, DbAddr *free, uint32_t type, uint32_t size) {
uint32_t dup = FrameSlots;
Frame *frame;
DbAddr slot;
	
	slot.bits = type;

	if (size * dup > 4096 * 4096)
		dup >>= 5;

	else if (size * dup > 1024 * 1024)
		dup >>= 3;

	else if (size * dup > 256 * 256)
		dup >>= 1;

	if (!(slot.bits = allocMap(map, size * dup)))
		return false;

	if (!free->addr)
		if (!(free->addr = allocFrame(map)))
			return false;

	free->type = FrameType;
	free->nslot = dup;

	frame = getObj(map, *free);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	while (dup--) {
		frame->slots[dup].bits = slot.bits;
		slot.bits += size >> 3;
	}

	return true;
}

//  allocate frame full of empty frames for free list
//  call with freeFrame latched.

bool initFreeFrame (DbMap *map) {
uint64_t addr = allocMap (map, sizeof(Frame) * (FrameSlots + 1));
uint32_t dup = FrameSlots;
DbAddr head, slot;
Frame *frame;

	if (!addr)
		return false;

	head.bits = addr;
	head.type = FrameType;
	head.nslot = FrameSlots;
	head.mutex = 1;

	frame = getObj(map, head);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	while (dup--) {
		addr += sizeof(Frame) >> 3;
		slot.bits = addr;
		slot.type = FrameType;
		slot.nslot = FrameSlots;
		frame->slots[dup].bits = slot.bits;
	}

	map->arena->freeFrame->bits = head.bits;
	return true;
}

//	obtain available frame

uint64_t allocFrame(DbMap *map) {
Frame *frame;
DbAddr slot;

	lockLatch(map->arena->freeFrame->latch);

	while (!(slot.bits = getFreeFrame(map)))
		if (!initFreeFrame (map)) {
			unlockLatch(map->arena->freeFrame->latch);
			return false;
		}

	unlockLatch(map->arena->freeFrame->latch);
	frame = getObj(map, slot);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	slot.type = FrameType;
	return slot.bits;
}

//  Add empty frame to free-list

void returnFreeFrame(DbMap *map, DbAddr slot) {
Frame *frame;

	lockLatch(map->arena->freeFrame->latch);

	// space in current free-list frame?

	if (map->arena->freeFrame->addr && map->arena->freeFrame->nslot < FrameSlots) {
		frame = getObj(map, *map->arena->freeFrame);
		frame->slots[map->arena->freeFrame->nslot++].bits = slot.bits;
		unlockLatch(map->arena->freeFrame->latch);
		return;
	}

	// otherwise turn slot into new freeFrame

	frame = getObj(map, slot);
	frame->next.bits = map->arena->freeFrame->bits;
	frame->prev.bits = 0;

	slot.nslot = 0;
	slot.mutex = 0;
	map->arena->freeFrame->bits = slot.bits;
}

//  Add node to free frame

bool addSlotToFrame(DbMap *map, DbAddr *head, uint64_t addr) {
DbAddr slot2;
Frame *frame;

	lockLatch(head->latch);

	//  space in current frame?

	if (head->addr && head->nslot < FrameSlots) {
		frame = getObj(map, *head);
		frame->slots[head->nslot++].bits = addr;

		unlockLatch(head->latch);
		return true;
	}

	//  otherwise add slot to new frame

	if (!(slot2.bits = allocFrame(map)) )
		return false;

	frame = getObj(map, slot2);
	frame->slots->bits = addr;  // install in slot zero
	frame->prev.bits = 0;

	if ( (frame->next.bits = head->addr) ) {
		Frame *frame2 = getObj(map, *head);
		frame2->timestamp = map->arena->delTs;
		frame2->prev.bits = slot2.bits;
	}

	// install new head, with lock cleared

	slot2.nslot = 1;
	head->bits = slot2.bits;
	return true;
}

//  pull free frame from free list
//	call with freeFrame locked

uint64_t getFreeFrame(DbMap *map) {
uint64_t addr;
Frame *frame;

	if (!map->arena->freeFrame->addr)
		return 0;

	frame = getObj(map, *map->arena->freeFrame);

	// are there available free frames?

	if (map->arena->freeFrame->nslot)
		return frame->slots[--map->arena->freeFrame->nslot].addr;

	// is there more than one freeFrame?

	if (!frame->next.bits)
		return 0;

	addr = map->arena->freeFrame->addr;
	frame->next.nslot = FrameSlots;
	frame->next.mutex = 1;

	map->arena->freeFrame->bits = frame->next.bits;
	return addr;
}

//  pull available node from free object frame
//   call with free object frame locked.

uint64_t getNodeFromFrame(DbMap *map, DbAddr* queue) {
	if (queue->addr) 
		do {
			Frame *frame = getObj(map, *queue);
			DbAddr slot;
			//  are there available free objects?

			if (queue->nslot)
				return frame->slots[--queue->nslot].addr;
	
			//  is there another frame of free objects after the empty frame?

			if (!frame->next.bits)
				return 0;
	
			slot.bits = queue->bits;
			queue->addr = frame->next.addr;
			queue->nslot = FrameSlots;
			returnFreeFrame(map, slot);
		} while (true);

	return 0;
}

//	pull frame from wait queue to free list
//	call with free latched

bool getNodeWait(DbMap *map, DbAddr* free, DbAddr* tail) {
Frame *frame, *frame2;

	if (!tail || !tail->addr)
		return false;

	frame = getObj(map, *tail);

	if (!frame->prev.addr)
		return false;

	//	is the frame timestamp greater than the lowest handle timestamp?

	if (frame->timestamp >= map->arena->lowTs)
		return false;

	// wait time has expired, so we can
	// pull frame from tail of wait queue
	// to the empty free list

	if (free->addr)
		returnFreeFrame(map, *free);

	tail->mutex = 1;
	free->bits = tail->bits;

	//  is this the last frame in the prev chain?

	tail->bits = frame->prev.addr;
	tail->nslot = FrameSlots;

	frame2 = getObj(map, frame->prev);
	frame2->next.bits = 0;
	return true;
}

//	initialize frame of available ObjId

bool initObjIdFrame(DbMap *map, DbAddr *free) {
uint32_t dup = FrameSlots;
uint64_t max, addr;
Frame *frame;

	lockLatch(map->arena->mutex);

	max = map->arena->segs[map->arena->currSeg].size -
		map->arena->segs[map->arena->currSeg].nextObj.index * map->arena->objSize;
	max -= dup * map->arena->objSize;

	if (map->arena->nextObject.offset * 8ULL > max )
		if (!newSeg(map, dup * map->arena->objSize))
			return false;

	// allocate a batch of ObjIds

	map->arena->segs[map->arena->currSeg].nextObj.index += dup;
	addr = map->arena->segs[map->arena->currSeg].nextObj.bits;

	unlockLatch(map->arena->mutex);

	if (!free->addr)
		if (!(free->addr = allocFrame(map)))
		   return false;

	free->type = FrameType;
	free->nslot = FrameSlots;

	frame = getObj(map, *free);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	while (dup--) {
		frame->slots[dup].bits = 0;
		frame->slots[dup].type = ObjIdType;
		frame->slots[dup].addr = addr - dup;
	}

	return true;
}
