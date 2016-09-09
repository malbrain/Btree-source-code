#pragma once

#define MUTEX_BIT  0x1
#define DEAD_BIT   0x2

#include "db_error.h"
#include "db_malloc.h"
#include "db_object.h"
#include "db_lock.h"

/**
 *	map allocations
 */

uint64_t allocMap(DbMap *map, uint32_t size);
uint64_t allocObj(DbMap *map, DbAddr *free, DbAddr *wait, int type, uint32_t size, bool zeroit);
uint64_t allocObjId(DbMap *map, FreeList *list);
void *fetchObjSlot (DbMap *map, ObjId objId);
void *getObj(DbMap *map, DbAddr addr); 

uint64_t allocNode(DbMap *map, FreeList *list, int type, uint32_t size, bool zeroit);
uint64_t getNodeFromFrame (DbMap *map, DbAddr *queue);
bool getNodeWait (DbMap *map, DbAddr *queue, DbAddr *tail);
bool initObjFrame (DbMap *map, DbAddr *queue, uint32_t type, uint32_t size);
bool addSlotToFrame(DbMap *map, DbAddr *head, uint64_t addr);

void *fetchIdSlot (DbMap *map, ObjId objId);
uint64_t getFreeFrame(DbMap *map);
uint64_t allocFrame(DbMap *map);

// void *cursorNext(DbCursor *cursor, DbMap *index);
// void *cursorPrev(DbCursor *cursor, DbMap *index);

/**
 * spin latches
 */

void lockLatch(volatile char* latch);
void unlockLatch(volatile char* latch);
void waitNonZero(volatile char *zero);
void waitNonZero64(volatile uint64_t *zero);
void art_yield();

/**
 * atomic integer ops
 */

void kill_slot(volatile char* latch);

int64_t atomicAdd64(volatile int64_t *value, int64_t amt);
int32_t atomicAdd32(volatile int32_t *value, int32_t amt);
int64_t atomicOr64(volatile int64_t *value, int64_t amt);
int32_t atomicOr32(volatile int32_t *value, int32_t amt);
uint64_t compareAndSwap(uint64_t* target, uint64_t compare_val, uint64_t swap_val);

/**
 *	file system lock
 */

#ifdef _WIN32
void lockArena(void *hndl, char *fName);
void unlockArena(void *hndl, char *fName);
#else
void lockArena(int hndl, char *fName);
void unlockArena(int hndl, char *fName);
#endif

bool fileExists(char *path);
