#pragma once

//
// Reference Counting
//

typedef struct RawObj {
	int64_t weakCnt[1];
	int64_t refCnt[1];
	DbAddr addr[1];
} rawobj_t;

//
//	reference counting
//

void *db_realloc(void *old, uint32_t size, bool zero);
uint64_t db_rawalloc(uint32_t amt, bool zeroit);
void *db_rawaddr(uint64_t rawAddr);
void db_rawfree(uint64_t rawAddr);
void *db_alloc(uint32_t amt, bool zero);
uint32_t db_size (void *obj);
void db_free (void *obj);

