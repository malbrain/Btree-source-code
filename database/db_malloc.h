#pragma once

void *db_realloc(void *old, uint32_t size, bool zero);
uint64_t db_rawalloc(uint32_t amt, bool zeroit);
void *db_malloc(uint32_t amt, bool zero);
void *db_rawaddr(uint64_t rawAddr);
void db_rawfree(uint64_t rawAddr);
uint32_t db_size (void *obj);
void db_free (void *obj);

