//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>


void initialize();
void *createObjStore(char *path, bool onDisk);
void *createIndex(char *docStore, char *idxName, int bits, int xtra, bool onDisk);
void *createCursor(void *index);
void *cloneHandle(void *hndl);

int addObject(void *hndl, void *obj, uint32_t len, uint64_t *addr);
int insertKey(void *index, uint8_t *key, uint32_t len);
int addObjId(uint8_t *key, uint64_t addr);
