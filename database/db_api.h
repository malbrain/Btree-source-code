//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>


void initialize();
void *openDatabase(char *name, uint32_t nameLen, bool onDisk);
void *openDocStore(void *database, char *name, uint32_t nameLen, bool onDisk);
void *createIndex(void *docStore, char *idxName, uint32_t nameLen, int bits, int xtra, bool onDisk);
void *createCursor(void *index);
void *cloneHandle(void *hndl);

int addDocument(void *hndl, void *obj, uint32_t size, uint64_t *objid, uint64_t txnAddr);
int insertKey(void *index, uint8_t *key, uint32_t len);
int addObjId(uint8_t *key, uint64_t addr);
