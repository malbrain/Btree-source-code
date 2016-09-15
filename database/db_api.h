//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>


#define MAX_key		4096	// maximum key size in bytes

void initialize();
void *openDatabase(char *name, uint32_t nameLen, bool onDisk);
void *openDocStore(void *database, char *name, uint32_t nameLen, bool onDisk);
void *createIndex(void *docStore, char *idxName, uint32_t nameLen, void *keySpec, uint16_t specSize, int bits, int xtra, bool onDisk);
void *createCursor(void *index);
void *cloneHandle(void *hndl);

void *beginTxn(void *database);
int rollbackTxn(void *database, void *txn);
int commitTxn(void *database, void *txn);

int addDocument(void *hndl, void *obj, uint32_t objSize, uint64_t *objId, void *txn);
int insertKey(void *index, uint8_t *key, uint32_t len);
int addObjId(uint8_t *key, uint64_t addr);
