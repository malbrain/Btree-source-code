//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#define MAX_key		4096	// maximum key size in bytes

typedef struct {
	Handle *hndl;		// docStore handle
	RWLock2 lock[1];	// index list r/w lock
	uint64_t childId;	// last child installed
	SkipHead indexes[1];// index handles by Id
} DocHndl;

void initialize();
void *openDatabase(char *name, uint32_t nameLen, bool onDisk);
void *openDocStore(void *database, char *name, uint32_t nameLen, bool onDisk);
void *createIndex(void *docHndl, char *idxName, uint32_t nameLen, void *keySpec, uint16_t specSize, int bits, int xtra, bool onDisk);
void *createCursor(void *index);
void *cloneHandle(void *hndl);

uint64_t beginTxn(void *hndl);
int rollbackTxn(void *database, uint64_t txnId);
int commitTxn(void *database, uint64_t txnId);

int addDocument(void *hndl, void *obj, uint32_t objSize, uint64_t *objId, ObjId txnId);
int insertKey(void *index, uint8_t *key, uint32_t len);
int addObjId(uint8_t *key, uint64_t addr);
