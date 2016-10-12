//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#include "db_error.h"

void initialize();

Status openDatabase(void **hndl, char *name, uint32_t nameLen, Params *params);
Status openDocStore(void **hndl, void **database, char *name, uint32_t nameLen, Params *params);
Status createIndex(void **hndl, void **docHndl, ArenaType type, char *idxName, uint32_t nameLen, void *keySpec, uint16_t specSize, Params *params);
Status createCursor(void **hndl, void **index, ObjId txnId, char type);
Status cloneHandle(void **hndl, void **fromhndl);

uint64_t beginTxn(void **hndl);
Status rollbackTxn(void **database, ObjId txnId);
Status commitTxn(void **database, ObjId txnId);

Status addDocument(void **hndl, void *obj, uint32_t objSize, ObjId *objId, ObjId txnId);
Status insertKey(void **index, uint8_t *key, uint32_t len);
Status nextDoc(void **hndl, Document **doc, uint8_t *maxKey, uint32_t maxLen);
Status prevDoc(void **hndl, Document **doc, uint8_t *maxKey, uint32_t maxLen);
Status addIndexKeys(void **dochndl);

Status nextKey(void **hndl, uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen);
Status prevKey(void **hndl, uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen);

bool positionCursor(void **hndl, uint8_t *key, uint32_t keyLen);
