#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

void initialize() {
	memInit();
}

Status openDatabase(void **hndl, char *name, uint32_t nameLen, Params *params) {
ArenaDef arenaDef[1];
DbMap *map;

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->objSize = sizeof(Txn);

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return ERROR_createdatabase;

	*map->arena->type = DatabaseType;
	*hndl = makeHandle(map);
	return OK;
}

Status openDocStore(void **hndl, void **dbhndl, char *path, uint32_t pathLen, Params *params) {
DbMap *map, *parent = NULL;
DocStore *docStore;
DocHndl *docHndl;
uint64_t *inUse;
Handle *dbHndl;
DataBase *db;
DbAddr *addr;
int idx, jdx;

	*hndl = NULL;

	docHndl = db_malloc(sizeof(DocHndl), true);

	if (dbhndl)
	  if ((dbHndl = bindHandle(dbhndl)))
		parent = dbHndl->map, db = database(parent);
	  else
		return ERROR_arenadropped;

	//  create the docStore and assign database txn idx

	if (parent)
		lockLatch(parent->arenaDef->nameTree->latch);

	if ((map = createMap(parent, path, pathLen, 0, sizeof(DocStore), sizeof(ObjId), params)))
		docStore = docstore(map);
	else
		return ERROR_arenadropped;

	//	allocate a map index for use in TXN document steps

	if (!docStore->init) {
	  if (parent)
		docStore->docIdx = arrayAlloc(parent, db->txnIdx, sizeof(uint64_t));

	  docStore->init = 1;
	}

	if (dbhndl)
		releaseHandle(dbHndl);

	map->arena->type[0] = DocStoreType;
	docHndl->hndl = makeHandle(map);

	if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	*hndl = docHndl;
	return OK;
}

Status createIndex(void **hndl, void **dochndl, ArenaType type, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, Params *params) {
DocHndl *docHndl = NULL;
uint32_t baseSize = 0;
DbMap *map, *parent;
Handle *idxhndl;
DbIndex *index;
Object *obj;

	*hndl = NULL;

	if (*dochndl)
	  if (!(docHndl = *dochndl))
		return ERROR_arenadropped;
	  else
		parent = docHndl->hndl->map;
	else
		parent = NULL;

	if (parent)
		lockLatch(parent->arenaDef->nameTree->latch);

	switch (type) {
	case ARTreeIndexType:
		baseSize = sizeof(ArtIndex);
		break;
	case Btree1IndexType:
		baseSize = sizeof(Btree1Index);
		break;
	}

	map = createMap(parent, name, nameLen, 0, baseSize, sizeof(ObjId), params);

	if (!map) {
	  if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	  return ERROR_createindex;
	}

	idxhndl = makeHandle(map);

	if (bindHandle((void **)&idxhndl))
		index = dbindex(map);
	else {
		if (parent)
		  unlockLatch(parent->arenaDef->nameTree->latch);

		return ERROR_arenadropped;
	}

	if (!index->keySpec.addr) {
		index->keySpec.bits = allocBlk(map, specSize + sizeof(Object), false);
		obj = getObj(map, index->keySpec);

		memcpy(obj + 1, keySpec, specSize);
		obj->size = specSize;

		switch (type) {
		case ARTreeIndexType:
			artInit(idxhndl, params);
			break;

		case Btree1IndexType:
			btree1Init(idxhndl, params);
			break;
		}
	}

	if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	if (idxhndl)
		releaseHandle(idxhndl);

	if (docHndl)
		releaseHandle(docHndl->hndl);

	*hndl = idxhndl;
	return OK;
}

//	create new cursor

Status createCursor(void **hndl, void **hndl1, ObjId txnId, char type) {
uint64_t timestamp;
DbCursor *cursor;
Handle *idxhndl;
Txn *txn;

	if (!(idxhndl = bindHandle(hndl1)))
		return ERROR_arenadropped;

	if (txnId.bits) {
		txn = fetchIdSlot(idxhndl->map->db, txnId);
		timestamp = txn->timestamp;
	} else
		timestamp = allocateTimestamp(idxhndl->map->db, en_reader);

	switch (*idxhndl->map->arena->type) {
	case ARTreeIndexType:
		cursor = artNewCursor(idxhndl, timestamp, txnId, type);
		break;

	case Btree1IndexType:
		cursor = btree1NewCursor(idxhndl, timestamp, txnId, type);
		break;
	}

	releaseHandle(idxhndl);
	*cursor->idx = *hndl1;
	*hndl = cursor;
	return OK;
}

Status returnCursor(void **hndl) {
DbCursor *cursor;
Handle *idxhndl;

	if (!(cursor = *hndl))
		return ERROR_handleclosed;

	if (!(idxhndl = bindHandle(cursor->idx)))
		return ERROR_arenadropped;

	switch (*idxhndl->map->arena->type) {
	case ARTreeIndexType:
		artReturnCursor(cursor);
		break;

	case Btree1IndexType:
		btree1ReturnCursor(cursor);
		break;
	}

	*hndl = NULL;
	return OK;
}

//	position cursor on a key

bool positionCursor(void **hndl, uint8_t *key, uint32_t keyLen) {
DbCursor *cursor;
Status stat;

	if (!(cursor = *hndl))
		return ERROR_handleclosed;

	if ((stat = dbPositionCursor(cursor, key, keyLen)))
		return false;

	return cursor->foundKey;
}

//	iterate cursor to next key
//	return zero on Eof

Status nextKey(void **hndl, uint8_t **key, uint32_t *keyLen, uint8_t *endKey, uint32_t endLen) {
DbCursor *cursor;
Status stat;

	if ((cursor = *hndl))
		stat = dbNextKey(cursor, NULL, endKey, endLen);
	else
		return ERROR_arenadropped;

	if (stat)
		return stat;

	if (key)
		*key = cursor->key;
	if (keyLen)
		*keyLen = cursor->keyLen;

	return OK;
}

//	iterate cursor to prev key
//	return zero on Bof

uint32_t prevKey(void **hndl, uint8_t **key, uint32_t *keyLen, uint8_t *endKey, uint32_t endLen) {
DbCursor *cursor;
Status stat;

	if ((cursor = *hndl))
		stat = dbPrevKey(cursor, NULL, endKey, endLen);
	else
		return ERROR_arenadropped;

	if (stat)
		return stat;

	if (key)
		*key = cursor->key;
	if (keyLen)
		*keyLen = cursor->keyLen;

	return OK;
}

//	iterate cursor to next document

Status nextDoc(void **hndl, Document **doc, uint8_t *endKey, uint32_t endLen) {
DbCursor *cursor;
Status stat;

	if ((cursor = *hndl))
		stat = dbNextDoc(cursor, endKey, endLen);
	else
		return ERROR_handleclosed;

	if (!stat)
		*doc = cursor->doc;

	return stat;
}

//	iterate cursor to previous document

Status prevDoc(void **hndl, Document **doc, uint8_t *endKey, uint32_t endLen) {
DbCursor *cursor;
Status stat;

	if ((cursor = *hndl))
		stat = dbPrevDoc(cursor, endKey, endLen);
	else
		return ERROR_handleclosed;

	if (!stat)
		*doc = cursor->doc;

	return stat;
}

Status cloneHandle(void **newhndl, void **oldhndl) {
Handle *hndl;

	if ((hndl = bindHandle(oldhndl))) {
		*newhndl = makeHandle(hndl->map);
		return OK;
	}

	return ERROR_handleclosed;
}

Status rollbackTxn(void **hndl, ObjId txnId);

Status commitTxn(void **hndl, ObjId txnId);

Status addIndexKeys(void **dochndl) {
DocHndl *docHndl;

	if (!(docHndl = *dochndl))
		return ERROR_handleclosed;

	return installIndexes(docHndl);
}

Status addDocument(void **dochndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
DocHndl *docHndl;
Handle *hndl;
Status stat;

	if (!(docHndl = *dochndl))
		return ERROR_handleclosed;

	if (!(hndl = bindHandle((void **)&docHndl->hndl)))
		return ERROR_arenadropped;

	stat = storeDoc(docHndl, hndl, obj, objSize, result, txnId);
	releaseHandle(hndl);
	return stat;
}

Status insertKey(void **hndl, uint8_t *key, uint32_t len) {
Handle *idxhndl;
Status stat;

	if (!(idxhndl = bindHandle(hndl)))
		return ERROR_arenadropped;

	switch (*idxhndl->map->arena->type) {
	case ARTreeIndexType:
		stat = artInsertKey(idxhndl, key, len);
		break;

	case Btree1IndexType:
		stat = btree1InsertKey(idxhndl, key, len, 0, Btree1_indexed);
		break;
	}

	releaseHandle(idxhndl);
	return stat;
}
