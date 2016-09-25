#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"

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
DocStore *docStore;
DocHndl *docHndl;
uint64_t *inUse;
Handle *dbHndl;
DataBase *db;
DbAddr *addr;
int idx, jdx;
DbMap *map;

	*hndl = NULL;

	docHndl = db_malloc(sizeof(DocHndl), true);

	if ((dbHndl = bindHandle(dbhndl)))
		db = database(dbHndl->map);
	else
		return ERROR_arenadropped;

	//  create the docStore and assign database txn idx

	lockLatch(dbHndl->map->arenaDef->nameTree->latch);

	if ((map = createMap(dbHndl->map, path, pathLen, 0, sizeof(DocStore), sizeof(ObjId), 0, params[OnDisk].boolVal)))
		docStore = docstore(map);
	else
		return ERROR_arenadropped;

	if (!docStore->init) {
		docStore->docIdx = arrayAlloc(dbHndl->map, db->txnIdx, sizeof(uint64_t));
		docStore->init = 1;
	}

	releaseHandle(dbHndl);

	map->arena->type[0] = DocStoreType;
	docHndl->hndl = makeHandle(map);

	unlockLatch(dbHndl->map->arenaDef->nameTree->latch);
	*hndl = docHndl;
	return OK;
}

//  open and install index map in docHndl cache

void installIdx(DocHndl *docHndl, SkipEntry *skipEntry) {
RedBlack *rbEntry;
Handle *idxhndl;
Handle **hndl;
DbAddr rbAddr;

	rbAddr.bits = *skipEntry->val;
	rbEntry = getObj(docHndl->hndl->map->parent, rbAddr);

	idxhndl = makeHandle(arenaRbMap(docHndl->hndl->map, rbEntry));
	hndl = skipAdd(docHndl->hndl->map, docHndl->indexes->head, *skipEntry->key);
	*hndl = idxhndl;
}

//	install new indexes

void installIndexes(DocHndl *docHndl) {
ArenaDef *arenaDef = docHndl->hndl->map->arenaDef;
DbAddr *next = arenaDef->idList->head;
uint64_t maxId = 0;
SkipList *skipList;
SkipEntry *entry;
int idx;

	if (docHndl->childId < arenaDef->childId)
		readLock2 (arenaDef->idList->lock);
	else
		return;

	writeLock2 (docHndl->indexes->lock);

	//	transfer Id slots from arena childId list to our handle list

	while (next->addr) {
		skipList = getObj(docHndl->hndl->map, *next);
		idx = next->nslot;

		if (!maxId)
			maxId = *skipList->array[next->nslot - 1].key;

		while (idx--)
			if (*skipList->array[idx].key > docHndl->childId)
				installIdx(docHndl, &skipList->array[idx]);
			else
				break;

		next = skipList->next;
	}

	docHndl->childId = maxId;
	writeUnlock2 (docHndl->indexes->lock);
	readUnlock2 (arenaDef->idList->lock);
}

Status createIndex(void **hndl, void **dochndl, ArenaType type, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, Params *params) {
uint32_t baseSize = 0;
DocHndl *docHndl;
Handle *idxhndl;
DbIndex *index;
Object *obj;
DbMap *map;

	*hndl = NULL;

	if (!(docHndl = *dochndl))
		return ERROR_arenadropped;

	lockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);

	switch (type) {
	case Btree1IndexType:
		baseSize = sizeof(Btree1Index);
		break;
	}

	map = createMap(docHndl->hndl->map, name, nameLen, 0, baseSize, sizeof(ObjId), 0, params[OnDisk].boolVal);

	if (!map) {
		unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
		return ERROR_createindex;
	}

	idxhndl = makeHandle(map);

	if (bindHandle((void **)&idxhndl))
		index = dbIndex(map);
	else {
		unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
		return ERROR_arenadropped;
	}

	if (!index->keySpec.addr) {
		index->keySpec.bits = allocBlk(map, specSize + sizeof(Object), false);
		obj = getObj(map, index->keySpec);

		memcpy(obj + 1, keySpec, specSize);
		obj->size = specSize;

		switch (type) {
		case Btree1IndexType:
			btree1Init(idxhndl, params);
			break;
		}
	}

	unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);

	releaseHandle(idxhndl);
	releaseHandle(docHndl->hndl);
	*hndl = idxhndl;
	return OK;
}

//	create new cursor

Status createCursor(void **hndl, void **hndl1, ObjId txnId) {
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
	case Btree1IndexType:
		cursor = btree1NewCursor(idxhndl, timestamp, txnId);
	}

	releaseHandle(idxhndl);
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
	case Btree1IndexType:
		btree1ReturnCursor(cursor);
	}

	*hndl = NULL;
	return OK;
}

//	iterate cursor to next document

Status nextDoc(void **hndl, Document **doc) {
DbCursor *cursor;
Status stat;

	if ((cursor = *hndl))
		stat = dbNextKey(cursor);
	else
		return ERROR_handleclosed;

	if (!stat)
		*doc = cursor->doc;

	return stat;
}

//	iterate cursor to previous document

Status prevDoc(void **hndl, Document **doc) {
DbCursor *cursor;
Status stat;

	if ((cursor = *hndl))
		stat = dbPrevKey(cursor);
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

Status installIndexKey(DocHndl *docHndl, SkipEntry *entry, Document *doc) {
uint8_t key[MAX_key];
Handle *idxhndl;
DbIndex *index;
void *hndl[1];
Object *spec;
Status stat;
int keyLen;

	*hndl = (Handle *)(*entry->val);

	if ((idxhndl = bindHandle(hndl)))
		index = dbIndex(idxhndl->map);
	else
		return ERROR_arenadropped;

	spec = getObj(idxhndl->map, index->keySpec);
	keyLen = keyGenerator(key, doc + 1, doc->size, spec + 1, spec->size);

	keyLen = store64(key, keyLen, doc->docId.bits);
	keyLen = store64(key, keyLen, doc->version);

	switch (*idxhndl->map->arena->type) {
	case Btree1IndexType:
		stat = btree1InsertKey(idxhndl, key, keyLen, 0, Btree1_indexed);
	}

	releaseHandle(idxhndl);
	return stat;
}

Status installIndexKeys(DocHndl *docHndl, Document *doc) {
DbAddr *next = docHndl->indexes->head;
SkipList *skipList;
Status stat;
int idx;

	readLock2 (docHndl->indexes->lock);

	//	install keys for document

	while (next->addr) {
	  skipList = getObj(docHndl->hndl->map, *next);
	  idx = next->nslot;

	  while (idx--) {
		if (~*skipList->array[idx].key & CHILDID_DROP)
		  if ((stat = installIndexKey(docHndl, &skipList->array[idx], doc)))
			return stat;
	  }

	  next = skipList->next;
	}

	readUnlock2 (docHndl->indexes->lock);
	return OK;
}

Status addDocument(void **dochndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
DocStore *docStore;
ArenaDef *arenaDef;
DocHndl *docHndl;
Status stat = OK;
Txn *txn = NULL;
Document *doc;
DbAddr *slot;
Handle *hndl;
ObjId docId;
DbAddr addr;
int idx;

	if (!(docHndl = *dochndl))
		return ERROR_handleclosed;

	if (!(hndl = bindHandle((void **)&docHndl->hndl)))
		return ERROR_arenadropped;

	docStore = docstore(hndl->map);

	if (txnId.bits)
		txn = fetchIdSlot(hndl->map->db, txnId);

	if ((addr.bits = allocNode(hndl->map, hndl->list, -1, objSize + sizeof(Document), false)))
		doc = getObj(hndl->map, addr);
	else
		return ERROR_outofmemory;

	docId.bits = allocObjId(hndl->map, hndl->list, docStore->docIdx);

	memset (doc, 0, sizeof(Document));

	if (txn)
		doc->txnId.bits = txnId.bits;

	doc->docId.bits = docId.bits;
	doc->size = objSize;

	memcpy (doc + 1, obj, objSize);

	if (result)
		result->bits = docId.bits;

	// assign document to docId slot

	slot = fetchIdSlot(hndl->map, docId);
	slot->bits = addr.bits;

	//  install any recent index arrivals from another process/thread

	installIndexes(docHndl);

	//	add keys for the document
	//	enumerate docHndl children (e.g. indexes)

	installIndexKeys(docHndl, doc);

	if (txn)
		addIdToTxn(hndl->map->db, txn, docId, addDoc); 

	releaseHandle(hndl);
	return OK;
}

Status rollbackTxn(void **hndl, ObjId txnId);

Status commitTxn(void **hndl, ObjId txnId);

Status insertKey(void **hndl, uint8_t *key, uint32_t len) {
Handle *idxhndl;
Status stat;

	if (!(idxhndl = bindHandle(hndl)))
		return ERROR_arenadropped;

	switch (*idxhndl->map->arena->type) {
	case Btree1IndexType:
		stat = btree1InsertKey(idxhndl, key, len, 0, Btree1_indexed);
	}

	releaseHandle(idxhndl);
	return stat;
}
