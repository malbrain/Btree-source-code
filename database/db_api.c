#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1.h"

void initialize() {
	memInit();
}

void *openDatabase(char *name, uint32_t nameLen, bool onDisk) {
ArenaDef arenaDef[1];
DataBase *db;
DbMap *map;

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->objSize = sizeof(Txn);
	arenaDef->onDisk = onDisk;

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return NULL;

	db = database(map);

	map->arenaDef = db->arenaDef;
	memcpy(map->arenaDef, arenaDef, sizeof(arenaDef));

	map->arena->type[0] = DatabaseType;
	return makeHandle(map);
}

void *openDocStore(void *hndl, char *path, uint32_t pathLen, bool onDisk) {
Handle *db = (Handle *)hndl;
DocStore *docStore;
uint64_t *inUse;
DbAddr *addr;
int idx, jdx;
DbMap *map;

	docStore = db_malloc(sizeof(DocStore), true);

	if (bindHandle(db))
		lockLatch(db->map->arenaDef->arenaNames->latch);
	else
		return NULL;

	map = createMap(db->map, path, pathLen, 0, 0, sizeof(ObjId), 0, onDisk);
	releaseHandle(db);

	map->arena->type[0] = DocStoreType;

	docStore->hndl = makeHandle(map);
	docStore->count = 0;

	unlockLatch(db->map->arenaDef->arenaNames->latch);

	if (!map->childMaps->addr)
		return docStore;

	lockLatch(map->childMaps->latch);

	addr = getObj(map, *map->childMaps);

	//	create index handles from all open children arenas

	for (idx = 0; idx <= map->childMaps->maxidx; idx++) {
	  inUse = getObj(map, addr[idx]);

	  for (jdx = 0; jdx < 64; jdx++)
		if (inUse[0] & 1ULL << jdx)
		  if (docStore->count < 64)
			docStore->indexes[docStore->count++] = makeHandle(((DbMap **)(inUse + 1))[jdx]);
		  else
			break;
	}

	unlockLatch(map->childMaps->latch);
	return docStore;
}

void *createIndex(void *hndl, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, int bits, int xtra, bool onDisk) {
DocStore *docStore = hndl;
BtreeIndex *btree;
Handle *index;
Object *obj;
DbMap *map;

	if (bits > Btree_maxbits) {
		fprintf(stderr, "createIndex: bits = %d > max = %d\n", bits, Btree_maxbits);
		exit(1);
	}

	if (bits + xtra > Btree_maxbits) {
		fprintf(stderr, "createIndex: bits = %d + xtra = %d > max = %d\n", bits, xtra, Btree_maxbits);
		exit(1);
	}

	if (bindHandle(docStore->hndl)) {
		lockLatch(docStore->hndl->map->arenaDef->arenaNames->latch);
		map = createMap(docStore->hndl->map, name, nameLen, 0, sizeof(BtreeIndex), sizeof(ObjId), 0, onDisk);
	} else
		return NULL;

	if (!map) {
		unlockLatch(docStore->hndl->map->arenaDef->arenaNames->latch);
		return NULL;
	}

	index = makeHandle(map);

	if (bindHandle(index))
		btree = btreeIndex(map);
	else {
		unlockLatch(docStore->hndl->map->arenaDef->arenaNames->latch);
		return NULL;
	}

	if (!btree->keySpec.addr) {
		btree->pageSize = 1 << bits;
		btree->pageBits = bits;
		btree->leafXtra = xtra;

		btree->keySpec.bits = allocBlk(map, specSize + sizeof(Object), false);
		obj = getObj(map, btree->keySpec);

		memcpy(obj + 1, keySpec, specSize);
		obj->size = specSize;

		btreeInit(index);
	}

	// add index to docStore index handle array

	if (docStore->count < 64)
		docStore->indexes[docStore->count++] = index;

	unlockLatch(docStore->hndl->map->arenaDef->arenaNames->latch);

	releaseHandle(index);
	releaseHandle(docStore->hndl);
	return index;
}

void *createCursor(void *index) {
	return (void *)btreeCursor((Handle *)index);
}

void *cloneHandle(void *hndl) {
	return (void *)makeHandle(((Handle *)hndl)->map);
}

int addDocument(void *hndl, void *obj, uint32_t objSize, uint64_t *result, ObjId txnId) {
DocStore *docStore = hndl;
uint8_t key[MAX_key];
ArenaDef *arenaDef;
BtreeIndex *btree;
Status stat = OK;
Txn *txn = NULL;
Handle *index;
Document *doc;
Object *spec;
DbAddr *slot;
ObjId docId;
DbAddr addr;
int keyLen;
int idx;

	if (!bindHandle(docStore->hndl))
		return ERROR_arenadropped;

	if (txnId.bits)
		txn = fetchIdSlot(docStore->hndl->map->db, txnId);

	if ((addr.bits = allocNode(docStore->hndl->map, docStore->hndl->list, -1, objSize + sizeof(Document), false)))
		doc = getObj(docStore->hndl->map, addr);
	else
		return ERROR_outofmemory;

	docId.bits = allocObjId(docStore->hndl->map, docStore->hndl->list);

	memset (doc, 0, sizeof(Document));

	if (txn)
		doc->txnId.bits = txnId.bits;

	doc->docId.bits = docId.bits;
	doc->size = objSize;

	memcpy (doc + 1, obj, objSize);

	if (result)
		*result = docId.bits;

	// assign document to docId slot

	slot = fetchIdSlot(docStore->hndl->map, docId);
	slot->bits = addr.bits;
/*
	//  any recent index arrivals from another process/thread?

	while (map->arenaId < map->arenaDef->arenaId) {
	}
*/
	//	add keys for the document
	//	enumerate docStore children (e.g. indexes)

	for (idx = 0; idx < docStore->count; idx++) {
		index = docStore->indexes[idx];

		if (bindHandle(index))
			btree = btreeIndex(index->map);
		else
			return ERROR_arenadropped;

		spec = getObj(index->map, btree->keySpec);
		keyLen = keyGenerator(key, obj, objSize, spec + 1, spec->size);

		store64(key + keyLen, docId.bits);
		keyLen += sizeof(uint64_t);

		stat = btreeInsertKey(index, key, keyLen, 0, Btree_indexed);
		releaseHandle(index);
	}

	if (txn)
		addIdToTxn(docStore->hndl->map->db, txn, docId, addDoc); 

	releaseHandle(docStore->hndl);
	return OK;
}

int rollbackTxn(void *db, void *txn);

int commitTxn(void *db, void *txn);

int insertKey(void *hndl, uint8_t *key, uint32_t len) {
Handle *index = (Handle *)hndl;
int stat;

	if (bindHandle(index))
		stat = btreeInsertKey(index, key, len, 0, Btree_indexed);
	else
		return ERROR_arenadropped;

	releaseHandle(index);
	return stat;
}

int addObjId(uint8_t *key, uint64_t addr) {
	store64(key, addr);
	return sizeof(uint64_t);
}
