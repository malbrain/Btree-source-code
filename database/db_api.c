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
DbMap *map;

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->objSize = sizeof(Txn);
	arenaDef->onDisk = onDisk;

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return NULL;

	return makeHandle(map);
}

void *openDocStore(void *hndl, char *path, uint32_t pathLen, bool onDisk) {
Handle *dbHndl = (Handle *)hndl;
DocStore *docStore;
DocHndl *docHndl;
uint64_t *inUse;
DataBase *db;
DbAddr *addr;
int idx, jdx;
DbMap *map;

	docHndl = db_malloc(sizeof(DocHndl), true);

	if (bindHandle(dbHndl))
		db = database(dbHndl->map);
	else
		return NULL;

	//  create the docStore and assign database txn idx

	lockLatch(dbHndl->map->arenaDef->nameTree->latch);

	if ((map = createMap(dbHndl, path, pathLen, 0, sizeof(DocStore), sizeof(ObjId), 0, onDisk)))
		docStore = docstore(map);
	else
		return NULL;

	if (!docStore->init) {
		docStore->docIdx = arrayAlloc(dbHndl->map, db->txnIdx, sizeof(uint64_t));
		docStore->init = 1;
	}

	releaseHandle(dbHndl);

	map->arena->type[0] = DocStoreType;
	docHndl->hndl = makeHandle(map);

	unlockLatch(dbHndl->map->arenaDef->nameTree->latch);
	return docHndl;
}

//  open and install index map in docHndl cache

void installIdx(DocHndl *docHndl, SkipEntry *skipEntry) {
RedBlack *rbEntry;
DbAddr rbAddr;
Handle *index;
Handle **hndl;

	rbAddr.bits = *skipEntry->val;
	rbEntry = getObj(docHndl->hndl->map->parent, rbAddr);

	index = makeHandle(arenaRbMap(docHndl->hndl, rbEntry));
	hndl = skipAdd(docHndl->hndl, docHndl->indexes->head, *skipEntry->key);
	*hndl = index;
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

void *createIndex(void *hndl, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, int bits, int xtra, bool onDisk) {
DocHndl *docHndl = hndl;
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

	if (bindHandle(docHndl->hndl)) {
		lockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
		map = createMap(docHndl->hndl, name, nameLen, 0, sizeof(BtreeIndex), sizeof(ObjId), 0, onDisk);
	} else
		return NULL;

	if (!map) {
		unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
		return NULL;
	}

	index = makeHandle(map);

	if (bindHandle(index))
		btree = btreeIndex(map);
	else {
		unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
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

	unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);

	releaseHandle(index);
	releaseHandle(docHndl->hndl);
	return index;
}

void *createCursor(void *index) {
	return (void *)btreeCursor((Handle *)index);
}

void *cloneHandle(void *hndl) {
	return (void *)makeHandle(((Handle *)hndl)->map);
}

int installIndexKey(DocHndl *docHndl, SkipEntry *entry, void *obj, uint32_t objSize, ObjId docId) {
Handle *index = (Handle *)(*entry->val);
uint8_t key[MAX_key];
BtreeIndex *btree;
Object *spec;
int keyLen;
int stat;

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
	return stat;
}

int installIndexKeys(DocHndl *docHndl, Document *doc) {
DbAddr *next = docHndl->indexes->head;
SkipList *skipList;
int idx, stat;

	readLock2 (docHndl->indexes->lock);

	//	install keys for document

	while (next->addr) {
		skipList = getObj(docHndl->hndl->map, *next);
		idx = next->nslot;

		while (idx--) {
			if (~*skipList->array[idx].key & CHILDID_DROP)
				if ((stat = installIndexKey(docHndl, &skipList->array[idx], doc + 1, doc->size, doc->docId)))
					return stat;
		}

		next = skipList->next;
	}

	readUnlock2 (docHndl->indexes->lock);
	return OK;
}

int addDocument(void *hndl, void *obj, uint32_t objSize, uint64_t *result, ObjId txnId) {
DocHndl *docHndl = hndl;
DocStore *docStore;
ArenaDef *arenaDef;
Status stat = OK;
Txn *txn = NULL;
Document *doc;
DbAddr *slot;
ObjId docId;
DbAddr addr;
int idx;

	if (!bindHandle(docHndl->hndl))
		return ERROR_arenadropped;

	docStore = docstore(docHndl->hndl->map);

	if (txnId.bits)
		txn = fetchIdSlot(docHndl->hndl->map->db, txnId);

	if ((addr.bits = allocNode(docHndl->hndl->map, docHndl->hndl->list, -1, objSize + sizeof(Document), false)))
		doc = getObj(docHndl->hndl->map, addr);
	else
		return ERROR_outofmemory;

	docId.bits = allocObjId(docHndl->hndl->map, docHndl->hndl->list, docStore->docIdx);

	memset (doc, 0, sizeof(Document));

	if (txn)
		doc->txnId.bits = txnId.bits;

	doc->docId.bits = docId.bits;
	doc->size = objSize;

	memcpy (doc + 1, obj, objSize);

	if (result)
		*result = docId.bits;

	// assign document to docId slot

	slot = fetchIdSlot(docHndl->hndl->map, docId);
	slot->bits = addr.bits;

	//  install any recent index arrivals from another process/thread

	installIndexes(docHndl);

	//	add keys for the document
	//	enumerate docHndl children (e.g. indexes)

	installIndexKeys(docHndl, doc);

	if (txn)
		addIdToTxn(docHndl->hndl->map->db, txn, docId, addDoc); 

	releaseHandle(docHndl->hndl);
	return OK;
}

int rollbackTxn(void *db, uint64_t txnId);

int commitTxn(void *db, uint64_t txnId);

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
