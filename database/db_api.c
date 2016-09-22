#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"

void initialize() {
	memInit();
}

int openDatabase(void **hndl, char *name, uint32_t nameLen, bool onDisk) {
ArenaDef arenaDef[1];
DbMap *map;

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->objSize = sizeof(Txn);
	arenaDef->onDisk = onDisk;

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return ERROR_createdatabase;

	*hndl = makeHandle(map);
	return OK;
}

int openDocStore(void **hndl, void **dbhndl, char *path, uint32_t pathLen, bool onDisk) {
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

	if ((map = createMap(dbHndl->map, path, pathLen, 0, sizeof(DocStore), sizeof(ObjId), 0, onDisk)))
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
DbAddr rbAddr;
Handle *index;
Handle **hndl;

	rbAddr.bits = *skipEntry->val;
	rbEntry = getObj(docHndl->hndl->map->parent, rbAddr);

	index = makeHandle(arenaRbMap(docHndl->hndl->map, rbEntry));
	hndl = skipAdd(docHndl->hndl->map, docHndl->indexes->head, *skipEntry->key);
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

int createIndex(void **hndl, void **dochndl, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, int bits, int xtra, bool onDisk) {
BtreeIndex *btree;
DocHndl *docHndl;
Handle *index;
Object *obj;
DbMap *map;

	*hndl = NULL;

	if (!(docHndl = *dochndl))
		return ERROR_arenadropped;

	if (bits > Btree_maxbits) {
		fprintf(stderr, "createIndex: bits = %d > max = %d\n", bits, Btree_maxbits);
		exit(1);
	}

	if (bits + xtra > Btree_maxbits) {
		fprintf(stderr, "createIndex: bits = %d + xtra = %d > max = %d\n", bits, xtra, Btree_maxbits);
		exit(1);
	}

	lockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
	map = createMap(docHndl->hndl->map, name, nameLen, 0, sizeof(BtreeIndex), sizeof(ObjId), 0, onDisk);

	if (!map) {
		unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
		return ERROR_createindex;
	}

	index = makeHandle(map);

	if (bindHandle((void **)&index))
		btree = btreeIndex(map);
	else {
		unlockLatch(docHndl->hndl->map->arenaDef->nameTree->latch);
		return ERROR_arenadropped;
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
	*hndl = index;
	return OK;
}

//	create new cursor

int createCursor(void **hndl, void **idxhndl) {
BtreeCursor *cursor;
Handle *index;

	if ((index = bindHandle(idxhndl)))
		cursor = btreeCursor(index);
	else
		return ERROR_arenadropped;

	releaseHandle(index);
	*hndl = cursor;
	return OK;
}

int returnCursor(void **hndl) {
BtreeCursor *cursor;
Handle *index;

	if (!(cursor = *hndl))
		return ERROR_handleclosed;

	btreeReturnCursor(cursor);
	*hndl = NULL;
	return OK;
}

//	iterate cursor to next document
/*
int nextCursorDoc(void **hndl) {
BtreeCursor *cursor;

	if ((cursor = *hndl))
		docId = btreeNextKey(cursor);
	else
		return ERROR_handleclosed;
}
*/
int cloneHandle(void **newhndl, void **oldhndl) {
Handle *hndl;

	if ((hndl = bindHandle(oldhndl))) {
		*newhndl = makeHandle(hndl->map);
		return OK;
	}

	return ERROR_handleclosed;
}

int installIndexKey(DocHndl *docHndl, SkipEntry *entry, void *obj, uint32_t objSize, ObjId docId) {
uint8_t key[MAX_key];
BtreeIndex *btree;
void *hndl[1];
Handle *index;
Object *spec;
int keyLen;
int stat;

	*hndl = (Handle *)(*entry->val);

	if (index = bindHandle(hndl))
		btree = btreeIndex(index->map);
	else
		return ERROR_arenadropped;

	spec = getObj(index->map, btree->keySpec);
	keyLen = keyGenerator(key, obj, objSize, spec + 1, spec->size);

	keyLen = store64(key, keyLen, docId.bits);

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

int addDocument(void **dochndl, void *obj, uint32_t objSize, uint64_t *result, ObjId txnId) {
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
		*result = docId.bits;

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

int rollbackTxn(void **hndl, uint64_t txnId);

int commitTxn(void **hndl, uint64_t txnId);

int insertKey(void **hndl, uint8_t *key, uint32_t len) {
Handle *index;
int stat;

	if ((index = bindHandle(hndl)))
		stat = btreeInsertKey(index, key, len, 0, Btree_indexed);
	else
		return ERROR_arenadropped;

	releaseHandle(index);
	return stat;
}

uint32_t addObjId(uint8_t *key, uint32_t len, uint64_t addr) {
	return store64(key, len, addr);
}
