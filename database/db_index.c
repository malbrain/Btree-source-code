#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"

//  open and install index map in docHndl cache

void installIdx(DocHndl *docHndl, ArrayEntry *entry) {
RedBlack *rbEntry;
Handle *idxhndl;
Handle **hndl;
DbAddr rbAddr;

	//  no more than 255 indexes

	if (docHndl->idxCnt < 255)
		docHndl->idxCnt++;
	else
		return;

	rbAddr.bits = *entry->val;
	rbEntry = getObj(docHndl->hndl->map->parent, rbAddr);

	idxhndl = makeHandle(arenaRbMap(docHndl->hndl->map, rbEntry));
	hndl = skipAdd(docHndl->hndl->map, docHndl->indexes->head, *entry->key);
	*hndl = idxhndl;
}

//	install new indexes

void installIndexes(DocHndl *docHndl) {
ArenaDef *arenaDef = docHndl->hndl->map->arenaDef;
DbAddr *next = arenaDef->idList->head;
uint64_t maxId = 0;
SkipList *skipList;
ArrayEntry *entry;
int idx;

	if (docHndl->childId < arenaDef->childId)
		readLock2 (arenaDef->idList->lock);
	else
		return;

	writeLock2 (docHndl->indexes->lock);

	//	transfer Id slots from arena childId list to our handle list

	while (next->addr) {
		skipList = getObj(docHndl->hndl->map->db, *next);
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

Status installIndexKey(DocHndl *docHndl, ArrayEntry *entry, Document *doc) {
ArrayEntry *array = getObj(docHndl->hndl->map, *doc->verKeys);
uint8_t key[MAX_key];
uint64_t *verPtr;
Handle *idxhndl;
DbIndex *index;
void *hndl[1];
Object *spec;
Status stat;
int keyLen;

	*hndl = (Handle *)(*entry->val);

	if ((idxhndl = bindHandle(hndl)))
		index = dbindex(idxhndl->map);
	else
		return ERROR_arenadropped;

	spec = getObj(idxhndl->map, index->keySpec);
	keyLen = keyGenerator(key, doc + 1, doc->size, spec + 1, spec->size);

	keyLen = store64(key, keyLen, doc->docId.bits);
	keyLen = store64(key, keyLen, doc->version);

	verPtr = arrayAdd(array, doc->verKeys->nslot++, *entry->key);
	*verPtr = doc->version;

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
ArrayEntry *array;
Status stat;
int idx;

	readLock2 (docHndl->indexes->lock);

	//	install keys for document

	doc->verKeys->bits = allocBlk (docHndl->hndl->map, docHndl->idxCnt * sizeof(ArrayEntry), true);

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

Status storeDoc(DocHndl *docHndl, Handle *hndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
DocStore *docStore;
ArenaDef *arenaDef;
Txn *txn = NULL;
Document *doc;
DbAddr *slot;
ObjId docId;
DbAddr addr;
int idx;

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

