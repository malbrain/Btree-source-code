#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "btree1.h"

void initialize() {
	memInit();
}

void *openDatabase(char *name, uint32_t nameLen, bool onDisk) {
ArenaDef arenaDef[1];
DbMap *map;

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(ArenaDef);
	arenaDef->objSize = sizeof(Txn);
	arenaDef->onDisk = onDisk;

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return NULL;

	if (map->created)
		map->arena->type[0] = DatabaseType;

	map->arenaDef = (ArenaDef *)(map->arena + 1);
	memcpy(map->arenaDef, arenaDef, sizeof(arenaDef));
	return makeHandle(map);
}

void *openDocStore(void *hndl, char *path, uint32_t pathLen, bool onDisk) {
Handle *database = (Handle *)hndl;
DbMap *map;

	if (bindHandle(database))
		map = createMap(database->map, path, pathLen, 0, 0, sizeof(ObjId), 0, onDisk);
	else
		return NULL;

	releaseHandle(database);

	if (map->created)
		map->arena->type[0] = DocStoreType;

	return makeHandle(map);
}

void *createIndex(void *hndl, char *name, uint32_t nameLen, int bits, int xtra, bool onDisk) {
Handle *index, *docStore = (Handle *)hndl;
BtreeIndex *btree;
DbMap *map;

	if (bits > Btree_maxbits) {
		fprintf(stderr, "createIndex: bits = %d > max = %d\n", bits, Btree_maxbits);
		exit(1);
	}

	if (bits + xtra > Btree_maxbits) {
		fprintf(stderr, "createIndex: bits = %d + xtra = %d > max = %d\n", bits, xtra, Btree_maxbits);
		exit(1);
	}

	if (bindHandle(docStore))
		map = createMap(docStore->map, name, nameLen, 0, sizeof(BtreeIndex), sizeof(ObjId), 0, onDisk);
	else
		return NULL;

	if (!map)
		return NULL;

	index = makeHandle(map);

	btree = btreeIndex(map);
	btree->pageSize = 1 << bits;
	btree->pageBits = bits;
	btree->leafXtra = xtra;

	if (map->created)
		btreeInit(index);

	return index;
}

void *createCursor(void *index) {
	return (void *)btreeCursor((Handle *)index);
}

void *cloneHandle(void *hndl) {
	return (void *)makeHandle(((Handle *)hndl)->map);
}

int addDocument(void *hndl, void *obj, uint32_t size, uint64_t *result, uint64_t txnAddr) {
Handle *docStore = (Handle *)hndl;
Status stat = OK;
Document *doc;
ObjId objId;
DbAddr addr;

	if (bindHandle(docStore))
	  if ((addr.bits = allocNode(docStore->map, docStore->array->list, -1, size + sizeof(Document), false)))
		doc = getObj(docStore->map, addr);
	  else
		return ERROR_outofmemory;
	else
		return ERROR_arenadropped;

	objId.bits = allocObjId(docStore->map, &docStore->array->list[ObjIdType]);

	memset (doc, 0, sizeof(Document));
	doc->objId.bits = objId.bits;
	doc->txn.bits = txnAddr;
	doc->size = size;

	memcpy (doc + 1, obj, size);

	*result = objId.bits;
	return OK;
}

int commitDocument(void *hndl, uint64_t document) {
Handle *docStore = (Handle *)hndl;
Document *doc;
ObjId docId;

	if (bindHandle(docStore))
		docId.bits = document;
	else
		return ERROR_arenadropped;

	doc = fetchObjSlot(docStore->map, docId);
	return OK;
}

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
