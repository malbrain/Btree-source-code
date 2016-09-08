#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"
#include "btree1.h"

void initialize() {
	memInit();
}

void *createObjStore(char *path, bool onDisk) {
Handle *hndl = createMap(path, 0, sizeof(ObjId), 0, onDisk);

	if (hndl->map->created)
		hndl->map->arena->type[0] = ObjStoreType;

	return hndl;
}

void *createIndex(char *docStore, char *idxName, int bits, int xtra, bool onDisk) {
int len = strlen(docStore);
char path[4096];
Handle *hndl;

	memcpy (path, docStore, len);
	path[len++] = '.';
	strcpy (path + len, idxName);

	hndl = createMap(path, sizeof(BtreeIndex), sizeof(ObjId), 0, onDisk);

	if (hndl->map->created)
		btreeInit(hndl);

	return hndl;
}

void *createCursor(void *index) {
	return (void *)btreeCursor((Handle *)index);
}

void *cloneHandle(void *hndl) {
	return (void *)makeHandle(((Handle *)hndl)->map);
}

int addObject(void *arg, void *obj, uint32_t size, uint64_t *result) {
Handle *hndl = (Handle *)arg;
Status stat = OK;
DbAddr addr;
void *dest;

	if (bindHandle(hndl))
		*result = allocNode(hndl->map, hndl->freeList, -1, size, false); 
	else
		return ERROR_arenadropped;

	addr.bits = *result;

	dest = getObj(hndl->map, addr);
	memcpy (dest, obj, size);
	return OK;
}

int insertKey(void *index, uint8_t *key, uint32_t len) {
Handle *hndl = (Handle *)index;
int stat;

	if (bindHandle(hndl))
		stat = btreeInsertKey(hndl, key, len, 0, Btree_indexed);
	else
		return ERROR_arenadropped;

	releaseHandle(hndl);
	return stat;
}

int addObjId(uint8_t *key, uint64_t addr) {
	store64(key, addr);
	return sizeof(uint64_t);
}
