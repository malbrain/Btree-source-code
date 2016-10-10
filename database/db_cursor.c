#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_txn.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	position cursor

Status dbPositionCursor(DbCursor *cursor, uint8_t *key, uint32_t keyLen) {
Handle *index;
bool found;

	if (!(index = bindHandle(cursor->idx)))
		return ERROR_arenadropped;

	switch (*index->map->arena->type) {
	  case ARTreeIndexType: {
		cursor->foundKey = artFindKey(cursor, index->map, key, keyLen);
		break;
	  }

	  case Btree1IndexType: {
		cursor->foundKey = btree1FindKey(cursor, index->map, key, keyLen);
		break;
	  }
	}

	releaseHandle(index);
	return OK;
}

Status dbNextDoc(DbCursor *cursor) {
ArrayEntry *array;
Txn *txn = NULL;
uint64_t *ver;
Handle *index;
Status stat;

	if (!(index = bindHandle(cursor->idx)))
		return ERROR_arenadropped;

	while (true) {
	  if ((stat = dbNextKey(cursor, index)))
		break;

	  if (index->map->arenaDef->useTxn)
	  	cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->ver);

	  cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->docId.bits);

	  if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(index->map->db, cursor->txnId);

	  if (!(cursor->doc = findDocVer(index->map->parent, cursor->docId, txn)))
		continue;

	  array = getObj(index->map->parent, *cursor->doc->verKeys);

	  if ((ver = arrayFind(array, cursor->doc->verKeys->nslot, index->map->arenaDef->id)))
		if (*ver == cursor->ver)
		  break;
	}

	releaseHandle(index);
	return stat;
}

Status dbPrevDoc(DbCursor *cursor) {
ArrayEntry *array;
Txn *txn = NULL;
uint64_t *ver;
Handle *index;
Status stat;

	if (!(index = bindHandle(cursor->idx)))
		return ERROR_arenadropped;

	while (true) {
	  if ((stat = dbPrevKey(cursor, index)))
		break;

	  if (index->map->arenaDef->useTxn)
	  	cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->ver);

	  cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->docId.bits);

	  if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(index->map->db, cursor->txnId);

	  if (!(cursor->doc = findDocVer(index->map->parent, cursor->docId, txn)))
		continue;

	  array = getObj(index->map->parent, *cursor->doc->verKeys);

	  if ((ver = arrayFind(array, cursor->doc->verKeys->nslot, index->map->arenaDef->id)))
		if (*ver == cursor->ver)
		  break;
	}

	releaseHandle(index);
	return stat;
}

Status dbNextKey(DbCursor *cursor, Handle *index) {
bool release = false;
Status stat;

	if (!index)
	  if ((index = bindHandle(cursor->idx)))
		release = true;
	  else
		return ERROR_arenadropped;

	switch(*index->map->arena->type) {
	case ARTreeIndexType:
		stat = artNextKey (cursor, index->map);
		break;

	case Btree1IndexType:
		stat = btree1NextKey (cursor, index->map);
		break;

	default:
		stat = ERROR_indextype;
		break;
	}

	if (release)
		releaseHandle(index);

	return stat;
}

Status dbPrevKey(DbCursor *cursor, Handle *index) {
bool release = false;
Status stat;

	if (!index)
	  if ((index = bindHandle(cursor->idx)))
		release = true;
	  else
		return ERROR_arenadropped;

	switch(*index->map->arena->type) {
	case ARTreeIndexType:
		stat = artPrevKey (cursor, index->map);
		break;

	case Btree1IndexType:
		stat = btree1PrevKey (cursor, index->map);
		break;

	default:
		stat = ERROR_indextype;
		break;
	}

	if (release)
		releaseHandle(index);

	return stat;
}
