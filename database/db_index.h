#pragma once

#define MAX_key		4096	// maximum key size in bytes

//	Index data structure after DbArena object

typedef struct {
	uint64_t numEntries[1];	// number of keys in index
	DbAddr keySpec;			// key construction document
} DbIndex;

// database index cursor

typedef struct {
    void *idx[1];           // index handle
	uint64_t ver;			// cursor doc version
    uint64_t ts;            // cursor timestamp
    ObjId txnId;            // cursor transaction
    ObjId docId;            // current doc ID
    Document *doc;          // current document
	uint32_t keyLen;
	uint8_t *key;
	bool foundKey;			// cursor position found the key
} DbCursor;

typedef struct {
	Handle *hndl;			// docStore handle
	RWLock2 lock[1];		// index list r/w lock
	SkipHead indexes[1];	// index handles by Id
	uint64_t childId;		// last child installed
	uint32_t idxCnt;		// number of indexes
} DocHndl;

#define dbindex(map) ((DbIndex *)(map->arena + 1))

Status storeDoc(DocHndl *docHndl, Handle *hndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId);
Status installIndexes(DocHndl *docHndl);

Status dbPositionCursor(DbCursor *cursor, uint8_t *key, uint32_t keyLen);
Status dbNextKey(DbCursor *cursor, Handle *index, uint8_t *endKey, uint32_t endLen);
Status dbPrevKey(DbCursor *cursor, Handle *index, uint8_t *endKey, uint32_t endLen);
Status dbNextDoc(DbCursor *cursor, uint8_t *endKey, uint32_t endLen);
Status dbPrevDoc(DbCursor *cursor, uint8_t *endKey, uint32_t endLen);
