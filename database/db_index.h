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
} DbCursor;

#define dbIndex(map) ((DbIndex *)(map->arena + 1))
#define dbCursor(map) ((DbCursor *)(map->arena + 1))

