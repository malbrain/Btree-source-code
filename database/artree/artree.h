#pragma once

#define MAX_cursor 4096

// Artree interior nodes

enum ARTNodeType {
	UnusedSlot = 0,					// slot is not yet in use
	Array4,							// node contains 4 radix slots
	Array14,						// node contains 14 radix slots
	Array64,						// node contains 64 radix slots
	Array256,						// node contains 256 radix slots
	KeyEnd,							// node end of the complete key
	SpanNode,						// node contains up to 8 key bytes
	SpanNode256 = SpanNode + 16,	// node spans up to 256 bytes
	MaxARTType = SpanNode256 + 4	// node spans up to 1024 bytes
};

/**
 * radix node with four slots and their key bytes
 */

typedef struct {
	uint64_t timestamp;
	volatile uint8_t alloc;
	volatile uint8_t keys[4];
	uint8_t filler[3];
	DbAddr radix[4];
} ARTNode4;

/**
 * radix node with fourteen slots and their key bytes
 */

typedef struct {
	uint64_t timestamp;
	volatile uint16_t alloc;
	volatile uint8_t keys[14];
	DbAddr radix[14];
} ARTNode14;

/**
 * radix node with sixty-four slots and a 256 key byte array
 */

typedef struct {
	uint64_t timestamp;
	volatile uint64_t alloc;
	volatile uint8_t keys[256];
	DbAddr radix[64];
} ARTNode64;

/**
 * radix node all 256 slots
 */

typedef struct {
	uint64_t timestamp;
	volatile uint64_t alloc[4];
	DbAddr radix[256];
} ARTNode256;

/**
 * span node containing up to 8 consecutive key bytes
 * span nodes are used to compress linear chains of key bytes
 */

typedef struct {
	uint64_t timestamp;
	DbAddr next[1];		// next node after span
	uint8_t bytes[8];
} ARTSpan;

/**
 * Span node base length calc
 */

#define SPANLEN(type) (type > SpanNode ? (type < SpanNode256 ? 0 : ((type - SpanNode256) + 1) << 8 ) : 0)

typedef struct {
	DbIndex base[1];	// basic db index
	DbAddr root[1];		// root of the arttree
	int64_t numEntries[1];
} ArtIndex;

typedef struct {
	DbAddr slot[1];	// slot that points to node
	DbAddr *addr;	// tree addr of slot
	uint16_t off;	// offset within key
	int16_t ch;		// character of key
	bool dir;
} CursorStack;

typedef struct {
	DbCursor base[1];				// common cursor header (must be first)
	bool atRightEOF;				// needed to support 'atEOF()'
	bool atLeftEOF;					// needed to support 'atEOF()'
	uint32_t depth;					// current depth of cursor
	uint32_t keySize;				// current size of the key
	uint8_t key[MAX_key];			// current cursor key
	CursorStack stack[MAX_cursor];	// cursor stack
} ArtCursor;

#define artIndexAddr(map)((ArtIndex *)(map->arena + 1))

DbCursor *artNewCursor(Handle *index, uint64_t timestamp, ObjId txnId);
uint8_t *artCursorKey(DbCursor *dbCursor, uint32_t *len);

Status artNextKey(DbCursor *dbCursor, DbMap *index);
Status artPrevKey(DbCursor *dbCursor, DbMap *index);
Status artSeekKey(DbCursor *cursor, uint8_t *key, uint32_t keylen);

Status artInit(Handle *hndl, Params *params);
Status artInsertKey (Handle *hndl, uint8_t *key, uint32_t keyLen);
uint64_t artAllocateNode(Handle *index, int type, uint32_t size);

void addSlotToWaitList(Handle *index, DbAddr slot);
