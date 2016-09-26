#pragma once

//	red-black tree descent stack

#define RB_bits		24

struct PathStk_ {
	uint64_t lvl;			// height of the stack
	DbAddr entry[RB_bits];	// stacked tree nodes
};

typedef struct RedBlack_ {
	uint32_t keyLen;		// length of key after entry
	uint32_t payload;		// length of payload after key
	DbAddr left, right;		// next nodes down
	DbAddr addr;			// this entry addr in map
	char latch[1];			// this entry latch
	char red;				// is tree node red?
} RedBlack;

#define rbPayload(entry) ((void *)((char *)(entry + 1) + entry->keyLen))

typedef Status (*RbFcnPtr)(DbMap *map, RedBlack *entry, void *params);

Status rbList(DbMap *map, DbAddr *root, RbFcnPtr fcn, void *key, uint32_t keyLen, void *params);
RedBlack *rbFind(DbMap *parent, DbAddr *childNames, char *name, uint32_t nameLen, PathStk *path);
RedBlack *rbNew (DbMap *map, void *key, uint32_t keyLen, uint32_t payload);
void rbAdd(DbMap *map, DbAddr *root, RedBlack *entry, PathStk *path);
bool rbDel (DbMap *map, DbAddr *root, void *key, uint32_t keylen);

