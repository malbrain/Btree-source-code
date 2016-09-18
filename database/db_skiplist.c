//	skip list implementation

#include "db.h"
#include "db_object.h"
#include "db_map.h"

#define SKIP_node 31

typedef struct {
	uint64_t key[1];	// entry key
	uint64_t val[1];	// entry value
} SkipEntry;

typedef struct {
	SkipEntry array[SKIP_node];	// array of key/value pairs
	DbAddr next[1];				// next block of keys
} SkipList;

//	search SkipList node for key value

SkipEntry *skipSearch(SkipList *skipList, int high, uint64_t key) {
int low = 0, diff;

	//	high is tested gt key
	//	low is tested le key

	while ((diff = (high - low) / 2)) {
		if (*skipList->array[low + diff].key > key)
			high = low + diff;
		else
			low += diff;
	}

	return skipList->array + low;
}

//	find key value in skiplist, return value address

uint64_t *skipFind(Handle *hndl, DbAddr *skip, uint64_t key) {
DbAddr *next = skip;
SkipList *skipList;
SkipEntry *entry;

  while (next->addr) {
	skipList = getObj(hndl->map, *next);

	if (*skipList->array->key <= key) {
	  entry = skipSearch(skipList, next->nslot, key);

	  if (*entry->key == key)
		return entry->val;

	  return NULL;
	}

	next = skipList->next;
  }

  return NULL;
}

//	remove key from skip list

void skipDel(Handle *hndl, DbAddr *skip, uint64_t key) {
SkipList *skipList = NULL, *prevList;
DbAddr *next = skip;
SkipEntry *entry;

  while (next->addr) {
	prevList = skipList;
	skipList = getObj(hndl->map, *next);

	if (*skipList->array->key <= key) {
	  entry = skipSearch(skipList, next->nslot, key);

	  if (*entry->key != key)
		return;

	  //  remove the entry slot

	  if (--next->nslot) {
		while (entry - skipList->array < next->nslot) {
		  entry[0] = entry[1];
		  entry++;
		}

		return;
	  }

	  //  skip list node is empty, remove it

	  if (prevList)
		prevList->next->bits = skipList->next->bits;
	  else
		skip->bits = skipList->next->bits;

	  freeNode(hndl->map, hndl->list, *next);
	  return;
	}

	next = skipList->next;
  }
}

//	Push new maximal key onto head of skip list

void skipPush(Handle *hndl, DbAddr *skip, uint64_t key, uint64_t val) {
SkipList *skipList;
SkipEntry *entry;
uint64_t next;

	if (!skip->addr || skip->nslot == SKIP_node) {
		next = skip->bits;
		skip->bits = allocNode(hndl->map, hndl->list, SkipType, sizeof(SkipList), true);
		skipList = getObj(hndl->map, *skip);
		skipList->next->bits = next;
	}

	entry = skipList->array + skip->nslot++;
	*entry->key = key;
	*entry->val = val;
}

