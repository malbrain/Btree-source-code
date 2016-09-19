//	skip list implementation

#include "db.h"
#include "db_map.h"

//	search SkipList node for key value
//	return highest entry <= key

SkipEntry *skipSearch(SkipList *skipList, int high, uint64_t key) {
int low = 0, diff;

	//	key < high
	//	key >= low

	while ((diff = (high - low) / 2))
		if (key < *skipList->array[low + diff].key)
			high = low + diff;
		else
			low += diff;

	return skipList->array + low;
}

//	find key value in skiplist, return entry address

SkipEntry *skipFind(Handle *hndl, DbAddr *skip, uint64_t key) {
DbAddr *next = skip;
SkipList *skipList;
SkipEntry *entry;

  while (next->addr) {
	skipList = getObj(hndl->map, *next);

	if (*skipList->array->key <= key) {
	  entry = skipSearch(skipList, next->nslot, key);

	  if (*entry->key == key)
		return entry;

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

//	Add new key to skip list
//	return val address

void *skipAdd(Handle *hndl, DbAddr *skip, uint64_t key) {
SkipList *skipList = NULL, *nextList;
DbAddr *next = skip;
uint64_t prevBits;
SkipEntry *entry;
int min, max;

  while (next->addr) {
	skipList = getObj(hndl->map, *next);

	//  find skipList node that covers key

	if (skipList->next->bits && *skipList->array->key > key) {
	  next = skipList->next;
	  continue;
	}

	if (*skipList->array->key <= key) {
	  entry = skipSearch(skipList, next->nslot, key);
	
	  //  does key already exist?

	  if (*entry->key == key)
		return entry->val;

	  min = ++entry - skipList->array;
	} else
	  min = 0;

	//  split node if already full

	if (next->nslot == SKIP_node) {
	  prevBits = skipList->next->bits;
	  skipList->next->bits = allocNode(hndl->map, hndl->list, SkipType, sizeof(SkipList), true);

	  nextList = getObj(hndl->map, *skipList->next);
	  nextList->next->bits = prevBits;
	  memcpy(nextList->array, skipList->array + SKIP_node / 2, sizeof(SkipList) * (SKIP_node - SKIP_node / 2));

	  skipList->next->nslot = SKIP_node - SKIP_node / 2;
	  next->nslot = SKIP_node / 2;
	  continue;
	}

	//  insert new entry slot

	max = next->nslot++;

	while (max > min)
	  skipList->array[max] = skipList->array[max - 1], max--;

	return skipList->array[max].val;
  }

  // initialize empty list

  skip->bits = allocNode(hndl->map, hndl->list, SkipType, sizeof(SkipList), true);
  skipList = getObj(hndl->map, *skip);

  *skipList->array->key = key;
  skip->nslot = 1;

  return skipList->array->val;
}
