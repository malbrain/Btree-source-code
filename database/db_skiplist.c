//	skip list implementation

#include "db.h"
#include "db_map.h"

//	find key value in skiplist, return entry address

void *skipFind(DbMap *map, DbAddr *skip, uint64_t key) {
DbAddr *next = skip;
SkipList *skipList;
ArrayEntry *entry;

  while (next->addr) {
	skipList = getObj(map, *next);

	if (*skipList->array->key <= key) {
	  entry = arraySearch(skipList->array, next->nslot, key);

	  if (*entry->key == key)
		return entry->val;

	  return NULL;
	}

	next = skipList->next;
  }

  return NULL;
}

//	remove key from skip list

void skipDel(DbMap *map, DbAddr *skip, uint64_t key) {
SkipList *skipList = NULL, *prevList;
DbAddr *next = skip;
ArrayEntry *entry;

  while (next->addr) {
	prevList = skipList;
	skipList = getObj(map, *next);

	if (*skipList->array->key <= key) {
	  entry = arraySearch(skipList->array, next->nslot, key);

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

	  freeBlk(map, next);
	  return;
	}

	next = skipList->next;
  }
}

//	Push new maximal key onto head of skip list
//	return the value slot address

void *skipPush(DbMap *map, DbAddr *skip, uint64_t key) {
SkipList *skipList;
ArrayEntry *entry;
uint64_t next;

	if (!skip->addr || skip->nslot == SKIP_node) {
		next = skip->bits;
		skip->bits = allocBlk(map, sizeof(SkipList), true);
		skipList = getObj(map, *skip);
		skipList->next->bits = next;
	}

	entry = skipList->array + skip->nslot++;
	*entry->key = key;
	return entry->val;
}

//	Add new key to skip list
//	return val address

void *skipAdd(DbMap *map, DbAddr *skip, uint64_t key) {
SkipList *skipList = NULL, *nextList;
DbAddr *next = skip;
uint64_t prevBits;
ArrayEntry *entry;
int min, max;

  while (next->addr) {
	skipList = getObj(map, *next);

	//  find skipList node that covers key

	if (skipList->next->bits && *skipList->array->key > key) {
	  next = skipList->next;
	  continue;
	}

	if (*skipList->array->key <= key) {
	  entry = arraySearch(skipList->array, next->nslot, key);
	
	  //  does key already exist?

	  if (*entry->key == key)
		return entry->val;

	  min = ++entry - skipList->array;
	} else
	  min = 0;

	//  split node if already full

	if (next->nslot == SKIP_node) {
	  prevBits = skipList->next->bits;
	  skipList->next->bits = allocBlk(map, sizeof(SkipList), true);

	  nextList = getObj(map, *skipList->next);
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

  skip->bits = allocBlk(map, sizeof(SkipList), true);
  skipList = getObj(map, *skip);

  *skipList->array->key = key;
  skip->nslot = 1;

  return skipList->array->val;
}

// add array entry

void *arrayAdd(ArrayEntry *array, uint32_t max, uint64_t key) {

  while (max)
	if (*array[max - 1].key < key)
	  break;
	else
	  array[max] = array[max - 1];

  *array[max].key = key;
  return array[max].val;
}

// find array entry, or return NULL

void *arrayFind(ArrayEntry *array, int high, uint64_t key) {
ArrayEntry *entry = arraySearch(array, high, key);

	if(*entry->key == key)
		return entry->val;

	return NULL;
}

//	search Array node for key value
//	return highest entry <= key

ArrayEntry *arraySearch(ArrayEntry *array, int high, uint64_t key) {
int low = 0, diff;

	//	key < high
	//	key >= low

	while ((diff = (high - low) / 2))
		if (key < *array[low + diff].key)
			high = low + diff;
		else
			low += diff;

	return array + low;
}

