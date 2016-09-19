#include "db.h"
#include "db_map.h"

//	red/black entry

#define getRb(x,y)	((RedBlack *)getObj(x,y))

RedBlack *rbNext(DbMap *map, PathStk *path);

void rbInsert (DbMap *map, DbAddr *root, DbAddr slot, PathStk *path);
void rbRemove (DbMap *map, DbAddr *root, PathStk *path);

//  compare two keys, returning > 0, = 0, or < 0
//  as the comparison value
//	-1 -> go right
//	1 -> go left

int rbKeyCmp (RedBlack *node, uint8_t *key2, uint32_t len2) {
uint32_t len1 = node->keyLen;
int ans;

	if ((ans = memcmp (node->key, key2, len1 > len2 ? len2 : len1)))
		return ans > 0 ? 1 : -1;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find entry from key and produce path stack
//	return NULL if not found.

RedBlack *rbFind(DbMap *map, DbAddr *root, char *key, uint32_t len, PathStk *path) {
RedBlack *node = NULL;
DbAddr slot = *root;
int rbcmp;

	path->lvl = 0;

	while (slot.addr) {
		if (path)
			path->entry[path->lvl].bits = slot.bits;

		node = getObj(map,slot);
		rbcmp = rbKeyCmp (node, key, len);

		if (path)
			path->entry[path->lvl].rbcmp = rbcmp;

		if (rbcmp == 0)
			return node;

		if (rbcmp > 0)
			slot.bits = node->left.bits;
		else
			slot.bits = node->right.bits;

		if (path)
			path->lvl++;
	}

	return NULL;
}

//	left rotate parent node
//	call with root locked

void rbLeftRotate (DbMap *map, DbAddr *root, DbAddr slot, RedBlack *parent, int cmp) {
RedBlack *x = getObj(map,slot);
DbAddr right = x->right;
RedBlack *y = getObj(map,right);

	x->right = y->left;

	if( !parent ) //	is x the root node?
		root->bits = right.bits | ADDR_MUTEX_SET;
	else if( cmp == 1 )
		parent->left = right;
	else
		parent->right = right;

	y->left = slot;
}

//	right rotate parent node

void rbRightRotate (DbMap *map, DbAddr *root, DbAddr slot, RedBlack *parent, int cmp) {
RedBlack *x = getObj(map,slot);
DbAddr left = x->left;
RedBlack *y = getObj(map,left);

	x->left = y->right;

	if( !parent ) //	is y the root node?
		root->bits = left.bits | ADDR_MUTEX_SET;
	else if( cmp == 1 )
		parent->left = left;
	else
		parent->right = left;

	y->right = slot;
}

//	add entry to red/black tree
//	call with root locked
//	and path set from find

void rbAdd (DbMap *map, DbAddr *root, RedBlack *entry, PathStk *path) {
RedBlack *parent, *uncle, *grand;
int lvl = path->lvl;
DbAddr slot;

	if (!lvl) {
		root->bits = entry->addr.bits | ADDR_MUTEX_SET;
		return;
	}

	slot.bits = entry->addr.bits;

	if (path->entry[lvl].bits)
		parent = getObj(map,path->entry[lvl]);
	else
		parent = getObj(map,*root);

	if( path->entry[lvl].rbcmp == 1 )
		parent->left = slot;
	else
		parent->right = slot;

	entry->red = 1;

	while( lvl > 0 && parent->red ) {
	  grand = getObj(map,path->entry[lvl-1]);

	  if( path->entry[lvl-1].rbcmp == 1 ) { // was grandparent left followed?
		uncle = getObj(map,grand->right);
		if( grand->right.bits && uncle->red ) {
		  parent->red = 0;
		  uncle->red = 0;
		  grand->red = 1;

		  // move to grandparent & its parent (if any)

	  	  slot = path->entry[--lvl];
		  if( !lvl )
			break;
	  	  parent = getObj(map,path->entry[--lvl]);
		  continue;
		}

		// was the parent right link followed?
		// if so, left rotate parent

	  	if( path->entry[lvl].rbcmp == -1 ) {
		  rbLeftRotate(map, root, path->entry[lvl], grand, path->entry[lvl-1].rbcmp);
		  parent = getObj(map,slot);	// slot was rotated to parent
		}

		parent->red = 0;
		grand->red = 1;

		//	get pointer to grandparent's parent

		if( lvl>1 )
	    	grand = getObj(map,path->entry[lvl-2]);
		else
			grand = NULL;

		//  right rotate the grandparent slot

		slot = path->entry[lvl-1];
		rbRightRotate(map, root, slot, grand, path->entry[lvl-2].rbcmp);
		return;
	  } else {	// symmetrical case
		uncle = getObj(map,grand->left);
		if( grand->left.bits && uncle->red ) {
		  uncle->red = 0;
		  parent->red = 0;
		  grand->red = 1;

		  // move to grandparent & its parent (if any)
	  	  slot = path->entry[--lvl];
		  if( !lvl )
			break;
	  	  parent = getObj(map,path->entry[--lvl]);
		  continue;
		}

		// was the parent left link followed?
		// if so, right rotate parent

	  	if( path->entry[lvl].rbcmp == 1 ) {
		  rbRightRotate(map, root, path->entry[lvl], grand, path->entry[lvl-1].rbcmp);
		  parent = getObj(map,slot);	// slot was rotated to parent
		}

		parent->red = 0;
		grand->red = 1;

		//	get pointer to grandparent's parent

		if( lvl>1 )
	    	grand = getObj(map,path->entry[lvl-2]);
		else
			grand = NULL;

		//  left rotate the grandparent slot

		slot = path->entry[lvl-1];
		rbLeftRotate(map, root, slot, grand, path->entry[lvl-2].rbcmp);
		return;
	  }
	}

	//	reset root color

	getRb(map, *root)->red = 0;
}

//	delete found entry from rbtree at top of path stack

void rbRemove (DbMap *map, DbAddr *root, PathStk *path) {
DbAddr slot = path->entry[path->lvl];
RedBlack *node = getObj (map, slot);
RedBlack *parent, *sibling, *grand;
uint8_t red = node->red, lvl, idx;
DbAddr left;

	if( (lvl =  path->lvl) ) {
		parent = getObj(map,path->entry[lvl - 1]);
		parent->right = node->left;
	} else
		root->bits = node->left.bits | ADDR_MUTEX_SET;

	if( node->left.bits )
		node = getObj(map,node->left);
	else {
		freeBlk(map, &slot);
		--path->lvl;
		return;
	}

	//	fixup colors

	if( !red )
	 while( !node->red && lvl ) {
		left = parent->left;
		sibling = getObj(map,left);
		if( sibling->red ) {
		  sibling->red = 0;
		  parent->red = 1;
		  if( lvl > 1 )
		  	grand = getObj(map,path->entry[lvl-2]);
		  else
			grand = NULL;
		  rbRightRotate(map, root, path->entry[lvl-1], grand, -1);
		  sibling = getObj(map,parent->left);

		  for( idx = ++path->lvl; idx > lvl - 1; idx-- )
			path->entry[idx] = path->entry[idx-1];

		  path->entry[idx] = left; 
		}

		if( !sibling->right.bits || !getRb(map,sibling->right)->red )
		  if( !sibling->left.bits || !getRb(map,sibling->left)->red ) {
			sibling->red = 1;
			node = parent;
			parent = grand;
			lvl--;
			continue;
		  }

		if( !sibling->left.bits || !getRb(map,sibling->left)->red ) {
			if( sibling->right.bits )
			  getRb(map,sibling->right)->red = 0;

			sibling->red = 1;
			rbLeftRotate (map, root, parent->left, parent, 1);
			sibling = getObj(map,parent->left);
		}

		getRb(map, sibling->left)->red = 0;
		sibling->red = parent->red;
		parent->red = 0;
		rbRightRotate(map, root, path->entry[lvl-1], grand, -1);
		break;
	 }

	freeBlk(map, &slot);
	getRb(map, *root)->red = 0;
}

//	delete red/black tree entry

bool rbDel (DbMap *map, DbAddr *root, void *key, uint32_t keyLen) {
RedBlack *entry;
PathStk path[1];

	lockLatch(root->latch);

	if ((entry = rbFind(map, root, key, keyLen, path))) {
		rbRemove (map, root, path);
		unlockLatch(root->latch);
		return true;
	}

	unlockLatch(root->latch);
	return false;
}

//	make new red/black entry

RedBlack *rbNew (DbMap *map, void *key, uint32_t keyLen, uint32_t payload) {
RedBlack *entry = NULL;
DbAddr child;

  if ((child.bits = allocBlk(map, sizeof(RedBlack) + keyLen + payload, true))) {
	entry = getObj(map, child);
	entry->keyLen = keyLen;
	entry->payload = payload;
	entry->addr.bits = child.bits;
	memcpy (entry->key, key, keyLen);
  }
#ifdef DEBUG
	else
		fprintf(stderr, "Out of Memory -- rbNew\n");
#endif
  return entry;
}

//	return next entry in red/black tree path

RedBlack *rbNext(DbMap *map, PathStk *path) {
RedBlack *entry;

	do {
	  if (path->entry[path->lvl].bits)
		entry = getObj(map, path->entry[path->lvl]);
	  else
		continue;

	  // went left last time, now return entry

	  if (path->entry[path->lvl].rbcmp > 0) {
	  	path->entry[path->lvl].rbcmp = 0;
		return entry;
	  }

	  // went right last time, back up tree level

	  if (path->entry[path->lvl].rbcmp < 0)
		continue;

	  // returned entry last time, now go right
	  // or back up one level

	  if ((path->entry[path->lvl].bits = entry->right.bits))
	    entry = getObj(map, entry->right);
	  else
		continue;

	  // go all the way left from right child

	  path->entry[path->lvl].rbcmp = -1;

	  while (entry->left.bits) {
	  	path->entry[++path->lvl].rbcmp = 1;
		path->entry[path->lvl].bits = entry->left.bits;
	    entry = getObj(map, entry->left);
	  }

	  return entry;
	} while (path->lvl--);

	return NULL;
}

//	enumerate red/black tree node addresses

Status rbList(DbMap *map, DbAddr *root, RbFcnPtr fcn, void *key, uint32_t keyLen, void *params) {
PathStk path[1];
RedBlack *entry;
Status stat;

	rbFind(map, root, key, keyLen, path);

	while ((entry = rbNext(map, path)))
	  if ((stat = fcn(map, entry, params)))
		return stat;

	return OK;
}
