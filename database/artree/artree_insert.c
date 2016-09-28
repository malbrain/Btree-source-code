#include "../db.h"
#include "../db_object.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

typedef struct {
	DbAddr *slot;
	DbAddr *prev;
	DbAddr newSlot[1];

	uint8_t *key;
	Handle *index;

	uint32_t keylen;	// length of the key
	uint32_t depth;		// current tree depth
	uint32_t off;	 	// progress down the key bytes
	uint8_t ch;			// current node character
} ParamStruct;

typedef enum {
	ContinueSearch,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

ReturnState insertKeySpan(ARTSpan*, ParamStruct *);
ReturnState insertKeyNode4(ARTNode4*, ParamStruct *);
ReturnState insertKeyNode14(ARTNode14*, ParamStruct *);
ReturnState insertKeyNode64(ARTNode64*, ParamStruct *);
ReturnState insertKeyNode256(ARTNode256*, ParamStruct *);

uint64_t artAllocateNode(Handle *index, int type, uint32_t size) {
DbAddr *free = index->list[type].free;
DbAddr *tail = index->list[type].tail;

	return allocObj(index->map, free, tail, type, size, true);
}

uint64_t allocSpanNode(ParamStruct *p, uint32_t len) {
int type = SpanNode, size = sizeof(ARTSpan);

	if ( len > 8) {
	  if (len < 256) {
		type = SpanNode + (len - 8 + 15) / 16;
		size += (len - 8 + 15) / 16 * 16;
	  } else {
		type = SpanNode256 + (len - 8 + 255) / 256;
		size += (len - 8 + 255) / 256 * 256;
	  }
	}

	return artAllocateNode(p->index, type, size);
}

//
// fill in the empty slot with span node
//	with remaining key bytes
//	return false if out of memory
//
bool fillKey(ParamStruct *p) {
ARTSpan *spanNode;
uint32_t len;
DbAddr addr;

	p->slot->bits = 0;

	while ( (len = (p->keylen - p->off)) ) {
		if ((addr.bits = allocSpanNode(p, len)))
			spanNode = getObj(p->index->map, addr);
		else
			return false;

		addr.nbyte = len - 1;
		spanNode->timestamp = allocateTimestamp(p->index->map->db, en_writer);
		memcpy(spanNode->bytes, p->key + p->off, len);
		p->slot->bits = addr.bits;

		p->slot = spanNode->next;
		p->off += len;
	}

	return true;
}

Status artInsertKey( Handle *index, uint8_t *key, uint32_t keylen) {
bool restart = true;
bool pass = false;
ParamStruct p[1];
DbAddr slot;

	memset(p, 0, sizeof(p));

	do {
		restart = false;

		p->off = 0;
		p->depth = 0;
		p->key = key;
		p->index = index;
		p->keylen = keylen;
		p->slot = artIndexAddr(index->map)->root;

		//  if we are waiting on a dead bit to clear
		if (pass)
			yield();
		else
			pass = true;

		while (p->off < p->keylen) {
			ReturnState rt = ContinueSearch;
			p->newSlot->bits = p->slot->bits;
			p->prev = p->slot;

			switch (p->newSlot->type < SpanNode ? p->newSlot->type : SpanNode) {
				case KeyEnd: {
					rt = EndSearch;
					break;
				}
				case SpanNode: {
					ARTSpan *spanNode = getObj(index->map, *p->newSlot);
					rt = insertKeySpan(spanNode, p);
					break;
				}
				case Array4: {
					ARTNode4 *radix4Node = getObj(index->map, *p->newSlot);
					rt = insertKeyNode4(radix4Node, p);
					break;
				}
				case Array14: {
					ARTNode14 *radix14Node = getObj(index->map, *p->newSlot);
					rt = insertKeyNode14(radix14Node, p);
					break;
				}
				case Array64: {
					ARTNode64 *radix64Node = getObj(index->map, *p->newSlot);
					rt = insertKeyNode64(radix64Node, p);
					break;
				}
				case Array256: {
					ARTNode256 *radix256Node = getObj(index->map, *p->newSlot);
					rt = insertKeyNode256(radix256Node, p);
					break;
				}
				case UnusedSlot: {
					// obtain write lock on the node
					lockLatch(p->slot->latch);

					// restart if slot has been killed
					// or node has changed.
					if (p->slot->dead) {
						unlockLatch(p->slot->latch);
						rt = RestartSearch;
						break;
					}

					if (p->slot->type) {
						unlockLatch(p->slot->latch);
						rt = RetrySearch;
						break;
					}

					p->slot = p->newSlot;
					fillKey(p);
					rt = EndSearch;
					break;
				}

			}  // end switch

			if (ErrorSearch == rt)		//	out of memory error
				return ERROR_outofmemory;
			if (ContinueSearch == rt) {	//	move down the trie one node
				p->depth++;
				continue;
			} else if (RetrySearch == rt)//	retry the current trie node
				continue;
			else if (RestartSearch == rt) {
				if (!p->depth)
					return ARTREE_error;
				restart = true;
				break;
			}

			// save existing slot value
			slot.bits = p->prev->bits;

			// install and unlock node slot value
			p->newSlot->mutex = 0;
			p->prev->bits = p->newSlot->bits;

			// add old slot to free/wait list, if changed
			if (slot.type)
			  if (!addSlotToFrame(index->map, index->list[slot.type].tail, slot))
				return ERROR_outofmemory;

			break;

		}	// end while (p->off < p->keylen)

	} while (restart);

	p->slot->type = KeyEnd;
	return OK;
}

ReturnState insertKeyNode4(ARTNode4 *node, ParamStruct *p) {
ARTNode14 *radix14Node;
uint32_t idx, out;

	for (idx = 0; idx < 4; idx++) {
		if (node->alloc & (1 << idx))
			if (p->key[p->off] == node->keys[idx]) {
				p->slot = node->radix + idx;
				p->off++;
				return ContinueSearch;
			}
	}

	// obtain write lock on the node
	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.
	if (p->slot->dead) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->addr != p->newSlot->addr) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	// retry search under lock
	for (idx = 0; idx < 4; idx++) {
		if (node->alloc & (1 << idx))
			if (p->key[p->off] == node->keys[idx]) {
				unlockLatch(p->slot->latch);
				p->slot = node->radix + idx;
				p->off++;
				return ContinueSearch;
			}
	}

	// add to radix4 node if room
	if (node->alloc < 0xF) {
#ifdef _WIN32
		_BitScanForward((DWORD *)&idx, ~node->alloc);
#else
		idx = __builtin_ctz(~node->alloc);
#endif

		node->keys[idx] = p->key[p->off++];
		p->slot = node->radix + idx;
		if (!fillKey(p))
			return ErrorSearch;
		node->alloc |= 1 << idx;
		return EndSearch;
	}

	// the radix node is full, promote to the next larger size.

	if ( (p->newSlot->bits = artAllocateNode(p->index, Array14, sizeof(ARTNode14))) )
		radix14Node = getObj(p->index->map, *p->newSlot);
	else {
		unlockLatch(p->slot->latch);
		return ErrorSearch;
	}

	radix14Node->timestamp = node->timestamp;

	for (idx = 0; idx < 4; idx++) {
		DbAddr *slot = node->radix + idx;
		lockLatch(slot->latch);

		if (!slot->dead) {
#ifdef _WIN32
			_BitScanForward((DWORD *)&out, ~radix14Node->alloc);
#else
			out = __builtin_ctz(~radix14Node->alloc);
#endif
			radix14Node->alloc |= 1 << out;
			radix14Node->radix[out].bits = slot->bits;  // copies mutex also
			radix14Node->keys[out] = node->keys[idx];
			radix14Node->radix[out].mutex = 0;
			kill_slot(slot->latch);
		}
		unlockLatch(slot->latch);
	}

#ifdef _WIN32
	_BitScanForward((DWORD *)&out, ~radix14Node->alloc);
#else
	out = __builtin_ctz(~radix14Node->alloc);
#endif

	radix14Node->keys[out] = p->key[p->off++];

	// fill in rest of the key in span nodes
	p->slot = radix14Node->radix + out;

	if (!fillKey(p))
		return ErrorSearch;

	radix14Node->alloc |= 1 << out;
	return EndSearch;
}

ReturnState insertKeyNode14(ARTNode14 *node, ParamStruct *p) {
ARTNode64 *radix64Node;
uint32_t idx, out;

	for (idx = 0; idx < 14; idx++) {
		if (node->alloc & (1 << idx))
			if (p->key[p->off] == node->keys[idx]) {
				p->slot = node->radix + idx;
				p->off++;
				return ContinueSearch;
			}
	}

	// obtain write lock on the node
	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.
	if (p->slot->dead) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->addr != p->newSlot->addr) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	//  retry search under lock
	for (idx = 0; idx < 14; idx++) {
		if (node->alloc & (1 << idx))
			if (p->key[p->off] == node->keys[idx]) {
				unlockLatch(p->slot->latch);
				p->slot = node->radix + idx;
				p->off++;
				return ContinueSearch;
			}
	}

	// add to radix node if room
	if (node->alloc < 0x3fff) {
#ifdef _WIN32
		_BitScanForward((DWORD *)&idx, ~node->alloc);
#else
		idx = __builtin_ctz(~node->alloc);
#endif

		node->keys[idx] = p->key[p->off++];
		p->slot = node->radix + idx;
		if (!fillKey(p))
			return ErrorSearch;
		node->alloc |= 1 << idx;
		return EndSearch;
	}

	// the radix node is full, promote to the next larger size.
	// mark all the keys as currently unused.

	if ( (p->newSlot->bits = artAllocateNode(p->index, Array64, sizeof(ARTNode64))) )
		radix64Node = getObj(p->index->map,*p->newSlot);
	else
		return ErrorSearch;

	memset((void*)radix64Node->keys, 0xff, sizeof(radix64Node->keys));
	radix64Node->timestamp = node->timestamp;

	for (idx = 0; idx < 14; idx++) {
		DbAddr *slot = node->radix + idx;
		lockLatch(slot->latch);
		if (!slot->dead) {

#ifdef _WIN32
			_BitScanForward64((DWORD *)&out, ~radix64Node->alloc);
#else
			out = __builtin_ctzl(~radix64Node->alloc);
#endif

			radix64Node->alloc |= 1ULL << out;
			radix64Node->radix[out].bits = slot->bits;  // copies mutex
			radix64Node->keys[node->keys[idx]] = out;
			radix64Node->radix[out].mutex = 0;
			kill_slot(slot->latch);
		}
		unlockLatch(slot->latch);
	}

#ifdef _WIN32
	_BitScanForward64((DWORD *)&out, ~radix64Node->alloc);
#else
	out = __builtin_ctzl(~radix64Node->alloc);
#endif

	radix64Node->keys[p->key[p->off++]] = out;

	// fill in rest of the key bytes into span nodes
	p->slot = radix64Node->radix + out;

	if (!fillKey(p))
		return ErrorSearch;

	radix64Node->alloc |= 1ULL << out;
	return EndSearch;
}

ReturnState insertKeyNode64(ARTNode64 *node, ParamStruct *p) {
uint32_t idx = node->keys[p->key[p->off]], out;
ARTNode256 *radix256Node;

	if (idx < 0xff && node->alloc & (1ULL << idx)) {
		p->slot = node->radix + idx;
		p->off++;
		return ContinueSearch;
	}

	// obtain write lock on the node
	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.
	if (p->slot->dead) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->addr != p->newSlot->addr) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	//  retry under lock
	idx = node->keys[p->key[p->off]];
	if (idx < 0xff && node->alloc & (1ULL << idx)) {
		unlockLatch(p->slot->latch);
		p->slot = node->radix + idx;
		p->off++;
		return ContinueSearch;
	}

	// if room, add to radix node
	if (node->alloc < 0xffffffffffffffffULL) {
		idx = p->key[p->off++];
#ifdef _WIN32
		_BitScanForward64((DWORD *)&out, ~node->alloc);
#else
		out = __builtin_ctzl(~node->alloc);
#endif
		node->keys[idx] = out;

		p->slot = node->radix + out;

		if (!fillKey(p))
			return ErrorSearch;

		node->alloc |= 1ULL << out;
		return EndSearch;
	}

	// the radix node is full, promote to the next larger size.
	if ( (p->newSlot->bits = artAllocateNode(p->index, Array256, sizeof(ARTNode256))) )
		radix256Node = getObj(p->index->map,*p->newSlot);
	else
		return ErrorSearch;

	radix256Node->timestamp = node->timestamp;

	for (idx = 0; idx < 256; idx++) {
		if (node->keys[idx] < 0xff)
			if (node->alloc & (1ULL << node->keys[idx])) {
				DbAddr *slot = node->radix + node->keys[idx];
				lockLatch(slot->latch);
				if (!slot->dead) {
					radix256Node->alloc[idx / 64] |= 1ULL << (idx % 64);
					radix256Node->radix[idx].bits = slot->bits;  // copies mutex
					radix256Node->radix[idx].mutex = 0;
					kill_slot(slot->latch);
				}
				unlockLatch(slot->latch);
			}
	}

	// fill in the rest of the key bytes into Span nodes
	idx = p->key[p->off++];
	p->slot = radix256Node->radix + idx;
	radix256Node->alloc[idx / 64] |= 1ULL << (idx % 64);
	if (!fillKey(p))
		return ErrorSearch;
	return EndSearch;
}

ReturnState insertKeyNode256(ARTNode256 *node, ParamStruct *p) {
uint32_t idx = p->key[p->off];

	if (node->alloc[idx / 64] & (1ULL << (idx % 64))) {
		p->slot = node->radix + idx;
		p->off++;
		return ContinueSearch;
	}

	// obtain write lock on the node
	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.
	if (p->slot->dead) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->addr != p->newSlot->addr) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	//  retry under lock
	if (node->alloc[idx / 64] & (1ULL << (idx % 64))) {
		unlockLatch(p->slot->latch);
		p->slot = node->radix + idx;
		p->off++;
		return ContinueSearch;
	}

	p->off++;
	p->slot = node->radix + idx;
	node->alloc[idx / 64] |= 1ULL << (idx % 64);

	if (!fillKey(p))
		return ErrorSearch;

	return EndSearch;
}

ReturnState insertKeySpan(ARTSpan *node, ParamStruct *p) {
uint32_t len = p->newSlot->nbyte + 1;
uint32_t max = len, idx;
DbAddr *contSlot = NULL;
DbAddr *nxtSlot = NULL;
ARTNode4 *radix4Node;

	len += SPANLEN(p->newSlot->type);

	if (len > p->keylen - p->off)
		len = p->keylen - p->off;

	for (idx = 0; idx < len; idx++)
		if (p->key[p->off + idx] != node->bytes[idx])
			break;

	// did we use the entire span node exactly?

	if (idx == max) {
		p->off += idx;
		p->slot = node->next;
		return ContinueSearch;
	}

	// obtain write lock on the node

	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.

	if (p->slot->dead) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->addr != p->newSlot->addr) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	lockLatch(node->next->latch);

	if (node->next->dead) {
		unlockLatch(p->slot->latch);
		unlockLatch(node->next->latch);
		return RestartSearch;
	}

	p->off += idx;

	// copy matching prefix bytes to a new span node

	if (idx) {
		ARTSpan *spanNode2;

		if ((p->newSlot->bits = allocSpanNode(p, idx)))
			spanNode2 = getObj(p->index->map,*p->newSlot);
		else
			return ErrorSearch;

		memcpy(spanNode2->bytes, node->bytes, idx);
		spanNode2->timestamp = node->timestamp;
		p->newSlot->nbyte = idx - 1;
		nxtSlot = spanNode2->next;
		contSlot = nxtSlot;
	} else {
		// else replace the original span node with a radix4 node.
		// note that p->off < p->keylen, which will set contSlot
		nxtSlot = p->newSlot;
	}

	// if we have more key bytes, insert a radix node after span1 and before
	// possible
	// span2 for the next key byte and the next remaining original span byte (if
	// any).
	// note:  max > idx

	if (p->off < p->keylen) {
		if ( (nxtSlot->bits = artAllocateNode(p->index, Array4, sizeof(ARTNode4))) )
			radix4Node = getObj(p->index->map,*nxtSlot);
		else
			return ErrorSearch;

		// fill in first radix element with first of the remaining span bytes
		radix4Node->timestamp = node->timestamp;
		radix4Node->keys[0] = node->bytes[idx++];
		radix4Node->alloc |= 1;
		nxtSlot = radix4Node->radix + 0;

		// fill in second radix element with next byte of our search key
		radix4Node->keys[1] = p->key[p->off++];
		radix4Node->alloc |= 2;
		contSlot = radix4Node->radix + 1;
	}

	// place original span bytes remaining after the preceeding node
	// in a second span node after the radix or span node
	// i.e. fill in nxtSlot.

	if (max - idx) {
		ARTSpan *overflowSpanNode;

		if ((nxtSlot->bits = allocSpanNode(p, max - idx)))
			overflowSpanNode = getObj(p->index->map, *nxtSlot);
		else
			return ErrorSearch;

		memcpy(overflowSpanNode->bytes, node->bytes + idx, max - idx);
		overflowSpanNode->timestamp = node->timestamp;
		overflowSpanNode->next->bits = node->next->bits;
		overflowSpanNode->next->mutex = 0;

		// append second span node after span or radix node from above
		nxtSlot->nbyte = max - idx - 1;
	} else {
		// otherwise hook remainder of the trie into the
		// span or radix node's next slot (nxtSlot)
		nxtSlot->bits = node->next->bits;
		nxtSlot->mutex = 0;
	}

	kill_slot(node->next->latch);
	unlockLatch(node->next->latch);

	// fill in the rest of the key into the radix or overflow span node

	p->slot = contSlot;

	if (p->off < p->keylen)
		if (!fillKey(p))
			return ErrorSearch;

	return EndSearch;
}

