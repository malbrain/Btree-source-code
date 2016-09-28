#include "../db.h"
#include "../db_object.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

typedef enum {
	ContinueSearch,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

Status artDeleteKey(Handle *index, ArtCursor *cursor) {
DbAddr *slot, newSlot;
ReturnState rt;
uint32_t bit;
uint8_t ch;

	//	now that we have the trie nodes in the cursor stack
	//	we can go through them backwards to remove empties.

	while (cursor->depth) {
		CursorStack *stack = &cursor->stack[--cursor->depth];
		uint32_t pass = 0;
		bool retry = true;

		ch = stack->ch;
		slot = stack->addr;

		//	wait if we run into a dead slot
		do {
			if (pass)
				yield();
			else
				pass = 1;

			// obtain write lock on the node
			lockLatch(slot->latch);
			newSlot.bits = stack->addr->bits;

			if ((retry = newSlot.dead))
				unlockLatch(slot->latch);

		} while (retry);

		switch (newSlot.type < SpanNode ? newSlot.type : SpanNode) {
			case UnusedSlot: {
				rt = EndSearch;
				break;
			}
			case KeyEnd: {
				rt = EndSearch;
				break;
			}

			case SpanNode: {
				kill_slot(slot->latch);

				if (!addSlotToFrame(index->map, index->list[newSlot.type].tail, newSlot))
					rt = ErrorSearch;
				else
					rt = ContinueSearch;

				break;
			}
			case Array4: {
				ARTNode4 *node = getObj(index->map, *stack->addr);

				for (bit = 0; bit < 4; bit++) {
					if (node->alloc & (1 << bit))
						if (ch == node->keys[bit])
							break;
				}

				if (bit == 4) {
					rt = EndSearch;  // key byte not found
					break;
				}

				// we are not the last entry in the node?

				node->alloc &= ~(1 << bit);

				if (node->alloc) {
					rt = EndSearch;
					break;
				}

				kill_slot(slot->latch);

				if (!addSlotToFrame(index->map, index->list[newSlot.type].tail, newSlot)) {
					rt = ErrorSearch;
					break;
				}

				rt = ContinueSearch;
				break;
			}
			case Array14: {
				ARTNode14 *node = getObj(index->map, *stack->addr);

				for (bit = 0; bit < 14; bit++) {
					if (node->alloc & (1 << bit))
						if (ch == node->keys[bit])
							break;
				}

				if (bit == 14) {
					rt = EndSearch;  // key byte not found
					break;
				}

				// we are not the last entry in the node?

				node->alloc &= ~(1 << bit);

				if (node->alloc) {
					rt = EndSearch;
					break;
				}

				kill_slot(slot->latch);

				if (!addSlotToFrame(index->map, index->list[newSlot.type].tail, newSlot))
					rt = ErrorSearch;
				else
					rt = ContinueSearch;

				break;
			}

			case Array64: {
				ARTNode64 *node = getObj(index->map, *stack->addr);
				bit = node->keys[ch];

				if (bit == 0xff) {
					rt = EndSearch;
					break;
				}

				node->keys[ch] = 0xff;
				node->alloc &= ~(1ULL << bit);

				if (node->alloc) {
					rt = EndSearch;
					break;
				}

				kill_slot(slot->latch);

				if (!addSlotToFrame(index->map, index->list[newSlot.type].tail, newSlot))
					rt = ErrorSearch;
				else
					rt = ContinueSearch;

				break;
			}

			case Array256: {
				ARTNode256 *node = getObj(index->map, *stack->addr);
				bit = ch;

				if (~node->alloc[bit / 64] & (1ULL << (bit % 64))) {
					rt = EndSearch;
					break;
				}

				node->alloc[bit / 64] &= ~(1ULL << (bit % 64));

				if (node->alloc[0] | node->alloc[1] | node->alloc[2] | node->alloc[3]) {
					rt = EndSearch;
					break;
				}

				kill_slot(slot->latch);

				if (!addSlotToFrame(index->map, index->list[newSlot.type].tail, newSlot))
					rt = ErrorSearch;
				else
					rt = ContinueSearch;

				break;
			}
		}	// end switch

		unlockLatch(stack->addr->latch);

		if (rt == ContinueSearch)
			continue;

		break;

	}	// end while

	//	zero out root?

	if (!cursor->depth && rt == ContinueSearch)
		slot->bits = 0;

	atomicAdd64(artIndexAddr(index->map)->numEntries, -1);
	return OK;
}
