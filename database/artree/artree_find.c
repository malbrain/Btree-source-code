#include "../db.h"
#include "../db_object.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

bool artFindKey( DbCursor *dbCursor, DbMap *index, uint8_t *key, uint32_t keyLen) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
uint32_t idx, offset = 0, spanMax;
volatile DbAddr *slot;
CursorStack* stack;

	if (cursor) {
		cursor->atLeftEOF = false;
		cursor->base->keyLen = 0;
		cursor->depth = 0;
	}

	// loop through the key bytes

	slot = artIndexAddr(index)->root;

	while (offset < keyLen) {
		if (cursor)
		  if (cursor->depth < MAX_cursor)
			stack = cursor->stack + cursor->depth++;
		  else
			return false;

		if (cursor) {
			stack->off = cursor->base->keyLen;
			stack->slot->bits = slot->bits;;
			stack->ch = key[offset];
			stack->addr = slot;
		}

		switch (slot->type < SpanNode ? slot->type : SpanNode) {
		  case KeyPass: {
		   	ARTSplice* splice = getObj(index, *slot);

			slot = splice->next;

			if (cursor)
				stack->ch = 256;

			continue;
		  }

		  case KeyEnd: {
			memcpy (cursor->key, key, cursor->base->keyLen);
			return false;
		  }

		  case SpanNode: {
			ARTSpan* spanNode = getObj(index, *slot);
			uint32_t amt = keyLen - offset;
			int diff;

			spanMax = slot->nbyte + 1;

			if (amt > spanMax)
				amt = spanMax;

			diff = memcmp(key + offset, spanNode->bytes, amt);

			//  does the key end inside the span?

			if (spanMax > amt || diff) {
			  if (cursor) {
				if (diff <= 0)
					stack->ch = -1;
				else
					stack->ch = 256;
			  }

			  break;
			}

			//  continue to the next slot

			cursor->base->keyLen += spanMax;
			slot = spanNode->next;
			offset += spanMax;
			continue;
		  }

		  case Array4: {
			ARTNode4 *node = getObj(index, *slot);

			// simple loop comparing bytes

			for (idx = 0; idx < 4; idx++)
			  if (node->alloc & (1 << idx))
				if (key[offset] == node->keys[idx])
				  break;

			if (idx < 4) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case Array14: {
			ARTNode14 *node = getObj(index, *slot);

			// simple loop comparing bytes

			for (idx = 0; idx < 14; idx++)
			  if (node->alloc & (1 << idx))
				if (key[offset] == node->keys[idx])
				  break;

			if (idx < 14) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case Array64: {
			ARTNode64* node = getObj(index, *slot);
			idx = node->keys[key[offset]];

			if (idx < 0xff && (node->alloc & (1ULL << idx))) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case Array256: {
			ARTNode256* node = getObj(index, *slot);
			idx = key[offset];

			if (node->radix[idx].type) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case UnusedSlot: {
			cursor->atRightEOF = true;
			break;
		  }
		}  // end switch

		break;
	}  // end while (offset < keylen)

	memcpy (cursor->key, key, cursor->base->keyLen);

	if (slot->type == KeyEnd)
		return true;

	if (slot->type == KeyPass)
		return true;

	return false;
}
