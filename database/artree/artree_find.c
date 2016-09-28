#include "../db.h"
#include "../db_object.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

int slot4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys);
int slot64(int ch, uint64_t alloc, volatile uint8_t* keys);

DbAddr *artFindNxtFld( DbMap *index, ArtCursor *cursor, DbAddr *slot, uint8_t *key, uint32_t keylen) {
uint32_t startSize = cursor->keySize;
uint32_t startDepth = cursor->depth;
uint32_t idx, offset = 0, spanMax;
bool restart = true;
bool pass = false;
CursorStack* stack;
DbAddr newSlot[1];

	cursor->atLeftEOF = false;

	do {
		restart = false;
		cursor->keySize = startSize;
		cursor->depth = startDepth;

		//  if we are waiting on a dead bit to clear

		if (pass)
			yield();
		else
			pass = true;

		// loop through all the key bytes

		stack = cursor->stack + cursor->depth;

		while (offset < keylen) {
			if (slot->dead) {
				restart = true;
				break;
			}

			stack = &cursor->stack[cursor->depth++];
			stack->slot->bits = slot->bits;
			stack->addr = slot;
			stack->off = cursor->keySize;
			stack->ch = -1;

			newSlot->bits = slot->bits;
			spanMax = SPANLEN(slot->type);

			switch (slot->type < SpanNode ? slot->type : SpanNode) {
				case KeyEnd: {
					break;
				}

            	case SpanNode: {
                	ARTSpan* spanNode = getObj(index, *slot);
					uint32_t amt = keylen - offset;
					int diff;

                	spanMax += stack->slot->nbyte + 1;

					if (amt > spanMax)
						amt = spanMax;

					diff = memcmp(key + offset, spanNode->bytes, amt);

					//  is span size > key size?

					if (spanMax > amt) {
					  if (diff <= 0)
						stack->ch = -1;
					  else
						stack->ch = 256;

					  break;
					}

					//  does key end in the middle of the span?
					//  if so, is the key larger or smaller?

					if (diff) {
						stack->ch = diff < 0 ? -1 : 256;
						break;
					}

                	//  copy key bytes and continue to the next slot

                   	memcpy(cursor->key + cursor->keySize, spanNode->bytes, spanMax);
                   	cursor->keySize += spanMax;
					slot = spanNode->next;
					offset += spanMax;
					stack->ch = 0;
					continue;
            	}

				case Array4: {
					ARTNode4 *node = getObj(index, *slot);

					// simple loop comparing bytes

					for (idx = 0; idx < 4; idx++) {
						if (node->alloc & (1 << idx))
							if (key[offset] == node->keys[idx])
								break;
					}

					if (idx < 4) {
						slot = node->radix + idx;  // slot points to child node
						cursor->key[cursor->keySize++] = node->keys[idx];
						stack->ch = node->keys[idx];
						offset++;				// update key byte offset
						continue;
					}

					// key byte not found

					idx = slot4x14(stack->ch, 4, node->alloc, node->keys);

					if (idx < 4)
						stack->ch = node->keys[idx];
					else
						stack->ch = 256;

					break;
				}

				case Array14: {
					ARTNode14 *node = getObj(index, *slot);

					// simple loop comparing bytes

					for (idx = 0; idx < 14; idx++) {
						if (node->alloc & (1 << idx))
							if (key[offset] == node->keys[idx])
								break;
					}

					if (idx < 14) {
						slot = node->radix + idx;  // slot points to child node
						cursor->key[cursor->keySize++] = node->keys[idx];
						stack->ch = node->keys[idx];
						offset++;				// update key byte offset
						continue;
					}

					// key byte not found

					idx = slot4x14(stack->ch, 14, node->alloc, node->keys);

					if (idx < 14)
						stack->ch = node->keys[idx];
					else
						stack->ch = 256;

					break;
				}

				case Array64: {
					ARTNode64* node = getObj(index, *slot);
					idx = node->keys[key[offset]];

					if (idx < 0xff && (node->alloc & (1ULL << idx))) {
						slot = node->radix + idx;  // slot points to child node
						cursor->key[cursor->keySize++] = key[offset];
						stack->ch = key[offset];
						offset++;  // update key offset
						continue;
					}

					// key byte not found

					idx = slot64(stack->ch, node->alloc, node->keys);

					if (idx < 256)
						stack->ch = idx;
					else
						stack->ch = 256;

					break;
				}

				case Array256: {
					ARTNode256* node = getObj(index, *slot);
					idx = key[offset];

					if (node->alloc[idx / 64] & (1ULL << (idx % 64))) {
						cursor->key[cursor->keySize++] = idx;
						slot = node->radix + idx;  // slot points to child node
						stack->ch = idx;
						offset++;			// update key byte offset
						continue;
					}

					// key byte not found

					while (stack->ch < 256) {
						idx = ++stack->ch;
						if (idx < 256 && node->alloc[idx / 64] & (1ULL << (idx % 64)))
							break;
					}

					break;
				}

				case UnusedSlot: {
					cursor->depth--;
					if (!cursor->depth)
						cursor->atRightEOF = true;

					break;
				}
				default: {
					// unreachable
				}

			}  // end switch

			return slot;

		}  // end while (offset < keylen)

	} while (restart);

	return slot;
}
