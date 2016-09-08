#pragma once

//	red-black tree descent stack

#define RB_bits		24

typedef struct {
	uint64_t lvl;			// height of the stack
	DbAddr entry[RB_bits];	// stacked tree nodes
} PathStk;

typedef struct {
	uint32_t keyLen;		// length of key
	DbAddr left, right;		// next nodes down
	DbAddr addr;			// entry addr in parent
	char red;				// is tree node red?
	char key[0];			// entry key
} RedBlack;

