#include "../db.h"
#include "../db_object.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

//	initialize ARTree

Status artInit(Handle *hndl, Params *params) {

	hndl->map->arena->type[0] = ARTreeIndexType;
	return OK;
}

