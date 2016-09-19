#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

void addIdToTxn(DbMap *database, Txn *txn, ObjId docId, TxnCmd cmd) {

	docId.cmd = cmd;
	addSlotToFrame (database, txn->frame, docId.bits);
}

uint64_t beginTxn (Handle *database) {
ObjId txnId;
Txn *txn;

	txnId.bits = allocObjId(database->map, database->list, 0);
	txn = fetchIdSlot(database->map, txnId);
	txn->timestamp = allocateTimestamp(database->map, en_reader);
	return txnId.bits;
}

//  find appropriate document version per txn beginning timestamp

uint64_t findDocVer(DbMap *docStore, ObjId docId, Txn *txn) {
DbAddr *addr = fetchIdSlot(docStore, docId);
DbMap *db = docStore->db;
uint64_t txnTs;
Document *doc;
Txn *docTxn;

  //	examine prior versions

  while (addr->bits) {
	doc = getObj(docStore, *addr);

	// is this outside a txn? or
	// is version in same txn?

	if (!txn || doc->txnId.bits == txn->txnId.bits)
		return addr->bits;

	// is the version permanent?

	if (!doc->txnId.bits)
		return addr->bits;

	// is version committed before our txn began?

	if (doc->txnId.bits) {
		docTxn = fetchIdSlot(db, doc->txnId);

		if (isCommitted(docTxn->timestamp))
		  if (docTxn->timestamp < txn->timestamp)
			return addr->bits;
	}

	//	advance txn ts past doc version ts
	//	and move onto next doc version

	while (isReader((txnTs = txn->timestamp)) && txnTs < docTxn->timestamp)
		compareAndSwap(&txn->timestamp, txnTs, docTxn->timestamp);

	addr = doc->prevDoc;
  }

  return 0;
}

