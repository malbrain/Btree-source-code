// multi-root-btree/btreeRoot.h

// the basic idea is to always have a possibly out-of-date read only root node
// that does not require read-locking the root.

// interface:

addRootEntry(MrbtHandle *mrbtHandle, uint8_t *key, uint16_t len);
delRootEntry(MrbtHandle *mrbtHandle, uint8_t *key, uint16_t len);

