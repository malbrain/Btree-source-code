// reader-writer Phase-Fair FIFO lock -- type 1

typedef volatile union {
  struct {
	uint16_t writer[1];
	uint16_t reader[1];
  };
  uint32_t bits[1];
} Counter;
	
typedef struct {
  Counter requests[1];
  Counter completions[1];
} RWLock1;

#define RDINCR 0x10000

void writeLock1 (RWLock1 *lock);
void writeUnlock1 (RWLock1 *lock);
void readLock1 (RWLock1 *lock);
void readUnlock1 (RWLock1 *lock);

// reader-writer mutex lock (Neither FIFO nor Fair) -- type 2

#ifdef FUTEX
//	Mutex based reader-writer lock

typedef enum {
	FREE = 0,
	LOCKED,
	CONTESTED
} MutexState;

typedef struct {
	volatile MutexState state[1];
} Mutex;
#else
typedef volatile struct {
	char lock[1];
} Mutex;
#endif
void mutex_lock(Mutex* mutex);
void mutex_unlock(Mutex* mutex);

typedef struct {
  Mutex xcl[1];
  Mutex wrt[1];
  uint16_t readers[1];
} RWLock2;

void writeLock2 (RWLock2 *lock);
void writeUnlock2 (RWLock2 *lock);
void readLock2 (RWLock2 *lock);
void readUnlock2 (RWLock2 *lock);

// reader-writer Phase Fair/FIFO lock -- type 3

enum {
	QueRd = 1,	// reader queue
	QueWr = 2	// writer queue
} RWQueue;

typedef volatile union {
	struct {
	  uint16_t rin[1];
	  uint16_t rout[1];
	  uint16_t serving[1];
	  uint16_t ticket[1];
	};
	uint32_t rw[2];
} RWLock3;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4
