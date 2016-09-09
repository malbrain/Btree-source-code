//  a phase fair reader/writer lock implementation, version 2
//	by Karl Malbrain, malbrain@cal.berkeley.edu
//	20 JUN 2016

#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <memory.h>
#include <errno.h>

#include "db_lock.h"

#ifndef _WIN32
#include <sched.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#endif

#ifdef FUTEX
#include <linux/futex.h>
#define SYS_futex 202

int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}
#endif

#ifdef unix
#define pause() asm volatile("pause\n": : : "memory")

void lock_sleep (int cnt) {
struct timespec ts[1];

	ts->tv_sec = 0;
	ts->tv_nsec = cnt;
	nanosleep(ts, NULL);
}

int lock_spin (int *cnt) {
volatile int idx;

	if (!*cnt)
	  *cnt = 8;
	else if (*cnt < 1024 * 1024)
	  *cnt += *cnt / 4;

	if (*cnt < 1024 )
	  for (idx = 0; idx < *cnt; idx++)
		pause();
	else
		return 1;

	return 0;
}
#else

void lock_sleep (int ticks) {
LARGE_INTEGER start[1], freq[1], next[1];
int idx, interval;
double conv;

	QueryPerformanceFrequency(freq);
	QueryPerformanceCounter(next);
	conv = (double)freq->QuadPart / 1000000000; 

	for (idx = 0; idx < ticks; idx += interval) {
		*start = *next;
		Sleep(0);
		QueryPerformanceCounter(next);
		interval = (next->QuadPart - start->QuadPart) / conv;
	}
}

int lock_spin (uint32_t *cnt) {
volatile int idx;

	if (!*cnt)
	  *cnt = 8;

	if (*cnt < 1024 * 1024)
	  *cnt += *cnt / 4;

	if (*cnt < 1024 )
	  for (idx = 0; idx < *cnt; idx++)
		YieldProcessor();
 	else
 		return 1;

	return 0;
}
#endif

//	mutex implementation

#ifndef FUTEX
#ifdef unix
void mutex_lock(Mutex* mutex) {
uint32_t spinCount = 0;
uint32_t prev;

  while (__sync_fetch_and_or(mutex->lock, 1) & 1)
	while (*mutex->lock)
	  if (lock_spin (&spinCount))
		lock_sleep(spinCount);
}

void mutex_unlock(Mutex* mutex) {
	asm volatile ("" ::: "memory");
	*mutex->lock = 0;
}
#else
void mutex_lock(Mutex* mutex) {
uint32_t spinCount = 0;

  while (_InterlockedOr8(mutex->lock, 1) & 1)
	while (*mutex->lock & 1)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void mutex_unlock(Mutex* mutex) {
	*mutex->lock = 0;
}
#endif
#else
void mutex_lock(Mutex *mutex) {
MutexState nxt =  LOCKED;
uint32_t spinCount = 0;

  while (__sync_val_compare_and_swap(mutex->state, FREE, nxt) != FREE)
	while (*mutex->state != FREE)
	  if (lock_spin (&spinCount)) {
		if (*mutex->state == LOCKED)
    	  if (__sync_val_compare_and_swap(mutex->state, LOCKED, CONTESTED) == FREE)
			break;

		sys_futex((void *)mutex->state, FUTEX_WAIT, CONTESTED, NULL, NULL, 0);
		nxt = CONTESTED;
		break;
	  }
}

void mutex_unlock(Mutex* mutex) {
	if (__sync_fetch_and_sub(mutex->state, 1) == CONTESTED)  {
   		*mutex->state = FREE;
 		sys_futex( (void *)mutex->state, FUTEX_WAKE, 1, NULL, NULL, 0);
   }
}
#endif

#ifdef FUTEX
//  a phase fair reader/writer lock implementation

void WriteLock3(RWLock3 *lock)
{
uint32_t spinCount = 0;
uint16_t w, r, tix;
uint32_t prev;

	tix = __sync_fetch_and_add (lock->ticket, 1);

	// wait for our ticket to come up in serving

	while( 1 ) {
		prev = lock->rw[1];
		if( tix == (uint16_t)prev )
		  break;

		// add ourselves to the waiting for write ticket queue

		sys_futex( (void *)&lock->rw[1], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueWr );
	}

	// wait for existing readers to drain while allowing new readers queue

	w = PRES | (tix & PHID);
	r = __sync_fetch_and_add (lock->rin, w);

	while( 1 ) {
		prev = lock->rw[0];
		if( r == (uint16_t)(prev >> 16))
		  break;

		// we're the only writer waiting on the readers ticket number

		sys_futex( (void *)&lock->rw[0], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueWr );
	}
}

void WriteUnlock3 (RWLock3 *lock)
{
	// clear writer waiting and phase bit
	//	and advance writer ticket

	__sync_fetch_and_and (lock->rin, ~MASK);
	lock->serving[0]++;

	if( (*lock->rin & ~MASK) != (*lock->rout & ~MASK) )
	  if( sys_futex( (void *)&lock->rw[0], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueRd ) )
		return;

	//  is writer waiting (holding a ticket)?

	if( *lock->ticket == *lock->serving )
		return;

	//	are rest of writers waiting for this writer to clear?
	//	(have to wake all of them so ticket holder can proceed.)

	sys_futex( (void *)&lock->rw[1], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
}

void ReadLock3 (RWLock3 *lock)
{
uint32_t spinCount = 0;
uint32_t prev;
uint16_t w;

	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;

	if( w )
	  while( 1 ) {
		prev = lock->rw[0];
		if( w != (prev & MASK))
		  break;
		sys_futex( (void *)&lock->rw[0], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueRd );
	  }
}

void ReadUnlock3 (RWLock3 *lock)
{
	__sync_fetch_and_add (lock->rout, RINC);

	// is a writer waiting for this reader to finish?

	if( *lock->rin & PRES )
	  sys_futex( (void *)&lock->rw[0], FUTEX_WAKE_BITSET, 1, NULL, NULL, QueWr );

	// is a writer waiting for reader cycle to finish?

	else if( *lock->ticket != *lock->serving )
	  sys_futex( (void *)&lock->rw[1], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
}
#endif
//	simple Phase-Fair FIFO rwlock

void WriteLock1 (RWLock1 *lock)
{
Counter prev[1], next[1];
uint32_t spinCount = 0;

	do {
	  *prev->bits = *lock->requests->bits;
	  *next->bits = *prev->bits;
	  next->writer[0]++;
# ifndef _WIN32
	} while (!__sync_bool_compare_and_swap(lock->requests->bits, *prev->bits, *next->bits));
# else
	} while (_InterlockedCompareExchange(lock->requests->bits, *next->bits, *prev->bits) != *prev->bits);
# endif

	while (lock->completions->bits[0] != prev->bits[0])
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void WriteUnlock1 (RWLock1 *lock)
{
# ifndef _WIN32
	__sync_fetch_and_add (lock->completions->writer, 1);
# else
	_InterlockedExchangeAdd16(lock->completions->writer, 1);
# endif
}

void ReadLock1 (RWLock1 *lock)
{
uint32_t spinCount = 0;
Counter prev[1];

# ifndef _WIN32
	*prev->bits = __sync_fetch_and_add (lock->requests->bits, RDINCR);
# else
	*prev->bits =_InterlockedExchangeAdd(lock->requests->bits, RDINCR);
# endif
	
	while (*lock->completions->writer != *prev->writer)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void ReadUnlock1 (RWLock1 *lock)
{
# ifndef _WIN32
	__sync_fetch_and_add (lock->completions->reader, 1);
# else
	_InterlockedExchangeAdd16(lock->completions->reader, 1);
# endif
}

//	mutex based reader-writer lock

void WriteLock2 (RWLock2 *lock)
{
	mutex_lock(lock->xcl);
	mutex_lock(lock->wrt);
	mutex_unlock(lock->xcl);
}

void WriteUnlock2 (RWLock2 *lock)
{
	mutex_unlock(lock->wrt);
}

void ReadLock2 (RWLock2 *lock)
{
	mutex_lock(lock->xcl);
#ifdef unix
	if( !__sync_fetch_and_add (lock->readers, 1) )
#else
	if( !(_InterlockedIncrement16 (lock->readers)-1) )
#endif
		mutex_lock(lock->wrt);

	mutex_unlock(lock->xcl);
}

void ReadUnlock2 (RWLock2 *lock)
{
#ifdef unix
	if( !__sync_sub_and_fetch (lock->readers, 1) )
#else
	if( !_InterlockedDecrement16 (lock->readers) )
#endif
		mutex_unlock(lock->wrt);
}

#ifndef FUTEX
void WriteLock3 (RWLock3 *lock)
{
uint32_t spinCount = 0;
uint16_t w, r, tix;

#ifdef unix
	tix = __sync_fetch_and_add (lock->ticket, 1);
#else
	tix = _InterlockedExchangeAdd16 (lock->ticket, 1);
#endif
	// wait for our ticket to come up

	while( tix != lock->serving[0] )
	  if (lock_spin(&spinCount))
	    lock_sleep (spinCount);

	//	add the writer present bit and tix phase

	spinCount = 0;
	w = PRES | (tix & PHID);
#ifdef  unix
	r = __sync_fetch_and_add (lock->rin, w);
#else
	r = _InterlockedExchangeAdd16 (lock->rin, w);
#endif

	while( r != *lock->rout )
	  if (lock_spin(&spinCount))
		lock_sleep (spinCount);
}

void WriteUnlock3 (RWLock3 *lock)
{
#ifdef unix
	__sync_fetch_and_and (lock->rin, ~MASK);
#else
	_InterlockedAnd16 (lock->rin, ~MASK);
#endif
	lock->serving[0]++;
}

void ReadLock3 (RWLock3 *lock)
{
uint32_t spinCount = 0;
uint16_t w;

#ifdef unix
	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;
#else
	w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
#endif
	if( w )
	  while( w == (*lock->rin & MASK) )
	   if (lock_spin(&spinCount))
		lock_sleep (spinCount);
}

void ReadUnlock3 (RWLock3 *lock)
{
#ifdef unix
	__sync_fetch_and_add (lock->rout, RINC);
#else
	_InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}
#endif

#ifdef STANDALONE
#include <stdio.h>

#ifndef unix
double getCpuTime(int type)
{
FILETIME crtime[1];
FILETIME xittime[1];
FILETIME systime[1];
FILETIME usrtime[1];
SYSTEMTIME timeconv[1];
double ans = 0;

	memset (timeconv, 0, sizeof(SYSTEMTIME));

	switch( type ) {
	case 0:
		GetSystemTimeAsFileTime (xittime);
		FileTimeToSystemTime (xittime, timeconv);
		ans = (double)timeconv->wDayOfWeek * 3600 * 24;
		break;
	case 1:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (usrtime, timeconv);
		break;
	case 2:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (systime, timeconv);
		break;
	}

	ans += (double)timeconv->wHour * 3600;
	ans += (double)timeconv->wMinute * 60;
	ans += (double)timeconv->wSecond;
	ans += (double)timeconv->wMilliseconds / 1000;
	return ans;
}
#else
#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
struct rusage used[1];
struct timeval tv[1];

	switch( type ) {
	case 0:
		gettimeofday(tv, NULL);
		return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

	case 1:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

	case 2:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
	}

	return 0;
}
#endif

#ifdef unix
unsigned char Array[256] __attribute__((aligned(64)));
#include <pthread.h>
pthread_rwlock_t lock0[1] = {PTHREAD_RWLOCK_INITIALIZER};
#else
__declspec(align(64)) unsigned char Array[256];
SRWLOCK lock0[1] = {SRWLOCK_INIT};
#endif
RWLock1 lock1[1];
RWLock2 lock2[1];
RWLock3 lock3[1];

enum {
	systemType,
	RW1Type,
	RW2Type,
	RW3Type
} LockType;

typedef struct {
	int threadCnt;
	int threadNo;
	int loops;
	int type;
} Arg;

void work (int usecs, int shuffle) {
volatile int cnt = usecs * 300;
int first, idx;

	while(shuffle && usecs--) {
	  first = Array[0];
	  for (idx = 0; idx < 255; idx++)
		Array[idx] = Array[idx + 1];

	  Array[255] = first;
	}

	while (cnt--)
#ifdef unix
		__sync_fetch_and_add(&usecs, 1);
#else
		_InterlockedIncrement(&usecs);
#endif
}

#ifdef _WIN32
void __cdecl launch(Arg *arg) {
#else
void *launch(void *vals) {
Arg *arg = (Arg *)vals;
#endif
int idx;

	for( idx = 0; idx < arg->loops; idx++ ) {
	  if (arg->type == systemType)
#ifdef unix
		pthread_rwlock_rdlock(lock0), work(1, 0), pthread_rwlock_unlock(lock0);
#else
		AcquireSRWLockShared(lock0), work(1, 0), ReleaseSRWLockShared(lock0);
#endif
	  else if (arg->type == RW1Type)
		ReadLock1(lock1), work(1, 0), ReadUnlock1(lock1);
	  else if (arg->type == RW2Type)
		ReadLock2(lock2), work(1, 0), ReadUnlock2(lock2);
	  else if (arg->type == RW3Type)
		ReadLock3(lock3), work(1, 0), ReadUnlock3(lock3);
	  else
		work(1,0);
	  if( (idx & 511) == 0)
	    if (arg->type == systemType)
#ifdef unix
		  pthread_rwlock_wrlock(lock0), work(10, 1), pthread_rwlock_unlock(lock0);
#else
		  AcquireSRWLockExclusive(lock0), work(10, 1), ReleaseSRWLockExclusive(lock0);
#endif
	  	else if (arg->type == RW1Type)
		  WriteLock1(lock1), work(10, 1), WriteUnlock1(lock1);
	  	else if (arg->type == RW2Type)
		  WriteLock2(lock2), work(10, 1), WriteUnlock2(lock2);
	  	else if (arg->type == RW3Type)
		  WriteLock3(lock3), work(10, 1), WriteUnlock3(lock3);
		else
		  work(10,1);
#ifdef DEBUG
	  if (arg->type >= 0)
	   if (!(idx % 100000))
		fprintf(stderr, "Thread %d loop %d\n", arg->threadNo, idx);
#endif
	}

#ifdef DEBUG
	if (arg->type >= 0)
		fprintf(stderr, "Thread %d finished\n", arg->threadNo);
#endif
#ifndef _WIN32
	return NULL;
#endif
}

int main (int argc, char **argv)
{
double start, elapsed, overhead[3];
int threadCnt, idx;
Arg *args, base[1];

#ifdef unix
pthread_t *threads;
#else
DWORD thread_id[1];
HANDLE *threads;
#endif

	if (argc < 2) {
		fprintf(stderr, "Usage: %s #thrds lockType\n", argv[0]); 
		printf("sizeof RWLock0: %d\n", (int)sizeof(lock0));
		printf("sizeof RWLock1: %d\n", (int)sizeof(lock1));
		printf("sizeof RWLock2: %d\n", (int)sizeof(lock2));
		printf("sizeof RWLock3: %d\n", (int)sizeof(lock3));
		exit(1);
	}

	for (idx = 0; idx < 256; idx++)
		Array[idx] = idx;

	//	calculate non-lock timing

	base->loops = 1000000;
	base->threadCnt = 1;
	base->threadNo = 0;
	base->type = -1;

	start = getCpuTime(0);
	launch(base);

	overhead[0] = getCpuTime(0) - start;
	overhead[1] = getCpuTime(1);
	overhead[2] = getCpuTime(2);

	threadCnt = atoi(argv[1]);
	LockType = atoi(argv[2]);

	args = calloc(threadCnt, sizeof(Arg));

#ifdef unix
	threads = malloc (threadCnt * sizeof(pthread_t));
#else
	threads = malloc (threadCnt * sizeof(HANDLE));
#endif
	for (idx = 0; idx < threadCnt; idx++) {
	  args[idx].loops = 1000000 / threadCnt;
	  args[idx].threadCnt = threadCnt;
	  args[idx].threadNo = idx;
	  args[idx].type = LockType;
#ifdef _WIN32
	  while ( (int64_t)(threads[idx] = (HANDLE)_beginthread(launch, 65536, args + idx)) < 0LL )
		fprintf(stderr, "Unable to create thread %d, errno = %d\n", idx, errno);
#else
	  if (pthread_create(threads + idx, NULL, launch, (void *)(args + idx)))
		fprintf(stderr, "Unable to create thread %d, errno = %d\n", idx, errno);

#endif
	}

	// 	wait for termination

#ifdef unix
	for (idx = 0; idx < threadCnt; idx++)
		pthread_join (threads[idx], NULL);
#else

	for (idx = 0; idx < threadCnt; idx++) {
		WaitForSingleObject (threads[idx], INFINITE);
		CloseHandle(threads[idx]);
	}
#endif
	for( idx = 0; idx < 256; idx++)
	  if (Array[idx] != (unsigned char)(Array[(idx+1) % 256] - 1))
		fprintf (stderr, "Array out of order\n");

	elapsed = getCpuTime(0) - start - overhead[0];
	if (elapsed < 0)
		elapsed = 0;
	fprintf(stderr, " real %.3fus\n", elapsed);
	elapsed = getCpuTime(1) - overhead[1];
	if (elapsed < 0)
		elapsed = 0;
	fprintf(stderr, " user %.3fus\n", elapsed);
	elapsed = getCpuTime(2) - overhead[2];
	if (elapsed < 0)
		elapsed = 0;
	fprintf(stderr, " sys  %.3fus\n", elapsed);
	fprintf(stderr, " nanosleeps %d\n", NanoCnt[0]);
}
#endif

