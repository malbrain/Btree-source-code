#ifdef linux
#define _GNU_SOURCE
#endif

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <sched.h>

#define pause() asm volatile("pause\n": : : "memory")
#endif

#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

#ifdef _WIN32
HANDLE openPath(char *path, uint32_t segNo) {
int off = strlen(path);
int init = off;
HANDLE hndl;

	strcpy (path + off, "._seg000");
	off += 8;

	while (segNo) {
		path[--off] += segNo % 10;
		segNo /= 10;
	}

	hndl = CreateFile(path, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

	if (hndl == INVALID_HANDLE_VALUE) {
		fprintf(stderr, "Unable to create/open %s, error = %d\n", path, (int)GetLastError());
		return NULL;
	}

	path[init] = 0;
	return hndl;
}
#else
int openPath(char *path, uint32_t segNo) {
int hndl, flags;

	flags = O_RDWR | O_CREAT;

	hndl = open (path, flags, 0666);

	if (hndl == -1) {
		fprintf (stderr, "Unable to open/create %s, error = %d", path, errno);
		return -1;
	}

	return hndl;
}
#endif

void waitNonZero(volatile char *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void waitNonZero64(volatile uint64_t *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void lockLatch(volatile char* latch) {
#ifndef _WIN32
	while (__sync_fetch_and_or(latch, MUTEX_BIT) & MUTEX_BIT) {
#else
	while (_InterlockedOr8(latch, MUTEX_BIT) & MUTEX_BIT) {
#endif
		do
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
		while (*latch & MUTEX_BIT);
	}
}

void unlockLatch(volatile char* latch) {
#ifndef _WIN32
	__sync_fetch_and_and(latch, ~MUTEX_BIT);
#else
	_InterlockedAnd8( latch, ~MUTEX_BIT);
#endif
}

int64_t atomicAdd64(volatile int64_t *value, int64_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_add(value, amt) + amt;
#else
	return _InterlockedAdd64( value, amt);
#endif
}

int32_t atomicAdd32(volatile int32_t *value, int32_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_add(value, amt) + amt;
#else
	return _InterlockedAdd( (volatile long *)value, amt);
#endif
}

int64_t atomicOr64(volatile int64_t *value, int64_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_or (value, amt);
#else
	return _InterlockedOr64( value, amt);
#endif
}

int32_t atomicOr32(volatile int32_t *value, int32_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_or(value, amt);
#else
	return _InterlockedOr( (volatile long *)value, amt);
#endif
}

#ifdef _WIN32
void lockArena (HANDLE hndl, char *path) {
OVERLAPPED ovl[1];

	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = 0x80000000;

	if (LockFileEx (hndl, LOCKFILE_EXCLUSIVE_LOCK, 0, sizeof(DbArena), 0, ovl))
		return;

	fprintf (stderr, "Unable to lock %s, error = %d", path, (int)GetLastError());
	exit(1);
}
#else
void lockArena (int hndl, char *path) {
struct flock lock[1];

	memset (lock, 0, sizeof(lock));
	lock->l_len = sizeof(DbArena);
	lock->l_type = F_WRLCK;

	if (!fcntl(hndl, F_SETLKW, lock))
		return;

	fprintf (stderr, "Unable to lock %s, error = %d", path, errno);
	exit(1);
}
#endif

#ifdef _WIN32
void unlockArena (HANDLE hndl, char *path) {
OVERLAPPED ovl[1];

	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = 0x80000000;

	if (UnlockFileEx (hndl, 0, sizeof(DbArena), 0, ovl))
		return;

	fprintf (stderr, "Unable to unlock %s, error = %d", path, (int)GetLastError());
	exit(1);
}
#else
void unlockArena (int hndl, char *path) {
struct flock lock[1];

	memset (lock, 0, sizeof(lock));
	lock->l_len = sizeof(DbArena);
	lock->l_type = F_UNLCK;

	if (!fcntl(hndl, F_SETLKW, lock))
		return;

	fprintf (stderr, "Unable to unlock %s, error = %d", path, errno);
	exit(1);
}
#endif

void *mapMemory (DbMap *map, uint64_t offset, uint64_t size, uint32_t segNo) {
void *mem;

#ifndef _WIN32
int flags = MAP_SHARED;

	if( map->hndl[0] < 0 )
		flags |= MAP_ANON;

	mem = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, map->hndl[0], offset);

	if (mem == MAP_FAILED) {
		fprintf (stderr, "Unable to mmap %s, offset = %llx, error = %d", map->path, offset, errno);
		return NULL;
	}
#else
	if (!map->onDisk)
		return VirtualAlloc(NULL, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);

	map->hndl[segNo] = openPath(map->path, segNo);
	
	if (map->hndl[segNo] == INVALID_HANDLE_VALUE)
		return NULL;

	if (!(map->maphndl[segNo] = CreateFileMapping(map->hndl[segNo], NULL, PAGE_READWRITE, (DWORD)(size >> 32), (DWORD)(size), NULL))) {
		fprintf (stderr, "Unable to CreateFileMapping %s, size = %llx, segment = %d error = %d\n", map->path, size, segNo, (int)GetLastError());
		return NULL;
	}

	mem = MapViewOfFile(map->maphndl[segNo], FILE_MAP_WRITE, 0, 0, size);

	if (!mem) {
		fprintf (stderr, "Unable to CreateFileMapping %s, size = %llx, error = %d\n", map->path, size, (int)GetLastError());
		return NULL;
	}
#endif

	return mem;
}

void unmapSeg (DbMap *map, uint32_t segNo) {
#ifndef _WIN32
	munmap(map->base[segNo], map->arena->segs[segNo].size);
#else
	if (!map->onDisk) {
		VirtualFree(map->base[segNo], 0, MEM_RELEASE);
		return;
	}

	UnmapViewOfFile(map->base[segNo]);
	CloseHandle(map->maphndl[segNo]);
#endif
}

uint64_t compareAndSwap(uint64_t* target, uint64_t compare_val, uint64_t swap_val) {
#ifndef _WIN32
	return __sync_val_compare_and_swap(target, compare_val, swap_val);
#else
	return _InterlockedCompareExchange64((volatile __int64*)target, swap_val, compare_val);
#endif
}

void kill_slot(volatile char *latch) {
#ifndef _WIN32
	__sync_fetch_and_or(latch, DEAD_BIT);
#else
	_InterlockedOr8(latch, DEAD_BIT);
#endif
}

bool fileExists(char *path) {
#ifdef _WIN32
	int attr = GetFileAttributes(path);

	if( attr == 0xffffffff)
		return false;

	if (attr & FILE_ATTRIBUTE_DIRECTORY)
		return false;

	return true;
#else
	return !access(path, F_OK);
#endif
}
