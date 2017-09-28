#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>

#ifdef _WIN32
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
#include <unistd.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <sys/time.h>

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

uint64_t myrandom(uint64_t modulo) {
uint64_t ans = 0;

	ans |= rand() % 32768;
	ans <<= 15;
	ans |= rand() % 32768;

	if (modulo >> 30) {
		ans <<= 15;
		ans |= rand() % 32768;
	}

	return ans % modulo;
}

int towerHeight(uint32_t range) {
uint32_t value = myrandom(range);
int height = 1;

	while(range >>= 1)
	  if(value < range)
		height++;
	  else
		break;

	return height;
}

typedef struct {
	int fd, idx, cnt, upd;
	char *base, *map, type;
	uint64_t size;
} ThreadArg;

#ifndef _WIN32
void *execround (void *vals) {
#else
uint32_t __stdcall execround (void *vals) {
#endif
ThreadArg *args = vals;
int height, i, j, k;
char *page;
off_t off;

	page = malloc(262144);

	for (i = 0; i < args->cnt; i++) {
		off = myrandom(args->size - 262144) & ~0xfffLL;
		height = towerHeight(262144);

		// simulate operation on interior node
	
		if (i % args->upd) {
	  	  for (j = 0; j < height; j++)
			args->base[off + myrandom(262144)] += 1;

	  	  continue;
		}

		// simulate leaf level disk operation

		switch(args->type) {
		case 'm':
			madvise(args->map + off, 262144, MADV_WILLNEED);

			for(k = 0; k < args->upd; k++) {
			 height = towerHeight(262144);

			 for(j = 0; j < height; j++) {
			  uint32_t x = myrandom(262144);
			  args->map[off + x] = args->base[off + x];
			 }
			}

			madvise(args->map + off, 262144, MADV_DONTNEED);
			break;

		case 'd':
			j = pread (args->fd, page, 262144, off);

			if (j < 262144) {
			  printf("pread failed, errno = %d offset = %" PRIx64 " len = %d\n", errno, off, j);
			  exit(1);
			}

			for(k = 0; k < args->upd; k++) {
			 height = towerHeight(262144);

			 for(j = 0; j < height; j++) {
			  uint32_t x = myrandom(262144);
			  page[x] = args->base[off+ x];
			 }
			}

			j = pwrite (args->fd, page, 262144, off);

			if (j < 262144) {
			  printf("pwrite failed, errno = %d offset = %" PRIx64 " len = %d\n", errno, off, j);
			  exit(1);
			}

			continue;
		}
	}
}

char usage[] = "usage: %s type filename reps megs upd thrds\n"
	"	where type is:\n"
	"		m - use full file memory map\n"
	"		d - use disk read/writes for leaves\n"
	"	reps is the number of random pages to simulate\n"
	"	filename is the name of the test disk file\n"
	"	megs is the size of the test file in megabytes\n"
	"	upd is number of buffered leaf updates\n"
	"	thrds is the number of threads to fire\n\n";
	
int main (int argc, char **argv) {
int fd = open (argv[2], O_CREAT | O_RDWR, 0666);
uint64_t size = 1024LL * 1024LL, off;
int cnt = atoi(argv[3]), idx, err;
int scale = atoi(argv[4]);
int upd = atoi(argv[5]);
int nthrds = atoi(argv[6]);
#ifdef unix
pthread_t *threads;
#else
HANDLE *threads;
#endif
ThreadArg *args;
char page[262144];
char *base, *map;
double start[3];
float elapsed;
int height;

	if (argc < 2) {
		fprintf (stderr, usage, argv[0]);
		exit(1);
	}

	//	store interior nodes in first segment

	off = 0;
	size *= scale;

	if (lseek(fd, 0L, 2) < size)
	  while(off < size)
		pwrite (fd, page, 262144, off), off += 262144; 

	base = mmap (NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	off = size;

	//	store leaf nodes in second segment

	if (lseek(fd, 0L, 2) < 2 * size)
	  while (off < 2 * size)
		pwrite (fd, page, 262144, off), off += 262144; 

	switch(argv[1][0]) {
	  case 'm':
		map = mmap (NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, size);

		if (map == MAP_FAILED) {
			printf("mmap failed, errno = %d\n", errno);
			exit(1);
		}

		break;

	  case 'd':
		break;

	  default:
		printf("invalid simulation type: %c\n\n", argv[1][0]);
		fprintf (stderr, usage, argv[0]);
		exit(1);
	}

#ifdef unix
	threads = malloc (nthrds * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, nthrds * sizeof(HANDLE));
#endif
	args = malloc (nthrds * sizeof(ThreadArg));

	//	fire off threads

	for( idx = 0; idx < nthrds; idx++ ) {
		args[idx].type = argv[1][0];
		args[idx].size = size;
		args[idx].base = base;
		args[idx].map = map;
		args[idx].idx = idx;
		args[idx].upd = upd;
		args[idx].cnt = cnt;
		args[idx].fd = fd;
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, execround, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
#else
		threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, execround, args + idx, 0, NULL);
#endif
	}

	// 	wait for termination

#ifdef unix
	for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);
#else
	WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	for( idx = 0; idx < cnt; idx++ )
		CloseHandle(threads[idx]);

#endif
	start[0] = getCpuTime(0);
	start[1] = getCpuTime(1);
	start[2] = getCpuTime(2);

	elapsed = getCpuTime(0) - start[0];
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1) - start[1];
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2) - start[2];
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

	return 0;
}
