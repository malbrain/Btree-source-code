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

char usage[] = "usage: %s type filename reps megs [upd]\n"
	"	where type is:\n"
	"		m - use full file memory map\n"
	"		d - use disk read/writes for leaves\n"
	"	reps is the number of random pages to simulate\n"
	"	filename is the name of the test disk file\n"
	"	megs is the size of the test file in megabytes\n"
	"	upd is number of buffered leaf updates\n\n";
	
int main (int argc, char **argv) {
int fd = open (argv[2], O_CREAT | O_RDWR, 0666);
uint64_t size = 1024LL * 1024LL, off;
int cnt = atoi(argv[3]), i, j, k;
int scale = atoi(argv[4]);
int upd = atoi(argv[5]);
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

	start[0] = getCpuTime(0);
	start[1] = getCpuTime(1);
	start[2] = getCpuTime(2);

	for (i = 0; i < cnt; i++) {
		off = myrandom(size - 262144) & ~0xfffLL;
		height = towerHeight(262144);


		// simulate operation on interior node
	
		if (i % upd) {
	  	  for (j = 0; j < height; j++)
			base[off + myrandom(262144)] += 1;

	  	  continue;
		}

		// simulate leaf level disk operation

		switch(argv[1][0]) {
		case 'm':
			madvise(map + off, 262144, MADV_WILLNEED);

			for(k = 0; k < upd; k++) {
			 height = towerHeight(262144);

			 for(j = 0; j < height; j++) {
			  uint32_t x = myrandom(262144);
			  map[off + x] = base[off + x];
			 }
			}

			madvise(map + off, 262144, MADV_DONTNEED);
			break;

		case 'd':
			j = pread (fd, page, 262144, off);

			if (j < 262144) {
			  printf("pread failed, errno = %d offset = %" PRIx64 " len = %d\n", errno, off, j);
			  exit(1);
			}

			for(k = 0; k < upd; k++) {
			 height = towerHeight(262144);

			 for(j = 0; j < height; j++) {
			  uint32_t x = myrandom(262144);
			  page[x] = base[off+ x];
			 }
			}

			j = pwrite (fd, page, 262144, off);

			if (j < 262144) {
			  printf("pwrite failed, errno = %d offset = %" PRIx64 " len = %d\n", errno, off, j);
			  exit(1);
			}

			continue;
		}
	}

	elapsed = getCpuTime(0) - start[0];
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1) - start[1];
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2) - start[2];
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

	return 0;
}
