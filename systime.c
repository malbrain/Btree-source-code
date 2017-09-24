#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/mman.h>

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

char usage[] = "usage: %s type filename reps megs [mem]\n"
	"	where type is:\n"
	"		c - use full file memory map\n"
	"		d - use disk read/writes for leaves\n"
	"	reps is the number of random pages to process\n"
	"	filename is the name of the test disk file\n"
	"	megs is the size of the test file in megabytes\n"
	"	mem is the limit of core used in megabytes\n\n";
	
int main (int argc, char **argv) {
uint64_t size = 1024LL * 1024LL, off, core = 0;
int fd = open (argv[2], O_CREAT | O_RDWR, 0666);
int cnt = atoi(argv[3]), i, j, k;
int scale = atoi(argv[4]);
char *buff, *map;
int sum = 0;

	if (argc < 2) {
		fprintf (stderr, usage, argv[0]);
		exit(1);
	}

	if (argc > 5)
		core = (uint64_t)atoi(argv[5]) * 1024LL * 1024LL;

	off = 0;
	size *= scale;
	buff = calloc (1024, 1024);

	if (lseek(fd, 0L, 2) < size)
	  while (off < size)
		pwrite (fd, buff, 1024 * 1024, off), off += 1024 * 1024;

	map = mmap (NULL, size, PROT_READ, MAP_SHARED, fd, 0);

	if (map == MAP_FAILED) {
		printf("core mmap failed, errno = %d\n", errno);
		exit(1);
	}

	madvise(map, core, MADV_WILLNEED);
	madvise(map + core, size - core, MADV_DONTNEED);

	for (i = 0; i < cnt; i++) {
		off = myrandom(size - 262144) & ~0xfffLL;

		// simulate in-memory operation on interior or leaf node

		if (off < core) {
		  for (j = 0; j < 32; j++)
			sum += map[off + myrandom(262144)];

		  continue;
		}

		// simulate leaf level on-disk operation

		switch(argv[1][0]) {
		case 'c':
			madvise(map + off, 262144, MADV_WILLNEED);

			for(k = 0; k < 400; k++)
			 for(j = 0; j < 32; j++)
			  sum += buff[myrandom(262144)];

			madvise(map + off, 262144, MADV_DONTNEED);
			break;

		case 'd':
			j = pread (fd, buff, 262144, off);

			if (j < 262144) {
			  printf("pread failed, errno = %d offset = %" PRIx64 "len = %d\n", errno, off, j);
			  exit(1);
			}

			for(k = 0; k < 400; k++)
			 for(j = 0; j < 32; j++)
			  sum += buff[myrandom(262144)];

			continue;
		}
	}

	printf("sum %d\n", sum);
	return 0;
}
