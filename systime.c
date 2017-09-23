#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/mman.h>

unsigned int myrandom() {
	return rand() * RAND_MAX + rand();
}

int main (int argc, char **argv) {
int fd = open (argv[2], O_CREAT | O_RDWR, 0666);
uint64_t size = 1024LL * 1024LL * 1024LL, off;
int cnt = atoi(argv[3]), i, j;
int scale = atoi(argv[4]);
char *buff, *map;
int sum = 0;

	off = 0;
	size *= scale;
	buff = calloc (1024, 1024);

	if (lseek(fd, 0L, 2) < size)
	  while (off < size)
		pwrite (fd, buff, 1024 * 1024, off), off += 1024 * 1024;

	for (i = 0; i < cnt; i++) {
	  off = myrandom() % (size - 262144) & ~0xfff;

	  switch(argv[1][0]) {
	  case 'c':
		if (!i)
			map = mmap (NULL, size, PROT_READ, MAP_SHARED, fd, 0);

		if (map == MAP_FAILED) {
			printf("core mmap failed, errno = %d\n", errno);
			exit(1);
		}

		for (j = 0; j < 32; j++)
			sum += map[off + myrandom() % 262144];

		continue;

	  case 'm':
		map = mmap (NULL, 262144, PROT_READ, MAP_SHARED, fd, off);
		madvise(map, 262144, MADV_RANDOM);

		if (map == MAP_FAILED) {
			printf("mmap failed, errno = %d offset = %" PRIx64 "\n", errno, off);
			exit(1);
		}

		for (j = 0; j < 32; j++)
			sum += map[myrandom() % 262144];

		munmap (map, 262144);
		continue;

	  case 'd':
		j = pread (fd, buff, 262144, off);
		if (j < 262144) {
			printf("pread failed, errno = %d offset = %" PRIx64 "len = %d\n", errno, off, j);
			exit(1);
		}

		for(j = 0; j < 262144/32; j++)
			sum += buff[myrandom() % 262144];
		continue;
	  }
	}

	printf("sum %d\n", sum);
	return 0;
}
