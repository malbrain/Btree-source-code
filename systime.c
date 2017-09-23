#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

unsigned int myrandom() {
	return rand() * RAND_MAX + rand();
}

int main (int argc, char **argv) {
int fd = open (argv[2], O_CREAT | O_RDWR, 0666);
int size = 1024 * 1024 * 1024;
int cnt = atoi(argv[3]), i, j;
int cell = atoi(argv[4]);
char *buff, *map;
int sum = 0, off;

	buff = calloc (size/1024, 1);
	buff[cell] = 1;

	if (lseek(fd, 0L, 2) < size)
	  for (i=0; i < 1024; i++)
		pwrite (fd, buff, size/1024, i * 1024 * 1024);

	for (i = 0; i < cnt; i++) {
	  off = myrandom() % (size - 262144) & ~0xfff;

	  switch(argv[1][0]) {
	  case 'c':
		if (!i)
			map = mmap (NULL, size, PROT_READ, MAP_SHARED, fd, 0);

		for (j = 0; j < 32; j++)
			sum += map[off + myrandom() % 262144];

		continue;

	  case 'm':
		map = mmap (NULL, 262144, PROT_READ, MAP_SHARED, fd, off);
		madvise(map, 262144, MADV_RANDOM);

		if (map == MAP_FAILED) {
			printf("mmap failed, errno = %d offset = %x\n", errno, off);
			exit(1);
		}

		for (j = 0; j < 32; j++)
			sum += map[myrandom() % 262144];

		munmap (map, 262144);
		continue;

	  case 'd':
		j = pread (fd, buff, 262144, off);
		if (j < 262144) {
			printf("pread failed, errno = %d offset = %x len = %d\n", errno, off, j);
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
