#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

void main (int argc, char **argv) {
int fd = open (argv[2], O_CREAT | O_RDWR, 0666);
int cnt = atoi(argv[3]), i, j;
int cell = atoi(argv[4]);
char *buff, *map;
int sum = 0, off;

	buff = calloc (1024 * 1024, 1);
	buff[cell] = 1;

	write (fd, buff, 1024 * 1024);

	for (i = 0; i < cnt; i++) {
	  off = random() % (1024 * 1024 - 262144) & ~0xfff;

	  switch(argv[1][0]) {
	  case 'm':
		map = mmap (NULL, 262144, PROT_READ, MAP_SHARED, fd, off);

		if (map == MAP_FAILED) {
			printf("mmap failed, errno = %d offset = %x\n", errno, off);
			exit(1);
		}

		sum += map[random() % 262144];

		if (!i)
		  for (j = 0; j < 262144; j++)
			if (map[j] )
				printf("Found idx = %d\n", j);

		munmap (map, 262144);
		continue;

	  case 'd':
		pread (fd, buff, 262144, off);
		sum += buff[random() % 262144];
		continue;
	  }
	}

	printf("sum %d\n", sum);
}
