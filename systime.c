#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

void main (int argc, char **argv) {
int fd = open (argv[1], O_CREAT | O_RDWR, 0666);
int cnt = atoi(argv[2]), i, j;
int cell = atoi(argv[3]);
char *buff;

	buff = calloc (262144, 1);
	buff[cell] = 1;

	write (fd, buff, 262144);

	for (i = 0; i < cnt; i++) {
		char *map = mmap (NULL, 262144, PROT_READ, MAP_SHARED, fd, 0);

		if (map == MAP_FAILED) {
			printf("mmap failed, errno = %d\n", errno);
			exit(1);
		}

		if (!i)
		  for (j = 0; j < 262144; j++)
			if (map[j] )
				printf("Found idx = %d\n", j);

		munmap (map, 262144);
	}
}
