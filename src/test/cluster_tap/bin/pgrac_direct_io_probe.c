#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum
{
	PROBE_OK = 0,
	PROBE_ARGUMENT = 2,
	PROBE_OPEN = 3,
	PROBE_WRITE = 4,
	PROBE_SYNC = 5,
	PROBE_READ = 6,
	PROBE_COMPARE = 7,
	PROBE_MEMORY = 8
};

static uint32_t
crc32c(const unsigned char *data, size_t length)
{
	uint32_t crc = UINT32_C(0xffffffff);

	for (size_t i = 0; i < length; i++)
	{
		crc ^= data[i];
		for (unsigned bit = 0; bit < 8; bit++)
			crc = (crc >> 1) ^ ((crc & 1) ? UINT32_C(0x82f63b78) : 0);
	}
	return crc ^ UINT32_C(0xffffffff);
}

static uint64_t
parse_u64(const char *value, const char *name)
{
	char *end = NULL;
	unsigned long long parsed;

	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0')
	{
		fprintf(stderr, "invalid %s: %s\n", name, value);
		exit(PROBE_ARGUMENT);
	}
	return (uint64_t) parsed;
}

static void
fill_pattern(unsigned char *buffer, size_t length, const char *attempt,
	uint64_t node, uint64_t sequence, uint64_t offset)
{
	uint64_t state = UINT64_C(1469598103934665603);
	int header;
	uint32_t crc;

	for (const unsigned char *p = (const unsigned char *) attempt; *p; p++)
		state = (state ^ *p) * UINT64_C(1099511628211);
	state ^= node + UINT64_C(0x9e3779b97f4a7c15);
	state ^= sequence << 17;
	state ^= offset << 1;
	for (size_t i = 0; i < length - sizeof(crc); i++)
	{
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		buffer[i] = (unsigned char) state;
	}
	header = snprintf((char *) buffer, length - sizeof(crc),
		"PGRAC-DIO|attempt=%s|node=%" PRIu64 "|sequence=%" PRIu64
		"|offset=%" PRIu64 "|", attempt, node, sequence, offset);
	if (header < 0 || (size_t) header >= length - sizeof(crc))
	{
		fprintf(stderr, "probe pattern header exceeds region\n");
		exit(PROBE_ARGUMENT);
	}
	crc = crc32c(buffer, length - sizeof(crc));
	memcpy(buffer + length - sizeof(crc), &crc, sizeof(crc));
}

static int
full_pattern_valid(const unsigned char *buffer, size_t length)
{
	uint32_t stored;

	memcpy(&stored, buffer + length - sizeof(stored), sizeof(stored));
	return stored == crc32c(buffer, length - sizeof(stored));
}

int
main(int argc, char **argv)
{
	const char *device = NULL;
	const char *attempt = NULL;
	uint64_t offset = 0;
	uint64_t length64 = 0;
	uint64_t node = UINT64_MAX;
	uint64_t sequences = 0;
	unsigned char *expected = NULL;
	unsigned char *observed = NULL;
	size_t length;
	long page_size;
	size_t alignment;

	for (int i = 1; i < argc; i += 2)
	{
		if (i + 1 >= argc)
			return PROBE_ARGUMENT;
		if (strcmp(argv[i], "--device") == 0)
			device = argv[i + 1];
		else if (strcmp(argv[i], "--offset") == 0)
			offset = parse_u64(argv[i + 1], "offset");
		else if (strcmp(argv[i], "--length") == 0)
			length64 = parse_u64(argv[i + 1], "length");
		else if (strcmp(argv[i], "--node") == 0)
			node = parse_u64(argv[i + 1], "node");
		else if (strcmp(argv[i], "--attempt") == 0)
			attempt = argv[i + 1];
		else if (strcmp(argv[i], "--sequences") == 0)
			sequences = parse_u64(argv[i + 1], "sequences");
		else
			return PROBE_ARGUMENT;
	}
	if (device == NULL || attempt == NULL || *attempt == '\0'
		|| length64 < 4096 || length64 > SIZE_MAX || node >= 4
		|| sequences != 16 || offset % 512 != 0 || length64 % 512 != 0)
		return PROBE_ARGUMENT;
	for (const char *p = attempt; *p; p++)
		if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')))
			return PROBE_ARGUMENT;

	length = (size_t) length64;
	page_size = sysconf(_SC_PAGESIZE);
	alignment = page_size > 4096 ? (size_t) page_size : 4096;
	if (posix_memalign((void **) &expected, alignment, length) != 0
		|| posix_memalign((void **) &observed, alignment, length) != 0)
		return PROBE_MEMORY;

	for (uint64_t sequence = 1; sequence <= sequences; sequence++)
	{
		int fd;
		ssize_t written;
		ssize_t read_bytes;

		fill_pattern(expected, length, attempt, node, sequence, offset);
		memset(observed, 0, length);
		fd = open(device, O_DIRECT | O_RDWR | O_CLOEXEC);
		if (fd < 0)
			return PROBE_OPEN;
		written = pwrite(fd, expected, length, (off_t) offset);
		if (written != (ssize_t) length)
		{
			close(fd);
			return PROBE_WRITE;
		}
		if (fdatasync(fd) != 0)
		{
			close(fd);
			return PROBE_SYNC;
		}
		if (close(fd) != 0)
			return PROBE_SYNC;

		fd = open(device, O_DIRECT | O_RDWR | O_CLOEXEC);
		if (fd < 0)
			return PROBE_OPEN;
		read_bytes = pread(fd, observed, length, (off_t) offset);
		if (read_bytes != (ssize_t) length)
		{
			close(fd);
			return PROBE_READ;
		}
		if (close(fd) != 0)
			return PROBE_READ;
		if (!full_pattern_valid(observed, length)
			|| memcmp(expected, observed, length) != 0)
			return PROBE_COMPARE;
	}

	free(observed);
	free(expected);
	return PROBE_OK;
}
