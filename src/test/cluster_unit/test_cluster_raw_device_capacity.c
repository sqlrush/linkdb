/*-------------------------------------------------------------------------
 *
 * test_cluster_raw_device_capacity.c
 *	  Focused raw block-device capacity query tests.
 *
 * The production translation unit is included so this test can replace only
 * the operating-system fstat/ioctl boundary.  All layout and allocator paths
 * are dead-stripped; the test exercises raw_device_size() itself.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/fs.h>
#endif

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Test-only request value; Linux production obtains this from <linux/fs.h>. */
#ifndef __linux__
#define BLKGETSIZE64 0x80081272UL
#endif

static mode_t test_device_mode;
static off_t test_stat_size;
static uint64 test_block_capacity;
static int test_ioctl_calls;
static int test_ioctl_result;
static int test_ioctl_errno;

static int
test_fstat(int fd pg_attribute_unused(), struct stat *st)
{
	memset(st, 0, sizeof(*st));
	st->st_mode = test_device_mode;
	st->st_size = test_stat_size;
	return 0;
}

static int
test_ioctl(int fd pg_attribute_unused(), unsigned long request, ...)
{
	va_list args;
	uint64 *capacity;

	UT_ASSERT_EQ(request, BLKGETSIZE64);
	va_start(args, request);
	capacity = va_arg(args, uint64 *);
	va_end(args);
	test_ioctl_calls++;
	if (test_ioctl_result != 0) {
		errno = test_ioctl_errno;
		return test_ioctl_result;
	}
	*capacity = test_block_capacity;
	return test_ioctl_result;
}

#define fstat test_fstat
#define ioctl test_ioctl
#define USE_CLUSTER_UNIT
#include "../../backend/cluster/storage/cluster_shared_fs_block_device.c"
#undef USE_CLUSTER_UNIT
#undef ioctl
#undef fstat

UT_TEST(test_linux_block_device_uses_blkgetsize64_capacity)
{
	RawDeviceSizeFailure failure;
	uint64 reported_capacity;
	off_t size;

	test_device_mode = S_IFBLK | 0600;
	test_stat_size = 0;
	test_block_capacity = UINT64CONST(8) * 1024 * 1024;
	test_ioctl_calls = 0;
	test_ioctl_result = 0;
	cluster_raw_device_fd = 37;
	size = raw_device_size(&failure, &reported_capacity);

	UT_ASSERT_EQ((uint64)size, test_block_capacity);
	UT_ASSERT_EQ(failure, RAW_DEVICE_SIZE_FAILURE_NONE);
	UT_ASSERT_EQ(reported_capacity, test_block_capacity);
	UT_ASSERT_EQ(test_ioctl_calls, 1);
}

UT_TEST(test_regular_image_uses_stat_size_without_ioctl)
{
	RawDeviceSizeFailure failure;
	uint64 reported_capacity;
	off_t size;

	test_device_mode = S_IFREG | 0600;
	test_stat_size = 12345;
	test_block_capacity = UINT64CONST(8) * 1024 * 1024;
	test_ioctl_calls = 0;
	test_ioctl_result = 0;
	cluster_raw_device_fd = 38;
	size = raw_device_size(&failure, &reported_capacity);

	UT_ASSERT_EQ(size, test_stat_size);
	UT_ASSERT_EQ(failure, RAW_DEVICE_SIZE_FAILURE_NONE);
	UT_ASSERT_EQ(reported_capacity, 0);
	UT_ASSERT_EQ(test_ioctl_calls, 0);
}

UT_TEST(test_block_capacity_ioctl_failure_is_preserved)
{
	RawDeviceSizeFailure failure;
	uint64 reported_capacity;
	off_t size;

	test_device_mode = S_IFBLK | 0600;
	test_stat_size = 0;
	test_ioctl_calls = 0;
	test_ioctl_result = -1;
	test_ioctl_errno = EACCES;
	cluster_raw_device_fd = 39;
	errno = 0;
	size = raw_device_size(&failure, &reported_capacity);

	UT_ASSERT_EQ(size, -1);
	UT_ASSERT_EQ(failure, RAW_DEVICE_SIZE_FAILURE_BLOCK_QUERY);
	UT_ASSERT_EQ(reported_capacity, 0);
	UT_ASSERT_EQ(errno, EACCES);
	UT_ASSERT_EQ(test_ioctl_calls, 1);
}

UT_TEST(test_block_capacity_overflow_fails_closed)
{
	RawDeviceSizeFailure failure;
	uint64 reported_capacity;
	off_t size;

	test_device_mode = S_IFBLK | 0600;
	test_stat_size = 0;
	test_block_capacity = (uint64)PG_INT64_MAX + 1;
	test_ioctl_calls = 0;
	test_ioctl_result = 0;
	cluster_raw_device_fd = 40;
	errno = 0;
	size = raw_device_size(&failure, &reported_capacity);

	UT_ASSERT_EQ(size, -1);
	UT_ASSERT_EQ(failure, RAW_DEVICE_SIZE_FAILURE_OVERFLOW);
	UT_ASSERT_EQ(reported_capacity, test_block_capacity);
	UT_ASSERT_EQ(errno, EOVERFLOW);
	UT_ASSERT_EQ(test_ioctl_calls, 1);
}

UT_TEST(test_non_sector_aligned_block_capacity_fails_closed)
{
	RawDeviceSizeFailure failure;
	uint64 reported_capacity;
	off_t size;

	test_device_mode = S_IFBLK | 0600;
	test_stat_size = 0;
	test_block_capacity = UINT64CONST(8) * 1024 * 1024 + 1;
	test_ioctl_calls = 0;
	test_ioctl_result = 0;
	cluster_raw_device_fd = 41;
	errno = 0;
	size = raw_device_size(&failure, &reported_capacity);

	UT_ASSERT_EQ(size, -1);
	UT_ASSERT_EQ(failure, RAW_DEVICE_SIZE_FAILURE_SECTOR_ALIGNMENT);
	UT_ASSERT_EQ(reported_capacity, test_block_capacity);
	UT_ASSERT_EQ(errno, EINVAL);
	UT_ASSERT_EQ(test_ioctl_calls, 1);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_linux_block_device_uses_blkgetsize64_capacity);
	UT_RUN(test_regular_image_uses_stat_size_without_ioctl);
	UT_RUN(test_block_capacity_ioctl_failure_is_preserved);
	UT_RUN(test_block_capacity_overflow_fails_closed);
	UT_RUN(test_non_sector_aligned_block_capacity_fails_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
