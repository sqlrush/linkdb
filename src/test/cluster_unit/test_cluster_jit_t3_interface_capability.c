/*-------------------------------------------------------------------------
 *
 * test_cluster_jit_t3_interface_capability.c
 *	  Compiler-native capability and fail-closed checks for JIT Task3.
 *
 * This test never supplies substitute declarations, types, objects or weak
 * symbols.  Before a production capability exists it does not reference the
 * absent symbol; after it exists the real strong owner is called directly.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_jit_t3_interface_capability.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <string.h>

#include "access/xloginsert.h"
#include "access/xlogreader.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_space.h")
#include "cluster/cluster_space.h"
#define TEST_HAS_CLUSTER_SPACE_HEADER 1
#endif
#endif

#ifndef TEST_HAS_CLUSTER_SPACE_HEADER
#define TEST_HAS_CLUSTER_SPACE_HEADER 0
#endif

static int
observe_capability(bool present, const char *name)
{
	if (present)
	{
		printf("JIT_CAPABILITY\t%s\tPASS\n", name);
		return 0;
	}

	printf("JIT_SEMANTIC_RED:%s\n", name);
	return 1;
}

int
main(void)
{
	int			failures = 0;

	printf("JIT_CONTROL:T3-I-COMPILE-LINK-RUN:PASS\n");

#if defined(RF_PAGE_VERSION_EQUAL_INTERFACE_V1)
	{
		RfPageVersionV1 left = {{0}, 17};
		RfPageVersionV1 right;
		RfPageVersionV1 left_before;
		RfPageVersionV1 right_before;
		bool		result;

		left.segment_incarnation[0] = 1;
		right = left;
		left_before = left;
		right_before = right;
		result = rf_page_version_equal_v1(&left, &right);
		if (result || memcmp(&left, &left_before, sizeof(left)) != 0 ||
			memcmp(&right, &right_before, sizeof(right)) != 0)
		{
			printf("JIT_CONTROL:T3-PAGEVERSION-FAIL-CLOSED:FAIL\n");
			failures++;
		}
		else
			printf("JIT_CONTROL:T3-PAGEVERSION-FAIL-CLOSED:PASS\n");
		failures += observe_capability(true,
			"T3-PAGEVERSION-EQUALITY-INTERFACE");
	}
#else
	failures += observe_capability(false,
		"T3-PAGEVERSION-EQUALITY-INTERFACE");
#endif

#if defined(XLOG_PAGE_VERSION_EDGE_ENCODER_V1)
	{
		uint8		output[128];
		uint8		output_before[sizeof(output)];
		Size		output_size = 73;
		RfPageVersionEdgeEntryV1 entry;
		bool		result;

		memset(output, 0xa5, sizeof(output));
		memcpy(output_before, output, sizeof(output));
		memset(&entry, 0, sizeof(entry));
		entry.block_id = 0;
		entry.page_class = RF_PAGE_CLASS_ORDINARY;
		entry.before_kind = RF_PAGE_STATE_ABSENT;
		entry.result_kind = RF_PAGE_STATE_PRESENT;
		entry.edge_flags = RF_PAGE_EDGE_WILL_INIT |
			RF_PAGE_EDGE_FULL_COVERAGE;
		entry.result_incarnation[0] = 1;
		result = XLogEncodePageVersionEdgeV1(output, sizeof(output), 19,
										 &entry, 1, &output_size);
		if (result || output_size != 73 ||
			memcmp(output, output_before, sizeof(output)) != 0)
		{
			printf("JIT_CONTROL:T3-ENCODER-FAIL-CLOSED:FAIL\n");
			failures++;
		}
		else
			printf("JIT_CONTROL:T3-ENCODER-FAIL-CLOSED:PASS\n");
		failures += observe_capability(true, "T3-EDGE-ENCODER-INTERFACE");
	}
#else
	failures += observe_capability(false, "T3-EDGE-ENCODER-INTERFACE");
#endif

#if TEST_HAS_CLUSTER_SPACE_HEADER && defined(CLUSTER_SPACE_CODEC_INTERFACE_V1)
	{
		ClusterSpaceIdentityV1 left;
		ClusterSpaceIdentityV1 right;
		ClusterSpaceIdentityV1 left_before;
		ClusterSpaceIdentityV1 right_before;
		bool		result;

		memset(&left, 0, sizeof(left));
		left.system_identifier = 1;
		left.space_incarnation[0] = 2;
		right = left;
		left_before = left;
		right_before = right;
		result = cluster_space_identity_equal(&left, &right);
		if (result || memcmp(&left, &left_before, sizeof(left)) != 0 ||
			memcmp(&right, &right_before, sizeof(right)) != 0)
		{
			printf("JIT_CONTROL:T3-SPACE-CODEC-FAIL-CLOSED:FAIL\n");
			failures++;
		}
		else
			printf("JIT_CONTROL:T3-SPACE-CODEC-FAIL-CLOSED:PASS\n");
		failures += observe_capability(true, "T3-SPACE-CODEC-INTERFACE");
	}
#else
	failures += observe_capability(false, "T3-SPACE-CODEC-INTERFACE");
#endif

	return failures == 0 ? 0 : 1;
}
