/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_multi_subx_2pc.c
 *    Stage 8 R4 native MultiXact composition, subxact and 2PC tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_multi_subx_2pc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/multixact.h"
#include "cluster/cluster_multixact.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

typedef int (*ClusterR4NativeMultiReadFn)(MultiXactId, MultiXactMember **, bool, bool,
										  MultiXactOffset *);

StaticAssertDecl(__builtin_types_compatible_p(__typeof__(&GetMultiXactIdMembersWithOffset),
											ClusterR4NativeMultiReadFn),
				 "R4 native MultiXact reader signature must remain exact");

static void
fill_members(MultiXactMember *members, int count)
{
	int i;

	for (i = 0; i < count; i++) {
		members[i].xid = (TransactionId)(100 + i);
		members[i].status = (MultiXactStatus)(i % (MaxMultiXactStatus + 1));
	}
}

UT_TEST(test_native_reader_signature_exists)
{
	UT_ASSERT(true);
}

UT_TEST(test_exact_two_member_snapshot_matches)
{
	MultiXactMember left[2];
	MultiXactMember right[2];

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	UT_ASSERT(cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_exact_256_member_snapshot_matches)
{
	MultiXactMember left[256];
	MultiXactMember right[256];

	fill_members(left, 256);
	memcpy(right, left, sizeof(left));
	UT_ASSERT(cluster_multixact_native_snapshot_equal(29, 256, left, 29, 256, right));
}

UT_TEST(test_generation_change_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 18, 2, members));
}

UT_TEST(test_zero_generation_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(0, 2, members, 0, 2, members));
}

UT_TEST(test_zero_members_refuses)
{
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 0, NULL, 17, 0, NULL));
}

UT_TEST(test_one_member_refuses)
{
	MultiXactMember member;

	fill_members(&member, 1);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 1, &member, 17, 1, &member));
}

UT_TEST(test_257_members_refuses)
{
	MultiXactMember members[257];

	fill_members(members, 257);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 257, members, 17, 257, members));
}

UT_TEST(test_member_count_change_refuses)
{
	MultiXactMember members[3];

	fill_members(members, 3);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 3, members));
}

UT_TEST(test_null_first_member_array_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, NULL, 17, 2, members));
}

UT_TEST(test_null_second_member_array_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 2, NULL));
}

UT_TEST(test_member_xid_change_refuses)
{
	MultiXactMember left[2];
	MultiXactMember right[2];

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	right[1].xid++;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_member_status_change_refuses)
{
	MultiXactMember left[2];
	MultiXactMember right[2];

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	right[1].status = MultiXactStatusUpdate;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_member_order_change_refuses)
{
	MultiXactMember left[2];
	MultiXactMember right[2];
	MultiXactMember swap;

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	swap = right[0];
	right[0] = right[1];
	right[1] = swap;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_invalid_member_xid_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	members[0].xid = InvalidTransactionId;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 2, members));
}

UT_TEST(test_invalid_member_status_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	members[0].status = (MultiXactStatus)(MaxMultiXactStatus + 1);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 2, members));
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_native_reader_signature_exists);
	UT_RUN(test_exact_two_member_snapshot_matches);
	UT_RUN(test_exact_256_member_snapshot_matches);
	UT_RUN(test_generation_change_refuses);
	UT_RUN(test_zero_generation_refuses);
	UT_RUN(test_zero_members_refuses);
	UT_RUN(test_one_member_refuses);
	UT_RUN(test_257_members_refuses);
	UT_RUN(test_member_count_change_refuses);
	UT_RUN(test_null_first_member_array_refuses);
	UT_RUN(test_null_second_member_array_refuses);
	UT_RUN(test_member_xid_change_refuses);
	UT_RUN(test_member_status_change_refuses);
	UT_RUN(test_member_order_change_refuses);
	UT_RUN(test_invalid_member_xid_refuses);
	UT_RUN(test_invalid_member_status_refuses);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
