/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_cr_walk.c
 *	R4 synchronous-CR undo-walk contract tests.
 *
 * B1 owns only the transaction-head and previous-edge identity subset in
 * this file.  Later R4 batches extend the same target with physical target,
 * payload, ordering, depth, horizon, foreign-fetch and FULL-only cases.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_uba.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_XID ((TransactionId)797)
#define TEST_WRAP ((uint16)7)
#define TEST_SEGMENT ((uint32)1)
#define TEST_TT_OFFSET ((uint16)3)

static UBA
make_record_uba(uint32 block, uint16 row)
{
	return uba_encode(TEST_SEGMENT, block, TEST_TT_OFFSET, row);
}

static ClusterTxLocator
make_locator(void)
{
	ClusterTxLocator locator;

	memset(&locator, 0, sizeof(locator));
	locator.uba = make_record_uba(7, 400);
	locator.xid = TEST_XID;
	locator.tt_wrap = TEST_WRAP;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 2;
	return locator;
}

static UndoRecordHeader
make_record(UBA record_uba, OffsetNumber target_offset)
{
	UndoRecordHeader record;
	uint32 segment;
	uint32 block;
	uint16 tt_offset;
	uint16 row;

	memset(&record, 0, sizeof(record));
	UT_ASSERT(uba_decode(record_uba, &segment, &block, &tt_offset, &row));
	record.record_type = UNDO_RECORD_UPDATE;
	record.payload_length = sizeof(UndoUpdatePayload);
	record.xid = TEST_XID;
	record.origin_node_id = 0;
	record.tt_slot_segment_id = (uint16)segment;
	record.tt_slot_id = cluster_tt_slot_offset_to_id(tt_offset);
	record.write_scn = (SCN)900;
	record.target_locator.relNumber = 123;
	record.target_fork = MAIN_FORKNUM;
	record.target_block = 408;
	record.target_offset = target_offset;
	record.tt_wrap_plus1 = (uint16)(TEST_WRAP + 1);
	return record;
}

static void
assert_head_rejected(const ClusterTxLocator *locator, UBA record_uba,
					 const UndoRecordHeader *record, ClusterTxResolveReason expected_reason)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT(!cluster_undo_record_validate_identity(locator, record_uba, record, &reason));
	UT_ASSERT_EQ(reason, expected_reason);
}

static void
assert_edge_rejected(const ClusterTxLocator *locator, UBA current_uba,
					 const UndoRecordHeader *current, UBA previous_uba,
					 const UndoRecordHeader *previous, ClusterTxResolveReason expected_reason)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT(!cluster_undo_record_validate_prev_edge(locator, current_uba, current, previous_uba,
													  previous, &reason));
	UT_ASSERT_EQ(reason, expected_reason);
}

UT_TEST(test_head_identity_accepts_exact_uba_xid_wrap_and_tt_slot)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT(cluster_undo_record_validate_identity(&locator, locator.uba, &head, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_head_target_offset_is_not_transaction_identity)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 99);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	/* The queried row may be lp1 while the transaction head targets lp99. */
	UT_ASSERT(cluster_undo_record_validate_identity(&locator, locator.uba, &head, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_head_exact_uba_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA other = make_record_uba(7, 401);
	UndoRecordHeader head = make_record(other, 2);

	assert_head_rejected(&locator, other, &head, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_head_malformed_uba_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	locator.uba.raw[1] |= UINT64CONST(1) << 63;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_head_xid_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.xid = (TransactionId)(TEST_XID + 1);
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_XID_MISMATCH);
}

UT_TEST(test_head_missing_wrap_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_wrap_plus1 = 0;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_head_wrap_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_wrap_plus1++;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_head_tt_segment_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_slot_segment_id++;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_head_tt_slot_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_slot_id++;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_head_origin_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.origin_node_id = 1;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_accepts_same_tx_different_target_offset)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	head.prev_uba = previous_uba;
	UT_ASSERT(cluster_undo_record_validate_prev_edge(&locator, locator.uba, &head, previous_uba,
													 &previous, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_previous_edge_pointer_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = make_record_uba(7, 200);
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_malformed_uba_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	previous_uba.raw[1] |= UINT64CONST(1) << 63;
	head.prev_uba = previous_uba;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_self_cycle_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.prev_uba = locator.uba;
	assert_edge_rejected(&locator, locator.uba, &head, locator.uba, &head,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_forward_motion_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 500);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_xid_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.xid = (TransactionId)(TEST_XID + 1);
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_XID_MISMATCH);
}

UT_TEST(test_previous_edge_wrap_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.tt_wrap_plus1++;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_previous_edge_tt_segment_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.tt_slot_segment_id++;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_previous_edge_tt_slot_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.tt_slot_id++;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_previous_edge_origin_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.origin_node_id = 1;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

int
main(int argc, char **argv)
{
	UT_PLAN(20);

	UT_RUN(test_head_identity_accepts_exact_uba_xid_wrap_and_tt_slot);
	UT_RUN(test_head_target_offset_is_not_transaction_identity);
	UT_RUN(test_head_exact_uba_mismatch_is_rejected);
	UT_RUN(test_head_malformed_uba_is_rejected);
	UT_RUN(test_head_xid_mismatch_is_rejected);
	UT_RUN(test_head_missing_wrap_is_rejected);
	UT_RUN(test_head_wrap_mismatch_is_rejected);
	UT_RUN(test_head_tt_segment_mismatch_is_rejected);
	UT_RUN(test_head_tt_slot_mismatch_is_rejected);
	UT_RUN(test_head_origin_mismatch_is_rejected);
	UT_RUN(test_previous_edge_accepts_same_tx_different_target_offset);
	UT_RUN(test_previous_edge_pointer_mismatch_is_rejected);
	UT_RUN(test_previous_edge_malformed_uba_is_rejected);
	UT_RUN(test_previous_edge_self_cycle_is_rejected);
	UT_RUN(test_previous_edge_forward_motion_is_rejected);
	UT_RUN(test_previous_edge_xid_mismatch_is_rejected);
	UT_RUN(test_previous_edge_wrap_mismatch_is_rejected);
	UT_RUN(test_previous_edge_tt_segment_mismatch_is_rejected);
	UT_RUN(test_previous_edge_tt_slot_mismatch_is_rejected);
	UT_RUN(test_previous_edge_origin_mismatch_is_rejected);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
