/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_outcome.c
 *    Stage 8 R4 closed outcome/proof compatibility table.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_tx_outcome.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/clog.h"
#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_ORIGIN_XID ((TransactionId)797)
#define TEST_ORIGIN_WRAP ((uint16)7)
#define TEST_RECORD_SEGMENT ((uint32)1)
#define TEST_TT_SEGMENT ((uint32)2)
#define TEST_TT_OFFSET ((uint16)3)

int cluster_node_id = 0;

static ClusterTxLocator test_origin_locator;
static UndoRecordHeader test_origin_record;
static int test_undo_read_calls;
static int test_tt_exact_calls;
static int test_native_status_calls;
static int test_by_xid_scan_calls;
static uint32 test_tt_segment_seen;
static uint16 test_tt_slot_seen;
static TransactionId test_tt_xid_seen;
static uint16 test_tt_wrap_seen;
static XidStatus test_native_status;
static SCN test_commit_scn;
static uint64 test_formation_epoch;
static XLogRecPtr test_flush_lsn;
static uint64 test_tt_generation;
static SCN test_authority_scn;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

static bool
test_uba_equal(UBA left, UBA right)
{
	return left.raw[0] == right.raw[0] && left.raw[1] == right.raw[1];
}

size_t
cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size)
{
	test_undo_read_calls++;
	UT_ASSERT(test_uba_equal(uba, test_origin_locator.uba));
	UT_ASSERT(out_buffer != NULL);
	UT_ASSERT(buffer_size >= sizeof(test_origin_record));
	memcpy(out_buffer, &test_origin_record, sizeof(test_origin_record));
	return sizeof(test_origin_record);
}

bool
cluster_tt_slot_durable_lookup_committed_stable(
	uint32 segment_id, uint16 slot_offset, TransactionId xid, uint32 expected_wrap,
	ClusterTTDurableXidCommitCheck xid_committed, SCN *commit_scn)
{
	test_tt_exact_calls++;
	test_tt_segment_seen = segment_id;
	test_tt_slot_seen = slot_offset;
	test_tt_xid_seen = xid;
	test_tt_wrap_seen = (uint16)expected_wrap;
	if (xid_committed == NULL || commit_scn == NULL || !xid_committed(xid))
		return false;
	*commit_scn = test_commit_scn;
	return true;
}

ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node pg_attribute_unused(),
											  TransactionId xid pg_attribute_unused(),
											  uint32 expected_wrap pg_attribute_unused(),
											  SCN *commit_scn pg_attribute_unused(),
											  uint16 *out_seg pg_attribute_unused(),
											  uint16 *out_slot pg_attribute_unused(),
											  uint16 *out_wrap pg_attribute_unused())
{
	test_by_xid_scan_calls++;
	return CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;
}

XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	test_native_status_calls++;
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	if (lsn != NULL)
		*lsn = test_flush_lsn;
	return test_native_status;
}

uint64
cluster_epoch_get_current(void)
{
	return test_formation_epoch;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI)
{
	if (insertTLI != NULL)
		*insertTLI = 1;
	return test_flush_lsn;
}

uint64
cluster_undo_tt_retention_rollover_count(void)
{
	return test_tt_generation;
}

SCN
cluster_scn_current(void)
{
	return test_authority_scn;
}

static void
reset_exact_origin_fixture(void)
{
	memset(&test_origin_locator, 0, sizeof(test_origin_locator));
	test_origin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 400);
	test_origin_locator.xid = TEST_ORIGIN_XID;
	test_origin_locator.tt_wrap = TEST_ORIGIN_WRAP;
	test_origin_locator.itl_kind = ITL_FLAG_ACTIVE;
	test_origin_locator.itl_slot_index = 2;

	memset(&test_origin_record, 0, sizeof(test_origin_record));
	test_origin_record.record_type = UNDO_RECORD_UPDATE;
	test_origin_record.xid = TEST_ORIGIN_XID;
	test_origin_record.origin_node_id = 0;
	test_origin_record.tt_slot_segment_id = TEST_TT_SEGMENT;
	test_origin_record.tt_slot_id = cluster_tt_slot_offset_to_id(TEST_TT_OFFSET);
	test_origin_record.tt_wrap_plus1 = (uint16)(TEST_ORIGIN_WRAP + 1);

	test_undo_read_calls = 0;
	test_tt_exact_calls = 0;
	test_native_status_calls = 0;
	test_by_xid_scan_calls = 0;
	test_tt_segment_seen = 0;
	test_tt_slot_seen = 0;
	test_tt_xid_seen = InvalidTransactionId;
	test_tt_wrap_seen = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	test_commit_scn = scn_encode(0, 80);
	test_formation_epoch = 42;
	test_flush_lsn = (XLogRecPtr)UINT64CONST(0x12345678);
	test_tt_generation = 9;
	test_authority_scn = scn_encode(0, 100);
}

static const bool expected[5][8] = {
	[CLUSTER_TX_UNKNOWN] = {
		[CLUSTER_TX_PROOF_NONE] = true,
		[CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON] = true,
	},
	[CLUSTER_TX_IN_PROGRESS] = {
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
	},
	[CLUSTER_TX_PREPARED] = {
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_TWOPHASE] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
	},
	[CLUSTER_TX_COMMITTED] = {
		[CLUSTER_TX_PROOF_ITL_CLEANOUT] = true,
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
		[CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED] = true,
	},
	[CLUSTER_TX_ABORTED] = {
		[CLUSTER_TX_PROOF_ITL_CLEANOUT] = true,
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
		[CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED] = true,
	},
};

static void
run_pair(unsigned int pair)
{
	ClusterTxOutcome outcome = (ClusterTxOutcome)(pair / 8);
	ClusterTxProofKind proof = (ClusterTxProofKind)(pair % 8);

	UT_ASSERT_EQ(cluster_tx_outcome_proof_is_valid(outcome, proof), expected[outcome][proof]);
}

#define DEFINE_PAIR_TEST(n) \
	UT_TEST(test_outcome_proof_pair_##n) { run_pair(n); }

#define RUN_PAIR_TEST(n) UT_RUN(test_outcome_proof_pair_##n)

DEFINE_PAIR_TEST(0)
DEFINE_PAIR_TEST(1)
DEFINE_PAIR_TEST(2)
DEFINE_PAIR_TEST(3)
DEFINE_PAIR_TEST(4)
DEFINE_PAIR_TEST(5)
DEFINE_PAIR_TEST(6)
DEFINE_PAIR_TEST(7)
DEFINE_PAIR_TEST(8)
DEFINE_PAIR_TEST(9)
DEFINE_PAIR_TEST(10)
DEFINE_PAIR_TEST(11)
DEFINE_PAIR_TEST(12)
DEFINE_PAIR_TEST(13)
DEFINE_PAIR_TEST(14)
DEFINE_PAIR_TEST(15)
DEFINE_PAIR_TEST(16)
DEFINE_PAIR_TEST(17)
DEFINE_PAIR_TEST(18)
DEFINE_PAIR_TEST(19)
DEFINE_PAIR_TEST(20)
DEFINE_PAIR_TEST(21)
DEFINE_PAIR_TEST(22)
DEFINE_PAIR_TEST(23)
DEFINE_PAIR_TEST(24)
DEFINE_PAIR_TEST(25)
DEFINE_PAIR_TEST(26)
DEFINE_PAIR_TEST(27)
DEFINE_PAIR_TEST(28)
DEFINE_PAIR_TEST(29)
DEFINE_PAIR_TEST(30)
DEFINE_PAIR_TEST(31)
DEFINE_PAIR_TEST(32)
DEFINE_PAIR_TEST(33)
DEFINE_PAIR_TEST(34)
DEFINE_PAIR_TEST(35)
DEFINE_PAIR_TEST(36)
DEFINE_PAIR_TEST(37)
DEFINE_PAIR_TEST(38)
DEFINE_PAIR_TEST(39)

UT_TEST(test_out_of_domain_values_fail_closed)
{
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid((ClusterTxOutcome)-1, CLUSTER_TX_PROOF_NONE));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid((ClusterTxOutcome)5, CLUSTER_TX_PROOF_NONE));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid(CLUSTER_TX_UNKNOWN, (ClusterTxProofKind)-1));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid(CLUSTER_TX_UNKNOWN, (ClusterTxProofKind)8));
}

UT_TEST(test_exact_origin_committed_uses_canonical_tt_identity_and_direct_clog)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterTxOutcome outcome;

	reset_exact_origin_fixture();
	memset(&resolution, 0xa5, sizeof(resolution));
	outcome = cluster_runtime_visibility_resolve_exact_origin(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch, &resolution,
		&reason);

	UT_ASSERT_EQ(outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
							 sizeof(test_origin_locator)),
				 0);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(resolution.authority.origin_epoch, test_formation_epoch);
	UT_ASSERT_EQ(resolution.authority.live_hwm_lsn, test_flush_lsn);
	UT_ASSERT_EQ(resolution.authority.tt_generation, test_tt_generation);
	UT_ASSERT_EQ(resolution.authority.authority_scn, test_authority_scn);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_tt_segment_seen, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_tt_slot_seen, TEST_TT_OFFSET);
	UT_ASSERT_EQ((int)test_tt_xid_seen, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_tt_wrap_seen, TEST_ORIGIN_WRAP);
}

UT_TEST(test_exact_origin_bad_record_wrap_fails_before_tt_or_clog)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_origin_record.tt_wrap_plus1++;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

int
main(void)
{
	UT_PLAN(43);
	RUN_PAIR_TEST(0);
	RUN_PAIR_TEST(1);
	RUN_PAIR_TEST(2);
	RUN_PAIR_TEST(3);
	RUN_PAIR_TEST(4);
	RUN_PAIR_TEST(5);
	RUN_PAIR_TEST(6);
	RUN_PAIR_TEST(7);
	RUN_PAIR_TEST(8);
	RUN_PAIR_TEST(9);
	RUN_PAIR_TEST(10);
	RUN_PAIR_TEST(11);
	RUN_PAIR_TEST(12);
	RUN_PAIR_TEST(13);
	RUN_PAIR_TEST(14);
	RUN_PAIR_TEST(15);
	RUN_PAIR_TEST(16);
	RUN_PAIR_TEST(17);
	RUN_PAIR_TEST(18);
	RUN_PAIR_TEST(19);
	RUN_PAIR_TEST(20);
	RUN_PAIR_TEST(21);
	RUN_PAIR_TEST(22);
	RUN_PAIR_TEST(23);
	RUN_PAIR_TEST(24);
	RUN_PAIR_TEST(25);
	RUN_PAIR_TEST(26);
	RUN_PAIR_TEST(27);
	RUN_PAIR_TEST(28);
	RUN_PAIR_TEST(29);
	RUN_PAIR_TEST(30);
	RUN_PAIR_TEST(31);
	RUN_PAIR_TEST(32);
	RUN_PAIR_TEST(33);
	RUN_PAIR_TEST(34);
	RUN_PAIR_TEST(35);
	RUN_PAIR_TEST(36);
	RUN_PAIR_TEST(37);
	RUN_PAIR_TEST(38);
	RUN_PAIR_TEST(39);
	UT_RUN(test_out_of_domain_values_fail_closed);
	UT_RUN(test_exact_origin_committed_uses_canonical_tt_identity_and_direct_clog);
	UT_RUN(test_exact_origin_bad_record_wrap_fails_before_tt_or_clog);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
