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
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "cluster/cluster_cr.h"
#include "storage/ipc.h"
#include "utils/snapmgr.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_ORIGIN_XID ((TransactionId)797)
#define TEST_ORIGIN_WRAP ((uint16)7)
#define TEST_RECORD_SEGMENT ((uint32)1)
#define TEST_TT_SEGMENT ((uint32)2)
#define TEST_TT_OFFSET ((uint16)3)

int cluster_node_id = 0;
TransactionId TransactionXmin = FirstNormalTransactionId;
volatile sig_atomic_t InterruptPending = false;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

static ClusterTxLocator test_origin_locator;
static UndoRecordHeader test_origin_record;
static int test_undo_read_calls;
static int test_tt_exact_calls;
static int test_tt_snapshot_calls;
static int test_native_status_calls;
static int test_by_xid_scan_calls;
static uint32 test_tt_segment_seen;
static uint16 test_tt_slot_seen;
static TransactionId test_tt_xid_seen;
static uint16 test_tt_wrap_seen;
static XidStatus test_native_status;
static TransactionId test_native_script_xids[8];
static XidStatus test_native_script_statuses[8];
static int test_native_script_count;
static int test_native_script_pos;
static TransactionId test_subtrans_chain[CLUSTER_R4_SUBTRANS_MAX_DEPTH + 1];
static int test_subtrans_chain_count;
static int test_subtrans_parent_calls;
static bool test_subtrans_mutate_recheck;
static bool test_subtrans_cycle;
static bool test_twophase_prepared;
static TransactionId test_twophase_xid;
static int test_twophase_calls;
static TTSlot test_tt_slot;
static SCN test_commit_scn;
static uint64 test_formation_epoch;
static XLogRecPtr test_flush_lsn;
static uint64 test_tt_generation;
static SCN test_authority_scn;
static int test_candidate_acquire_calls;
static int test_candidate_sample_calls;
static int test_candidate_block0_copy_calls;
static int test_candidate_data_copy_calls;
static int test_candidate_release_calls;
static int test_candidate_cancel_calls;
static int test_candidate_exit_hooks_ensure_calls;
static uint32 test_candidate_acquire_segments[8];
static int test_candidate_held_count;
static int test_candidate_max_held_count;
static int test_candidate_extract_calls;
static bool test_candidate_mutate_record_on_recheck;
static int test_regular_admission_recheck_calls;
static int test_terminal_census_recheck_calls;
static int test_regular_root_resolve_calls;
static int test_terminal_census_root_resolve_calls;
static ClusterUndoBlock0CurrentStep test_candidate_acquire_step
	= CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
static ClusterUndoBlock0CurrentStep test_candidate_poll_step
	= CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

void
ProcessInterrupts(void)
{}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}

void
cancel_before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
						 Datum arg pg_attribute_unused())
{}

void
cluster_undo_block0_current_ensure_exit_hooks(void)
{
	test_candidate_exit_hooks_ensure_calls++;
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
	if (test_tt_slot.status != TT_SLOT_COMMITTED || test_tt_slot.xid != xid
		|| test_tt_slot.wrap != (uint16)expected_wrap || !SCN_VALID(test_tt_slot.commit_scn)
		|| xid_committed == NULL || commit_scn == NULL || !xid_committed(xid))
		return false;
	*commit_scn = test_tt_slot.commit_scn;
	return true;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id, uint16 slot_offset,
											   TransactionId xid, uint16 expected_wrap,
											   TTSlot *slot_out)
{
	test_tt_snapshot_calls++;
	test_tt_segment_seen = segment_id;
	test_tt_slot_seen = slot_offset;
	test_tt_xid_seen = xid;
	test_tt_wrap_seen = expected_wrap;
	if (slot_out == NULL || test_tt_slot.status > TT_SLOT_RECYCLABLE
		|| test_tt_slot.xid != xid || test_tt_slot.wrap != expected_wrap)
		return false;
	*slot_out = test_tt_slot;
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
	XidStatus status;

	test_native_status_calls++;
	if (lsn != NULL)
		*lsn = test_flush_lsn;
	if (test_native_script_pos < test_native_script_count) {
		UT_ASSERT_EQ((int)xid, (int)test_native_script_xids[test_native_script_pos]);
		status = test_native_script_statuses[test_native_script_pos++];
	} else {
		UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
		status = test_native_status;
	}
	return status;
}

bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	int32 diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return id1 < id2;
	diff = (int32)(id1 - id2);
	return diff < 0;
}

TransactionId
SubTransGetParent(TransactionId xid)
{
	int i;
	int phase;

	test_subtrans_parent_calls++;
	if (test_subtrans_cycle)
		return xid;
	for (i = 0; i < test_subtrans_chain_count; i++) {
		if (test_subtrans_chain[i] != xid)
			continue;
		phase = (test_subtrans_parent_calls - 1) / test_subtrans_chain_count;
		if (test_subtrans_mutate_recheck && phase > 0 && i == 0)
			return test_subtrans_chain[i] - 2;
		return i + 1 < test_subtrans_chain_count ? test_subtrans_chain[i + 1]
											 : InvalidTransactionId;
	}
	UT_ASSERT(false);
	return InvalidTransactionId;
}

bool
TwoPhaseTransactionIdIsPrepared(TransactionId xid)
{
	test_twophase_calls++;
	UT_ASSERT_EQ((int)xid, (int)test_twophase_xid);
	return test_twophase_prepared;
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

static bool
test_admission_token_exact(const ClusterSemanticAdmissionToken *token)
{
	return token != NULL && token->entered
		   && token->feature_bit == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		   && token->side == CLUSTER_SEMANTIC_TARGET_SIDE
		   && token->formation_epoch == test_formation_epoch;
}

bool
cluster_semantic_activation_recheck(
	const ClusterSemanticAdmissionToken *token)
{
	test_regular_admission_recheck_calls++;
	return test_admission_token_exact(token);
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	test_terminal_census_recheck_calls++;
	return test_admission_token_exact(token);
}

static bool
test_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
	ClusterUndoPathIntent intent, uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	UT_ASSERT(test_admission_token_exact(token));
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(owner_instance, 1);
	UT_ASSERT(segment_id == TEST_RECORD_SEGMENT || segment_id == TEST_TT_SEGMENT);
	out->intent = intent;
	out->root_id = segment_id == TEST_RECORD_SEGMENT ? 91 : 92;
	out->root_generation = 7;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	test_regular_root_resolve_calls++;
	return test_resolve_shared_undo_root(token, intent, owner_instance,
		segment_id, out);
}

bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	test_terminal_census_root_resolve_calls++;
	return test_resolve_shared_undo_root(token, intent, owner_instance,
		segment_id, out);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
	int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	UT_ASSERT(test_candidate_acquire_calls
			  < (int)lengthof(test_candidate_acquire_segments));
	test_candidate_acquire_segments[test_candidate_acquire_calls]
		= key->segment_id;
	test_candidate_acquire_calls++;
	UT_ASSERT_EQ(key->owner_instance, 1);
	UT_ASSERT(key->segment_id == TEST_RECORD_SEGMENT
			  || key->segment_id == TEST_TT_SEGMENT);
	UT_ASSERT_EQ(mode, CLUSTER_UNDO_BLOCK0_SCUR);
	UT_ASSERT(test_admission_token_exact(admission));
	if (test_candidate_acquire_step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
		test_candidate_held_count++;
		if (test_candidate_max_held_count < test_candidate_held_count)
			test_candidate_max_held_count = test_candidate_held_count;
	}
	return test_candidate_acquire_step;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return test_candidate_poll_step;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterUndoBlock0Generation *observed)
{
	test_candidate_sample_calls++;
	UT_ASSERT(root->root_id == 91 || root->root_id == 92);
	*observed = (ClusterUndoBlock0Generation){ true,
		root->root_id == 91 ? 17 : 23 };
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_copy_resident(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root pg_attribute_unused(),
	const ClusterUndoBlock0Generation *expected, char private_page[BLCKSZ])
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)private_page;

	test_candidate_block0_copy_calls++;
	UT_ASSERT(expected->known);
	UT_ASSERT(expected->value == 17 || expected->value == 23);
	memset(private_page, 0, BLCKSZ);
	header->tt_slots[TEST_TT_OFFSET] = test_tt_slot;
	return CLUSTER_UNDO_BLOCK0_OK;
}

bool
cluster_undo_buf_copy_resident(uint32 segment_id, uint8 owner,
							   uint32 block_no, char dst[BLCKSZ])
{
	test_candidate_data_copy_calls++;
	UT_ASSERT_EQ(segment_id, TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(block_no, 7);
	memset(dst, 0x5a, BLCKSZ);
	return true;
}

bool
cluster_cr_r4_extract_resident_record(
	const char resident_undo_page[BLCKSZ] pg_attribute_unused(),
	const ClusterTxLocator *request_locator, char record_out[BLCKSZ],
	size_t *record_length_out, ClusterTxLocator *canonical_locator_out)
{
	test_candidate_extract_calls++;
	UT_ASSERT_EQ(request_locator->tt_wrap, TT_WRAP_INVALID);
	if (test_candidate_mutate_record_on_recheck
		&& test_candidate_extract_calls == 2)
		test_origin_record.payload_length++;
	memcpy(record_out, &test_origin_record, sizeof(test_origin_record));
	*record_length_out = sizeof(test_origin_record);
	*canonical_locator_out = *request_locator;
	canonical_locator_out->tt_wrap = TEST_ORIGIN_WRAP;
	return true;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	test_candidate_release_calls++;
	UT_ASSERT(test_candidate_held_count > 0);
	test_candidate_held_count--;
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

void
cluster_undo_block0_current_cancel(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	test_candidate_cancel_calls++;
}

ClusterTxOutcome
cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
	int32 origin_node pg_attribute_unused(),
	const ClusterTxLocator *locator pg_attribute_unused(),
	uint32 expected_physical_generation pg_attribute_unused(),
	uint64 formation_epoch pg_attribute_unused(),
	ClusterTxResolution *out pg_attribute_unused(),
	ClusterTxResolveReason *reason_out pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_TX_UNKNOWN;
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
	test_tt_snapshot_calls = 0;
	test_native_status_calls = 0;
	test_by_xid_scan_calls = 0;
	test_tt_segment_seen = 0;
	test_tt_slot_seen = 0;
	test_tt_xid_seen = InvalidTransactionId;
	test_tt_wrap_seen = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	test_native_script_count = 0;
	test_native_script_pos = 0;
	test_subtrans_chain_count = 0;
	test_subtrans_parent_calls = 0;
	test_subtrans_mutate_recheck = false;
	test_subtrans_cycle = false;
	test_twophase_prepared = false;
	test_twophase_xid = InvalidTransactionId;
	test_twophase_calls = 0;
	TransactionXmin = FirstNormalTransactionId;
	test_commit_scn = scn_encode(0, 80);
	memset(&test_tt_slot, 0, sizeof(test_tt_slot));
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = test_commit_scn;
	test_formation_epoch = 42;
	test_flush_lsn = (XLogRecPtr)UINT64CONST(0x12345678);
	test_tt_generation = 9;
	test_authority_scn = scn_encode(0, 100);
	test_candidate_acquire_calls = 0;
	test_candidate_sample_calls = 0;
	test_candidate_block0_copy_calls = 0;
	test_candidate_data_copy_calls = 0;
	test_candidate_release_calls = 0;
	test_candidate_cancel_calls = 0;
	test_candidate_exit_hooks_ensure_calls = 0;
	memset(test_candidate_acquire_segments, 0,
		   sizeof(test_candidate_acquire_segments));
	test_candidate_held_count = 0;
	test_candidate_max_held_count = 0;
	test_candidate_extract_calls = 0;
	test_candidate_mutate_record_on_recheck = false;
	test_regular_admission_recheck_calls = 0;
	test_terminal_census_recheck_calls = 0;
	test_regular_root_resolve_calls = 0;
	test_terminal_census_root_resolve_calls = 0;
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
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
	UT_ASSERT_EQ(test_tt_snapshot_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_tt_segment_seen, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_tt_slot_seen, TEST_TT_OFFSET);
	UT_ASSERT_EQ((int)test_tt_xid_seen, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_tt_wrap_seen, TEST_ORIGIN_WRAP);
}

UT_TEST(test_terminal_census_local_origin_uses_resident_candidate2_and_canonical_upgrade)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_exit_hooks_ensure_calls, 1);
	UT_ASSERT_EQ(test_candidate_sample_calls, 2);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
	UT_ASSERT_EQ(test_candidate_data_copy_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_undo_read_calls, 0);
	UT_ASSERT_EQ(test_tt_exact_calls, 0);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 0);
}

UT_TEST(test_terminal_census_same_owner_cross_segment_is_sequential_and_exact)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_acquire_segments[0], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[1], TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[2], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_regular_admission_recheck_calls, 0);
	UT_ASSERT(test_terminal_census_recheck_calls > 0);
	UT_ASSERT_EQ(test_regular_root_resolve_calls, 0);
	UT_ASSERT(test_terminal_census_root_resolve_calls > 0);
}

UT_TEST(test_visibility_same_owner_cross_segment_is_sequential_and_exact)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_acquire_segments[0], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[1], TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[2], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT(test_regular_admission_recheck_calls > 0);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 0);
	UT_ASSERT(test_regular_root_resolve_calls > 0);
	UT_ASSERT_EQ(test_terminal_census_root_resolve_calls, 0);
}

UT_TEST(test_visibility_precommit_committed_slot_with_live_origin_stays_in_progress)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind,
				 CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 2);
}

UT_TEST(test_terminal_census_rejects_precommit_committed_slot_with_live_origin)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
	UT_ASSERT_EQ(test_candidate_release_calls, 2);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_native_status_calls, 2);
}

UT_TEST(test_terminal_census_cross_segment_data_drift_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_mutate_record_on_recheck = true;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_STALE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 1);
}

UT_TEST(test_terminal_census_cross_owner_tt_alias_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	/* Segment 257 belongs to owner instance 2, while the DATA UBA belongs
	 * to owner instance 1.  It must not trigger a second current acquire. */
	test_origin_record.tt_slot_segment_id = 257;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 0);
}

UT_TEST(test_terminal_census_pending_acquire_failure_cancels_candidate_guard)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, &resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT_EQ(test_candidate_sample_calls, 0);
	UT_ASSERT_EQ(test_candidate_release_calls, 0);
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
	UT_ASSERT_EQ(test_tt_snapshot_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

UT_TEST(test_exact_origin_aborted_uses_exact_tt_and_direct_clog)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
							 sizeof(test_origin_locator)),
				 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_tt_segment_seen, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_tt_slot_seen, TEST_TT_OFFSET);
}

UT_TEST(test_exact_origin_conflicting_terminal_evidence_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_ABORTED;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

UT_TEST(test_exact_origin_active_and_native_in_progress_stays_live)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(resolution.horizon_scn, InvalidScn);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
							 sizeof(test_origin_locator)),
				 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

static void
set_native_status_sample(int index, TransactionId xid, XidStatus status)
{
	UT_ASSERT(index >= 0 && index < (int)lengthof(test_native_script_xids));
	test_native_script_xids[index] = xid;
	test_native_script_statuses[index] = status;
	if (test_native_script_count <= index)
		test_native_script_count = index + 1;
}

static void
set_subtrans_chain3(TransactionId child, TransactionId parent, TransactionId top)
{
	test_subtrans_chain[0] = child;
	test_subtrans_chain[1] = parent;
	test_subtrans_chain[2] = top;
	test_subtrans_chain_count = 3;
}

UT_TEST(test_exact_origin_prepared_is_distinct_live_outcome)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_TWOPHASE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_prepared_finish_abort_terminal_wins)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_ABORTED);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_prepared_finish_commit_without_exact_scn_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_COMMITTED);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_does_not_beat_direct_prepared_owner)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_TWOPHASE);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_without_direct_native_terminal_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_direct_second_clog_abort_is_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_ABORTED);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_direct_second_clog_commit_conflicts)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_COMMITTED);
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_does_not_beat_subtrans_prepared_owner)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_without_subtrans_native_terminal_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_subtrans_second_clog_abort_is_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_ABORTED);
	test_twophase_prepared = true;
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_subtrans_second_clog_commit_conflicts)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_COMMITTED);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_nested_subcommitted_top_commit_without_locator_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId parent = TEST_ORIGIN_XID - 1;
	TransactionId top = TEST_ORIGIN_XID - 2;

	reset_exact_origin_fixture();
	set_subtrans_chain3(TEST_ORIGIN_XID, parent, top);
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(2, top, TRANSACTION_STATUS_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 6);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subcommitted_top_aborted_is_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_ABORTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subcommitted_top_in_progress_stays_live)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_subcommitted_top_prepared_stays_live)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_subtrans_edge_change_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	test_subtrans_mutate_recheck = true;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_SUBTRANS_CHANGED);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
}

UT_TEST(test_exact_origin_subtrans_cycle_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_cycle = true;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_SUBTRANS_CYCLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subcommitted_before_transaction_xmin_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	TransactionXmin = TEST_ORIGIN_XID + 1;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 0);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subtrans_depth_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId child = 5000;
	int i;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_origin_locator.xid = child;
	test_origin_record.xid = child;
	test_tt_slot.xid = child;
	for (i = 0; i <= CLUSTER_R4_SUBTRANS_MAX_DEPTH; i++)
		test_subtrans_chain[i] = child - i;
	test_subtrans_chain_count = CLUSTER_R4_SUBTRANS_MAX_DEPTH + 1;
	set_native_status_sample(0, child, TRANSACTION_STATUS_SUB_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_SUBTRANS_DEPTH);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_subtrans_parent_calls, CLUSTER_R4_SUBTRANS_MAX_DEPTH);
}

UT_TEST(test_exact_origin_subtrans_max_chain_is_rechecked_once_per_edge)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId child = 5000;
	TransactionId top;
	int i;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_origin_locator.xid = child;
	test_origin_record.xid = child;
	test_tt_slot.xid = child;
	for (i = 0; i < CLUSTER_R4_SUBTRANS_MAX_DEPTH; i++)
		test_subtrans_chain[i] = child - i;
	test_subtrans_chain_count = CLUSTER_R4_SUBTRANS_MAX_DEPTH;
	top = test_subtrans_chain[CLUSTER_R4_SUBTRANS_MAX_DEPTH - 1];
	set_native_status_sample(0, child, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_ABORTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 2 * CLUSTER_R4_SUBTRANS_MAX_DEPTH);
}

int
main(void)
{
	UT_PLAN(74);
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
	UT_RUN(test_terminal_census_local_origin_uses_resident_candidate2_and_canonical_upgrade);
	UT_RUN(test_terminal_census_same_owner_cross_segment_is_sequential_and_exact);
	UT_RUN(test_visibility_same_owner_cross_segment_is_sequential_and_exact);
	UT_RUN(test_visibility_precommit_committed_slot_with_live_origin_stays_in_progress);
	UT_RUN(test_terminal_census_rejects_precommit_committed_slot_with_live_origin);
	UT_RUN(test_terminal_census_cross_segment_data_drift_fails_closed);
	UT_RUN(test_terminal_census_cross_owner_tt_alias_fails_closed);
	UT_RUN(test_terminal_census_pending_acquire_failure_cancels_candidate_guard);
	UT_RUN(test_exact_origin_bad_record_wrap_fails_before_tt_or_clog);
	UT_RUN(test_exact_origin_aborted_uses_exact_tt_and_direct_clog);
	UT_RUN(test_exact_origin_conflicting_terminal_evidence_fails_closed);
	UT_RUN(test_exact_origin_active_and_native_in_progress_stays_live);
	UT_RUN(test_exact_origin_prepared_is_distinct_live_outcome);
	UT_RUN(test_exact_origin_prepared_finish_abort_terminal_wins);
	UT_RUN(test_exact_origin_prepared_finish_commit_without_exact_scn_fails_closed);
	UT_RUN(test_exact_origin_tt_aborted_does_not_beat_direct_prepared_owner);
	UT_RUN(test_exact_origin_tt_aborted_without_direct_native_terminal_fails_closed);
	UT_RUN(test_exact_origin_tt_aborted_direct_second_clog_abort_is_terminal);
	UT_RUN(test_exact_origin_tt_aborted_direct_second_clog_commit_conflicts);
	UT_RUN(test_exact_origin_tt_aborted_does_not_beat_subtrans_prepared_owner);
	UT_RUN(test_exact_origin_tt_aborted_without_subtrans_native_terminal_fails_closed);
	UT_RUN(test_exact_origin_tt_aborted_subtrans_second_clog_abort_is_terminal);
	UT_RUN(test_exact_origin_tt_aborted_subtrans_second_clog_commit_conflicts);
	UT_RUN(test_exact_origin_nested_subcommitted_top_commit_without_locator_fails_closed);
	UT_RUN(test_exact_origin_subcommitted_top_aborted_is_terminal);
	UT_RUN(test_exact_origin_subcommitted_top_in_progress_stays_live);
	UT_RUN(test_exact_origin_subcommitted_top_prepared_stays_live);
	UT_RUN(test_exact_origin_subtrans_edge_change_fails_closed);
	UT_RUN(test_exact_origin_subtrans_cycle_fails_closed);
	UT_RUN(test_exact_origin_subcommitted_before_transaction_xmin_fails_closed);
	UT_RUN(test_exact_origin_subtrans_depth_fails_closed);
	UT_RUN(test_exact_origin_subtrans_max_chain_is_rechecked_once_per_edge);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
