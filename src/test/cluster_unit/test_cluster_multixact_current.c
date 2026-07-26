/*-------------------------------------------------------------------------
 *
 * test_cluster_multixact_current.c
 *	  ABI and public-symbol gates for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_multixact_current.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_mxid_stripe.h"
#include "storage/lock.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

pg_attribute_noreturn() void
pg_re_throw(void)
{
	abort();
}

static char *
read_source(const char *path)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static char *
read_current_multixact_source(void)
{
	return read_source(CLUSTER_MULTIXACT_CURRENT_SOURCE_PATH);
}

static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	const char *cursor = source;

	while ((cursor = strstr(cursor, needle)) != NULL) {
		count++;
		cursor += strlen(needle);
	}
	return count;
}


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * The production describe entry adds runtime routing in D2.  This standalone
 * pure-unit binary supplies fail-closed seams so it can continue linking the
 * decision object without shmem, pg_multixact SLRU, or DATA transport.
 */
int cluster_node_id = -1;
int cluster_subtrans_max_chain_depth = 32;
bool cluster_multixact_current_dml = true;
BackendId MyBackendId = 7;
static uint64 test_runtime_epoch;
static int test_runtime_mxid_origin = -1;
static int test_runtime_xid_origin = -1;
static bool test_runtime_xid_origin_by_value;
static bool test_runtime_mxid_mine;
static int test_runtime_native_describe_calls;
static int test_runtime_remote_describe_calls;
static bool test_runtime_remote_describe_ok;
static int test_runtime_proof_calls;
static int test_runtime_proof_fail_call;
static ClusterTTCurrentKeyVerdict test_runtime_candidate_verdict
	= CLUSTER_TT_CURRENT_KEY_UNKNOWN;
static ClusterTTStatusKey test_runtime_candidate_current_key;
static ClusterTTStatusResult test_runtime_candidate_current_result;

static void test_member(ClusterCurrentMxMemberDesc *member, TransactionId xid, uint8 status);
static void test_proof(ClusterCurrentMemberProof *proof,
					   const ClusterCurrentMxMemberDesc *member, uint16 ordinal,
					   ClusterCurrentMemberState state, uint16 origin, uint32 slot);

void
pfree(void *pointer)
{
	free(pointer);
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat pg_attribute_unused())
{
}

uint64
cluster_epoch_get_current(void)
{
	return test_runtime_epoch;
}

int
cluster_mxid_origin_slot(MultiXactId mxid pg_attribute_unused())
{
	return test_runtime_mxid_origin;
}

bool
cluster_mxid_is_mine(MultiXactId mxid pg_attribute_unused())
{
	return test_runtime_mxid_mine;
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	if (test_runtime_xid_origin_by_value)
		return 4 + (int)(xid % 3);
	return test_runtime_xid_origin;
}

int
GetMultiXactIdMembers(MultiXactId multi pg_attribute_unused(), MultiXactMember **members,
					  bool from_pgupgrade pg_attribute_unused(),
					  bool isLockOnly pg_attribute_unused())
{
	MultiXactMember *out;

	test_runtime_native_describe_calls++;
	if (members == NULL)
		return -1;
	out = (MultiXactMember *)malloc(2 * sizeof(*out));
	memset(out, 0, 2 * sizeof(*out));
	out[0].xid = 100;
	out[0].status = MultiXactStatusForShare;
	out[1].xid = 101;
	out[1].status = MultiXactStatusNoKeyUpdate;
	*members = out;
	return 2;
}

ClusterMxDescribeResult
cluster_gcs_current_mx_describe_fetch_and_wait(
	int32 origin_node pg_attribute_unused(), const ClusterCurrentMxKey *key pg_attribute_unused(),
	ClusterCurrentMxMemberDesc *members pg_attribute_unused(),
	uint16 members_cap pg_attribute_unused(), uint16 *members_count pg_attribute_unused(),
	uint32 *reported_total_members pg_attribute_unused())
{
	test_runtime_remote_describe_calls++;
	if (!test_runtime_remote_describe_ok)
		return CMX_DESC_UNKNOWN;
	if (members == NULL || members_count == NULL || reported_total_members == NULL
		|| members_cap < 2)
		return CMX_DESC_UNKNOWN;
	memset(members, 0, 2 * sizeof(*members));
	members[0].xid = 200;
	members[0].member_status = MultiXactStatusForShare;
	members[1].xid = 201;
	members[1].member_status = MultiXactStatusNoKeyUpdate;
	*members_count = 2;
	*reported_total_members = 2;
	return CMX_DESC_OK;
}

bool
cluster_tt_status_lookup_current_own_xid(TransactionId xid pg_attribute_unused(),
										 ClusterTTStatusKey *key,
										 ClusterTTStatusResult *result)
{
	if (key != NULL)
		memset(key, 0, sizeof(*key));
	if (result != NULL) {
		memset(result, 0, sizeof(*result));
		result->status = CLUSTER_TT_STATUS_UNKNOWN;
	}
	return false;
}

ClusterTTCurrentKeyVerdict
cluster_tt_status_lookup_current_own_xid_candidate(
	TransactionId xid pg_attribute_unused(),
	const ClusterTTStatusKey *candidate pg_attribute_unused(), ClusterTTStatusKey *key,
	ClusterTTStatusResult *result)
{
	if (key != NULL)
		*key = test_runtime_candidate_current_key;
	if (result != NULL)
		*result = test_runtime_candidate_current_result;
	return test_runtime_candidate_verdict;
}

bool
cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key pg_attribute_unused(),
							   ClusterTTStatusResult *result)
{
	if (result != NULL) {
		memset(result, 0, sizeof(*result));
		result->status = CLUSTER_TT_STATUS_UNKNOWN;
	}
	return false;
}

bool
TransactionIdIsCurrentTransactionId(TransactionId xid pg_attribute_unused())
{
	return false;
}

ClusterMxResolveResult
cluster_gcs_current_mx_member_proof_fetch_and_wait(
	int32 origin_node, ClusterCurrentMxProofForwardV2 *request,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof)
{
	ClusterCurrentMxProofForwardV2 decoded;
	uint8 i;

	test_runtime_proof_calls++;
	if (proof_count != NULL)
		*proof_count = 0;
	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (test_runtime_proof_fail_call == test_runtime_proof_calls
		|| !cluster_multixact_current_wire_validate_proof_forward(
			request, sizeof(*request), request->prefix.original_requester_node, origin_node,
			request->prefix.epoch, &decoded))
		return CMX_RESOLVE_UNKNOWN;

	if (decoded.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
		if (proofs == NULL || proof_count == NULL
			|| proofs_cap < decoded.prefix.entry_count)
			return CMX_RESOLVE_UNKNOWN;
		for (i = 0; i < decoded.prefix.entry_count; i++) {
			ClusterCurrentMxMemberDesc member;

			test_member(&member, decoded.trailer.body.asks[i].xid,
						decoded.trailer.body.asks[i].member_status);
			test_proof(&proofs[i], &member, decoded.trailer.body.asks[i].member_ordinal,
					   CCM_ABORTED, origin_node, 20 + i);
		}
		*proof_count = decoded.prefix.entry_count;
		return CMX_RESOLVE_OK;
	}

	if (proofs == NULL || proof_count == NULL || proofs_cap < 1 || updater_proof == NULL)
		return CMX_RESOLVE_UNKNOWN;
	{
		ClusterCurrentMxMemberDesc member;

		test_member(&member, decoded.trailer.body.updater.challenge.updater_xid,
					decoded.trailer.body.updater.challenge.member_status);
		test_proof(&proofs[0], &member,
				   decoded.trailer.body.updater.challenge.member_ordinal, CCM_COMMITTED,
				   origin_node, 20);
	}
	*proof_count = 1;
	updater_proof->mxkey = decoded.prefix.mxkey;
	updater_proof->candidate_next_xmin_key
		= decoded.trailer.body.updater.challenge.candidate_next_xmin_key;
	updater_proof->updater_xid = decoded.trailer.body.updater.challenge.updater_xid;
	updater_proof->member_ordinal = decoded.trailer.body.updater.challenge.member_ordinal;
	updater_proof->verdict = CUCP_MATCH;
	return CMX_RESOLVE_OK;
}


/*
 * Standalone unit reference for PostgreSQL's LockConflicts[] source.  The
 * production object resolves this symbol to lock.c.
 */
bool
DoLockModesConflict(LOCKMODE mode1, LOCKMODE mode2)
{
	switch (mode1) {
	case AccessShareLock:
		return mode2 == AccessExclusiveLock;
	case RowShareLock:
		return mode2 == ExclusiveLock || mode2 == AccessExclusiveLock;
	case ExclusiveLock:
		return mode2 == RowShareLock || mode2 == ExclusiveLock || mode2 == AccessExclusiveLock;
	case AccessExclusiveLock:
		return true;
	default:
		return true;
	}
}

bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	return id1 < id2;
}


/*
 * Compile-time ABI gates.  Wire structs keep explicit reserved fields so
 * every receiver can reject nonzero future semantics.
 */
StaticAssertDecl(sizeof(ClusterCurrentMxKey) == 16, "current MX key must remain 16 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, origin_node_id) == 0,
				 "current MX key origin offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, reserved16) == 2,
				 "current MX key reserved16 offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, multixact_id) == 4,
				 "current MX key mxid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, cluster_epoch) == 8,
				 "current MX key epoch offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, reserved32) == 12,
				 "current MX key reserved32 offset changed");

StaticAssertDecl(sizeof(ClusterCurrentMxMemberDesc) == 8,
				 "current MX descriptor entry must remain 8 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxMemberDesc, xid) == 0,
				 "current MX descriptor xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxMemberDesc, member_status) == 4,
				 "current MX descriptor status offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxMemberDesc, reserved8) == 5,
				 "current MX descriptor reserved offset changed");

StaticAssertDecl(sizeof(ClusterCurrentMemberProof) == 48,
				 "current MX member proof must remain 48 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, key) == 0,
				 "current MX proof key offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, commit_scn) == 24,
				 "current MX proof commit SCN offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, member_xid) == 32,
				 "current MX proof xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, member_ordinal) == 36,
				 "current MX proof ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, member_status) == 38,
				 "current MX proof status offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, state) == 39,
				 "current MX proof state offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, reserved8) == 40,
				 "current MX proof reserved offset changed");

StaticAssertDecl(sizeof(ClusterCurrentUpdaterProof) == 48,
				 "current MX updater proof must remain 48 bytes");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, mxkey) == 0,
				 "current MX updater proof mxkey offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, candidate_next_xmin_key) == 16,
				 "current MX updater proof candidate offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, updater_xid) == 40,
				 "current MX updater proof xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, member_ordinal) == 44,
				 "current MX updater proof ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, verdict) == 46,
				 "current MX updater proof verdict offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, reserved8) == 47,
				 "current MX updater proof reserved offset changed");

StaticAssertDecl(sizeof(ClusterCurrentUpdaterChallenge) == 32,
				 "current MX updater challenge must remain 32 bytes");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, candidate_next_xmin_key) == 0,
				 "current MX updater challenge key offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, updater_xid) == 24,
				 "current MX updater challenge xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, member_ordinal) == 28,
				 "current MX updater challenge ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, reserved16) == 30,
				 "current MX updater challenge reserved offset changed");

StaticAssertDecl(CLUSTER_CURRENT_MX_MAX_MEMBERS == 256,
				 "current MX positive-path member limit changed");
StaticAssertDecl(CLUSTER_CURRENT_MX_MAX_CHUNKS == 256
					 && CLUSTER_CURRENT_MX_MAX_PROOFS_PER_CHUNK == 32,
				 "current MX proof chunk limits changed");
StaticAssertDecl(CCM_SELF == 0 && CCM_ACTIVE == 1 && CCM_COMMITTED == 2 && CCM_ABORTED == 3
					 && CCM_UNKNOWN == 4,
				 "current MX member-state values changed");
StaticAssertDecl(CMX_DESC_OK == 0 && CMX_DESC_DENIED == 1 && CMX_DESC_SUPPORTED_LIMIT == 2
					 && CMX_DESC_TIMEOUT == 3 && CMX_DESC_UNKNOWN == 4,
				 "current MX describe-result endpoints changed");
StaticAssertDecl(CMX_RESOLVE_OK == 0 && CMX_RESOLVE_DENIED == 1 && CMX_RESOLVE_SUPPORTED_LIMIT == 2
					 && CMX_RESOLVE_TIMEOUT == 3 && CMX_RESOLVE_UNKNOWN == 4,
				 "current MX resolve-result endpoints changed");
StaticAssertDecl(CCMUPO_COMMITTED == 0 && CCMUPO_WAIT_MEMBER == 1
					 && CCMUPO_FAIL_CLOSED == 2,
				 "requester-local updater-proof outcomes changed");
StaticAssertDecl(CCM_ACTION_UPDATE == 0 && CCM_ACTION_DELETE == 1 && CCM_ACTION_LOCK == 2
					 && CCM_ACTION_HOT_FOLLOW == 3,
				 "current MX action values changed");
StaticAssertDecl(CCM_SHAPE_LOCK_ONLY == 0 && CCM_SHAPE_UPDATED == 1 && CCM_SHAPE_DELETED == 2,
				 "current MX tuple-shape values changed");
StaticAssertDecl(CUCP_MATCH == 0 && CUCP_MISMATCH == 1 && CUCP_UNKNOWN == 2,
				 "current MX updater verdict values changed");
StaticAssertDecl(CMDL_CONTINUE == 0 && CMDL_INVISIBLE == 1 && CMDL_SELF_MODIFIED == 2
						 && CMDL_BEING_MODIFIED == 3 && CMDL_WAIT_MEMBER == 4 && CMDL_WOULD_BLOCK == 5
						 && CMDL_LOCK_NOT_AVAILABLE == 6 && CMDL_UPDATED == 7 && CMDL_DELETED == 8
						 && CMDL_UNKNOWN == 9,
				 "current MX decision values changed");
StaticAssertDecl(sizeof(ClusterCurrentMxRoutingPrefixWire) == 64,
				 "current MX routing prefix must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxRoutingPrefixWire, original_requester_node) == 36,
				 "current MX requester node offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxRoutingPrefixWire, requester_backend_id) == 40,
				 "current MX requester backend offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxRoutingPrefixWire, kind) == 63,
				 "current MX kind offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribePrefixWire) == 64,
				 "current MX describe prefix must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribePrefixWire, mxkey) == 16,
				 "current MX describe key offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeTrailerWire) == 64,
				 "current MX describe trailer must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeForwardV2) == 128,
				 "current MX describe V2 must remain 128 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeForwardV2, trailer) == 64,
				 "current MX describe trailer offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofAskWire) == 8,
				 "current MX proof ask must remain 8 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterChallengeWire) == 32,
				 "current MX updater challenge wire must remain 32 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofRequestBodyWire) == 56,
				 "current MX proof body must remain 56 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofTrailerWire) == 64,
				 "current MX proof trailer must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofForwardV2) == 128,
				 "current MX proof V2 must remain 128 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, mxkey) == 16,
				 "current MX proof key offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, total_count) == 44,
				 "current MX proof total offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, chunk_ordinal) == 48,
				 "current MX proof chunk ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, descriptor_hash_bytes) == 49,
				 "current MX proof hash offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, chunk_count_minus_one) == 57,
				 "current MX proof chunk count offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, kind) == 63,
				 "current MX proof kind offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeReplyHeader) == 64,
				 "current MX describe reply header must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyHeader, entry_count) == 52,
				 "current MX reply count offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyHeader, wire_length) == 56,
				 "current MX reply wire length offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeReplyPage) == BLCKSZ,
				 "current MX describe reply page must remain BLCKSZ");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyPage, members) == 64,
				 "current MX describe members offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyHeader) == 64,
				 "current MX proof reply header must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyHeader, entry_count) == 52,
				 "current MX proof reply count offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyHeader, wire_length) == 56,
				 "current MX proof reply wire length offset changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyBodyWire)
					 == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
							* sizeof(ClusterCurrentMemberProof),
				 "current MX proof reply body size changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyPage) == BLCKSZ,
				 "current MX proof reply page must remain BLCKSZ");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyPage, body) == 64,
				 "current MX proof reply body offset changed");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT == 21,
				 "current MX describe reply status must append at 21");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT == 22,
				 "current MX member-proof reply status must append at 22");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_STATS_RESULT == 23,
				 "current MX stats reply status must append at 23");


UT_TEST(test_current_multixact_public_symbols_link)
{
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_descriptor);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_proof_set);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_resolve_origin_member_proof);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_updater_candidate_verdict);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_descriptor_hash);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_updater_proof_outcome);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_updater_proof);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_decide);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_describe);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_members_resolve);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_recompose);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_forward);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_build_proof_requests);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_reply);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_reply_frame);
}

static ClusterCurrentMxKey
test_mxkey(void)
{
	ClusterCurrentMxKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 2;
	key.multixact_id = 17;
	key.cluster_epoch = 9;
	return key;
}


static ClusterTTStatusKey
test_ttkey(uint16 origin, TransactionId xid, uint32 slot)
{
	ClusterTTStatusKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = origin;
	key.undo_segment_id = 1;
	key.tt_slot_id = slot;
	key.cluster_epoch = 9;
	key.local_xid = xid;
	return key;
}


static void
test_member(ClusterCurrentMxMemberDesc *member, TransactionId xid, uint8 status)
{
	memset(member, 0, sizeof(*member));
	member->xid = xid;
	member->member_status = status;
}


static void
test_proof(ClusterCurrentMemberProof *proof, const ClusterCurrentMxMemberDesc *member,
		   uint16 ordinal, ClusterCurrentMemberState state, uint16 origin, uint32 slot)
{
	memset(proof, 0, sizeof(*proof));
	proof->member_xid = member->xid;
	proof->member_ordinal = ordinal;
	proof->member_status = member->member_status;
	proof->state = state;

	if (state == CCM_SELF || state == CCM_ACTIVE)
		proof->key = test_ttkey(origin, member->xid, slot);
	else if (state == CCM_COMMITTED)
		proof->commit_scn = 100 + ordinal;
}


static void
test_updater_proof_fixture(
	ClusterCurrentMxKey *key,
	ClusterCurrentMxMemberDesc members[2],
	ClusterCurrentMemberProof proofs[2],
	ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentUpdaterProof *updater_proof,
	ClusterCurrentMemberState updater_state)
{
	*key = test_mxkey();
	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, updater_state, 3, 51);

	memset(challenge, 0, sizeof(*challenge));
	challenge->candidate_next_xmin_key = test_ttkey(3, 101, 61);
	challenge->updater_xid = 101;
	challenge->member_ordinal = 1;

	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->mxkey = *key;
	updater_proof->candidate_next_xmin_key
		= challenge->candidate_next_xmin_key;
	updater_proof->updater_xid = challenge->updater_xid;
	updater_proof->member_ordinal = challenge->member_ordinal;
	updater_proof->verdict = CUCP_MATCH;
}


static void
assert_updater_proof_fail_closed(
	ClusterMxResolveResult resolve_result,
	const ClusterCurrentMxKey *key,
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof,
	uint16 updater_origin_node_id)
{
	ClusterTTStatusKey wait_key;
	ClusterTTStatusKey zero_key;

	memset(&wait_key, 0xa5, sizeof(wait_key));
	memset(&zero_key, 0, sizeof(zero_key));
	UT_ASSERT_EQ(cluster_multixact_current_updater_proof_outcome(
					 resolve_result, key, members, proofs, 2, challenge,
					 updater_proof, updater_origin_node_id, &wait_key),
				 CCMUPO_FAIL_CLOSED);
	UT_ASSERT_EQ(memcmp(&wait_key, &zero_key, sizeof(wait_key)), 0);
}


static void
test_proof_request(ClusterCurrentMxProofForwardV2 *request, uint8 body_kind, uint8 entry_count,
				   uint8 chunk_ordinal, uint8 chunk_count_minus_one)
{
	ClusterCurrentMxKey key = test_mxkey();
	uint8 i;

	memset(request, 0, sizeof(*request));
	request->prefix.request_id = 700;
	request->prefix.epoch = 9;
	request->prefix.mxkey = key;
	request->prefix.original_requester_node = 3;
	request->prefix.requester_backend_id = 7;
	request->prefix.total_count = 8;
	request->prefix.chunk_ordinal = chunk_ordinal;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&request->prefix,
												 UINT64CONST(0x8877665544332211));
	request->prefix.chunk_count_minus_one = chunk_count_minus_one;
	request->prefix.entry_count = entry_count;
	request->prefix.body_kind = body_kind;
	request->prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	request->trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request->trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;

	if (body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
		for (i = 0; i < entry_count; i++) {
			request->trailer.body.asks[i].xid = (TransactionId)(100 + i);
			request->trailer.body.asks[i].member_ordinal = i;
			request->trailer.body.asks[i].member_status = MultiXactStatusForShare;
		}
	} else {
		request->trailer.body.updater.challenge.candidate_next_xmin_key
			= test_ttkey(5, 100, 20);
		request->trailer.body.updater.challenge.updater_xid = 100;
		request->trailer.body.updater.challenge.member_ordinal = 0;
		request->trailer.body.updater.challenge.member_status
			= MultiXactStatusNoKeyUpdate;
	}
}


typedef struct TestExactLookupEntry {
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
} TestExactLookupEntry;


typedef struct TestExactLookupTable {
	TestExactLookupEntry entries[8];
	uint16 count;
	uint16 calls;
} TestExactLookupTable;


static bool
test_exact_lookup(const ClusterTTStatusKey *key, ClusterTTStatusResult *result, void *arg)
{
	TestExactLookupTable *table = (TestExactLookupTable *)arg;
	uint16 i;

	table->calls++;
	for (i = 0; i < table->count; i++)
		if (memcmp(key, &table->entries[i].key, sizeof(*key)) == 0) {
			*result = table->entries[i].result;
			return true;
		}
	memset(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	return false;
}


UT_TEST(test_current_multixact_descriptor_validation)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];

	test_member(&members[0], 100, MultiXactStatusForShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);

	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_OK);
	key.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 0, members, 2, 2),
				 CMX_DESC_OK);
	key.cluster_epoch = 9;
	UT_ASSERT_EQ(cluster_multixact_current_descriptor_hash(&key, members, 2),
				 UINT64CONST(3535096824512523990));
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 1, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 10, members, 2, 2),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, NULL, 0, 0),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 1, 1),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, NULL, 0, 257),
				 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 257),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 3),
				 CMX_DESC_DENIED);

	members[1].xid = members[0].xid;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[1].xid = 101;

	members[1].member_status = MaxMultiXactStatus + 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[1].member_status = MultiXactStatusNoKeyUpdate;

	members[0].member_status = MultiXactStatusUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[0].member_status = MultiXactStatusForShare;

	members[0].reserved8[2] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[0].reserved8[2] = 0;

	key.reserved32 = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	key = test_mxkey();
	key.origin_node_id = 200;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 200, 9, members, 2, 2),
				 CMX_DESC_DENIED);
}


UT_TEST(test_current_multixact_descriptor_accepts_cap_and_hashes_order)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint64 hash;
	uint16 i;

	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
		test_member(&members[i], (TransactionId)(1000 + i), MultiXactStatusForKeyShare);

	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members,
															   CLUSTER_CURRENT_MX_MAX_MEMBERS,
															   CLUSTER_CURRENT_MX_MAX_MEMBERS),
				 CMX_DESC_OK);

	hash = cluster_multixact_current_descriptor_hash(&key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_NE(hash, 0);
	UT_ASSERT_EQ(hash, cluster_multixact_current_descriptor_hash(&key, members,
																 CLUSTER_CURRENT_MX_MAX_MEMBERS));

	{
		ClusterCurrentMxMemberDesc tmp = members[0];

		members[0] = members[1];
		members[1] = tmp;
	}
	UT_ASSERT_NE(hash, cluster_multixact_current_descriptor_hash(&key, members,
																 CLUSTER_CURRENT_MX_MAX_MEMBERS));
}


UT_TEST(test_current_multixact_proof_binding_and_order)
{
	struct {
		ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
		uint64 canary;
		uint8 guard[64];
	} capped_output;
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proof01[2];
	ClusterCurrentMemberProof proof2[1];
	ClusterCurrentMemberProof ordered[3];
	ClusterCurrentProofChunkView chunks[3];
	ClusterCurrentUpdaterProof limit_updater_proof;
	uint16 member_origins[3] = { 4, 5, 6 };
	uint64 hash;

	test_member(&members[0], 100, MultiXactStatusForShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_member(&members[2], 102, MultiXactStatusForKeyShare);
	hash = cluster_multixact_current_descriptor_hash(&key, members, 3);

	test_proof(&proof01[0], &members[0], 0, CCM_ACTIVE, 4, 20);
	test_proof(&proof01[1], &members[1], 1, CCM_COMMITTED, 5, 21);
	test_proof(&proof2[0], &members[2], 2, CCM_ABORTED, 6, 22);

	memset(chunks, 0, sizeof(chunks));
	chunks[0].request_id = 77;
	chunks[0].mxkey = key;
	chunks[0].descriptor_hash = hash;
	chunks[0].total_count = 3;
	chunks[0].source_node_id = 4;
	chunks[0].chunk_ordinal = 0;
	chunks[0].chunk_count = 3;
	chunks[0].proof_count = 1;
	chunks[0].proofs = proof01;
	chunks[1] = chunks[0];
	chunks[1].source_node_id = 5;
	chunks[1].chunk_ordinal = 1;
	chunks[1].proof_count = 1;
	chunks[1].proofs = &proof01[1];
	chunks[2] = chunks[0];
	chunks[2].source_node_id = 6;
	chunks[2].chunk_ordinal = 2;
	chunks[2].proof_count = 1;
	chunks[2].proofs = proof2;

	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(ordered[0].member_xid, 100);
	UT_ASSERT_EQ(ordered[1].member_xid, 101);
	UT_ASSERT_EQ(ordered[2].member_xid, 102);

	/* SUBCOMMITTED keeps the child echo but ACTIVE authority names the final
	 * parent holder key.  Aggregation must preserve that wait identity. */
	proof01[0].key.local_xid = 90;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(ordered[0].member_xid, 100);
	UT_ASSERT_EQ(ordered[0].key.local_xid, 90);
	proof01[0].key.local_xid = 110;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[0].key.local_xid = 90;

	chunks[0].request_id++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(ordered[0].member_xid, InvalidTransactionId);
	UT_ASSERT_EQ(ordered[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[1].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[2].state, CCM_UNKNOWN);

	memset(&capped_output, 0xa5, sizeof(capped_output));
	capped_output.canary = UINT64CONST(0x1122334455667788);
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(
					 &key, members, member_origins, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, 77, hash,
					 chunks, 3, capped_output.proofs),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(capped_output.canary, UINT64CONST(0x1122334455667788));

	memset(&capped_output, 0xa5, sizeof(capped_output));
	capped_output.canary = UINT64CONST(0x8877665544332211);
	memset(&limit_updater_proof, 0, sizeof(limit_updater_proof));
	limit_updater_proof.verdict = CUCP_MATCH;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
					 &key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, hash, NULL,
					 capped_output.proofs, &limit_updater_proof),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(capped_output.canary, UINT64CONST(0x8877665544332211));
	UT_ASSERT_EQ(limit_updater_proof.verdict, CUCP_UNKNOWN);
	chunks[0].request_id--;

	chunks[0].descriptor_hash++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[0].descriptor_hash--;

	chunks[0].mxkey.multixact_id++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[0].mxkey.multixact_id--;

	chunks[1].total_count++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].total_count--;

	chunks[1].chunk_count++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].chunk_count--;

	chunks[1].source_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].source_node_id = 5;

	chunks[1].chunk_ordinal = 0;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].chunk_ordinal = 1;

	proof2[0].member_ordinal = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof2[0].member_ordinal = 2;

	proof01[1].member_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[1].member_xid--;

	proof01[1].member_status = MultiXactStatusUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[1].member_status = MultiXactStatusNoKeyUpdate;

	proof01[0].key._reserved = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(ordered[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[1].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[2].state, CCM_UNKNOWN);

	memset(ordered, 0xff, sizeof(ordered));
	UT_ASSERT_EQ(
		cluster_multixact_current_members_resolve(&key, members, 3, hash, NULL, ordered, NULL),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(ordered[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[1].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[2].state, CCM_UNKNOWN);
}

UT_TEST(test_current_multixact_updater_candidate_requires_current_exact_binding)
{
	ClusterTTStatusKey current = test_ttkey(5, 100, 20);
	ClusterTTStatusKey candidate = current;
	ClusterTTStatusKey selected;
	ClusterTTStatusResult selected_result;

	cluster_node_id = 5;
	test_runtime_candidate_current_key = current;
	memset(&test_runtime_candidate_current_result, 0,
		   sizeof(test_runtime_candidate_current_result));
	test_runtime_candidate_current_result.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	test_runtime_candidate_current_result.status_epoch = 9;
	test_runtime_candidate_current_result.authoritative = true;
	test_runtime_candidate_verdict = CLUSTER_TT_CURRENT_KEY_MATCH;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_MATCH);
	current.cluster_epoch = 0;
	candidate.cluster_epoch = 0;
	test_runtime_candidate_current_key = current;
	test_runtime_candidate_current_result.status_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 0, &selected, &selected_result),
				 CUCP_MATCH);
	current.cluster_epoch = 9;
	candidate.cluster_epoch = 9;
	test_runtime_candidate_current_key = current;
	test_runtime_candidate_current_result.status_epoch = 9;

	/* A different current raw-xid slot does not prove whether the challenged
	 * full key was recycled; without exact retained provenance it is UNKNOWN. */
	test_runtime_candidate_verdict = CLUSTER_TT_CURRENT_KEY_UNKNOWN;
	candidate.tt_slot_id++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	test_runtime_candidate_verdict = CLUSTER_TT_CURRENT_KEY_UNKNOWN;
	candidate = current;
	candidate.undo_segment_id++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);

	candidate = current;
	candidate.local_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	candidate = current;
	candidate.cluster_epoch++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);

	/* A retained-overlay miss and an ambiguous current binding both remain
	 * UNKNOWN; neither may be synthesized from durable history. */
	candidate = current;
	memset(&test_runtime_candidate_current_key, 0,
		   sizeof(test_runtime_candidate_current_key));
	memset(&test_runtime_candidate_current_result, 0,
		   sizeof(test_runtime_candidate_current_result));
	test_runtime_candidate_current_result.status
		= CLUSTER_TT_STATUS_UNKNOWN;
	test_runtime_candidate_verdict = CLUSTER_TT_CURRENT_KEY_UNKNOWN;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	test_runtime_candidate_current_key = current;
	test_runtime_candidate_current_result.status
		= CLUSTER_TT_STATUS_UNKNOWN;
	test_runtime_candidate_current_result.status_epoch = 9;
	test_runtime_candidate_current_result.authoritative = true;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	cluster_node_id = -1;
}

UT_TEST(test_current_multixact_local_updater_uses_shared_retained_exact_verdict)
{
	char *source = read_current_multixact_source();
	const char *resolve;
	const char *helper;
	const char *remote_arm;
	const char *exact_fallback;

	if (source == NULL)
		return;
	resolve = strstr(source, "\ncluster_multixact_current_members_resolve(");
	helper = resolve != NULL
				 ? strstr(resolve, "cluster_multixact_current_updater_candidate_verdict(")
				 : NULL;
	remote_arm = helper != NULL
					 ? strstr(helper, "cluster_gcs_current_mx_member_proof_fetch_and_wait(")
					 : NULL;
	exact_fallback
		= helper != NULL ? strstr(helper, "cluster_tt_status_lookup_exact(") : NULL;
	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(remote_arm);
	if (remote_arm != NULL)
		UT_ASSERT(exact_fallback == NULL || exact_fallback > remote_arm);
	free(source);
}


UT_TEST(test_current_multixact_origin_subcommitted_exact_chain)
{
	ClusterTTStatusKey child_key = test_ttkey(5, 100, 20);
	ClusterTTStatusKey parent_key = test_ttkey(5, 90, 21);
	ClusterTTStatusResult initial;
	ClusterCurrentMemberProof proof;
	TestExactLookupTable table;

	memset(&initial, 0, sizeof(initial));
	initial.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	initial.authoritative = true;
	initial.has_parent_key = true;
	initial.parent_key = parent_key;
	initial.status_epoch = 9;

	memset(&table, 0, sizeof(table));
	table.entries[0].key = parent_key;
	table.entries[0].result.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	table.entries[0].result.authoritative = true;
	table.entries[0].result.status_epoch = 9;
	table.count = 1;

	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
	UT_ASSERT_EQ(proof.member_xid, 100);
	UT_ASSERT_EQ(proof.member_ordinal, 3);
	UT_ASSERT_EQ(proof.key.local_xid, 90);
	UT_ASSERT_EQ(proof.key.tt_slot_id, 21);
	UT_ASSERT_EQ(table.calls, 1);
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, true, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_SELF);
	UT_ASSERT_EQ(proof.member_xid, 100);
	UT_ASSERT_EQ(proof.key.local_xid, 90);

	table.entries[0].result.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	table.entries[0].result.has_parent_key = true;
	table.entries[0].result.parent_key = test_ttkey(5, 80, 22);
	table.entries[1].key = table.entries[0].result.parent_key;
	table.entries[1].result.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	table.entries[1].result.authoritative = true;
	table.entries[1].result.status_epoch = 9;
	table.count = 2;
	cluster_subtrans_max_chain_depth = 1;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 false);
	cluster_subtrans_max_chain_depth = 2;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
	UT_ASSERT_EQ(proof.key.local_xid, 80);
	cluster_subtrans_max_chain_depth = 32;
	table.count = 1;

	table.entries[0].result.status = CLUSTER_TT_STATUS_COMMITTED;
	table.entries[0].result.has_parent_key = false;
	table.entries[0].result.commit_scn = 500;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_COMMITTED);
	UT_ASSERT_EQ(proof.commit_scn, 500);
	UT_ASSERT_EQ(proof.key.local_xid, InvalidTransactionId);

	table.entries[0].result.status = CLUSTER_TT_STATUS_ABORTED;
	table.entries[0].result.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_ABORTED);

	table.entries[0].result.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	table.entries[0].result.has_parent_key = true;
	table.entries[0].result.parent_key = child_key;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 false);
	UT_ASSERT_EQ(proof.state, CCM_UNKNOWN);

	table.entries[0].result.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	table.entries[0].result.has_parent_key = false;
	initial.parent_key.origin_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 false);
	UT_ASSERT_EQ(proof.state, CCM_UNKNOWN);
	initial.parent_key = parent_key;

	initial.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	initial.has_parent_key = false;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, true, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_SELF);
	UT_ASSERT_EQ(proof.key.local_xid, 100);

	child_key.cluster_epoch = 0;
	initial.status_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 0, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
}


UT_TEST(test_current_multixact_proof_forward_wire_binding)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofForwardV2 decoded;

	test_runtime_xid_origin = 5;
	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 7, 0, 1);

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 true);
	UT_ASSERT_EQ(decoded.prefix.entry_count, 7);
	UT_ASSERT_EQ(ClusterCurrentMxProofPrefixGetDescriptorHash(&decoded.prefix),
				 UINT64CONST(0x8877665544332211));
	request.prefix.epoch = 0;
	request.prefix.mxkey.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(0), &decoded),
				 true);
	request.prefix.epoch = 9;
	request.prefix.mxkey.cluster_epoch = 9;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&request.prefix, 0);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 true);
	ClusterCurrentMxProofPrefixSetDescriptorHash(&request.prefix,
												 UINT64CONST(0x8877665544332211));

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request.prefix), 3, 5, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request) - 1, 3, 5, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request) + 1, 3, 5, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 4, 5, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 4, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(0x100000009), &decoded),
				 false);

	request.prefix.entry_count = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.prefix.entry_count = 7;
	request.trailer.body.asks[6].reserved8 = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.trailer.body.asks[6].reserved8 = 0;
	request.trailer.body.asks[6].xid = request.trailer.body.asks[0].xid;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.trailer.body.asks[6].xid = 106;
	request.trailer.body.asks[6].member_ordinal = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.trailer.body.asks[6].member_ordinal = 6;
	request.trailer.body.asks[6].member_status = MaxMultiXactStatus + 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.trailer.body.asks[6].member_status = MultiXactStatusForShare;
	request.prefix.chunk_ordinal = 2;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.prefix.chunk_ordinal = 0;
	request.prefix.chunk_count_minus_one = request.prefix.total_count;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.prefix.chunk_count_minus_one = 0;
	request.prefix.flags = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.prefix.flags = 0;
	request.prefix.reserved_b[1] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.prefix.reserved_b[1] = 0;
	request.trailer.body.asks[7 - 1].xid = 106;
	request.prefix.entry_count = 6;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE, 1, 1, 1);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 true);
	request.prefix.entry_count = 2;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.prefix.entry_count = 1;
	request.trailer.body.updater.reserved[23] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.trailer.body.updater.reserved[23] = 0;
	request.trailer.body.updater.challenge.member_status = MultiXactStatusForShare;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
	request.trailer.body.updater.challenge.member_status = MultiXactStatusNoKeyUpdate;
	request.trailer.body.updater.challenge.candidate_next_xmin_key.cluster_epoch++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, UINT64CONST(9), &decoded),
				 false);
}


UT_TEST(test_current_multixact_proof_request_batching_and_limit)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 origins[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMxProofRequestPlan plans[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	ClusterCurrentUpdaterChallenge challenge;
	bool seen[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 plan_count = 0;
	uint16 i;
	uint16 j;
	uint16 seen_count;
	uint64 hash;

	test_runtime_xid_origin = 5;
	test_runtime_xid_origin_by_value = false;
	for (i = 0; i < 7; i++) {
		test_member(&members[i], (TransactionId)(200 + i), MultiXactStatusForShare);
		origins[i] = 5;
	}
	hash = cluster_multixact_current_descriptor_hash(&key, members, 7);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, 7, hash, NULL, 800, 9, 3, 7, plans,
					 lengthof(plans), &plan_count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(plan_count, 1);
	UT_ASSERT_EQ(plans[0].destination_node_id, 5);
	UT_ASSERT_EQ(plans[0].request.prefix.entry_count, 7);
	UT_ASSERT_EQ(plans[0].request.prefix.chunk_count_minus_one, 0);

	test_runtime_xid_origin_by_value = true;
	for (i = 0; i < 8; i++) {
		test_member(&members[i], (TransactionId)(100 + i), MultiXactStatusForShare);
		origins[i] = (uint16)cluster_xid_origin_slot(members[i].xid);
	}
	hash = cluster_multixact_current_descriptor_hash(&key, members, 8);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, 8, hash, NULL, 801, 9, 3, 7, plans,
					 lengthof(plans), &plan_count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(plan_count, 3);
	memset(seen, 0, sizeof(seen));
	seen_count = 0;
	for (i = 0; i < plan_count; i++) {
		ClusterCurrentMxProofForwardV2 decoded;

		UT_ASSERT_EQ(plans[i].request.prefix.chunk_ordinal, i);
		UT_ASSERT_EQ(plans[i].request.prefix.chunk_count_minus_one, plan_count - 1);
		UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
						 &plans[i].request, sizeof(plans[i].request), 3,
						 plans[i].destination_node_id, 9, &decoded),
					 true);
		for (j = 0; j < plans[i].request.prefix.entry_count; j++) {
			uint16 ordinal = plans[i].request.trailer.body.asks[j].member_ordinal;

			UT_ASSERT(ordinal < 8);
			UT_ASSERT(!seen[ordinal]);
			seen[ordinal] = true;
			seen_count++;
		}
	}
	UT_ASSERT_EQ(seen_count, 8);

	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++) {
		test_member(&members[i], (TransactionId)(1000 + i), MultiXactStatusForShare);
		origins[i] = (uint16)cluster_xid_origin_slot(members[i].xid);
	}
	members[5].member_status = MultiXactStatusNoKeyUpdate;
	memset(&challenge, 0, sizeof(challenge));
	challenge.candidate_next_xmin_key
		= test_ttkey(origins[5], members[5].xid, 50);
	challenge.updater_xid = members[5].xid;
	challenge.member_ordinal = 5;
	hash = cluster_multixact_current_descriptor_hash(
		&key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, CLUSTER_CURRENT_MX_MAX_MEMBERS, hash, &challenge, 802,
					 9, 3, 7, plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(plan_count, 39);
	memset(seen, 0, sizeof(seen));
	seen_count = 0;
	for (i = 0; i < plan_count; i++) {
		ClusterCurrentMxProofForwardV2 decoded;

		UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
						 &plans[i].request, sizeof(plans[i].request), 3,
						 plans[i].destination_node_id, 9, &decoded),
					 true);
		if (plans[i].request.prefix.body_kind
			== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
			UT_ASSERT_EQ(plans[i].request.trailer.body.updater.challenge.member_ordinal, 5);
			UT_ASSERT(!seen[5]);
			seen[5] = true;
			seen_count++;
		} else {
			UT_ASSERT(plans[i].request.prefix.entry_count <= 7);
			for (j = 0; j < plans[i].request.prefix.entry_count; j++) {
				uint16 ordinal = plans[i].request.trailer.body.asks[j].member_ordinal;

				UT_ASSERT(ordinal < CLUSTER_CURRENT_MX_MAX_MEMBERS);
				UT_ASSERT(!seen[ordinal]);
				seen[ordinal] = true;
				seen_count++;
			}
		}
	}
	UT_ASSERT_EQ(seen_count, CLUSTER_CURRENT_MX_MAX_MEMBERS);

	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, hash, &challenge,
					 803, 9, 3, 7, plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(plan_count, 0);
	test_runtime_xid_origin_by_value = false;
}


UT_TEST(test_current_multixact_members_resolve_256_all_or_nothing)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;
	uint64 hash;
	uint16 i;

	cluster_node_id = 3;
	test_runtime_epoch = 9;
	test_runtime_xid_origin_by_value = true;
	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
		test_member(&members[i], (TransactionId)(1000 + i), MultiXactStatusForShare);
	members[5].member_status = MultiXactStatusNoKeyUpdate;
	memset(&challenge, 0, sizeof(challenge));
	challenge.candidate_next_xmin_key
		= test_ttkey((uint16)cluster_xid_origin_slot(members[5].xid), members[5].xid, 50);
	challenge.updater_xid = members[5].xid;
	challenge.member_ordinal = 5;
	hash = cluster_multixact_current_descriptor_hash(
		&key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS);

	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 0;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
					 &key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS, hash, &challenge, proofs,
					 &updater_proof),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(test_runtime_proof_calls, 39);
	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
		UT_ASSERT_EQ(proofs[i].state, i == 5 ? CCM_COMMITTED : CCM_ABORTED);
	UT_ASSERT_EQ(updater_proof.verdict, CUCP_MATCH);

	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater_proof, 0xa5, sizeof(updater_proof));
	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 2;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
					 &key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS, hash, &challenge, proofs,
					 &updater_proof),
				 CMX_RESOLVE_UNKNOWN);
	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
		UT_ASSERT_EQ(proofs[i].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(updater_proof.verdict, CUCP_UNKNOWN);

	test_runtime_proof_fail_call = 0;
	test_runtime_xid_origin_by_value = false;
	cluster_node_id = -1;
}


UT_TEST(test_current_multixact_proof_reply_wire_binding)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofReplyPage page;
	ClusterCurrentMemberProof out[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentUpdaterProof updater_out;
	ClusterMxResolveResult frame_result;
	uint16 out_count = 99;
	uint8 i;

	test_runtime_xid_origin = 5;
	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 7, 0, 1);
	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	page.header.result = CMX_RESOLVE_OK;
	page.header.source_node_id = 5;
	page.header.request_id = request.prefix.request_id;
	page.header.mxkey = request.prefix.mxkey;
	page.header.descriptor_hash
		= ClusterCurrentMxProofPrefixGetDescriptorHash(&request.prefix);
	page.header.total_count = request.prefix.total_count;
	page.header.entry_count = request.prefix.entry_count;
	page.header.chunk_ordinal = request.prefix.chunk_ordinal;
	page.header.chunk_count_minus_one = request.prefix.chunk_count_minus_one;
	page.header.wire_length
		= sizeof(page.header)
		  + request.prefix.entry_count * sizeof(ClusterCurrentMemberProof);
	for (i = 0; i < request.prefix.entry_count; i++) {
		ClusterCurrentMxMemberDesc member;

		test_member(&member, request.trailer.body.asks[i].xid,
					request.trailer.body.asks[i].member_status);
		test_proof(&page.body.proofs[i], &member, request.trailer.body.asks[i].member_ordinal,
				   CCM_ACTIVE, 5, 20 + i);
	}

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(out_count, 7);
	UT_ASSERT_EQ(out[6].member_xid, 106);
	UT_ASSERT_EQ(updater_out.verdict, CUCP_UNKNOWN);
	page.body.proofs[0].key.local_xid = 90;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(out[0].key.local_xid, 90);
	page.body.proofs[0].key.local_xid = 110;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	page.body.proofs[0].key.local_xid = 100;

	page.header.request_id++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(out_count, 0);
	UT_ASSERT_EQ(out[0].state, CCM_UNKNOWN);
	page.header.request_id--;
	page.header.descriptor_hash++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	page.header.descriptor_hash--;
	page.body.proofs[1].member_ordinal = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	page.body.proofs[1].member_ordinal = 1;
	page.body.proofs[1].member_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	page.body.proofs[1].member_xid--;
	page.header.wire_length--;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	page.header.wire_length++;
	((uint8 *)&page)[page.header.wire_length] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	((uint8 *)&page)[page.header.wire_length] = 0;

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	page.header.result = CMX_RESOLVE_TIMEOUT;
	page.header.source_node_id = 5;
	page.header.request_id = request.prefix.request_id;
	page.header.mxkey = request.prefix.mxkey;
	page.header.descriptor_hash
		= ClusterCurrentMxProofPrefixGetDescriptorHash(&request.prefix);
	page.header.total_count = request.prefix.total_count;
	page.header.chunk_ordinal = request.prefix.chunk_ordinal;
	page.header.chunk_count_minus_one = request.prefix.chunk_count_minus_one;
	page.header.wire_length = sizeof(page.header);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);

	page.header.result = CMX_RESOLVE_UNKNOWN;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply_frame(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, &frame_result, out,
					 lengthof(out), &out_count, &updater_out),
				 true);
	UT_ASSERT_EQ(frame_result, CMX_RESOLVE_UNKNOWN);
	page.reserved[0] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply_frame(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, &frame_result, out,
					 lengthof(out), &out_count, &updater_out),
				 false);
	page.reserved[0] = 0;

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE, 1, 1, 1);
	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	page.header.result = CMX_RESOLVE_OK;
	page.header.source_node_id = 5;
	page.header.request_id = request.prefix.request_id;
	page.header.mxkey = request.prefix.mxkey;
	page.header.descriptor_hash
		= ClusterCurrentMxProofPrefixGetDescriptorHash(&request.prefix);
	page.header.total_count = request.prefix.total_count;
	page.header.entry_count = 1;
	page.header.chunk_ordinal = request.prefix.chunk_ordinal;
	page.header.chunk_count_minus_one = request.prefix.chunk_count_minus_one;
	page.header.wire_length = sizeof(page.header) + sizeof(ClusterCurrentMemberProof)
							  + sizeof(ClusterCurrentUpdaterProof);
	{
		ClusterCurrentMxMemberDesc member;

		test_member(&member, request.trailer.body.updater.challenge.updater_xid,
					request.trailer.body.updater.challenge.member_status);
		test_proof(&page.body.updater.member_proof, &member,
				   request.trailer.body.updater.challenge.member_ordinal, CCM_COMMITTED, 5, 20);
	}
	page.body.updater.updater_proof.mxkey = request.prefix.mxkey;
	page.body.updater.updater_proof.candidate_next_xmin_key
		= request.trailer.body.updater.challenge.candidate_next_xmin_key;
	page.body.updater.updater_proof.updater_xid
		= request.trailer.body.updater.challenge.updater_xid;
	page.body.updater.updater_proof.member_ordinal
		= request.trailer.body.updater.challenge.member_ordinal;
	page.body.updater.updater_proof.verdict = CUCP_MATCH;

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(out_count, 1);
	UT_ASSERT_EQ(out[0].state, CCM_COMMITTED);
	UT_ASSERT_EQ(updater_out.verdict, CUCP_MATCH);

	page.body.updater.updater_proof.candidate_next_xmin_key.tt_slot_id++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, UINT64CONST(9), &request, out, lengthof(out),
					 &out_count, &updater_out),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(updater_out.verdict, CUCP_UNKNOWN);
}


UT_TEST(test_current_multixact_describe_wire_binding)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxDescribeForwardV2 request;
	ClusterCurrentMxDescribeForwardV2 decoded;
	ClusterCurrentMxDescribeReplyPage page;
	ClusterCurrentMxMemberDesc out[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 out_count = 99;
	uint32 out_total = 99;
	uint64 hash;
	uint16 i;

	memset(&request, 0, sizeof(request));
	request.prefix.request_id = 501;
	request.prefix.epoch = 9;
	request.prefix.mxkey = key;
	request.prefix.original_requester_node = 3;
	request.prefix.requester_backend_id = 7;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;

	memset(&decoded, 0xff, sizeof(decoded));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 true);
	UT_ASSERT_EQ(decoded.prefix.request_id, 501);
	UT_ASSERT_EQ(decoded.prefix.mxkey.multixact_id, key.multixact_id);
	request.prefix.epoch = 0;
	request.prefix.mxkey.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(0), &decoded),
				 true);
	request.prefix.epoch = 9;
	request.prefix.mxkey.cluster_epoch = 9;

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request.prefix), 3, 2, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request) - 1, 3, 2, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request) + 1, 3, 2, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 4, 2, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 1, UINT64CONST(9), &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(0x100000009), &decoded),
				 false);
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	request.prefix.reserved_a[0] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.prefix.reserved_a[0] = 0;
	request.prefix.reserved_b[18] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.prefix.reserved_b[18] = 0;
	request.prefix.requester_backend_id = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.prefix.requester_backend_id = 7;
	request.trailer.magic++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.trailer.magic--;
	request.trailer.version++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.trailer.version--;
	request.trailer.flags = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.trailer.flags = 0;
	request.trailer.reserved[55] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &request, sizeof(request), 3, 2, UINT64CONST(9), &decoded),
				 false);
	request.trailer.reserved[55] = 0;

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = 2;
	page.header.entry_count = 2;
	page.header.chunk_count_minus_one = 0;
	page.header.wire_length
		= sizeof(page.header) + 2 * sizeof(ClusterCurrentMxMemberDesc);
	page.header.result = CMX_DESC_OK;
	test_member(&page.members[0], 100, MultiXactStatusForShare);
	test_member(&page.members[1], 101, MultiXactStatusNoKeyUpdate);
	hash = cluster_multixact_current_descriptor_hash(&key, page.members, 2);
	page.header.descriptor_hash = hash;

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(out_count, 2);
	UT_ASSERT_EQ(out_total, 2);
	UT_ASSERT_EQ(out[0].xid, 100);
	UT_ASSERT_EQ(out[1].xid, 101);

	page.header.request_id++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(out_count, 0);
	page.header.request_id--;

	page.header.mxkey.multixact_id++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.mxkey.multixact_id--;

	page.header.descriptor_hash++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.descriptor_hash--;

	page.header.magic++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.magic--;
	page.header.version++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.version--;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.flags = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.flags = 0;
	page.header.total_count++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.total_count--;
	page.header.entry_count--;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.entry_count++;
	page.header.chunk_ordinal = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.chunk_ordinal = 0;
	page.header.chunk_count_minus_one = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.chunk_count_minus_one = 0;
	page.header.reserved16 = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.reserved16 = 0;
	page.header.reserved32 = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.reserved32 = 0;
	page.header.wire_length--;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.wire_length++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page) - 1, 2, UINT64CONST(9), 501, &key, out, lengthof(out),
					 &out_count, &out_total),
				 CMX_DESC_UNKNOWN);
	((uint8 *)&page)[page.header.wire_length] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	((uint8 *)&page)[page.header.wire_length] = 0;
	page.header.source_node_id = 3;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.source_node_id = 2;

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = CLUSTER_CURRENT_MX_MAX_MEMBERS + 1;
	page.header.wire_length = sizeof(page.header);
	page.header.result = CMX_DESC_SUPPORTED_LIMIT;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(out_count, 0);
	UT_ASSERT_EQ(out_total, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1);

	page.header.result = CMX_DESC_TIMEOUT;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.wire_length = sizeof(page.header);
	page.header.result = CMX_DESC_DENIED;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_DENIED);
	page.header.descriptor_hash = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.descriptor_hash = 0;
	page.header.total_count = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	page.header.total_count = 0;
	page.reserved[0] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);

	/* The supported positive limit is a complete one-page round trip. */
	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = CLUSTER_CURRENT_MX_MAX_MEMBERS;
	page.header.entry_count = CLUSTER_CURRENT_MX_MAX_MEMBERS;
	page.header.chunk_count_minus_one = 0;
	page.header.wire_length = sizeof(page.header)
							  + CLUSTER_CURRENT_MX_MAX_MEMBERS
									* sizeof(ClusterCurrentMxMemberDesc);
	page.header.result = CMX_DESC_OK;
	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
		test_member(&page.members[i], 1000 + i, MultiXactStatusForShare);
	page.header.descriptor_hash
		= cluster_multixact_current_descriptor_hash(&key, page.members,
												   CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, UINT64CONST(9), 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(out_count, CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_EQ(out[CLUSTER_CURRENT_MX_MAX_MEMBERS - 1].xid,
				 1000 + CLUSTER_CURRENT_MX_MAX_MEMBERS - 1);
}

UT_TEST(test_current_multixact_describe_routes_by_mxid_authority)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[4];
	uint16 members_count;
	uint32 reported_total;

	cluster_node_id = 0;
	test_runtime_epoch = 9;
	test_runtime_native_describe_calls = 0;
	test_runtime_remote_describe_calls = 0;
	test_runtime_remote_describe_ok = false;

	/* Local origin is the only route allowed to touch local pg_multixact. */
	key.origin_node_id = 0;
	test_runtime_mxid_origin = 0;
	test_runtime_mxid_mine = true;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
													&members_count, &reported_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 0);
	UT_ASSERT_EQ(members_count, 2);
	UT_ASSERT_EQ(members[1].xid, 101);

	/* Foreign origin must use the authority RPC and never local SLRU. */
	key.origin_node_id = 2;
	test_runtime_mxid_origin = 2;
	test_runtime_mxid_mine = false;
	test_runtime_remote_describe_ok = true;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
													&members_count, &reported_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 1);
	UT_ASSERT_EQ(members[0].xid, 200);

	/* Epoch zero is the live initial cluster epoch, not an invalid sentinel. */
	key.cluster_epoch = 0;
	test_runtime_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
													&members_count, &reported_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 2);
	key.cluster_epoch = 9;
	test_runtime_epoch = 9;

	/* Underivable/wrong-origin identity fails before either authority read. */
	test_runtime_mxid_origin = -1;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
													&members_count, &reported_total),
				 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 2);
	UT_ASSERT_EQ(members_count, 0);

	cluster_node_id = -1;
	test_runtime_epoch = 0;
	test_runtime_mxid_origin = -1;
	test_runtime_mxid_mine = false;
	test_runtime_remote_describe_ok = false;
}


UT_TEST(test_current_multixact_native_conflict_matrix)
{
	static const bool expected[MaxMultiXactStatus + 1][LockTupleExclusive + 1] = {
		{ false, false, false, true }, { false, false, true, true }, { false, true, true, true },
		{ true, true, true, true },	   { false, true, true, true },	 { true, true, true, true },
	};
	int status;
	int mode;

	for (status = 0; status <= MaxMultiXactStatus; status++)
		for (mode = 0; mode <= LockTupleExclusive; mode++) {
			bool valid = false;

			UT_ASSERT_EQ(cluster_multixact_current_status_conflicts((uint8)status,
																	(LockTupleMode)mode, &valid),
						 expected[status][mode]);
			UT_ASSERT_EQ(valid, true);
		}

	{
		bool valid = true;

		UT_ASSERT_EQ(cluster_multixact_current_status_conflicts(MaxMultiXactStatus + 1,
																LockTupleExclusive, &valid),
					 false);
		UT_ASSERT_EQ(valid, false);
	}
}


static MultiXactStatus
test_lock_status(LockTupleMode mode)
{
	switch (mode) {
	case LockTupleKeyShare:
		return MultiXactStatusForKeyShare;
	case LockTupleShare:
		return MultiXactStatusForShare;
	case LockTupleNoKeyExclusive:
		return MultiXactStatusForNoKeyUpdate;
	case LockTupleExclusive:
		return MultiXactStatusForUpdate;
	}
	abort();
}


static void
test_context(ClusterCurrentMxRequestContext *ctx, const ClusterCurrentMxKey *key,
			 ClusterCurrentTupleAction action, MultiXactStatus desired_status, LockTupleMode mode)
{
	memset(ctx, 0, sizeof(*ctx));
	ctx->mxkey = *key;
	ctx->top_xid = 900;
	ctx->current_member_xid = 900;
	ctx->curcid = 5;
	ctx->tuple_cmax = 5;
	ctx->precheck_result = TM_Ok;
	ctx->desired_status = desired_status;
	ctx->lock_mode = mode;
	ctx->wait_policy = LockWaitBlock;
	ctx->action = action;
	ctx->tuple_shape = CCM_SHAPE_LOCK_ONLY;
	ctx->wait_for_conflict = 1;
	ctx->updater_origin_node_id = -1;
}


UT_TEST(test_current_multixact_compositor_status_mode_state_cross_product)
{
	static const bool conflict[MaxMultiXactStatus + 1][LockTupleExclusive + 1] = {
		{ false, false, false, true }, { false, false, true, true }, { false, true, true, true },
		{ true, true, true, true },	   { false, true, true, true },	 { true, true, true, true },
	};
	ClusterCurrentMxKey key = test_mxkey();
	int status;
	int mode;
	int state;

	for (status = 0; status <= MaxMultiXactStatus; status++)
		for (mode = 0; mode <= LockTupleExclusive; mode++)
			for (state = CCM_SELF; state <= CCM_UNKNOWN; state++) {
				ClusterCurrentMxMemberDesc members[2];
				ClusterCurrentMemberProof proofs[2];
				ClusterCurrentMxRequestContext ctx;
				ClusterCurrentMxDecision expected;
				TransactionId xid = state == CCM_SELF ? 900 : 100;

				test_member(&members[0], xid, (uint8)status);
				test_member(&members[1], 101, MultiXactStatusForKeyShare);
				test_proof(&proofs[0], &members[0], 0, (ClusterCurrentMemberState)state, 2, 70);
				test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 71);
				test_context(&ctx, &key, CCM_ACTION_LOCK, test_lock_status((LockTupleMode)mode),
							 (LockTupleMode)mode);
				ctx.tuple_shape
					= ISUPDATE_from_mxstatus(status) ? CCM_SHAPE_DELETED : CCM_SHAPE_LOCK_ONLY;

				switch ((ClusterCurrentMemberState)state) {
				case CCM_SELF:
					expected = ISUPDATE_from_mxstatus(status) ? CMDL_SELF_MODIFIED : CMDL_CONTINUE;
					break;
				case CCM_ACTIVE:
					expected = conflict[status][mode] ? CMDL_WAIT_MEMBER : CMDL_CONTINUE;
					break;
				case CCM_COMMITTED:
					expected = ISUPDATE_from_mxstatus(status) && conflict[status][mode]
								   ? CMDL_DELETED
								   : CMDL_CONTINUE;
					break;
				case CCM_ABORTED:
					expected = CMDL_CONTINUE;
					break;
				case CCM_UNKNOWN:
					expected = CMDL_UNKNOWN;
					break;
				}

				UT_ASSERT_EQ(
					cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
					expected);
			}
}


UT_TEST(test_current_multixact_active_wait_policies_and_stable_key)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;
	ClusterTTStatusKey wait_key;

	test_member(&members[0], 100, MultiXactStatusForUpdate);
	test_member(&members[1], 101, MultiXactStatusForUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 7, 30);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 3, 31);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate, LockTupleExclusive);

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_WAIT_MEMBER);
	UT_ASSERT_EQ(wait_key.origin_node_id, 3);
	UT_ASSERT_EQ(wait_key.local_xid, 101);

	ctx.wait_for_conflict = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_BEING_MODIFIED);

	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusForUpdate;
	ctx.wait_for_conflict = 1;
	ctx.wait_policy = LockWaitSkip;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_WOULD_BLOCK);
	ctx.wait_policy = LockWaitError;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_LOCK_NOT_AVAILABLE);
}


UT_TEST(test_current_multixact_terminal_nonconflict_and_unknown_precedence)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusForUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 60);
	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 3, 61);
	test_context(&ctx, &key, CCM_ACTION_LOCK, MultiXactStatusForNoKeyUpdate,
				 LockTupleNoKeyExclusive);

	/* A terminal locker is gone; the active KeyShare holder is compatible. */
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	/*
	 * An ACTIVE updater can be lock-compatible with KeyShare, but
	 * follow_updates must still lock its exact successor chain.  That
	 * continuation is allowed only with the full-key updater proof.
	 */
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	proofs[0].member_status = MultiXactStatusNoKeyUpdate;
	ctx.desired_status = MultiXactStatusForKeyShare;
	ctx.lock_mode = LockTupleKeyShare;
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	ctx.updater_origin_node_id = 2;
	memset(&challenge, 0, sizeof(challenge));
	challenge.candidate_next_xmin_key = proofs[0].key;
	challenge.updater_xid = members[0].xid;
	challenge.member_ordinal = 0;
	memset(&updater_proof, 0, sizeof(updater_proof));
	updater_proof.mxkey = key;
	updater_proof.candidate_next_xmin_key = challenge.candidate_next_xmin_key;
	updater_proof.updater_xid = challenge.updater_xid;
	updater_proof.member_ordinal = challenge.member_ordinal;
	updater_proof.verdict = CUCP_MATCH;
	ctx.follow_updates = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);
	ctx.follow_updates = 1;
	UT_ASSERT_EQ(cluster_multixact_current_decide(
					 members, proofs, 2, &ctx, &challenge, &updater_proof, NULL),
				 CMDL_FOLLOW_UPDATED);
	UT_ASSERT_EQ(cluster_multixact_current_decide(
					 members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	proofs[0].state = CCM_ABORTED;
	memset(&proofs[0].key, 0, sizeof(proofs[0].key));
	ctx.follow_updates = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	/* Any UNKNOWN member makes the complete proof set fail closed. */
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 60);
	proofs[1].state = CCM_UNKNOWN;
	proofs[1].commit_scn = InvalidScn;
	ctx.action = CCM_ACTION_DELETE;
	ctx.desired_status = MultiXactStatusUpdate;
	ctx.lock_mode = LockTupleExclusive;
	ctx.wait_for_conflict = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	/* A SELF locker is retained for recomposition but never waited on. */
	test_member(&members[0], 900, MultiXactStatusForUpdate);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 62);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 63);
	test_context(&ctx, &key, CCM_ACTION_HOT_FOLLOW, MultiXactStatusForKeyShare, LockTupleKeyShare);
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);
}


UT_TEST(test_current_multixact_member_states_and_self_cid)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;

	test_member(&members[0], 900, MultiXactStatusUpdate);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 40);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 41);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate, LockTupleExclusive);
	ctx.tuple_shape = CCM_SHAPE_UPDATED;

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_SELF_MODIFIED);
	ctx.tuple_cmax = 4;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_INVISIBLE);

	/* A previous child xid in this transaction tree is SELF even though it is
	 * neither the current subxid nor the top xid in the request context. */
	test_member(&members[0], 850, MultiXactStatusUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 42);
	ctx.tuple_cmax = 5;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_SELF_MODIFIED);

	proofs[0].state = CCM_ABORTED;
	memset(&proofs[0].key, 0, sizeof(proofs[0].key));
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	proofs[1].state = CCM_UNKNOWN;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	proofs[1].state = CCM_ABORTED;
	ctx.precheck_result = TM_Invisible;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_INVISIBLE);
	ctx.precheck_result = TM_SelfModified;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_SELF_MODIFIED);
}


UT_TEST(test_current_multixact_committed_updater_requires_exact_hot_proof)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 3, 51);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate, LockTupleExclusive);
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	ctx.updater_origin_node_id = 3;

	memset(&challenge, 0, sizeof(challenge));
	challenge.candidate_next_xmin_key = test_ttkey(3, 101, 51);
	challenge.updater_xid = 101;
	challenge.member_ordinal = 1;
	memset(&updater_proof, 0, sizeof(updater_proof));
	updater_proof.mxkey = key;
	updater_proof.candidate_next_xmin_key = challenge.candidate_next_xmin_key;
	updater_proof.updater_xid = 101;
	updater_proof.member_ordinal = 1;
	updater_proof.verdict = CUCP_MATCH;

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UPDATED);

	/*
	 * KeyShare is compatible with a committed NoKeyUpdate on the old
	 * version.  Native follow_updates=false may lock that old version, but
	 * follow_updates=true must authenticate the successor before advancing.
	 */
	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusForKeyShare;
	ctx.lock_mode = LockTupleKeyShare;
	ctx.follow_updates = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_CONTINUE);
	ctx.follow_updates = 1;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UPDATED);
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_UPDATE;
	ctx.desired_status = MultiXactStatusUpdate;
	ctx.lock_mode = LockTupleExclusive;
	ctx.follow_updates = 0;
	updater_proof.candidate_next_xmin_key.tt_slot_id++;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	updater_proof.candidate_next_xmin_key.tt_slot_id--;
	updater_proof.verdict = CUCP_MISMATCH;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);

	updater_proof.verdict = CUCP_MATCH;
	updater_proof.mxkey.cluster_epoch++;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	updater_proof.mxkey.cluster_epoch--;
	updater_proof.updater_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	updater_proof.updater_xid--;
	ctx.updater_origin_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	ctx.updater_origin_node_id = 3;

	ctx.tuple_shape = CCM_SHAPE_DELETED;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_DELETED);
}


UT_TEST(test_current_multixact_updater_proof_typed_positive_outcomes)
{
	ClusterCurrentMxKey key;
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;
	ClusterTTStatusKey wait_key;
	ClusterTTStatusKey zero_key;
	ClusterTTStatusKey direct_holder;
	ClusterTTStatusKey parent_holder = {
		.origin_node_id = 3,
		.undo_segment_id = 1,
		.tt_slot_id = 71,
		.cluster_epoch = 9,
		.local_xid = 90,
	};

	memset(&zero_key, 0, sizeof(zero_key));

	/* P1: a committed updater preserves the existing bool behavior. */
	test_updater_proof_fixture(
		&key, members, proofs, &challenge, &updater_proof,
		CCM_COMMITTED);
	memset(&wait_key, 0xa5, sizeof(wait_key));
	UT_ASSERT_EQ(cluster_multixact_current_updater_proof_outcome(
					 CMX_RESOLVE_OK, &key, members, proofs, 2, &challenge,
					 &updater_proof, 3, &wait_key),
				 CCMUPO_COMMITTED);
	UT_ASSERT_EQ(memcmp(&wait_key, &zero_key, sizeof(wait_key)), 0);
	UT_ASSERT(cluster_multixact_current_validate_updater_proof(
		&key, members, proofs, 2, &challenge, &updater_proof, 3));

	/* P2: only an authenticated direct ACTIVE holder is waitable. */
	test_updater_proof_fixture(
		&key, members, proofs, &challenge, &updater_proof,
		CCM_ACTIVE);
	direct_holder = proofs[1].key;
	memset(&wait_key, 0xa5, sizeof(wait_key));
	UT_ASSERT_EQ(cluster_multixact_current_updater_proof_outcome(
					 CMX_RESOLVE_OK, &key, members, proofs, 2, &challenge,
					 &updater_proof, 3, &wait_key),
				 CCMUPO_WAIT_MEMBER);
	UT_ASSERT_EQ(memcmp(&wait_key, &direct_holder, sizeof(wait_key)), 0);
	UT_ASSERT(!cluster_multixact_current_validate_updater_proof(
		&key, members, proofs, 2, &challenge, &updater_proof, 3));

	/*
	 * P3: a SUBCOMMITTED child echo waits on the final authenticated
	 * parent holder key, not the child or successor candidate key.
	 */
	proofs[1].key = parent_holder;
	memset(&wait_key, 0xa5, sizeof(wait_key));
	UT_ASSERT_EQ(cluster_multixact_current_updater_proof_outcome(
					 CMX_RESOLVE_OK, &key, members, proofs, 2, &challenge,
					 &updater_proof, 3, &wait_key),
				 CCMUPO_WAIT_MEMBER);
	UT_ASSERT_EQ(memcmp(&wait_key, &parent_holder, sizeof(wait_key)), 0);
	UT_ASSERT_NE(memcmp(&wait_key, &challenge.candidate_next_xmin_key,
						sizeof(wait_key)),
				 0);
	UT_ASSERT_EQ(wait_key.local_xid, 90);
}


UT_TEST(test_current_multixact_updater_proof_typed_negative_ballot)
{
	ClusterCurrentMxKey key;
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;
	ClusterCurrentMemberProof origin_proof;
	ClusterTTStatusKey origin_key;
	ClusterTTStatusResult origin_result;
	ClusterCurrentProofChunkView chunks[2];
	ClusterCurrentMemberProof ordered[2];
	uint16 origins[2] = { 2, 3 };
	uint64 hash;
	ClusterMxResolveResult hash_result;

	test_updater_proof_fixture(
		&key, members, proofs, &challenge, &updater_proof,
		CCM_ACTIVE);

	UT_ASSERT_EQ(cluster_multixact_current_updater_proof_outcome(
					 CMX_RESOLVE_OK, &key, members, proofs, 2, &challenge,
					 &updater_proof, 3, NULL),
				 CCMUPO_FAIL_CLOSED);

	/* N1/N2: no non-OK resolver result can authenticate a wait. */
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_UNKNOWN, &key, members, proofs, &challenge,
		&updater_proof, 3);
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_DENIED, &key, members, proofs, &challenge,
		&updater_proof, 3);
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_SUPPORTED_LIMIT, &key, members, proofs, &challenge,
		&updater_proof, 3);
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_TIMEOUT, &key, members, proofs, &challenge,
		&updater_proof, 3);

	/* N3/N4: candidate mismatch or current-overlay ambiguity is not authority. */
	updater_proof.verdict = CUCP_MISMATCH;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	updater_proof.verdict = CUCP_UNKNOWN;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	updater_proof.verdict = CUCP_MATCH;

	/* N5: holder epoch drift cannot cross the request epoch fence. */
	proofs[1].key.cluster_epoch++;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[1].key.cluster_epoch--;

	/* N6: a descriptor-hash mismatch collapses upstream to UNKNOWN. */
	hash = cluster_multixact_current_descriptor_hash(&key, members, 2);
	memset(chunks, 0, sizeof(chunks));
	chunks[0].request_id = 77;
	chunks[0].mxkey = key;
	chunks[0].descriptor_hash = hash;
	chunks[0].total_count = 2;
	chunks[0].source_node_id = 2;
	chunks[0].chunk_ordinal = 0;
	chunks[0].chunk_count = 2;
	chunks[0].proof_count = 1;
	chunks[0].proofs = &proofs[0];
	chunks[1] = chunks[0];
	chunks[1].source_node_id = 3;
	chunks[1].chunk_ordinal = 1;
	chunks[1].proofs = &proofs[1];
	hash_result = cluster_multixact_current_validate_proof_set(
		&key, members, origins, 2, 77, hash + 1, chunks, 2, ordered);
	UT_ASSERT_EQ(hash_result, CMX_RESOLVE_UNKNOWN);
	assert_updater_proof_fail_closed(
		hash_result, &key, members, proofs, &challenge, &updater_proof, 3);

	/* N7: the real origin resolver rejects a non-authoritative TT result. */
	origin_key = proofs[1].key;
	memset(&origin_result, 0, sizeof(origin_result));
	origin_result.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	origin_result.status_epoch = 9;
	origin_result.authoritative = false;
	UT_ASSERT(!cluster_multixact_current_resolve_origin_member_proof(
		101, members[1].member_status, 1, 3, 9, false, &origin_key,
		&origin_result, NULL, NULL, &origin_proof));
	UT_ASSERT_EQ(origin_proof.state, CCM_UNKNOWN);
	proofs[1] = origin_proof;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_UNKNOWN, &key, members, proofs, &challenge,
		&updater_proof, 3);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 3, 51);

	/* N8: wrong ordinal and duplicate descriptor identity select no updater. */
	updater_proof.member_ordinal = 0;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	updater_proof.member_ordinal = 1;
	members[0].xid = members[1].xid;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	members[0].xid = 100;

	/* N9: member and updater xid echoes must remain exact. */
	proofs[1].member_xid++;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[1].member_xid--;
	updater_proof.updater_xid++;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	updater_proof.updater_xid--;

	/* N10: incomplete, wrong-origin, or reserved holder keys never wait. */
	proofs[1].key.tt_slot_id = 0;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[1].key.tt_slot_id = 51;
	proofs[1].key.origin_node_id = 4;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[1].key.origin_node_id = 3;
	proofs[1].key._reserved = 1;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[1].key._reserved = 0;

	/* N11: ACTIVE with a commit SCN is malformed terminal evidence. */
	proofs[1].commit_scn = 500;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[1].commit_scn = InvalidScn;

	/* N12: an UNKNOWN peer makes the complete proof set unusable. */
	proofs[0].state = CCM_UNKNOWN;
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	proofs[0].state = CCM_ABORTED;

	/* N13: SELF and ABORTED updater states carry no waitable holder. */
	test_proof(&proofs[1], &members[1], 1, CCM_SELF, 3, 51);
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 51);
	assert_updater_proof_fail_closed(
		CMX_RESOLVE_OK, &key, members, proofs, &challenge,
		&updater_proof, 3);
}


UT_TEST(test_current_multixact_rejects_context_mode_mismatch)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 51);
	test_context(&ctx, &key, CCM_ACTION_LOCK, MultiXactStatusForKeyShare, LockTupleExclusive);

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_DELETE;
	ctx.desired_status = MultiXactStatusNoKeyUpdate;
	ctx.lock_mode = LockTupleNoKeyExclusive;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_UPDATE;
	ctx.desired_status = MultiXactStatusForNoKeyUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusNoKeyUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusForNoKeyUpdate;
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);
}


UT_TEST(test_current_multixact_recompose_filters_terminal_members)
{
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proofs[3];
	MultiXactMember normalized[4];
	uint16 normalized_count = 99;

	test_member(&members[0], 501, MultiXactStatusForShare);
	test_member(&members[1], 502, MultiXactStatusForKeyShare);
	test_member(&members[2], 503, MultiXactStatusNoKeyUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_COMMITTED, 2, 31);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 2, 32);
	test_proof(&proofs[2], &members[2], 2, CCM_ABORTED, 2, 33);

	UT_ASSERT_EQ(
		cluster_multixact_current_recompose(
			members, proofs, 3, 504, MultiXactStatusForShare,
			normalized, lengthof(normalized), &normalized_count),
		CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 502);
	UT_ASSERT_EQ(normalized[0].status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(normalized[1].xid, 504);
	UT_ASSERT_EQ(normalized[1].status, MultiXactStatusForShare);
}


UT_TEST(test_current_multixact_recompose_upgrades_requester_member)
{
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	MultiXactMember normalized[3];
	uint16 normalized_count = 0;

	test_member(&members[0], 601, MultiXactStatusForKeyShare);
	test_member(&members[1], 602, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 41);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 2, 42);

	UT_ASSERT_EQ(
		cluster_multixact_current_recompose(
			members, proofs, 2, 601, MultiXactStatusForUpdate,
			normalized, lengthof(normalized), &normalized_count),
		CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 601);
	UT_ASSERT_EQ(normalized[0].status, MultiXactStatusForUpdate);
	UT_ASSERT_EQ(normalized[1].xid, 602);
	UT_ASSERT_EQ(normalized[1].status, MultiXactStatusForShare);
}


UT_TEST(test_current_multixact_recompose_fails_closed_on_incomplete_proof)
{
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	MultiXactMember normalized[3];
	uint16 normalized_count = 99;

	test_member(&members[0], 701, MultiXactStatusForKeyShare);
	test_member(&members[1], 702, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 51);
	test_proof(&proofs[1], &members[1], 1, CCM_UNKNOWN, 2, 52);

	UT_ASSERT_EQ(
		cluster_multixact_current_recompose(
			members, proofs, 2, 703, MultiXactStatusForShare,
			normalized, lengthof(normalized), &normalized_count),
		CMX_RECOMPOSE_UNKNOWN);
	UT_ASSERT_EQ(normalized_count, 0);
	UT_ASSERT_EQ(normalized[0].xid, InvalidTransactionId);
}

UT_TEST(test_current_multixact_recompose_filters_terminal_before_cap_check)
{
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	MultiXactMember normalized[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 normalized_count = 0;
	uint16 i;

	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
	{
		test_member(&members[i], (TransactionId) (1000 + i),
					MultiXactStatusForKeyShare);
		test_proof(&proofs[i], &members[i], i, CCM_ABORTED, 2,
				   (uint16) (100 + i));
	}
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 100);

	UT_ASSERT_EQ(
		cluster_multixact_current_recompose(
			members, proofs, CLUSTER_CURRENT_MX_MAX_MEMBERS, 5000,
			MultiXactStatusForShare, normalized, lengthof(normalized),
			&normalized_count),
		CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 1000);
	UT_ASSERT_EQ(normalized[1].xid, 5000);
}

UT_TEST(test_current_multixact_heap_bridge_orders_authority_before_decision)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *bridge;
	const char *describe;
	const char *resolve;
	const char *decide;
	const char *recompose;
	const char *wait;

	if (source == NULL)
		return;
	bridge = strstr(source, "\ncluster_current_mx_authorize(");
	describe = bridge != NULL
		? strstr(bridge, "cluster_multixact_current_describe(") : NULL;
	resolve = describe != NULL
		? strstr(describe, "cluster_multixact_current_members_resolve(") : NULL;
	decide = resolve != NULL
		? strstr(resolve, "cluster_multixact_current_decide(") : NULL;
	recompose = decide != NULL
		? strstr(decide, "cluster_multixact_current_recompose(") : NULL;
	wait = decide != NULL ? strstr(decide, "cluster_tx_enqueue_wait(") : NULL;
	UT_ASSERT_NOT_NULL(bridge);
	UT_ASSERT_NOT_NULL(describe);
	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT_NOT_NULL(decide);
	UT_ASSERT_NOT_NULL(recompose);
	UT_ASSERT_NOT_NULL(wait);
	UT_ASSERT(describe < resolve && resolve < decide);
	free(source);
}

UT_TEST(test_current_multixact_heap_bridge_covers_four_dml_callers)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);

	if (source == NULL)
		return;
	UT_ASSERT_EQ(count_occurrences(source, "cluster_current_mx_authorize("), 5);
	UT_ASSERT_STR_CONTAINS(source, "CCM_ACTION_DELETE");
	UT_ASSERT_STR_CONTAINS(source, "CCM_ACTION_UPDATE");
	UT_ASSERT_EQ(count_occurrences(source, "CCM_ACTION_LOCK"), 2);
	UT_ASSERT_STR_CONTAINS(source, "cluster_current_mx_compose_remote_single(");
	UT_ASSERT_STR_CONTAINS(source, "CCMH_STAMP_PRIMARY");
	UT_ASSERT_STR_CONTAINS(source, "CCMH_STAMP_TEMP_LOCK");
	UT_ASSERT_STR_CONTAINS(source, "CCMH_STAMP_SURVIVORS");
	free(source);
}

UT_TEST(test_current_multixact_local_compose_installs_requester_tt_authority)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *lock_tuple;
	const char *compose;
	const char *bind;
	const char *bind_member;
	const char *active;
	const char *lock_tuple_end;

	if (source == NULL)
		return;
	lock_tuple = strstr(source, "\nheap_lock_tuple(");
	compose = lock_tuple != NULL
		? strstr(lock_tuple, "if (cluster_will_stamp_multixact_marker)") : NULL;
	bind = compose != NULL
		? strstr(compose, "cluster_tt_local_get_or_create_binding(") : NULL;
	bind_member = bind != NULL
		? strstr(bind, "cluster_multixact_member_xid") : NULL;
	active = bind != NULL
		? strstr(bind,
				 "cluster_tt_local_record_active("
				 "cluster_multixact_member_xid)") : NULL;
	lock_tuple_end = lock_tuple != NULL
		? strstr(lock_tuple, "\n}\n\n/*\n * Acquire heavyweight lock") : NULL;
	UT_ASSERT_NOT_NULL(lock_tuple);
	UT_ASSERT_NOT_NULL(compose);
	UT_ASSERT_NOT_NULL(bind);
	UT_ASSERT_NOT_NULL(bind_member);
	UT_ASSERT_NOT_NULL(active);
	UT_ASSERT_NOT_NULL(lock_tuple_end);
	if (active != NULL && lock_tuple_end != NULL)
		UT_ASSERT(active < lock_tuple_end);
	free(source);
}

UT_TEST(test_current_multixact_htsu_routes_peer_current_before_local_decode)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *fork;
	const char *multi;
	const char *peer_gate;
	const char *resolve;

	if (source == NULL)
		return;
	fork = strstr(source, "\ncluster_satisfies_update_fork(");
	multi = fork != NULL
		? strstr(fork, "if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)") : NULL;
	peer_gate = multi != NULL
		? strstr(multi, "if (cluster_peer_mode_enabled()") : NULL;
	resolve = multi != NULL
		? strstr(multi, "cluster_visibility_resolve_tuple(") : NULL;
	UT_ASSERT_NOT_NULL(fork);
	UT_ASSERT_NOT_NULL(multi);
	UT_ASSERT_NOT_NULL(peer_gate);
	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT(peer_gate < resolve);
	free(source);
}

UT_TEST(test_current_multixact_reader_overlay_unknown_asks_origin)
{
	char *source = read_source(CLUSTER_MULTIXACT_SOURCE_PATH);
	const char *resolve;
	const char *overlay_decision;
	const char *unknown_fallback;
	const char *origin_ask;
	const char *next_function;

	if (source == NULL)
		return;
	resolve = strstr(source, "\ncluster_multixact_remote_xmax_resolve(");
	overlay_decision = resolve != NULL
		? strstr(resolve, "decision = cluster_multixact_resolve_visibility(mxres, snap)") : NULL;
	unknown_fallback = overlay_decision != NULL
		? strstr(overlay_decision, "if (decision == CLUSTER_VISIBILITY_UNKNOWN)") : NULL;
	origin_ask = unknown_fallback != NULL
		? strstr(unknown_fallback,
				 "cluster_multixact_remote_xmax_ask_origin(origin_slot, mxid, snap)") : NULL;
	next_function = resolve != NULL
		? strstr(resolve, "\ncluster_multixact_get_member_count(") : NULL;
	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT_NOT_NULL(overlay_decision);
	UT_ASSERT_NOT_NULL(unknown_fallback);
	UT_ASSERT_NOT_NULL(origin_ask);
	UT_ASSERT_NOT_NULL(next_function);
	if (origin_ask != NULL && next_function != NULL)
		UT_ASSERT(origin_ask < next_function);
	free(source);
}

UT_TEST(test_current_multixact_latest_tid_uses_authoritative_hot_updater)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *latest;
	const char *latest_end;
	const char *all_current_multi;
	const char *exclusive;
	const char *authority;
	const char *same_buffer_recheck;
	const char *latest_full_key_handoff;
	const char *latest_share_downgrade;
	const char *native_decode;
	const char *hot_search;
	const char *hot_search_end;
	const char *hot_authority;
	const char *hot_full_key_handoff;
	const char *hot_share_downgrade;
	const char *hot_native_decode;

	if (source == NULL)
		return;
	latest = strstr(source, "\nheap_get_latest_tid(");
	latest_end = latest != NULL
		? strstr(latest, "\n}\n\n\n/*\n * UpdateXmaxHintBits") : NULL;
	all_current_multi = latest != NULL
		? strstr(latest, "cluster_authoritative_current_multi =") : NULL;
	exclusive = latest != NULL
		? strstr(latest, "LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)") : NULL;
	authority = latest != NULL
		? strstr(latest, "cluster_current_mx_hot_updater_for_chain(") : NULL;
	same_buffer_recheck = authority != NULL
		? strstr(authority, "goto cluster_latest_tid_recheck") : NULL;
	latest_full_key_handoff = latest != NULL
		? strstr(latest, "cluster_current_mx_successor_key_matches(") : NULL;
	latest_share_downgrade = authority != NULL
		? strstr(authority, "LockBuffer(buffer, BUFFER_LOCK_SHARE)") : NULL;
	native_decode = latest != NULL
		? strstr(latest, "HeapTupleHeaderGetUpdateXid(tp.t_data)") : NULL;
	hot_search = strstr(source, "\nheap_hot_search_buffer(");
	hot_search_end = hot_search != NULL
		? strstr(hot_search, "\n}\n\n/*\n *\theap_get_latest_tid") : NULL;
	hot_authority = hot_search != NULL
		? strstr(hot_search, "cluster_current_mx_hot_updater_for_chain(") : NULL;
	hot_full_key_handoff = hot_search != NULL
		? strstr(hot_search, "cluster_current_mx_successor_key_matches(") : NULL;
	hot_share_downgrade = hot_authority != NULL
		? strstr(hot_authority, "LockBuffer(buffer, BUFFER_LOCK_SHARE)") : NULL;
	hot_native_decode = hot_search != NULL
		? strstr(hot_search, "HeapTupleHeaderGetUpdateXid(heapTuple->t_data)") : NULL;
	UT_ASSERT_NOT_NULL(latest);
	UT_ASSERT_NOT_NULL(latest_end);
	UT_ASSERT_NOT_NULL(all_current_multi);
	UT_ASSERT_NOT_NULL(exclusive);
	UT_ASSERT_NOT_NULL(authority);
	UT_ASSERT_NOT_NULL(same_buffer_recheck);
	UT_ASSERT_NOT_NULL(latest_full_key_handoff);
	UT_ASSERT_NOT_NULL(latest_share_downgrade);
	UT_ASSERT_NOT_NULL(native_decode);
	UT_ASSERT_NOT_NULL(hot_search);
	UT_ASSERT_NOT_NULL(hot_search_end);
	UT_ASSERT_NOT_NULL(hot_authority);
	UT_ASSERT_NOT_NULL(hot_full_key_handoff);
	UT_ASSERT_NOT_NULL(hot_share_downgrade);
	UT_ASSERT_NOT_NULL(hot_native_decode);
	if (latest_end != NULL && exclusive != NULL && authority != NULL
		&& latest_full_key_handoff != NULL && latest_share_downgrade != NULL)
		UT_ASSERT(latest_full_key_handoff < authority
				  && exclusive < authority
				  && authority < latest_share_downgrade
				  && latest_share_downgrade < latest_end);
	if (latest_end != NULL && native_decode != NULL)
		UT_ASSERT(native_decode < latest_end);
	if (hot_search_end != NULL && hot_authority != NULL
		&& hot_full_key_handoff != NULL && hot_share_downgrade != NULL
		&& hot_native_decode != NULL)
		UT_ASSERT(hot_full_key_handoff < hot_authority
				  && hot_authority < hot_share_downgrade
				  && hot_share_downgrade < hot_native_decode
				  && hot_native_decode < hot_search_end);
	free(source);
}

UT_TEST(test_current_multixact_heap_bridge_uses_decoded_cmax_and_follows_active_updater)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *bridge;
	const char *bridge_end;
	const char *decoded_cmax;
	const char *raw_cid;
	const char *follow_decision;
	const char *follow_recompose;
	const char *follow_successor_proof;
	const char *lock_tuple;
	const char *lock_tuple_end;
	const char *follow_flag;
	const char *follow_helper;
	const char *recursive;
	const char *recursive_end;
	const char *proof_recheck;
	const char *foreign_xmin_bypass;
	const char *local_abort_bypass;
	const char *first_proof_clear;
	const char *clear_at_next_hop;

	if (source == NULL)
		return;
	bridge = strstr(source, "\ncluster_current_mx_authorize(");
	bridge_end = bridge != NULL
		? strstr(bridge, "\nstatic void\ncluster_current_mx_make_stamp") : NULL;
	decoded_cmax = bridge != NULL
		? strstr(bridge, "ctx.tuple_cmax = HeapTupleHeaderGetCmax(tuple->t_data)") : NULL;
	raw_cid = bridge != NULL
		? strstr(bridge, "ctx.tuple_cmax = HeapTupleHeaderGetRawCommandId(tuple->t_data)") : NULL;
	follow_decision = bridge != NULL
		? strstr(bridge, "case CMDL_FOLLOW_UPDATED:") : NULL;
	follow_recompose = follow_decision != NULL
		? strstr(follow_decision, "cluster_multixact_current_recompose(") : NULL;
	follow_successor_proof = follow_decision != NULL
		? strstr(follow_decision, "cluster_current_mx_set_successor_proof(") : NULL;
	lock_tuple = strstr(source, "\nheap_lock_tuple(");
	lock_tuple_end = lock_tuple != NULL
		? strstr(lock_tuple, "\n}\n\n/*\n * Acquire heavyweight lock") : NULL;
	follow_flag = lock_tuple != NULL
		? strstr(lock_tuple, "cluster_current_mx.follow_updated_chain") : NULL;
	follow_helper = lock_tuple != NULL
		? strstr(lock_tuple, "heap_lock_updated_tuple_authoritative(") : NULL;
	recursive = strstr(source, "\nheap_lock_updated_tuple_rec(");
	recursive_end = recursive != NULL
		? strstr(recursive, "\n}\n\n/*\n * heap_lock_updated_tuple") : NULL;
	proof_recheck = recursive != NULL
		? strstr(recursive, "cluster_current_mx_successor_matches(") : NULL;
	foreign_xmin_bypass = proof_recheck != NULL
		? strstr(proof_recheck,
				 "&& !expected_successor.valid\n"
				 "\t\t\t&& cluster_xid_provably_foreign") : NULL;
	local_abort_bypass = proof_recheck != NULL
		? strstr(proof_recheck,
				 "if (!expected_successor.valid\n"
				 "\t\t\t&& TransactionIdDidAbort") : NULL;
	first_proof_clear = proof_recheck != NULL
		? strstr(proof_recheck,
				 "memset(&expected_successor, 0, sizeof(expected_successor));") : NULL;
	clear_at_next_hop = proof_recheck != NULL
		? strstr(proof_recheck,
				 "memset(&expected_successor, 0, sizeof(expected_successor));\n"
				 "\t\tpriorXmax = HeapTupleHeaderGetUpdateXid") : NULL;
	UT_ASSERT_NOT_NULL(bridge);
	UT_ASSERT_NOT_NULL(bridge_end);
	UT_ASSERT_NOT_NULL(decoded_cmax);
	UT_ASSERT_NULL(raw_cid);
	UT_ASSERT_NOT_NULL(follow_decision);
	UT_ASSERT_NOT_NULL(follow_recompose);
	UT_ASSERT_NOT_NULL(follow_successor_proof);
	UT_ASSERT_NOT_NULL(lock_tuple);
	UT_ASSERT_NOT_NULL(lock_tuple_end);
	UT_ASSERT_NOT_NULL(follow_flag);
	UT_ASSERT_NOT_NULL(follow_helper);
	UT_ASSERT_NOT_NULL(recursive);
	UT_ASSERT_NOT_NULL(recursive_end);
	UT_ASSERT_NOT_NULL(proof_recheck);
	UT_ASSERT_NOT_NULL(foreign_xmin_bypass);
	UT_ASSERT_NOT_NULL(local_abort_bypass);
	UT_ASSERT_NOT_NULL(first_proof_clear);
	UT_ASSERT_NOT_NULL(clear_at_next_hop);
	UT_ASSERT(first_proof_clear == clear_at_next_hop);
	if (bridge_end != NULL && decoded_cmax != NULL)
		UT_ASSERT(decoded_cmax < bridge_end);
	if (bridge_end != NULL && follow_decision != NULL
		&& follow_recompose != NULL && follow_successor_proof != NULL)
		UT_ASSERT(follow_decision < follow_recompose
				  && follow_recompose < follow_successor_proof
				  && follow_successor_proof < bridge_end);
	if (lock_tuple_end != NULL && follow_flag != NULL && follow_helper != NULL)
		UT_ASSERT(follow_flag < follow_helper && follow_helper < lock_tuple_end);
	if (recursive_end != NULL && proof_recheck != NULL
		&& foreign_xmin_bypass != NULL && local_abort_bypass != NULL
		&& clear_at_next_hop != NULL)
		UT_ASSERT(proof_recheck < foreign_xmin_bypass
				  && foreign_xmin_bypass < local_abort_bypass
				  && local_abort_bypass < clear_at_next_hop
				  && clear_at_next_hop < recursive_end);
	free(source);
}

UT_TEST(test_current_multixact_operation_local_descriptor_memo_contract)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *lookup;
	const char *store;
	const char *finish;

	if (source == NULL)
		return;
	lookup = strstr(source, "\ncluster_current_mx_memo_lookup(");
	store = strstr(source, "\ncluster_current_mx_memo_store(");
	finish = strstr(source, "\ncluster_current_mx_operation_finish(");

	UT_ASSERT_NOT_NULL(lookup);
	UT_ASSERT_NOT_NULL(store);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_STR_CONTAINS(source, "memo->operation_id != operation->operation_id");
	UT_ASSERT_STR_CONTAINS(source, "cluster_current_mx_capture_equal(&memo->fingerprint");
	UT_ASSERT_STR_CONTAINS(source, "cluster_mxid_origin_slot(key->multixact_id)");
	UT_ASSERT_STR_CONTAINS(source, "cluster_multixact_current_descriptor_hash(");
	UT_ASSERT_STR_CONTAINS(source, "cluster_multixact_current_validate_descriptor(");
	UT_ASSERT_STR_CONTAINS(source, "cluster_multixact_current_stats_record_restarts(");
	UT_ASSERT_STR_CONTAINS(source, "CMX_STAT_WAIT_INTERRUPTED");
	UT_ASSERT_STR_CONTAINS(source, "CMX_STAT_FOREIGN_SLRU_GUARD");
	UT_ASSERT(strstr(source,
					 "ClusterCurrentMxOperationState "
					 "*cluster_current_mx_active_operation") == NULL);
	UT_ASSERT_STR_CONTAINS(source,
						   "cluster_current_mx_failclosed(\n"
						   "\tClusterCurrentMxOperationState *operation,\n"
						   "\tClusterCurrentMxFailurePhase phase,");
	UT_ASSERT_STR_CONTAINS(source, "case CCMH_FAIL_RECOMPOSE:");
	UT_ASSERT_STR_CONTAINS(source, "case CCMH_FAIL_HOT_PROOF:");
	UT_ASSERT_STR_CONTAINS(source, "phase=%s; %s");
	UT_ASSERT_STR_CONTAINS(source,
						   "cluster_current_mx_operation_finish(\n"
						   "\t\t\t\t\t&cluster_current_mx_operation);\n"
						   "#endif\n"
						   "\t\t\t\treturn true;");
	UT_ASSERT_STR_CONTAINS(source,
						   "cluster_current_mx_operation_finish(\n"
						   "\t\t&cluster_current_mx_operation);\n"
						   "#endif\n"
						   "\treturn false;");
	UT_ASSERT_STR_CONTAINS(source,
						   "cluster_current_mx_operation_finish(\n"
						   "\t\t&cluster_current_mx_operation);\n"
						   "#endif\n"
						   "}\n\n\n/*\n * UpdateXmaxHintBits");
	free(source);
}

UT_TEST(test_current_multixact_supported_limit_outer_mapping_contract)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);

	if (source == NULL)
		return;
	UT_ASSERT_STR_CONTAINS(source, "ERRCODE_FEATURE_NOT_SUPPORTED");
	UT_ASSERT_STR_CONTAINS(
		source,
		"cross-node current-DML does not support MultiXact with more than 256 members");
	UT_ASSERT_STR_CONTAINS(
		source,
		"PostgreSQL permits larger member sets, but this pgrac protocol version "
		"supports at most 256.");
	UT_ASSERT_STR_CONTAINS(
		source,
		"Reduce concurrent row lockers or retry after lockers finish; upgrade "
		"when chunked member-list support is available.");
	UT_ASSERT_EQ(count_occurrences(
					 source, "cluster_current_mx_supported_limit("),
				 7);
	UT_ASSERT_EQ(count_occurrences(
					 source,
					 "if (recompose_result == "
					 "CMX_RECOMPOSE_SUPPORTED_LIMIT) {\n"
					 "\t\t\t\tcluster_multixact_current_stats_bump(\n"
					 "\t\t\t\t\tCMX_STAT_RECOMPOSE_FAILCLOSED);\n"
					 "\t\t\t\tcluster_current_mx_supported_limit("),
				 2);
	free(source);
}

UT_TEST(test_current_multixact_gate_and_recompose_counter_are_publish_exact)
{
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	char *current_source = read_current_multixact_source();
	const char *remote_single;
	const char *remote_single_end;
	const char *gate;
	const char *recompose;
	const char *recompose_end;
	const char *make_stamp;
	const char *create;
	const char *success_counter;

	if (heap_source == NULL || current_source == NULL)
		goto out;

	remote_single
		= strstr(heap_source, "\ncluster_current_mx_compose_remote_single(");
	remote_single_end
		= remote_single != NULL
			  ? strstr(remote_single, "\nstatic ClusterCurrentMxHeapDisposition")
			  : NULL;
	gate = remote_single != NULL
			   ? strstr(remote_single, "!cluster_multixact_current_dml")
			   : NULL;
	UT_ASSERT_NOT_NULL(remote_single);
	UT_ASSERT_NOT_NULL(remote_single_end);
	UT_ASSERT_NOT_NULL(gate);
	UT_ASSERT(gate == NULL || remote_single_end == NULL
			  || gate < remote_single_end);

	recompose = strstr(current_source,
					   "\ncluster_multixact_current_recompose(");
	recompose_end = recompose != NULL ? strstr(recompose, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(recompose);
	UT_ASSERT_NOT_NULL(recompose_end);
	if (recompose != NULL && recompose_end != NULL)
		UT_ASSERT(strstr(recompose, "CMX_STAT_RECOMPOSE_SUCCESS") == NULL
				  || strstr(recompose, "CMX_STAT_RECOMPOSE_SUCCESS")
						 > recompose_end);

	make_stamp = strstr(heap_source, "\ncluster_current_mx_make_stamp(");
	create = make_stamp != NULL
				 ? strstr(make_stamp,
						  "MultiXactIdCreateFromCurrentMembers(")
				 : NULL;
	success_counter
		= create != NULL
			  ? strstr(create, "CMX_STAT_RECOMPOSE_SUCCESS")
			  : NULL;
	UT_ASSERT_NOT_NULL(make_stamp);
	UT_ASSERT_NOT_NULL(create);
	UT_ASSERT_NOT_NULL(success_counter);
	if (make_stamp != NULL && create != NULL && success_counter != NULL)
		UT_ASSERT(make_stamp < create && create < success_counter);
	UT_ASSERT_STR_CONTAINS(
		current_source,
		"CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);\n"
		"\t\t\tPG_RE_THROW();");
	UT_ASSERT_STR_CONTAINS(
		current_source,
		"CMX_STAT_MEMBER_PROOF_UNKNOWN);\n"
		"\t\t\t\tPG_RE_THROW();");

out:
	free(heap_source);
	free(current_source);
}


int
main(void)
{
	UT_PLAN(36);
	UT_RUN(test_current_multixact_public_symbols_link);
	UT_RUN(test_current_multixact_descriptor_validation);
	UT_RUN(test_current_multixact_descriptor_accepts_cap_and_hashes_order);
	UT_RUN(test_current_multixact_proof_binding_and_order);
	UT_RUN(test_current_multixact_origin_subcommitted_exact_chain);
	UT_RUN(test_current_multixact_updater_candidate_requires_current_exact_binding);
	UT_RUN(test_current_multixact_local_updater_uses_shared_retained_exact_verdict);
	UT_RUN(test_current_multixact_proof_forward_wire_binding);
	UT_RUN(test_current_multixact_proof_request_batching_and_limit);
	UT_RUN(test_current_multixact_members_resolve_256_all_or_nothing);
	UT_RUN(test_current_multixact_proof_reply_wire_binding);
	UT_RUN(test_current_multixact_describe_wire_binding);
	UT_RUN(test_current_multixact_describe_routes_by_mxid_authority);
	UT_RUN(test_current_multixact_native_conflict_matrix);
	UT_RUN(test_current_multixact_compositor_status_mode_state_cross_product);
	UT_RUN(test_current_multixact_active_wait_policies_and_stable_key);
	UT_RUN(test_current_multixact_terminal_nonconflict_and_unknown_precedence);
	UT_RUN(test_current_multixact_member_states_and_self_cid);
	UT_RUN(test_current_multixact_committed_updater_requires_exact_hot_proof);
	UT_RUN(test_current_multixact_updater_proof_typed_positive_outcomes);
	UT_RUN(test_current_multixact_updater_proof_typed_negative_ballot);
	UT_RUN(test_current_multixact_rejects_context_mode_mismatch);
	UT_RUN(test_current_multixact_recompose_filters_terminal_members);
	UT_RUN(test_current_multixact_recompose_upgrades_requester_member);
	UT_RUN(test_current_multixact_recompose_fails_closed_on_incomplete_proof);
	UT_RUN(test_current_multixact_recompose_filters_terminal_before_cap_check);
	UT_RUN(test_current_multixact_heap_bridge_orders_authority_before_decision);
	UT_RUN(test_current_multixact_heap_bridge_covers_four_dml_callers);
	UT_RUN(test_current_multixact_local_compose_installs_requester_tt_authority);
	UT_RUN(test_current_multixact_htsu_routes_peer_current_before_local_decode);
	UT_RUN(test_current_multixact_reader_overlay_unknown_asks_origin);
	UT_RUN(test_current_multixact_latest_tid_uses_authoritative_hot_updater);
	UT_RUN(test_current_multixact_heap_bridge_uses_decoded_cmax_and_follows_active_updater);
	UT_RUN(test_current_multixact_operation_local_descriptor_memo_contract);
	UT_RUN(test_current_multixact_supported_limit_outer_mapping_contract);
	UT_RUN(test_current_multixact_gate_and_recompose_counter_are_publish_exact);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
