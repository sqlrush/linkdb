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
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_mxid_stripe.h"
#include "storage/lock.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


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
static uint64 test_runtime_epoch;
static int test_runtime_mxid_origin = -1;
static bool test_runtime_mxid_mine;
static int test_runtime_native_describe_calls;
static int test_runtime_remote_describe_calls;
static bool test_runtime_remote_describe_ok;

void
pfree(void *pointer)
{
	free(pointer);
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
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT == 21,
				 "current MX describe reply status must append at 21");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT == 22,
				 "current MX member-proof reply status must append at 22");


UT_TEST(test_current_multixact_public_symbols_link)
{
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_descriptor);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_proof_set);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_descriptor_hash);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_updater_proof);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_decide);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_describe);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_members_resolve);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_recompose);
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


UT_TEST(test_current_multixact_descriptor_validation)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];

	test_member(&members[0], 100, MultiXactStatusForShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);

	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_OK);
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

	proof01[0].key.local_xid++;
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

	/* Underivable/wrong-origin identity fails before either authority read. */
	test_runtime_mxid_origin = -1;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
													&members_count, &reported_total),
				 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 1);
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

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusForUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 60);
	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 3, 61);
	test_context(&ctx, &key, CCM_ACTION_LOCK, MultiXactStatusForNoKeyUpdate,
				 LockTupleNoKeyExclusive);

	/* A terminal locker is gone; the active KeyShare holder is compatible. */
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	/* An ACTIVE updater uses the same matrix and can also be compatible. */
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	proofs[0].member_status = MultiXactStatusNoKeyUpdate;
	ctx.desired_status = MultiXactStatusForKeyShare;
	ctx.lock_mode = LockTupleKeyShare;
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	proofs[0].state = CCM_ABORTED;
	memset(&proofs[0].key, 0, sizeof(proofs[0].key));
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


int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_current_multixact_public_symbols_link);
	UT_RUN(test_current_multixact_descriptor_validation);
	UT_RUN(test_current_multixact_descriptor_accepts_cap_and_hashes_order);
	UT_RUN(test_current_multixact_proof_binding_and_order);
	UT_RUN(test_current_multixact_describe_wire_binding);
	UT_RUN(test_current_multixact_describe_routes_by_mxid_authority);
	UT_RUN(test_current_multixact_native_conflict_matrix);
	UT_RUN(test_current_multixact_compositor_status_mode_state_cross_product);
	UT_RUN(test_current_multixact_active_wait_policies_and_stable_key);
	UT_RUN(test_current_multixact_terminal_nonconflict_and_unknown_precedence);
	UT_RUN(test_current_multixact_member_states_and_self_cid);
	UT_RUN(test_current_multixact_committed_updater_requires_exact_hot_proof);
	UT_RUN(test_current_multixact_rejects_context_mode_mismatch);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
