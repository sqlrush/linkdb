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
#include "cluster/cluster_xid_stripe.h"
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
 * Runtime seams for the descriptor-authority routing test.  They keep this
 * unit binary independent of shmem, pg_multixact SLRU, and DATA transport.
 */
int cluster_node_id = -1;
int cluster_subtrans_max_chain_depth = 32;
static uint64 test_runtime_epoch;
static int test_runtime_mxid_origin = -1;
static bool test_runtime_mxid_mine;
static int test_runtime_native_describe_calls;
static int test_runtime_remote_describe_calls;
static bool test_runtime_remote_describe_ok;
static ClusterSemanticAdmissionResult test_runtime_tt_admission;
static ClusterTTStatusSourceResult test_runtime_tt_source_result;

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

int
cluster_xid_origin_slot(TransactionId xid)
{
	return 4 + (int)(xid % 3);
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

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op,
								  const ClusterTTStatusSourceRequest *request,
								  ClusterTTStatusSourceResult *result)
{
	UT_ASSERT_EQ(op, CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID_CANDIDATE);
	UT_ASSERT_NOT_NULL(request);
	UT_ASSERT_NOT_NULL(request->key);
	if (result != NULL)
		*result = test_runtime_tt_source_result;
	return test_runtime_tt_admission;
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
	return NormalTransactionIdPrecedes(id1, id2);
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
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_forward);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_build_proof_requests);
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
test_proof_request(ClusterCurrentMxProofForwardV2 *request, uint8 body_kind, uint8 entry_count)
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
	request->prefix.chunk_count_minus_one = 2;
	request->prefix.entry_count = entry_count;
	request->prefix.body_kind = body_kind;
	request->prefix.kind = CLUSTER_CURRENT_MX_MEMBER_PROOF_KIND_FROZEN;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&request->prefix,
											 UINT64CONST(0x8877665544332211));
	request->trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request->trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	if (body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
		for (i = 0; i < entry_count; i++) {
			request->trailer.body.asks[i].xid = (TransactionId)(100 + 3 * i);
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


typedef struct TestExactLookupEntry {
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
} TestExactLookupEntry;


typedef struct TestExactLookupTable {
	TestExactLookupEntry entries[4];
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


UT_TEST(test_current_multixact_origin_member_proof_follows_exact_parent)
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
	UT_ASSERT_EQ(table.calls, 1);

	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, true, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_SELF);

	table.entries[0].result.status = CLUSTER_TT_STATUS_COMMITTED;
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

	initial.parent_key.origin_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 false);
	UT_ASSERT_EQ(proof.state, CCM_UNKNOWN);
}


UT_TEST(test_current_multixact_updater_candidate_requires_current_exact_binding)
{
	ClusterTTStatusKey current = test_ttkey(5, 100, 20);
	ClusterTTStatusKey candidate = current;
	ClusterTTStatusKey selected;
	ClusterTTStatusResult selected_result;

	cluster_node_id = 5;
	memset(&test_runtime_tt_source_result, 0, sizeof(test_runtime_tt_source_result));
	test_runtime_tt_admission = CLUSTER_SEMANTIC_ADMISSION_OK;
	test_runtime_tt_source_result.bool_value = true;
	test_runtime_tt_source_result.current_key = current;
	test_runtime_tt_source_result.lookup.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	test_runtime_tt_source_result.lookup.status_epoch = 9;
	test_runtime_tt_source_result.lookup.authoritative = true;
	test_runtime_tt_source_result.current_key_verdict = CLUSTER_TT_CURRENT_KEY_MATCH;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_MATCH);
	UT_ASSERT_EQ(memcmp(&selected, &current, sizeof(current)), 0);

	test_runtime_tt_source_result.current_key_verdict = CLUSTER_TT_CURRENT_KEY_UNKNOWN;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	UT_ASSERT_EQ(selected_result.status, CLUSTER_TT_STATUS_UNKNOWN);

	test_runtime_tt_source_result.current_key_verdict = CLUSTER_TT_CURRENT_KEY_MATCH;
	test_runtime_tt_admission = CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);

	test_runtime_tt_admission = CLUSTER_SEMANTIC_ADMISSION_OK;
	candidate.local_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	candidate = current;
	candidate.cluster_epoch++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);

	cluster_node_id = -1;
	memset(&test_runtime_tt_source_result, 0, sizeof(test_runtime_tt_source_result));
	test_runtime_tt_admission = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}


UT_TEST(test_current_multixact_proof_forward_wire_binding)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofForwardV2 decoded;

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 7);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 true);
	UT_ASSERT_EQ(decoded.prefix.entry_count, 7);
	UT_ASSERT_EQ(ClusterCurrentMxProofPrefixGetDescriptorHash(&decoded.prefix),
				 UINT64CONST(0x8877665544332211));

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request) - 1, 3, 5, 9, &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request) + 1, 3, 5, 9, &decoded),
				 false);
	request.trailer.body.asks[6].xid = 101;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 false);

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE, 1);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 true);
	request.trailer.body.updater.challenge.member_status = MultiXactStatusForShare;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 false);
}


UT_TEST(test_current_multixact_proof_request_batches_by_member_origin)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[8];
	uint16 origins[8];
	ClusterCurrentMxProofRequestPlan plans[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	ClusterCurrentUpdaterChallenge challenge;
	bool seen[8];
	uint64 hash;
	uint16 plan_count = 0;
	uint16 seen_count = 0;
	uint16 i;
	uint16 j;

	for (i = 0; i < lengthof(members); i++) {
		test_member(&members[i], (TransactionId)(100 + i), MultiXactStatusForShare);
		origins[i] = (uint16)cluster_xid_origin_slot(members[i].xid);
	}
	members[5].member_status = MultiXactStatusNoKeyUpdate;
	memset(&challenge, 0, sizeof(challenge));
	challenge.candidate_next_xmin_key = test_ttkey(origins[5], members[5].xid, 50);
	challenge.updater_xid = members[5].xid;
	challenge.member_ordinal = 5;
	hash = cluster_multixact_current_descriptor_hash(&key, members, lengthof(members));

	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, lengthof(members), hash, &challenge, 801, 9, 3, 7,
					 plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(plan_count, 4);
	memset(seen, 0, sizeof(seen));
	for (i = 0; i < plan_count; i++) {
		ClusterCurrentMxProofForwardV2 decoded;

		UT_ASSERT_EQ(plans[i].request.prefix.chunk_ordinal, i);
		UT_ASSERT_EQ(plans[i].request.prefix.chunk_count_minus_one, plan_count - 1);
		UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
						 &plans[i].request, sizeof(plans[i].request), 3,
						 plans[i].destination_node_id, 9, &decoded),
					 true);
		if (decoded.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
			UT_ASSERT_EQ(decoded.trailer.body.updater.challenge.member_ordinal, 5);
			UT_ASSERT(!seen[5]);
			seen[5] = true;
			seen_count++;
		} else {
			for (j = 0; j < decoded.prefix.entry_count; j++) {
				uint16 ordinal = decoded.trailer.body.asks[j].member_ordinal;

				UT_ASSERT(ordinal < lengthof(members));
				UT_ASSERT(!seen[ordinal]);
				seen[ordinal] = true;
				seen_count++;
			}
		}
	}
	UT_ASSERT_EQ(seen_count, lengthof(members));

	origins[0] = (uint16)((origins[0] + 1) % CLUSTER_MAX_NODES);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, lengthof(members), hash, &challenge, 802, 9, 3, 7,
					 plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(plan_count, 0);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, hash, &challenge,
					 803, 9, 3, 7, plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
}


UT_TEST(test_current_multixact_describe_wire_binding)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxDescribeForwardV2 forward;
	ClusterCurrentMxDescribeForwardV2 decoded;
	ClusterCurrentMxDescribeReplyPage page;
	ClusterCurrentMxMemberDesc out[4];
	uint16 out_count;
	uint32 out_total;

	memset(&forward, 0, sizeof(forward));
	forward.prefix.request_id = 501;
	forward.prefix.epoch = 9;
	forward.prefix.mxkey = key;
	forward.prefix.original_requester_node = 1;
	forward.prefix.requester_backend_id = 44;
	forward.prefix.kind = CLUSTER_CURRENT_MX_DESCRIBE_KIND_FROZEN;
	forward.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	forward.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 1, 2, 9, &decoded),
				 true);
	UT_ASSERT_EQ(memcmp(&decoded, &forward, sizeof(decoded)), 0);

	forward.prefix.reserved_b[3] = 1;
	memset(&decoded, 0xa5, sizeof(decoded));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 1, 2, 9, &decoded),
				 false);
	UT_ASSERT_EQ(decoded.prefix.request_id, 0);
	forward.prefix.reserved_b[3] = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 3, 2, 9, &decoded),
				 false);

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = CLUSTER_CURRENT_MX_DESCRIBE_KIND_FROZEN;
	page.header.result = CMX_DESC_OK;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = 2;
	page.header.entry_count = 2;
	page.header.wire_length = sizeof(page.header) + 2 * sizeof(page.members[0]);
	test_member(&page.members[0], 100, MultiXactStatusForShare);
	test_member(&page.members[1], 101, MultiXactStatusNoKeyUpdate);
	page.header.descriptor_hash
		= cluster_multixact_current_descriptor_hash(&key, page.members, 2);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(out_count, 2);
	UT_ASSERT_EQ(out_total, 2);
	UT_ASSERT_EQ(out[1].xid, 101);

	page.header.descriptor_hash++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(out_count, 0);
	page.header.descriptor_hash--;

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = CLUSTER_CURRENT_MX_DESCRIBE_KIND_FROZEN;
	page.header.result = CMX_DESC_SUPPORTED_LIMIT;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = CLUSTER_CURRENT_MX_MAX_MEMBERS + 1;
	page.header.wire_length = sizeof(page.header);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(out_total, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1);
	page.reserved[0] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
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

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 3, 504, MultiXactStatusForShare, normalized,
					 lengthof(normalized), &normalized_count),
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

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 2, 601, MultiXactStatusForUpdate, normalized,
					 lengthof(normalized), &normalized_count),
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

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 2, 703, MultiXactStatusForShare, normalized,
					 lengthof(normalized), &normalized_count),
				 CMX_RECOMPOSE_UNKNOWN);
	UT_ASSERT_EQ(normalized_count, 0);
	UT_ASSERT_EQ(normalized[0].xid, InvalidTransactionId);

	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 2, 52);
	members[1].member_status = MultiXactStatusNoKeyUpdate;
	proofs[1].member_status = MultiXactStatusNoKeyUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 2, 703, MultiXactStatusForShare, normalized,
					 lengthof(normalized), &normalized_count),
				 CMX_RECOMPOSE_DENIED);
	UT_ASSERT_EQ(normalized_count, 0);
}


UT_TEST(test_current_multixact_recompose_filters_before_cap_check)
{
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	MultiXactMember normalized[2];
	uint16 normalized_count = 0;
	uint16 i;

	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++) {
		test_member(&members[i], (TransactionId)(1000 + i), MultiXactStatusForKeyShare);
		test_proof(&proofs[i], &members[i], i, CCM_ABORTED, 2, (uint16)(100 + i));
	}
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 100);

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, CLUSTER_CURRENT_MX_MAX_MEMBERS, 5000,
					 MultiXactStatusForShare, normalized, lengthof(normalized),
					 &normalized_count),
				 CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 1000);
	UT_ASSERT_EQ(normalized[1].xid, 5000);
}


int
main(void)
{
	UT_PLAN(21);
	UT_RUN(test_current_multixact_public_symbols_link);
	UT_RUN(test_current_multixact_descriptor_validation);
	UT_RUN(test_current_multixact_descriptor_accepts_cap_and_hashes_order);
	UT_RUN(test_current_multixact_proof_binding_and_order);
	UT_RUN(test_current_multixact_origin_member_proof_follows_exact_parent);
	UT_RUN(test_current_multixact_updater_candidate_requires_current_exact_binding);
	UT_RUN(test_current_multixact_proof_forward_wire_binding);
	UT_RUN(test_current_multixact_proof_request_batches_by_member_origin);
	UT_RUN(test_current_multixact_describe_wire_binding);
	UT_RUN(test_current_multixact_describe_routes_by_mxid_authority);
	UT_RUN(test_current_multixact_native_conflict_matrix);
	UT_RUN(test_current_multixact_compositor_status_mode_state_cross_product);
	UT_RUN(test_current_multixact_active_wait_policies_and_stable_key);
	UT_RUN(test_current_multixact_terminal_nonconflict_and_unknown_precedence);
	UT_RUN(test_current_multixact_member_states_and_self_cid);
	UT_RUN(test_current_multixact_committed_updater_requires_exact_hot_proof);
	UT_RUN(test_current_multixact_rejects_context_mode_mismatch);
	UT_RUN(test_current_multixact_recompose_filters_terminal_members);
	UT_RUN(test_current_multixact_recompose_upgrades_requester_member);
	UT_RUN(test_current_multixact_recompose_fails_closed_on_incomplete_proof);
	UT_RUN(test_current_multixact_recompose_filters_before_cap_check);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
