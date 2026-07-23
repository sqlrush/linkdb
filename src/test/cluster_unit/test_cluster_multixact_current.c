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

#include "cluster/cluster_multixact_current.h"

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
StaticAssertDecl(CCM_SELF == 0 && CCM_ACTIVE == 1 && CCM_COMMITTED == 2 && CCM_ABORTED == 3
					 && CCM_UNKNOWN == 4,
				 "current MX member-state values changed");
StaticAssertDecl(CMX_DESC_OK == 0 && CMX_DESC_UNKNOWN == 4,
				 "current MX describe-result endpoints changed");
StaticAssertDecl(CMX_RESOLVE_OK == 0 && CMX_RESOLVE_UNKNOWN == 4,
				 "current MX resolve-result endpoints changed");
StaticAssertDecl(CCM_ACTION_UPDATE == 0 && CCM_ACTION_HOT_FOLLOW == 3,
				 "current MX action values changed");
StaticAssertDecl(CCM_SHAPE_LOCK_ONLY == 0 && CCM_SHAPE_DELETED == 2,
				 "current MX tuple-shape values changed");
StaticAssertDecl(CUCP_MATCH == 0 && CUCP_MISMATCH == 1 && CUCP_UNKNOWN == 2,
				 "current MX updater verdict values changed");
StaticAssertDecl(CMDL_CONTINUE == 0 && CMDL_UNKNOWN == 9, "current MX decision values changed");


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


int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_current_multixact_public_symbols_link);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
