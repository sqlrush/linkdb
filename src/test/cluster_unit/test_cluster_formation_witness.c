/*-------------------------------------------------------------------------
 * STOP-02 \u00a717.6 opaque formation-witness lifecycle tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "cluster/cluster_recovery_duty.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterFormationSnapshotV1 snapshots[2];
static int snapshot_call;
static bool snapshot_available;
static ClusterFenceAuthorityProof durable_proof;
static ClusterFenceAuthorityReadResult durable_result;
static bool cache_publish_ok;
static ClusterFenceAuthorityCacheResult cache_result;
static uint64 cache_sequence;

void *palloc(Size size) { return malloc(size); }
void pfree(void *pointer) { free(pointer); }
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

void
pg_usleep(long microsec)
{
	(void)microsec;
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 origin_thread,
											ClusterFormationSnapshotV1 *out)
{
	(void)origin_thread;
	if (!snapshot_available)
		return false;
	*out = snapshots[snapshot_call % 2];
	snapshot_call++;
	return true;
}

ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	if (durable_result == CLUSTER_FENCE_AUTHORITY_OK)
		*out = durable_proof;
	return durable_result;
}

bool
cluster_write_fence_authority_cache_publish_if_unchanged(const ClusterFenceMarker *marker,
												 uint64 published_at_us,
												 uint64 expected_sequence)
{
	(void)marker;
	(void)published_at_us;
	(void)expected_sequence;
	return cache_publish_ok;
}

uint64
cluster_write_fence_authority_cache_sequence(void)
{
	return cache_sequence;
}

/* This fixture exercises only formation-witness entry points.  The same
 * product object also owns the runtime owner-rejoin path; keep those unused
 * dependencies fail-closed without widening this unit's link boundary. */
ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(
	int32 old_node_id, ClusterControlRootIdentity *out_identity,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token)
{
	(void)old_node_id;
	(void)out_identity;
	(void)out_snapshot;
	(void)out_token;
	return CLUSTER_CONTROL_ROOT_ABSENT;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_read_v1(
	int32 node_id, const ClusterWalThreadClaim *immutable_claim,
	uint64 frozen_admitted_bitmap_low, uint64 frozen_admitted_bitmap_high,
	uint64 *out_incarnation)
{
	(void)node_id;
	(void)immutable_claim;
	(void)frozen_admitted_bitmap_low;
	(void)frozen_admitted_bitmap_high;
	(void)out_incarnation;
	return CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED;
}

ClusterControlRootResult
cluster_control_root_compare_and_publish(
	const ClusterControlRootReadToken *expected_token,
	const ClusterControlRootPatch *patch, ClusterControlRootPublishReason reason,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token)
{
	(void)expected_token;
	(void)patch;
	(void)reason;
	(void)out_snapshot;
	(void)out_token;
	return CLUSTER_CONTROL_ROOT_ABSENT;
}

ClusterFenceAuthorityCacheResult
cluster_write_fence_revalidate_cached_nowait(const ClusterFenceMarker *expected, uint64 now_us)
{
	(void)expected;
	(void)now_us;
	return cache_result;
}

static void
build_ready_fixture(void)
{
	ClusterFormationSnapshotV1 *s = &snapshots[0];
	int32 origin = 3;

	memset(snapshots, 0, sizeof(snapshots));
	s->applied.event_id = UINT64_C(0xAA);
	s->applied.coordinator_node_id = 0;
	s->applied.new_epoch = 9;
	s->applied.cssd_dead_generation = 5;
	s->applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	s->applied.dead_bitmap[0] = 0x08;
	s->excluded_bitmap[0] = 0x08;
	s->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	s->membership.membership_state[origin] = CLUSTER_MEMBER_DEAD;
	s->membership.last_admitted_incarnation[origin] = 77;
	s->victim_incarnation = 77;
	s->local_epoch = 9;
	s->self_join_admitted = 1;
	snapshots[1] = snapshots[0];

	memset(&durable_proof, 0, sizeof(durable_proof));
	durable_proof.marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	durable_proof.marker.version = CLUSTER_FENCE_MARKER_VERSION;
	durable_proof.marker.fence_epoch = 9;
	durable_proof.marker.fence_event_id = UINT64_C(0xAA);
	durable_proof.marker.fence_generation = 5;
	durable_proof.marker.issuer_node_id = 0;
	durable_proof.marker.fenced_dead_bitmap[0] = 0x08;
	durable_proof.agree_disk_count = 2;
	durable_proof.total_disk_count = 3;
	snapshot_call = 0;
	snapshot_available = true;
	durable_result = CLUSTER_FENCE_AUTHORITY_OK;
	cache_publish_ok = true;
	cache_result = CLUSTER_FENCE_CACHE_MATCH;
	cache_sequence = 2;
}

UT_TEST(test_witness_bad_arguments_leave_null)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(0, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT);
	UT_ASSERT(witness == NULL);
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 0, &witness),
				 CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT);
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, NULL),
				 CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT);
}

UT_TEST(test_witness_ready_borrow_revalidate_destroy)
{
	ClusterFormationWitnessV1 *witness = NULL;
	const ClusterFenceAuthorityProof *proof;

	build_ready_fixture();
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	UT_ASSERT(witness != NULL);
	proof = cluster_formation_witness_authority(witness);
	UT_ASSERT(proof != NULL);
	UT_ASSERT_EQ(proof->marker.fence_epoch, 9);
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	cluster_formation_witness_destroy(&witness);
	UT_ASSERT(witness == NULL);
	cluster_formation_witness_destroy(&witness);
}

UT_TEST(test_witness_unstable_or_unavailable_never_installs_handle)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	snapshots[1].local_epoch++;
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 1, &witness),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	UT_ASSERT(witness == NULL);
	build_ready_fixture();
	durable_result = CLUSTER_FENCE_AUTHORITY_NO_CONFIG;
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE);
	UT_ASSERT(witness == NULL);
}

UT_TEST(test_witness_revalidate_maps_stale_and_unavailable)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	cache_result = CLUSTER_FENCE_CACHE_STALE;
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(witness),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	cache_result = CLUSTER_FENCE_CACHE_UNAVAILABLE;
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(witness),
				 CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE);
	cluster_formation_witness_destroy(&witness);
}

UT_TEST(test_live_witness_requires_same_stable_member_formation)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	snapshots[0].membership.last_admitted_incarnation[0] = 55;
	snapshots[1] = snapshots[0];
	UT_ASSERT_EQ(cluster_formation_witness_build_live_wait(1, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	UT_ASSERT_NOT_NULL(witness);
	cluster_formation_witness_destroy(&witness);

	build_ready_fixture();
	snapshots[0].membership.membership_state[0] = CLUSTER_MEMBER_JOINING;
	snapshots[0].membership.last_admitted_incarnation[0] = 55;
	snapshots[1] = snapshots[0];
	UT_ASSERT_EQ(cluster_formation_witness_build_live_wait(1, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH);
	UT_ASSERT_NULL(witness);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_witness_bad_arguments_leave_null);
	UT_RUN(test_witness_ready_borrow_revalidate_destroy);
	UT_RUN(test_witness_unstable_or_unavailable_never_installs_handle);
	UT_RUN(test_witness_revalidate_maps_stale_and_unavailable);
	UT_RUN(test_live_witness_requires_same_stable_member_formation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
