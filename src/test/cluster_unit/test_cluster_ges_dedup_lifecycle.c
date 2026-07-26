/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_dedup_lifecycle.c
 *	  S3-P0-10 GES retransmit-dedup normal-terminal lifecycle REDs.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>

#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifndef PROC_SOURCE_PATH
#define PROC_SOURCE_PATH "../../backend/storage/lmgr/proc.c"
#endif

#ifndef POSTINIT_SOURCE_PATH
#define POSTINIT_SOURCE_PATH "../../backend/utils/init/postinit.c"
#endif

static char *
read_source(const char *path)
{
	FILE *file;
	long size;
	char *source;

	file = fopen(path, "rb");
	UT_ASSERT(file != NULL);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	size = ftell(file);
	UT_ASSERT(size >= 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t) size + 1);
	UT_ASSERT(source != NULL);
	UT_ASSERT_EQ(fread(source, 1, (size_t) size, file), (size_t) size);
	source[size] = '\0';
	fclose(file);
	return source;
}

UT_TEST(opcode_predicate_is_one_authority)
{
	GesRequestPayload cleanup;

	UT_ASSERT(cluster_ges_dedup_opcode_uses_cache(GES_REQ_OPCODE_REQUEST));
	UT_ASSERT(cluster_ges_dedup_opcode_uses_cache(GES_REQ_OPCODE_CONVERT));
	UT_ASSERT(cluster_ges_dedup_opcode_uses_cache(GES_REQ_OPCODE_RELEASE));
	UT_ASSERT(cluster_ges_dedup_opcode_uses_cache(GES_REQ_OPCODE_REDECLARE));
	UT_ASSERT(cluster_ges_dedup_opcode_uses_cache(GES_REQ_OPCODE_REQUEST_NOWAIT));

	/* Fire-and-forget rollback has no retransmit/reply contract.  Registering
	 * it creates a permanent IN_FLIGHT row because no cached reply is owned. */
	UT_ASSERT(!cluster_ges_dedup_opcode_uses_cache(GES_REQ_OPCODE_CONVERT_ROLLBACK));

	memset(&cleanup, 0, sizeof(cleanup));
	cleanup.opcode = GES_REQ_OPCODE_RELEASE;
	UT_ASSERT(cluster_ges_dedup_request_uses_cache(&cleanup));
	cleanup.current_mode = GES_RELEASE_CURRENT_MODE_CLEANUP_BYPASS;
	UT_ASSERT(!cluster_ges_dedup_request_uses_cache(&cleanup));
}

UT_TEST(wire_assignments_and_shapes_are_fixed)
{
	UT_ASSERT_EQ(PGRAC_IC_MSG_GES_DEDUP_DONE, 68);
	UT_ASSERT_EQ(PGRAC_IC_MSG_GES_DEDUP_ACK, 69);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_GES_DEDUP_DONE_V1, UINT32_C(0x00008000));
	UT_ASSERT_EQ(sizeof(GesDedupLifecyclePayload), 64);
}

UT_TEST(exact_done_identity_rejects_wrong_boot_or_reserved_bits)
{
	GesDedupLifecyclePayload done;

	memset(&done, 0, sizeof(done));
	done.version = GES_DEDUP_LIFECYCLE_VERSION;
	done.kind = GES_DEDUP_LIFECYCLE_EXACT_DONE;
	done.origin_node_id = 3;
	done.holder_procno = 17;
	done.opcode = GES_REQ_OPCODE_REQUEST;
	done.request_id = 101;
	done.cluster_epoch = 7;
	done.shard_master_generation = 9;
	done.origin_boot_incarnation = 1001;
	done.target_boot_incarnation = 2001;
	done.link_generation = 23;

	UT_ASSERT(cluster_ges_dedup_lifecycle_payload_valid(&done, 3, 1001, 2001));
	done.origin_boot_incarnation++;
	UT_ASSERT(!cluster_ges_dedup_lifecycle_payload_valid(&done, 3, 1001, 2001));
	done.origin_boot_incarnation--;
	done.reserved = 1;
	UT_ASSERT(!cluster_ges_dedup_lifecycle_payload_valid(&done, 3, 1001, 2001));
}

UT_TEST(proc_exit_hwm_cannot_retire_reused_procno_request)
{
	/* request_id is node-global monotonic (spec-5.16).  A cleanup sampled at
	 * HWM=500 may retire the old procno's <=500 rows, never the reused
	 * procno's first fresh request 501. */
	UT_ASSERT(cluster_ges_dedup_hwm_covers_request(500, 500));
	UT_ASSERT(cluster_ges_dedup_hwm_covers_request(499, 500));
	UT_ASSERT(!cluster_ges_dedup_hwm_covers_request(501, 500));
}

UT_TEST(ack_must_echo_the_exact_done_identity)
{
	GesDedupLifecyclePayload done;
	GesDedupLifecyclePayload ack;

	memset(&done, 0, sizeof(done));
	done.version = GES_DEDUP_LIFECYCLE_VERSION;
	done.kind = GES_DEDUP_LIFECYCLE_EXACT_DONE;
	done.origin_node_id = 2;
	done.holder_procno = 31;
	done.opcode = GES_REQ_OPCODE_RELEASE;
	done.request_id = 808;
	done.cluster_epoch = 11;
	done.shard_master_generation = 13;
	done.origin_boot_incarnation = 17;
	done.target_boot_incarnation = 19;
	done.link_generation = 29;

	ack = done;
	ack.kind = GES_DEDUP_LIFECYCLE_ACK;
	ack.status = GES_DEDUP_ACK_REMOVED;
	UT_ASSERT(cluster_ges_dedup_ack_matches_done(&done, &ack));

	ack.request_id++;
	UT_ASSERT(!cluster_ges_dedup_ack_matches_done(&done, &ack));
}

UT_TEST(node_global_request_id_must_fail_closed_before_wrap)
{
	UT_ASSERT(cluster_ges_request_id_can_advance(0));
	UT_ASSERT(cluster_ges_request_id_can_advance(UINT64_MAX - 1));
	UT_ASSERT(!cluster_ges_request_id_can_advance(UINT64_MAX));
}

UT_TEST(auxiliary_proc_reuse_has_two_phase_cluster_identity)
{
	char *proc_source = read_source(PROC_SOURCE_PATH);
	char *postinit_source = read_source(POSTINIT_SOURCE_PATH);
	char *reset_definition;
	char *aux_reset;
	char *aux_pid_publish;

	/*
	 * S3-P0-10: a reused auxiliary PGPROC must discard its predecessor's
	 * identity before publishing the new pid.  The common finalizer is
	 * shared with ordinary backends and is retried by BaseInit, after
	 * EXEC_BACKEND has reattached the GRD shared-memory pointer.
	 */
	reset_definition = strstr(proc_source,
							  "ResetProcessClusterIdentity(PGPROC *proc)");
	aux_reset = strstr(proc_source,
					   "ResetProcessClusterIdentity(auxproc);");
	aux_pid_publish = strstr(proc_source,
							 "((volatile PGPROC *) auxproc)->pid = MyProcPid;");
	UT_ASSERT(reset_definition != NULL);
	UT_ASSERT(aux_reset != NULL);
	UT_ASSERT(aux_pid_publish != NULL);
	UT_ASSERT(aux_reset < aux_pid_publish);
	UT_ASSERT(strstr(proc_source, "InitProcessClusterIdentity(void)") != NULL);
	UT_ASSERT(strstr(proc_source,
					 "generation = cluster_grd_alloc_generation();") != NULL);
	UT_ASSERT(strstr(postinit_source, "InitProcessClusterIdentity();") != NULL);

	free(postinit_source);
	free(proc_source);
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(7);
	UT_RUN(opcode_predicate_is_one_authority);
	UT_RUN(wire_assignments_and_shapes_are_fixed);
	UT_RUN(exact_done_identity_rejects_wrong_boot_or_reserved_bits);
	UT_RUN(proc_exit_hwm_cannot_retire_reused_procno_request);
	UT_RUN(ack_must_echo_the_exact_done_identity);
	UT_RUN(node_global_request_id_must_fail_closed_before_wrap);
	UT_RUN(auxiliary_proc_reuse_has_two_phase_cluster_identity);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
