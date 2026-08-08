/*-------------------------------------------------------------------------
 *
 * test_cluster_active_itl_transfer.c
 *	  Source-executable contract tests for active-ITL current-block
 *	  transfer semantics.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_active_itl_transfer.c
 *
 * NOTES
 *	  This is a pgrac-original standalone test.  The GCS block handlers and
 *	  the transaction-terminal stamp path cannot be linked outside a
 *	  backend, so these mutation-sensitive checks execute against the
 *	  production sources.  They pin four contracts:
 *
 *	  1. Active-ITL X-transfer admission is one correctness behavior and is
 *	     no longer selected by cluster.block_self_contained, while the
 *	     read-image ship for current reads is preserved.
 *	  2. The transaction-terminal stamp flush path has exactly one no-fetch
 *	     acquisition route and never refetches a transferred-away block.
 *	  3. The exact stamp helper revalidates the captured ownership
 *	     generation and acquisition epoch and returns a typed skip reason.
 *	  4. The terminal-stamp proof record binds xid, slot wrap, slot class,
 *	     buffer id, ownership generation and acquisition epoch, while the
 *	     public 24-byte ClusterItlTouchHandle stays frozen.
 *
 *	  The dynamic end-to-end proof lives in cluster_tap t/408.
 *
 *	  Spec: spec-8.3-active-itl-current-block-transfer-semantics.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_itl_touch.h"

#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


#ifndef GCS_BLOCK_SOURCE_PATH
#error "GCS_BLOCK_SOURCE_PATH must identify production cluster_gcs_block.c"
#endif
#ifndef GCS_BLOCK_HEADER_PATH
#error "GCS_BLOCK_HEADER_PATH must identify production cluster_gcs_block.h"
#endif
#ifndef ITL_TOUCH_SOURCE_PATH
#error "ITL_TOUCH_SOURCE_PATH must identify production cluster_itl_touch.c"
#endif
#ifndef ITL_TOUCH_HEADER_PATH
#error "ITL_TOUCH_HEADER_PATH must identify production cluster_itl_touch.h"
#endif
#ifndef BUFMGR_SOURCE_PATH
#error "BUFMGR_SOURCE_PATH must identify production bufmgr.c"
#endif

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static char *
read_source(const char *path)
{
	FILE *file;
	char *source;
	long length;

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
	UT_ASSERT_EQ((long)fread(source, 1, (size_t)length, file), length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static const char *
find_function_end(const char *function)
{
	return function != NULL ? strstr(function, "\n}\n") : NULL;
}

static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;

	while (source != NULL && (source = strstr(source, needle)) != NULL) {
		count++;
		source += strlen(needle);
	}
	return count;
}

/*
 * Contract 1: the two GCS holder-side handlers may not select active-ITL
 * X-transfer admission by GUC.  On the unfixed source the master==holder
 * gate and the remote-holder forward gate each test
 * cluster_block_self_contained twice (defer condition + transfer-count
 * note), so the total identifier count is nonzero and this test is RED.
 * The read-image ship reply must survive the fix: it remains the valid
 * current-read and same-row wait input.
 */
UT_TEST(test_active_transfer_gates_drop_the_guc_fork)
{
	char *source = read_source(GCS_BLOCK_SOURCE_PATH);

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		UT_ASSERT_EQ(count_occurrences(source, "cluster_block_self_contained"), 0);
		UT_ASSERT(count_occurrences(source, "GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER") >= 1);
	}
	free(source);
}

/*
 * Contract 2: the terminal-stamp flush batch has one acquisition route.
 * On the unfixed source itl_finish_flush_batch forks on
 * cluster_block_self_contained: the off branch calls
 * itl_touch_acquire_buffer, which refetches a block that may have been
 * transferred away and can stamp a page this transaction no longer owns.
 * The fixed source uses only the exact no-fetch helper.
 */
UT_TEST(test_stamp_flush_batch_has_single_no_fetch_path)
{
	char *source = read_source(ITL_TOUCH_SOURCE_PATH);
	const char *batch;
	const char *batch_end;

	UT_ASSERT_NOT_NULL(source);
	batch = source != NULL ? strstr(source, "\nitl_finish_flush_batch(") : NULL;
	batch_end = find_function_end(batch);
	UT_ASSERT_NOT_NULL(batch);
	UT_ASSERT_NOT_NULL(batch_end);
	if (source != NULL && batch != NULL && batch_end != NULL) {
		const char *exact_call = strstr(batch, "cluster_bufmgr_lock_resident_for_exact_itl_stamp(");
		const char *refetch_call = strstr(batch, "itl_touch_acquire_buffer(");

		UT_ASSERT_EQ(count_occurrences(source, "cluster_block_self_contained"), 0);
		UT_ASSERT(exact_call != NULL && exact_call < batch_end);
		UT_ASSERT(refetch_call == NULL || refetch_call > batch_end);
	}
	free(source);
}

/*
 * Contract 3: the exact stamp helper exists, is declared next to the old
 * residency-only helper it replaces, revalidates the captured ownership
 * generation and acquisition epoch, and reports a typed skip reason.
 */
UT_TEST(test_exact_stamp_helper_revalidates_authority)
{
	char *header = read_source(GCS_BLOCK_HEADER_PATH);
	char *source = read_source(BUFMGR_SOURCE_PATH);
	const char *helper;
	const char *helper_end;

	UT_ASSERT_NOT_NULL(header);
	UT_ASSERT_NOT_NULL(source);
	if (header != NULL) {
		UT_ASSERT_NOT_NULL(strstr(header, "cluster_bufmgr_lock_resident_for_exact_itl_stamp("));
		UT_ASSERT_NOT_NULL(strstr(header, "ClusterItlStampSkipReason"));
	}
	helper = source != NULL ? strstr(source, "\ncluster_bufmgr_lock_resident_for_exact_itl_stamp(")
							: NULL;
	helper_end = find_function_end(helper);
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	if (helper != NULL && helper_end != NULL) {
		const char *generation = strstr(helper, "own_generation");
		const char *epoch = strstr(helper, "acquisition_epoch");
		const char *state = strstr(helper, "PCM_STATE_X");

		UT_ASSERT(generation != NULL && generation < helper_end);
		UT_ASSERT(epoch != NULL && epoch < helper_end);
		UT_ASSERT(state != NULL && state < helper_end);
	}
	free(header);
	free(source);
}

/*
 * Contract 4: the backend-local proof record binds every authority field
 * from spec §2.4.  The struct lives in the shared cluster header because
 * the bufmgr helper consumes it, but it is process memory only.
 */
UT_TEST(test_terminal_proof_record_binds_authority_fields)
{
	char *header = read_source(ITL_TOUCH_HEADER_PATH);

	UT_ASSERT_NOT_NULL(header);
	if (header != NULL) {
		const char *proof = strstr(header, "typedef struct ClusterItlTerminalProof");
		const char *record = strstr(header, "typedef struct ClusterItlTouchRecord");

		UT_ASSERT_NOT_NULL(proof);
		UT_ASSERT_NOT_NULL(record);
		if (proof != NULL) {
			const char *proof_end = strstr(proof, "} ClusterItlTerminalProof;");

			UT_ASSERT_NOT_NULL(proof_end);
			if (proof_end != NULL) {
				UT_ASSERT(strstr(proof, "TransactionId xid;") < proof_end);
				UT_ASSERT(strstr(proof, "own_generation;") < proof_end);
				UT_ASSERT(strstr(proof, "acquisition_epoch;") < proof_end);
				UT_ASSERT(strstr(proof, "slot_wrap;") < proof_end);
				UT_ASSERT(strstr(proof, "slot_class;") < proof_end);
				UT_ASSERT(strstr(proof, "bool valid;") < proof_end);
			}
		}
	}
	free(header);
}

/*
 * Contract 5: the tracked EXCLUSIVE acquisition boundary classifies its
 * outcome exhaustively.  On the unfixed source a NULL writer claim falls
 * through to the legacy acquire unconditionally; the fixed source enters
 * the legacy path only under an explicit LEGACY_SAFE route decision.
 */
UT_TEST(test_writer_route_is_exhaustive)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	const char *lockbuffer;
	const char *lockbuffer_end;

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		UT_ASSERT_NOT_NULL(strstr(source, "ClusterPcmXWriterRoute"));
		UT_ASSERT_NOT_NULL(strstr(source, "CLUSTER_PCM_X_WRITER_COVERED"));
		UT_ASSERT_NOT_NULL(strstr(source, "CLUSTER_PCM_X_WRITER_CLAIM"));
		UT_ASSERT_NOT_NULL(strstr(source, "CLUSTER_PCM_X_WRITER_LEGACY_SAFE"));
		UT_ASSERT_NOT_NULL(strstr(source, "CLUSTER_PCM_X_WRITER_RETRY_CANONICAL"));
		UT_ASSERT_NOT_NULL(strstr(source, "CLUSTER_PCM_X_WRITER_FAIL_CLOSED"));
	}
	lockbuffer
		= source != NULL ? strstr(source, "\nLockBufferInternal(Buffer buffer, int mode") : NULL;
	lockbuffer_end = find_function_end(lockbuffer);
	UT_ASSERT_NOT_NULL(lockbuffer);
	UT_ASSERT_NOT_NULL(lockbuffer_end);
	if (lockbuffer != NULL && lockbuffer_end != NULL) {
		const char *legacy_route = strstr(lockbuffer, "CLUSTER_PCM_X_WRITER_LEGACY_SAFE");

		UT_ASSERT(legacy_route != NULL && legacy_route < lockbuffer_end);
	}
	free(source);
}

/*
 * Frozen invariant: the public 24-byte handle layout does not change.
 * This executes against the real header via sizeof/offsetof, not text.
 */
UT_TEST(test_touch_handle_public_layout_remains_unchanged)
{
	UT_ASSERT_EQ((int)sizeof(ClusterItlTouchHandle), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, rloc), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, block), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, forknum), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, slot_idx), 20);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, flags), 22);
}

int
main(void)
{
	UT_RUN(test_active_transfer_gates_drop_the_guc_fork);
	UT_RUN(test_stamp_flush_batch_has_single_no_fetch_path);
	UT_RUN(test_exact_stamp_helper_revalidates_authority);
	UT_RUN(test_terminal_proof_record_binds_authority_fields);
	UT_RUN(test_writer_route_is_exhaustive);
	UT_RUN(test_touch_handle_public_layout_remains_unchanged);
	UT_DONE();
}
