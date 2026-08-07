/*-------------------------------------------------------------------------
 *
 * test_cluster_share_barrier.c
 *	  Source-executable contract tests for barrier-aware SHARE cleanup.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_share_barrier.c
 *
 * NOTES
 *	  This is a pgrac-original standalone test.  Full bufmgr.c cannot be
 *	  linked outside a backend, so these mutation-sensitive checks execute
 *	  against the production source and pin the refusal epilogue's ownership
 *	  order and public entry-point contract.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


#ifndef BUFMGR_SOURCE_PATH
#error "BUFMGR_SOURCE_PATH must identify production bufmgr.c"
#endif
#ifndef BUFMGR_HEADER_PATH
#error "BUFMGR_HEADER_PATH must identify production bufmgr.h"
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
	FILE	   *file;
	char	   *source;
	long		length;

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t) length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
	{
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ((long) fread(source, 1, (size_t) length, file), length);
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
	int			count = 0;

	while (source != NULL && (source = strstr(source, needle)) != NULL)
	{
		count++;
		source += strlen(needle);
	}
	return count;
}

UT_TEST(test_share_wrapper_reports_only_typed_refusal)
{
	char	   *header = read_source(BUFMGR_HEADER_PATH);
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *wrapper;
	const char *wrapper_end;

	UT_ASSERT_NOT_NULL(header);
	UT_ASSERT_NOT_NULL(source);
	if (header != NULL)
		UT_ASSERT_NOT_NULL(strstr(header,
								 "extern bool ClusterLockBufferShareBarrierAware(Buffer buffer);"));
	wrapper = source != NULL ? strstr(source,
									 "\nClusterLockBufferShareBarrierAware(Buffer buffer)") : NULL;
	wrapper_end = find_function_end(wrapper);
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper_end);
	if (wrapper != NULL && wrapper_end != NULL)
	{
		const char *call = strstr(wrapper,
								  "LockBufferInternal(buffer, BUFFER_LOCK_SHARE, &barrier_refused);");

		UT_ASSERT(call != NULL && call < wrapper_end);
		UT_ASSERT(strstr(wrapper, "return !barrier_refused;") < wrapper_end);
	}
	free(header);
	free(source);
}

UT_TEST(test_all_typed_refusals_converge_on_one_epilogue)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *lockbuffer;
	const char *lockbuffer_end;
	const char *label;

	UT_ASSERT_NOT_NULL(source);
	lockbuffer = source != NULL ? strstr(source,
									"\nLockBufferInternal(Buffer buffer, int mode") : NULL;
	lockbuffer_end = find_function_end(lockbuffer);
	label = lockbuffer != NULL ? strstr(lockbuffer,
									 "cluster_lockbuffer_barrier_refusal:") : NULL;
	UT_ASSERT_NOT_NULL(lockbuffer);
	UT_ASSERT_NOT_NULL(lockbuffer_end);
	UT_ASSERT(label != NULL && label < lockbuffer_end);
	if (lockbuffer != NULL && lockbuffer_end != NULL && label != NULL)
	{
		UT_ASSERT(count_occurrences(lockbuffer,
									"goto cluster_lockbuffer_barrier_refusal;") >= 3);
		UT_ASSERT(strstr(label,
							 "cluster_bufmgr_pcm_unwind_barrier_refusal(") < lockbuffer_end);
	}
	free(source);
}

UT_TEST(test_refusal_cleanup_preserves_required_order)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *cleanup;
	const char *cleanup_end;
	const char *holder;
	const char *writer;
	const char *master_release;
	const char *local_converge;
	const char *pending_abort;

	UT_ASSERT_NOT_NULL(source);
	cleanup = source != NULL ? strstr(source,
								   "\ncluster_bufmgr_pcm_unwind_barrier_refusal(") : NULL;
	cleanup_end = find_function_end(cleanup);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	if (cleanup != NULL && cleanup_end != NULL)
	{
		holder = strstr(cleanup, "cluster_bufmgr_pcm_x_holder_abort_acquiring(");
		writer = strstr(cleanup, "cluster_bufmgr_pcm_x_writer_abort_acquiring(");
		master_release = strstr(cleanup, "cluster_pcm_lock_release_buffer_for_eviction(");
		local_converge = strstr(cleanup,
								"cluster_pcm_own_abort_grant_after_master_rollback(");
		pending_abort = strstr(cleanup, "cluster_pcm_own_abort_grant_or_error(");
		UT_ASSERT(holder != NULL && holder < cleanup_end);
		UT_ASSERT(writer != NULL && holder < writer && writer < cleanup_end);
		UT_ASSERT(master_release != NULL && writer < master_release && master_release < cleanup_end);
		UT_ASSERT(local_converge != NULL && master_release < local_converge
				  && local_converge < cleanup_end);
		UT_ASSERT(pending_abort != NULL && local_converge < pending_abort
				  && pending_abort < cleanup_end);
	}
	free(source);
}

UT_TEST(test_refusal_cleanup_proves_empty_target_ownership)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *cleanup;
	const char *cleanup_end;

	UT_ASSERT_NOT_NULL(source);
	cleanup = source != NULL ? strstr(source,
								   "\ncluster_bufmgr_pcm_unwind_barrier_refusal(") : NULL;
	cleanup_end = find_function_end(cleanup);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	if (cleanup != NULL && cleanup_end != NULL)
	{
		const char *content = strstr(cleanup, "LWLockHeldByMe(");
		const char *holder = strstr(cleanup, "cluster_bufmgr_pcm_x_holder_find(buf)");
		const char *writer = strstr(cleanup, "cluster_bufmgr_pcm_x_writer_find(buf)");
		const char *pending = strstr(cleanup, "PCM_OWN_FLAG_GRANT_PENDING");
		const char *release_buffer = strstr(cleanup, "ReleaseBuffer(");

		/* The step-7 postcondition asserts, all inside this function body. */
		UT_ASSERT(content != NULL && content < cleanup_end);
		UT_ASSERT(holder != NULL && holder < cleanup_end);
		UT_ASSERT(writer != NULL && writer < cleanup_end);
		UT_ASSERT(pending != NULL && pending < cleanup_end);
		/* A hit must lie beyond the epilogue: the caller owns every pin. */
		UT_ASSERT(release_buffer == NULL || release_buffer > cleanup_end);
	}
	free(source);
}

UT_TEST(test_share_injection_uses_the_real_cleanup_boundary)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *lockbuffer;
	const char *inject;
	const char *lock_acquire;

	UT_ASSERT_NOT_NULL(source);
	lockbuffer = source != NULL ? strstr(source,
									"\nLockBufferInternal(Buffer buffer, int mode") : NULL;
	inject = lockbuffer != NULL ? strstr(lockbuffer,
									 "cluster-pcm-share-barrier-refuse-after-acquire") : NULL;
	UT_ASSERT_NOT_NULL(inject);

	/* Barrier-aware SHARE only: an ordinary caller passes a NULL refusal
	 * pointer and an EXCLUSIVE request has a different pcm_mode. */
	if (inject != NULL)
	{
		const char *gate_start = inject - 400 > lockbuffer ? inject - 400 : lockbuffer;

		UT_ASSERT_NOT_NULL(strstr(gate_start, "pcm_mode == PCM_LOCK_MODE_S"));
		UT_ASSERT_NOT_NULL(strstr(gate_start, "pcm_barrier_refused != NULL"));
	}

	/* It fires strictly before the target content lock, and only sets the
	 * flag: leaving the PG_TRY body with a goto would skip PG_END_TRY and
	 * corrupt the exception stack, so the refusal must travel through the
	 * ordinary post-PG_END_TRY exit. */
	lock_acquire = inject != NULL
		? strstr(inject, "LWLockAcquire(BufferDescriptorGetContentLock(buf)") : NULL;
	UT_ASSERT_NOT_NULL(lock_acquire);
	if (inject != NULL && lock_acquire != NULL)
	{
		const char *set_flag = strstr(inject, "*pcm_barrier_refused = true;");
		const char *end_try = strstr(inject, "PG_END_TRY();");
		const char *jump = strstr(inject, "goto cluster_lockbuffer_barrier_refusal;");

		UT_ASSERT(set_flag != NULL && set_flag < lock_acquire);
		UT_ASSERT(end_try != NULL && jump != NULL && end_try < jump);
	}
	free(source);
}

/*
 * Every barrier-aware exit inside the PG_TRY body must be a flag store, never
 * a jump: the epilogue label lives outside that block and PG_END_TRY has to
 * run exactly once first.
 */
UT_TEST(test_no_barrier_jump_escapes_the_try_block)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *lockbuffer;
	const char *try_start;
	const char *try_end;

	UT_ASSERT_NOT_NULL(source);
	lockbuffer = source != NULL ? strstr(source,
									"\nLockBufferInternal(Buffer buffer, int mode") : NULL;
	UT_ASSERT_NOT_NULL(lockbuffer);
	try_start = lockbuffer != NULL ? strstr(lockbuffer, "PG_TRY();") : NULL;
	try_end = try_start != NULL ? strstr(try_start, "PG_END_TRY();") : NULL;
	UT_ASSERT_NOT_NULL(try_start);
	UT_ASSERT_NOT_NULL(try_end);
	if (try_start != NULL && try_end != NULL)
	{
		const char *jump = strstr(try_start, "goto cluster_lockbuffer_barrier_refusal;");

		UT_ASSERT(jump == NULL || jump > try_end);
	}
	free(source);
}

/*
 * hio's two-buffer precedent (spec-8.2 §3.1) stays an independent row: the
 * epilogue unwinds only the target acquisition it was handed and never
 * touches a caller-owned buffer, pin or content lock.
 */
UT_TEST(test_epilogue_never_touches_caller_owned_buffers)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *cleanup;
	const char *cleanup_end;

	UT_ASSERT_NOT_NULL(source);
	cleanup = source != NULL ? strstr(source,
								   "\ncluster_bufmgr_pcm_unwind_barrier_refusal(") : NULL;
	cleanup_end = find_function_end(cleanup);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	if (cleanup != NULL && cleanup_end != NULL)
	{
		char		body[8192];
		size_t		length = (size_t) (cleanup_end - cleanup);

		if (length >= sizeof(body))
			length = sizeof(body) - 1;
		memcpy(body, cleanup, length);
		body[length] = '\0';
		UT_ASSERT_NULL(strstr(body, "ReleaseBuffer("));
		UT_ASSERT_NULL(strstr(body, "UnpinBuffer("));
		UT_ASSERT_NULL(strstr(body, "PinBuffer("));
		UT_ASSERT_NULL(strstr(body, "LWLockRelease("));
		UT_ASSERT_NULL(strstr(body, "LWLockAcquire("));
		UT_ASSERT_EQ(count_occurrences(body, "cluster_pcm_lock_release_buffer_for_eviction("), 1);
		UT_ASSERT_EQ(count_occurrences(body,
									   "cluster_pcm_own_abort_grant_after_master_rollback("), 1);
	}
	free(source);
}

/*
 * A durable-grant refusal releases the master grant before converging the
 * local reservation, and a convergence failure is the data-corruption-class
 * ERROR (spec-8.2 §2.2 steps 4-6).  Deleting either call or swapping their
 * order makes this RED.
 */
UT_TEST(test_durable_grant_refusal_orders_master_before_local)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *cleanup;
	const char *cleanup_end;

	UT_ASSERT_NOT_NULL(source);
	cleanup = source != NULL ? strstr(source,
								   "\ncluster_bufmgr_pcm_unwind_barrier_refusal(") : NULL;
	cleanup_end = find_function_end(cleanup);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	if (cleanup != NULL && cleanup_end != NULL)
	{
		const char *acquired_gate = strstr(cleanup, "if (pcm_acquired)");
		const char *master = strstr(cleanup, "cluster_pcm_lock_release_buffer_for_eviction(");
		const char *local = strstr(cleanup, "cluster_pcm_own_abort_grant_after_master_rollback(");
		const char *corrupt = strstr(cleanup, "ERRCODE_DATA_CORRUPTED");
		const char *pending_only = strstr(cleanup, "else if (pcm_pending_set)");

		UT_ASSERT(acquired_gate != NULL && master != NULL && acquired_gate < master);
		UT_ASSERT(local != NULL && master < local && local < cleanup_end);
		UT_ASSERT(corrupt != NULL && local < corrupt && corrupt < cleanup_end);
		/* READ_IMAGE and pre-acquire refusals take the pending-only branch. */
		UT_ASSERT(pending_only != NULL && corrupt < pending_only && pending_only < cleanup_end);
		UT_ASSERT_NOT_NULL(strstr(cleanup, "Assert(!pcm_acquired || pcm_pending_set);"));
	}
	free(source);
}

UT_TEST(test_ordinary_and_exclusive_entry_points_remain_separate)
{
	char	   *source = read_source(BUFMGR_SOURCE_PATH);
	const char *ordinary;
	const char *exclusive;

	UT_ASSERT_NOT_NULL(source);
	ordinary = source != NULL ? strstr(source, "\nLockBuffer(Buffer buffer, int mode)") : NULL;
	exclusive = source != NULL ? strstr(source,
									 "\nClusterLockBufferExclusiveBarrierAware(Buffer buffer)") : NULL;
	UT_ASSERT_NOT_NULL(ordinary);
	UT_ASSERT_NOT_NULL(exclusive);
	if (ordinary != NULL)
		UT_ASSERT_NOT_NULL(strstr(ordinary, "LockBufferInternal(buffer, mode, NULL);"));
	if (exclusive != NULL)
		UT_ASSERT_NOT_NULL(strstr(exclusive,
								 "LockBufferInternal(buffer, BUFFER_LOCK_EXCLUSIVE, &barrier_refused);"));
	free(source);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_share_wrapper_reports_only_typed_refusal);
	UT_RUN(test_all_typed_refusals_converge_on_one_epilogue);
	UT_RUN(test_refusal_cleanup_preserves_required_order);
	UT_RUN(test_refusal_cleanup_proves_empty_target_ownership);
	UT_RUN(test_share_injection_uses_the_real_cleanup_boundary);
	UT_RUN(test_no_barrier_jump_escapes_the_try_block);
	UT_RUN(test_epilogue_never_touches_caller_owned_buffers);
	UT_RUN(test_durable_grant_refusal_orders_master_before_local);
	UT_RUN(test_ordinary_and_exclusive_entry_points_remain_separate);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
