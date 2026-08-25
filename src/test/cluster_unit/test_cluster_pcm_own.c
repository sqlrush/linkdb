/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_own.c
 *	  C1 ownership-reservation and D5a buffer-reuse contract tests.
 *
 * This binary links the production cluster_pcm_own object.  Buffer-manager
 * behavior that cannot be linked standalone is covered in two paired ways:
 * the real reusable decision helpers are exercised here, and the production
 * bufmgr source is checked to prove both eviction paths call those helpers
 * before dropping header authority and use the saved-tag release API.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"

#include "unit_test.h"

#include <errno.h>
#include <limits.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

UT_DEFINE_GLOBALS();

int NBuffers = 4;

static union {
	uint64 align;
	char bytes[4096];
} fake_shmem;
static bool fake_found;

static char *read_bufmgr_source(void);
static void assert_ordered_in_function(const char *source, const char *function_start,
									   const char *function_end, const char *const *needles,
									   int needle_count);
static void assert_source_range_contains(const char *start, const char *end, const char *needle);

static bool
pipe_read_byte(int fd)
{
	char byte;
	ssize_t nread;

	do {
		nread = read(fd, &byte, 1);
	} while (nread < 0 && errno == EINTR);
	return nread == 1;
}

static bool
pipe_write_byte(int fd)
{
	const char byte = 'x';
	ssize_t nwritten;

	do {
		nwritten = write(fd, &byte, 1);
	} while (nwritten < 0 && errno == EINTR);
	return nwritten == 1;
}

typedef struct ParallelStableCoverRace {
	ClusterPcmOwnEntry entries[4];
	pg_atomic_uint32 descriptor_state;
	ClusterPcmOwnResult begin_result;
	ClusterPcmOwnResult commit_result;
	uint64 token;
	uint64 committed_generation;
} ParallelStableCoverRace;

static void
parallel_stable_cover_race_init(ParallelStableCoverRace *race)
{
	int i;

	memset(race, 0, sizeof(*race));
	for (i = 0; i < lengthof(race->entries); i++) {
		pg_atomic_init_u64(&race->entries[i].generation, 0);
		pg_atomic_init_u64(&race->entries[i].reservation_token, 0);
		pg_atomic_init_u64(&race->entries[i].writer_activation_token, 0);
		pg_atomic_init_u64(&race->entries[i].resource_x_activation_generation, 0);
		pg_atomic_init_u32(&race->entries[i].flags, 0);
	}
	pg_atomic_init_u32(&race->descriptor_state, (uint32)PCM_STATE_N);
}

static void
parallel_stable_cover_child(ParallelStableCoverRace *race, int start_fd, int done_fd)
{
	if (!pipe_read_byte(start_fd))
		_exit(10);
	race->begin_result = cluster_pcm_own_reservation_begin_exact(
		0, 0, PCM_OWN_FLAG_GRANT_PENDING, &race->token);
	if (race->begin_result == CLUSTER_PCM_OWN_OK)
		race->commit_result = cluster_pcm_own_grant_commit_exact(
			0, 0, race->token, &race->committed_generation);
	if (race->commit_result == CLUSTER_PCM_OWN_OK)
		pg_atomic_write_u32(&race->descriptor_state, (uint32)PCM_STATE_S);
	if (!pipe_write_byte(done_fd))
		_exit(11);
	close(start_fd);
	close(done_fd);
	_exit(0);
}

static ParallelStableCoverRace *
run_parallel_stable_cover_race(void)
{
	ParallelStableCoverRace *race;
	int start_pipe[2] = { -1, -1 };
	int done_pipe[2] = { -1, -1 };
	int status;
	int rc;
	pid_t child;

	race = mmap(NULL, sizeof(*race), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(race != MAP_FAILED);
	if (race == MAP_FAILED)
		return NULL;
	parallel_stable_cover_race_init(race);
	ClusterPcmOwnArray = race->entries;

	rc = pipe(start_pipe);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0)
		goto fail;
	rc = pipe(done_pipe);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0)
		goto fail;

	UT_ASSERT_EQ(pg_atomic_read_u32(&race->descriptor_state), (uint32)PCM_STATE_N);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child < 0)
		goto fail;
	if (child == 0) {
		close(start_pipe[1]);
		close(done_pipe[0]);
		parallel_stable_cover_child(race, start_pipe[0], done_pipe[1]);
	}
	close(start_pipe[0]);
	close(done_pipe[1]);
	UT_ASSERT(pipe_write_byte(start_pipe[1]));
	close(start_pipe[1]);
	UT_ASSERT(pipe_read_byte(done_pipe[0]));
	close(done_pipe[0]);
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	return race;

fail:
	if (start_pipe[0] >= 0)
		close(start_pipe[0]);
	if (start_pipe[1] >= 0)
		close(start_pipe[1]);
	if (done_pipe[0] >= 0)
		close(done_pipe[0]);
	if (done_pipe[1] >= 0)
		close(done_pipe[1]);
	ClusterPcmOwnArray = NULL;
	munmap(race, sizeof(*race));
	return NULL;
}

static void
assert_parallel_stable_cover_wiring(const char *source)
{
	static const char *const locked_begin_contract[]
		= { "*out_token = 0", "LockBufHdr", "cluster_pcm_own_snapshot_locked",
			"cluster_pcm_x_cached_cover_reverify_accepts(", "*out_covered = true",
			"cluster_pcm_own_reservation_begin_exact", "UnlockBufHdr" };
	static const char *const retry_cover_contract[]
		= { "cluster_bufmgr_pcm_begin_grant_reservation_wait(", "buf, pcm_mode",
			"&begin_covered", "if (begin_covered)",
			"*covered_generation = base->generation",
			"return CLUSTER_BUFMGR_PCM_RETRY_COVERED" };
	static const char *const initial_cover_contract[]
		= { "cluster_bufmgr_pcm_begin_grant_reservation_wait(", "buf, pcm_mode",
			"&pcm_covered", "if (pcm_covered)",
			"pcm_covered_gen = pcm_pending_base.generation", "goto pcm_legacy_acquire_done" };
	static const char *const revalidate_cover_contract[]
		= { "cluster_bufmgr_pcm_begin_grant_reservation_wait(", "buf, pcm_mode",
			"&pcm_covered", "if (pcm_covered)",
			"pcm_covered_gen = pcm_pending_base.generation",
			"goto pcm_revalidate_acquire_done" };

	assert_ordered_in_function(source, "\ncluster_pcm_own_begin_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   locked_begin_contract, lengthof(locked_begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_retry_denied_rearm(", "\nstatic ",
							   retry_cover_contract, lengthof(retry_cover_contract));
	assert_ordered_in_function(source, "Legacy acquire path:", "pcm_legacy_acquire_done:",
							   initial_cover_contract, lengthof(initial_cover_contract));
	assert_ordered_in_function(source, "cluster_pcm_note_writer_cover_stale_detected();",
							   "pcm_revalidate_acquire_done:", revalidate_cover_contract,
							   lengthof(revalidate_cover_contract));
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	UT_ASSERT(size <= sizeof(fake_shmem.bytes));
	*foundPtr = fake_found;
	fake_found = true;
	return fake_shmem.bytes;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

static void
reset_fixture(void)
{
	memset(&fake_shmem, 0xA5, sizeof(fake_shmem));
	fake_found = false;
	ClusterPcmOwnArray = NULL;
	cluster_pcm_own_shmem_init();
}

static void
assert_entry(uint64 generation, uint64 token, uint32 flags)
{
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].generation), generation);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].reservation_token), token);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmOwnArray[0].flags), flags);
}

static void
assert_writer_activation(uint64 token)
{
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].writer_activation_token), token);
}

static void
assert_resource_x_activation(uint64 generation)
{
	UT_ASSERT_EQ(
		pg_atomic_read_u64(&ClusterPcmOwnArray[0].resource_x_activation_generation), generation);
}

UT_TEST(test_shmem_initializes_complete_entry)
{
	int i;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_shmem_size(), (Size)NBuffers * sizeof(ClusterPcmOwnEntry));
	UT_ASSERT_EQ(sizeof(ClusterPcmOwnEntry), 40);
	UT_ASSERT_EQ(offsetof(ClusterPcmOwnEntry, resource_x_activation_generation), 24);
	for (i = 0; i < NBuffers; i++) {
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].generation), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].reservation_token), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].writer_activation_token), 0);
		UT_ASSERT_EQ(
			pg_atomic_read_u64(&ClusterPcmOwnArray[i].resource_x_activation_generation), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmOwnArray[i].flags), 0);
	}
}

UT_TEST(test_resource_x_activation_binding_is_exact_and_legacy_closed)
{
	uint64 committed_generation = 0;
	uint64 writer_token = 0;

	reset_fixture();
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &writer_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_grant_commit_exact(0, 0, writer_token, &committed_generation),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed_generation, 1);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(0);

	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation, writer_token, 0),
				 CLUSTER_PCM_OWN_INVALID);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation + 1, writer_token, 41),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation, writer_token + 1, 41),
				 CLUSTER_PCM_OWN_STALE);
	assert_resource_x_activation(0);

	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation, writer_token, 41),
				 CLUSTER_PCM_OWN_OK);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation, writer_token, 41),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation, writer_token, 42),
				 CLUSTER_PCM_OWN_STALE);
	assert_resource_x_activation(41);

	/* A generic legacy clear must not open a target Resource-X fence, and
	 * descriptor reuse cannot erase either live activation field. */
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(
					  0, committed_generation, writer_token),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, NULL));
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);

	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(
					  0, committed_generation, writer_token, 42),
				 CLUSTER_PCM_OWN_STALE);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(
					  0, committed_generation, writer_token, 41),
				 CLUSTER_PCM_OWN_OK);
	assert_resource_x_activation(0);
	assert_writer_activation(0);
}

UT_TEST(test_resource_x_reconfig_neutralize_is_generation_exact_and_nonblocking)
{
	ClusterPcmOwnSnapshot live;
	BufferTag tag;
	uint64 committed_generation = 0;
	uint64 neutral_generation = 0;
	uint64 writer_token = 0;
	char *source;
	const char *neutralize;
	const char *neutralize_end;

	reset_fixture();
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &writer_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_grant_commit_exact(0, 0, writer_token, &committed_generation),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(
					  0, committed_generation, writer_token, 41),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_neutralize_exact(
					  0, committed_generation, writer_token, 42, &neutral_generation),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(neutral_generation, 0);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_neutralize_exact(
					  0, committed_generation, writer_token, 41, &neutral_generation),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(neutral_generation, committed_generation + 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].generation), neutral_generation);
	assert_writer_activation(0);
	assert_resource_x_activation(0);

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 72;
	memset(&live, 0, sizeof(live));
	live.tag = tag;
	live.generation = committed_generation;
	live.reservation_token = writer_token;
	live.writer_activation_token = writer_token;
	live.resource_x_activation_generation = 41;
	live.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT(cluster_pcm_x_resource_x_r8_snapshot_exact(&tag, 17, 41, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_r8_snapshot_exact(&tag, 0, 41, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_r8_snapshot_exact(&tag, 17, 42, &live));

	source = read_bufmgr_source();
	neutralize = strstr(source, "\ncluster_bufmgr_resource_x_neutralize_exact(");
	neutralize_end = neutralize != NULL ? strstr(neutralize, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(neutralize);
	UT_ASSERT_NOT_NULL(neutralize_end);
	if (neutralize != NULL && neutralize_end != NULL) {
		const char *quarantine
			= strstr(neutralize, "buf->pcm_state = (uint8)PCM_STATE_N");
		const char *raw_clear
			= strstr(neutralize, "cluster_pcm_own_resource_x_neutralize_exact(");

		UT_ASSERT_NOT_NULL(strstr(source,
			"PGRAC_PCM_X_FENCE_TERMINAL_OWNER(R8_NEUTRALIZE, tag,"));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "BufTableLookup"));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "cluster_pcm_x_resource_x_r8_snapshot_exact("));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "cluster_pcm_own_resource_x_neutralize_exact("));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "buf->pcm_state = (uint8)PCM_STATE_N"));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "buf->buffer_type = (uint8)BUF_TYPE_PI"));
		UT_ASSERT_NOT_NULL(raw_clear);
		UT_ASSERT(quarantine != NULL && quarantine < raw_clear && raw_clear < neutralize_end);
		{
			const char *content_lock = strstr(neutralize, "BufferDescriptorGetContentLock");

			UT_ASSERT(content_lock == NULL || content_lock >= neutralize_end);
		}
	}
	free(source);
}

UT_TEST(test_writer_activation_fence_blocks_revoke_until_exact_clear)
{
	uint64 committed_generation = 0;
	uint64 revoke_token = UINT64_MAX;
	uint64 writer_token = 0;

	reset_fixture();
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &writer_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_grant_commit_exact(0, 0, writer_token, &committed_generation),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed_generation, 1);
	assert_entry(1, writer_token, 0);
	assert_writer_activation(writer_token);

	/* The committed X and its not-yet-activated writer are one shared
	 * linearization.  A downgrade cannot reserve the same tuple. */
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_REVOKING, &revoke_token),
		CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(revoke_token, 0);
	assert_entry(1, writer_token, 0);
	assert_writer_activation(writer_token);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, NULL));

	pg_atomic_write_u64(&ClusterPcmOwnArray[0].writer_activation_token, writer_token + 1);
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_REVOKING, &revoke_token),
		CLUSTER_PCM_OWN_CORRUPT);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].writer_activation_token, writer_token);

	/* A delayed or wrong clear is an exact no-op. */
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(0, 0, writer_token),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(0, 1, writer_token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_writer_activation(writer_token);

	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(0, 1, writer_token),
				 CLUSTER_PCM_OWN_OK);
	assert_writer_activation(0);
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_REVOKING, &revoke_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(revoke_token, writer_token + 1);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 1, revoke_token, PCM_OWN_FLAG_REVOKING),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &committed_generation));
	UT_ASSERT_EQ(committed_generation, 2);
	assert_writer_activation(0);
}

UT_TEST(test_begin_abort_is_exact_and_monotonic)
{
	uint64 token = UINT64_MAX;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 1);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	/* A second begin cannot overwrite or advance the live token. */
	token = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	/* Old/wrong cleanup is a strict no-op. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 2, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 1, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(0, 1, 0);

	/* A delayed duplicate abort cannot clear the next reservation. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 2);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(0, 2, PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_invalid_live_flag_shapes_are_corrupt_not_busy)
{
	static const char *const classifier_contract[]
		= { "cluster_pcm_own_reservation_token_get", "cluster_pcm_own_flags_get",
			"cluster_pcm_own_classify_live_flags", "live_result != CLUSTER_PCM_OWN_OK",
			"return live_result" };
	char *source;
	uint64 token = UINT64_MAX;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 7);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 7, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 7);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, (uint32)0x4);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 7, (uint32)0x4);

	/* Even a recognized singleton flag is corrupt without a published token. */
	reset_fixture();
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 0, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(0, 0), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(0, 7), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING, 7),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_REVOKING, 7),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(
		cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING, 7),
		CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING, 0),
				 CLUSTER_PCM_OWN_CORRUPT);

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\ncluster_pcm_own_bump_failure(", "\nstatic ",
							   classifier_contract, lengthof(classifier_contract));
	free(source);
}

UT_TEST(test_grant_commit_is_exact_and_bumps_once)
{
	uint64 token;
	uint64 committed = UINT64_MAX;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);

	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(0, token, PCM_OWN_FLAG_GRANT_PENDING);

	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 1);
	assert_entry(1, token, 0);

	committed = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(1, token, 0);

	/* A competing well-formed lifecycle is BUSY; a malformed tuple is
	 * corruption.  Neither may be flattened into a retryable stale result. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_BUSY);
	assert_entry(0, 9, PCM_OWN_FLAG_REVOKING);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(0, 9, PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_s_revoke_handoff_reuses_exact_token_and_bumps_once)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* Stale identities cannot steal or rewrite the source revoke lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 8, token), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* Handoff changes only the role of the same source lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* Malformed live tuples are corruption, never a stale/duplicate handoff. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, token);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
				 CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 0);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
				 CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(7, 0, PCM_OWN_FLAG_REVOKING);

	/* Restore the exact handed-off tuple before its sole generation bump. */
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, token);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 7, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_revoke_handoff_kinds_cover_n_s_x_with_one_lifecycle)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;
	uint64 committed = UINT64_MAX;
	uint64 token;
	uint8 states[] = { (uint8)PCM_STATE_N, (uint8)PCM_STATE_S, (uint8)PCM_STATE_X };
	ClusterPcmXGrantReservationKind expected_kinds[]
		= { CLUSTER_PCM_X_GRANT_RESERVATION_N_REVOKE_HANDOFF,
			CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF,
			CLUSTER_PCM_X_GRANT_RESERVATION_X_REVOKE_HANDOFF };
	int i;

	/* The added handoff arms must not broaden or shadow the ordinary new-token
	 * N reservation. */
	memset(&base, 0, sizeof(base));
	base.generation = 7;
	base.reservation_token = 4;
	base.pcm_state = (uint8)PCM_STATE_N;
	live = base;
	live.reservation_token = 5;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 5),
				 CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW);
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 4),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	for (i = 0; i < lengthof(states); i++) {
		reset_fixture();
		pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
					 CLUSTER_PCM_OWN_OK);

		memset(&base, 0, sizeof(base));
		base.generation = 7;
		base.reservation_token = token;
		base.flags = PCM_OWN_FLAG_REVOKING;
		base.pcm_state = states[i];
		live = base;
		live.flags = PCM_OWN_FLAG_GRANT_PENDING;

		UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, token), expected_kinds[i]);
		UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 7, token, &committed),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(committed, 8);
		assert_entry(8, token, 0);
	}
}

/*
 * Protocol pin for the t/400 fast-fail finish family (2026-07-20, loop9 and
 * loop10b DETAIL): a fresh-token GRANT_PENDING reservation taken from an
 * S/flags=0 base ("S_NEW" -- a stale-cover fallback wrongly entering the
 * legacy master acquire after an in-window X->S downgrade) must NEVER
 * become a legal finish shape.  Writer conversions are ordered by the
 * convert queue's FIFO/WFG; legalizing this shape at the finish would let
 * that fallback bypass the arbitration entirely (the original S3 unordered
 * multi-writer defect).  The fallback must re-enter the queue instead;
 * this classifier keeps refusing the bypass.
 */
UT_TEST(test_s_new_fresh_token_finish_shape_stays_invalid)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;

	/* The exact loop9/loop10b production tuple stays refused. */
	memset(&base, 0, sizeof(base));
	base.generation = 10;
	base.reservation_token = 5;
	base.pcm_state = (uint8)PCM_STATE_S;
	live = base;
	live.reservation_token = 6;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 6),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	/* A fresh-token X base is refused the same way: a live X cover never
	 * re-acquires through this path. */
	base.pcm_state = (uint8)PCM_STATE_X;
	live = base;
	live.reservation_token = 6;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 6),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);
}

UT_TEST(test_parallel_s_cover_is_rechecked_before_legacy_token_mint)
{
	ParallelStableCoverRace *race;
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;
	char *source;
	uint64 fresh_token = 0;

	/* The requester performs the optimistic probe before the peer publishes
	 * the compatible S grant for the same buffer. */
	race = run_parallel_stable_cover_race();
	if (race == NULL)
		return;
	UT_ASSERT_EQ(race->begin_result, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(race->commit_result, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(race->token, 1);
	UT_ASSERT_EQ(race->committed_generation, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&race->descriptor_state), (uint32)PCM_STATE_S);
	assert_entry(1, 1, 0);

	/* This is the exact entrance-race failure shape.  If bufmgr does not
	 * consume the stable cover under header authority, the raw begin can mint
	 * token 2 and strict finish correctly rejects the resulting S_NEW tuple. */
	memset(&base, 0, sizeof(base));
	base.generation = 1;
	base.reservation_token = 1;
	base.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(
					 0, 1, PCM_OWN_FLAG_GRANT_PENDING, &fresh_token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(fresh_token, 2);
	assert_entry(1, 2, PCM_OWN_FLAG_GRANT_PENDING);
	live = base;
	live.reservation_token = fresh_token;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, fresh_token),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	/* Exercise the production cover predicate directly.  S readers may accept
	 * a stable successor generation; X writers remain generation-exact. */
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_S, 1, 1, (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_S, 1, 1, (uint8)PCM_STATE_S,
		PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_X, 1, 1, (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_X, 1, 2, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_S, 1, 2, (uint8)PCM_STATE_S, 0, 0, 0));

	/* bufmgr is not standalone-linkable; keep only narrow wiring pins around
	 * the real helper above.  All three legacy callers must propagate COVERED
	 * to the existing content-lock revalidation path without a master call. */
	source = read_bufmgr_source();
	assert_parallel_stable_cover_wiring(source);
	free(source);

	ClusterPcmOwnArray = NULL;
	UT_ASSERT_EQ(munmap(race, sizeof(*race)), 0);
}

UT_TEST(test_share_cover_reverify_accepts_stable_successor_grant)
{
	static const char *const initial_fence_contract[]
		= { "cluster_bufmgr_pcm_own_snapshot(buf, &pcm_initial_own)",
			"!cluster_pcm_x_activation_fence_open(",
			"cluster_bufmgr_pcm_x_holder_retry_wait(",
			"cluster_pcm_x_cached_cover_bypasses_queue(" };
	static const char *const post_content_fence_contract[]
		= { "while (pcm_covered)", "cur_own.resource_x_activation_generation",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_holder_abort_acquiring(pcm_x_holder)",
			"cluster_bufmgr_pcm_x_holder_retry_wait(",
			"cluster_pcm_x_cached_cover_reverify_accepts(",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)",
			"continue;" };
	char *source;

	/* Once content authority is held, a stable current S/X successor is the
	 * exact node-level grant for a read.  Generation drift alone must not open
	 * a fresh legacy reservation from S (the forbidden S_NEW shape). */
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, UINT64_C(13),
												  UINT64_C(14), (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, UINT64_C(13),
												  UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));

	/* A writer keeps the stricter generation-exact gate and must re-enter the
	 * convert queue after any ownership round.  A non-covering or live
	 * lifecycle remains closed for both modes. */
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(13),
												   UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(14),
												  UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(14),
												   UINT64_C(14), (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, UINT64_C(14),
												   UINT64_C(14), (uint8)PCM_STATE_S,
												   PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, UINT64_C(14),
												   UINT64_C(14), (uint8)PCM_STATE_S,
												   PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_N, UINT64_C(14),
												   UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(14),
												   UINT64_C(14), (uint8)PCM_STATE_X, 0,
												   UINT64_C(91), 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(14),
												   UINT64_C(14), (uint8)PCM_STATE_X, 0,
												   UINT64_C(91), UINT64_C(22)));

	/* The production cached-X entrance must wait on the same live grant both
	 * before queue bypass and after the content-lock race closes.  It may
	 * re-arbitrate only after the coherent cover itself changes. */
	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\nLockBufferInternal(",
							   "\nvoid\nLockBuffer(", initial_fence_contract,
							   lengthof(initial_fence_contract));
	assert_ordered_in_function(source, "\t\t\twhile (pcm_covered)",
							   "cluster_pcm_note_writer_reverify_reacquire();",
							   post_content_fence_contract,
							   lengthof(post_content_fence_contract));
	free(source);
}

UT_TEST(test_revoke_commit_is_exact_and_classifies_live_races)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 1);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* A delayed/wrong lifecycle must not clear the current revoke. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, 0);

	/* A duplicate cannot bump the ownership generation twice. */
	committed = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(8, token, 0);

	/* A different well-formed lifecycle is contention, not corruption. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_BUSY);
	assert_entry(0, 9, PCM_OWN_FLAG_GRANT_PENDING);

	/* Malformed live metadata is corruption, not ordinary contention. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(0, 9, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_revoke_retain_commit_keeps_exact_token_until_release)
{
	ClusterPcmOwnEvictionCapture capture;
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);

	/* The retained commit bumps ownership exactly once but deliberately keeps
	 * the same live token: descriptor reuse remains fail-closed until the
	 * matching DRAIN/RELEASE_IMAGE arrives. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_commit_exact(0, 7, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_commit_exact(0, 7, token, &committed),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, PCM_OWN_FLAG_REVOKING);

	memset(&capture, 0, sizeof(capture));
	capture.generation = committed;
	capture.reservation_token = token;
	capture.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));

	/* A stale DRAIN from either the pre-commit generation or a prior token is
	 * a strict no-op and cannot unpin a newer retained round. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 7, token), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(8, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token), CLUSTER_PCM_OWN_OK);
	assert_entry(8, token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_revoke_commit_exhaustion_is_side_effect_free)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_MAX);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 17);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	token = 17;

	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, UINT64_MAX, token, &committed),
				 CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(UINT64_MAX, token, PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_token_and_generation_never_wrap)
{
	uint64 token = UINT64_MAX;
	uint64 last_token;
	uint64 generation = 0;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, UINT64_MAX, 0);

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_MAX - 1);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, UINT64_MAX - 1,
														 PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	last_token = token;
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, UINT64_MAX - 1, token, &generation),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(generation, UINT64_MAX);
	assert_entry(UINT64_MAX, token, 0);

	/* MAX is terminal: begin must fail before token/flag side effects. */
	token = UINT64_MAX;
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, UINT64_MAX, PCM_OWN_FLAG_GRANT_PENDING, &token),
		CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(token, 0);
	assert_entry(UINT64_MAX, last_token, 0);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, UINT64_MAX);
	assert_entry(UINT64_MAX, last_token, 0);
}

UT_TEST(test_ordinary_generation_bump_rejects_live_reservation)
{
	uint64 generation = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);

	/* Only the token-exact finish/revoke lifecycle may advance generation
	 * while a transient ownership flag is live.  The ordinary transition
	 * helper must be a no-op so it cannot bypass the reservation token. */
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 7);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* The same helper remains valid after exact cleanup makes the entry idle. */
	reset_fixture();
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 1);
	assert_entry(1, 0, 0);
}

UT_TEST(test_eviction_rejects_live_reservation_and_exhaustion)
{
	ClusterPcmOwnEvictionCapture capture;

	memset(&capture, 0, sizeof(capture));
	capture.generation = 9;
	UT_ASSERT(cluster_pcm_own_eviction_reuse_allowed(&capture));

	capture.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.flags = 0;
	capture.reservation_token = 3;
	/* The single token is monotonic and remains nonzero while idle; flags are
	 * the only active-lifecycle marker. */
	UT_ASSERT(cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
}

static char *
read_bufmgr_source(void)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(BUFMGR_SOURCE_PATH, "rb");
	UT_ASSERT_NOT_NULL(file);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static void
assert_commit_before_tail(const char *source, const char *function_start, const char *function_end)
{
	const char *begin = strstr(source, function_start);
	const char *end;
	const char *commit;
	const char *tail;

	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL)
		return;
	end = strstr(begin + strlen(function_start), function_end);
	UT_ASSERT_NOT_NULL(end);
	if (end == NULL)
		return;
	commit = strstr(begin, "cluster_pcm_own_eviction_commit_locked");
	tail = strstr(begin, "InvalidateBufferCommitTailLocked");
	UT_ASSERT_NOT_NULL(commit);
	UT_ASSERT_NOT_NULL(tail);
	if (commit == NULL || tail == NULL)
		return;
	UT_ASSERT(commit < tail);
	UT_ASSERT(commit < end);
	UT_ASSERT(tail < end);
}

static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	size_t needle_length = strlen(needle);

	while ((source = strstr(source, needle)) != NULL) {
		count++;
		source += needle_length;
	}
	return count;
}

static void
assert_ordered_in_function(const char *source, const char *function_start, const char *function_end,
						   const char *const *needles, int needle_count)
{
	const char *cursor = strstr(source, function_start);
	const char *end;
	int i;

	UT_ASSERT_NOT_NULL(cursor);
	if (cursor == NULL)
		return;
	end = strstr(cursor + strlen(function_start), function_end);
	UT_ASSERT_NOT_NULL(end);
	if (end == NULL)
		return;

	for (i = 0; i < needle_count; i++) {
		cursor = strstr(cursor, needles[i]);
		UT_ASSERT_NOT_NULL(cursor);
		if (cursor == NULL)
			return;
		UT_ASSERT(cursor < end);
		if (cursor >= end)
			return;
		cursor += strlen(needles[i]);
	}
}

static void
assert_source_range_contains(const char *start, const char *end, const char *needle)
{
	const char *found;

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	if (start == NULL || end == NULL)
		return;
	found = strstr(start, needle);
	UT_ASSERT_NOT_NULL(found);
	if (found != NULL)
		UT_ASSERT(found < end);
}

UT_TEST(test_bufmgr_d5a_commitlocked_uses_locked_commit_and_saved_tag_release)
{
	static const char *const commit_contract[]
		= { "ClusterPcmOwnEvictionCapture eviction_capture",
			"cluster_pcm_own_eviction_capture_locked", "cluster_pcm_own_eviction_commit_locked",
			"eviction_result != CLUSTER_PCM_OWN_OK", "InvalidateBufferCommitTailLocked" };
	static const char *const tail_contract[]
		= { "ClearBufferTag", "UnlockBufHdr", "cluster_pcm_lock_release_saved_tag_for_eviction" };
	char *source = read_bufmgr_source();

	/* Descriptor reuse is a single header-authority commit.  A live token,
	 * exhausted generation, or tuple mismatch must leave the old tag resident
	 * and return fail-closed; only an exact successful commit may clear the tag
	 * and later release the master holder by the saved immutable tag. */
	assert_commit_before_tail(source, "\nInvalidateBufferCommitLocked(",
							  "\n/*\n * InvalidateBufferCommitTailLocked");
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferCommitTailLocked", commit_contract,
							   lengthof(commit_contract));
	assert_ordered_in_function(source, "\nInvalidateBufferCommitTailLocked(",
							   "\n/*\n * InvalidateBufferTry", tail_contract,
							   lengthof(tail_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "static bool\nInvalidateBufferCommitLocked"));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_lock_release_saved_tag_for_eviction"));
	UT_ASSERT_NULL(strstr(source, "buf->tag = *oldTag"));
	free(source);
}

UT_TEST(test_bufmgr_abort_cleanup_is_never_silent)
{
	static const char *const normal_cleanup[]
		= { "cluster_pcm_own_abort_grant_reservation", "CLUSTER_PCM_OWN_OK", "ereport(ERROR" };
	static const char *const error_cleanup[]
		= { "cluster_pcm_own_abort_grant_reservation", "CLUSTER_PCM_OWN_OK", "elog(LOG" };
	char *source = read_bufmgr_source();

	/* Every normal false/READ_IMAGE exit must prove exact cleanup or ERROR.
	 * During PG_CATCH, preserve the original error but emit LOG evidence when
	 * exact cleanup did not converge.  No call may discard the result. */
	UT_ASSERT_NULL(strstr(source, "(void) cluster_pcm_own_abort_grant_reservation"));
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_or_error(",
							   "\nstatic void\ncluster_pcm_own_abort_grant_after_error(",
							   normal_cleanup, lengthof(normal_cleanup));
	assert_ordered_in_function(
		source, "\ncluster_pcm_own_abort_grant_after_error(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_abort_grant_after_master_rollback(",
		error_cleanup, lengthof(error_cleanup));
	free(source);
}

UT_TEST(test_bufmgr_finish_failure_rolls_back_acquired_master_grant)
{
	static const char *const rollback_contract[]
		= { "cluster_pcm_own_finish_grant_reservation",
			"PG_TRY",
			"cluster_pcm_lock_release_buffer_for_eviction",
			"PG_CATCH",
			"elog(LOG",
			"PG_RE_THROW",
			"cluster_pcm_own_abort_grant_after_master_rollback",
			"ereport(ERROR" };
	char *source = read_bufmgr_source();

	/* Definition + direct-lock caller + LockBuffer caller.  Keeping the real
	 * acquire and exact finish behind one helper prevents either entrance from
	 * leaking a master holder when local finish rejects the token/tuple. */
	UT_ASSERT(count_occurrences(source, "cluster_pcm_own_finish_grant_or_rollback(") >= 3);
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_or_rollback(", "\nstatic ",
							   rollback_contract, lengthof(rollback_contract));
	free(source);
}

UT_TEST(test_bufmgr_s_base_rollback_normalizes_to_n_under_header_authority)
{
	static const char *const s_to_n_contract[] = { "LockBufHdr",
												   "base->pcm_state != (uint8)PCM_STATE_N",
												   "base->pcm_state != (uint8)PCM_STATE_S",
												   "base->pcm_state == (uint8)PCM_STATE_S",
												   "base->generation == UINT64_MAX",
												   "cluster_pcm_own_reservation_abort_exact",
												   "base->pcm_state == (uint8)PCM_STATE_S",
												   "cluster_pcm_own_gen_bump_checked",
												   "buf->pcm_state = (uint8)PCM_STATE_N",
												   "UnlockBufHdr" };
	char *source = read_bufmgr_source();

	/* A legacy S-base acquire that reached master X cannot restore S after a
	 * failed local finish: the master rollback is X->N.  The local half must
	 * therefore exact-abort the live token and commit S->N plus one checked
	 * generation bump under a single header-lock hold.  N-base skips the bump
	 * and remains N.  All prechecks precede abort, so rejection leaves the live
	 * flag as fail-closed evidence rather than advertising successful cleanup. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_after_master_rollback(",
							   "\nstatic void\ncluster_pcm_own_finish_grant_or_rollback(",
							   s_to_n_contract, lengthof(s_to_n_contract));
	free(source);
}

UT_TEST(test_lockbuffer_content_error_uses_post_master_rollback_contract)
{
	static const char *const rethrow_contract[]
		= { "Assert(original_error != NULL)",
			"PG_TRY",
			"cluster_pcm_lock_release_buffer_for_eviction",
			"PG_CATCH",
			"CopyErrorData",
			"FlushErrorState",
			"elog(LOG",
			"if (master_released)",
			"cluster_pcm_own_abort_grant_after_master_rollback",
			"ReThrowError" };
	static const char *const content_error_contract[]
		= { "MemoryContextSwitchTo(pcm_error_context)",
			"original_error = CopyErrorData()",
			"FlushErrorState()",
			"cluster_bufmgr_pcm_x_holder_abort_acquiring",
			"if (pcm_acquired)",
			"cluster_pcm_own_rollback_grant_after_error_and_rethrow",
			"else if (pcm_pending_set)",
			"cluster_pcm_own_abort_grant_after_error",
			"ReThrowError(original_error)" };
	char *source = read_bufmgr_source();

	/* A content-lock error can occur after a durable master grant but before
	 * local finish.  Both holder detach and remote release may themselves
	 * throw, so LockBuffer must copy and flush the original ErrorData before
	 * either cleanup begins.  Cleanup failure is LOGged, leaves exact evidence
	 * fail-closed, and still rethrows the original error.  Exact local
	 * convergence is legal only after release succeeded. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_rollback_grant_after_error_and_rethrow(",
							   "\nstatic ", rethrow_contract, lengthof(rethrow_contract));
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		content_error_contract, lengthof(content_error_contract));
	free(source);
}

UT_TEST(test_bufmgr_generation_bump_failure_is_classified_under_header_lock)
{
	static const char *const diagnostic_contract[] = { "cluster_pcm_own_reservation_token_get",
													   "cluster_pcm_own_flags_get",
													   "cluster_pcm_own_classify_live_flags",
													   "live_result != CLUSTER_PCM_OWN_OK",
													   "generation == UINT64_MAX",
													   "CLUSTER_PCM_OWN_EXHAUSTED" };
	static const char *const transition_contract[]
		= { "LockBufHdr", "cluster_pcm_own_bump_locked", "UnlockBufHdr",
			"cluster_pcm_own_report_bump_failure" };
	char *source = read_bufmgr_source();

	/* A checked bump can reject either a live exact lifecycle or terminal MAX.
	 * Both observations must be made while header authority is still held and
	 * must not be collapsed into the misleading "exhausted" diagnosis. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_bump_failure(", "\nstatic ",
							   diagnostic_contract, lengthof(diagnostic_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_transition(", "\n/*", transition_contract,
							   lengthof(transition_contract));
	UT_ASSERT(count_occurrences(source, "cluster_pcm_own_bump_failure(") >= 2);
	UT_ASSERT_NOT_NULL(strstr(source, "active reservation"));
	UT_ASSERT_NOT_NULL(strstr(source, "generation exhausted"));
	free(source);
}

UT_TEST(test_lockbuffer_reservation_failures_use_busy_corrupt_classifier)
{
	static const char *const initial_reservation_contract[]
		= { "cluster_bufmgr_pcm_begin_grant_reservation_wait",
			"pcm_pending_result != CLUSTER_PCM_OWN_OK", "cluster_pcm_own_report_bump_failure",
			"pcm_pending_set = true" };
	static const char *const revalidate_reservation_contract[]
		= { "cluster_bufmgr_pcm_begin_grant_reservation_wait",
			"pcm_pending_result != CLUSTER_PCM_OWN_OK", "cluster_pcm_own_report_bump_failure",
			"pcm_pending_set = true" };
	char *source = read_bufmgr_source();

	/* A live queue reservation is BUSY, not damaged metadata.  Both legacy
	 * LockBuffer begin sites must preserve that distinction while malformed
	 * flag/token shapes continue through the same helper as DATA_CORRUPTED. */
	assert_ordered_in_function(source, "Legacy acquire path:", "PG_END_TRY();",
							   initial_reservation_contract,
							   lengthof(initial_reservation_contract));
	assert_ordered_in_function(source, "cluster_pcm_note_writer_cover_stale_detected();",
							   "cluster_pcm_note_writer_reverify_reacquire();",
							   revalidate_reservation_contract,
							   lengthof(revalidate_reservation_contract));
	free(source);
}

UT_TEST(test_lockbuffer_reservation_busy_barrier_is_typed_before_failure_report)
{
	char *source = read_bufmgr_source();
	const char *wait_fn;
	const char *wait_end;
	const char *guard;
	const char *typed;
	const char *set_refused;
	const char *note_unwind;
	const char *return_busy;
	const char *initial;
	const char *initial_call;
	const char *initial_call_end;
	const char *initial_arg;
	const char *initial_check;
	const char *initial_goto;
	const char *initial_report;
	const char *rearm;
	const char *rearm_call;
	const char *rearm_call_end;
	const char *rearm_arg;
	const char *rearm_check;
	const char *rearm_return;
	const char *revalidate;
	const char *revalidate_call;
	const char *revalidate_call_end;
	const char *revalidate_arg;
	const char *revalidate_check;
	const char *revalidate_skip;
	const char *revalidate_report;

	/* Regression A: if a live GRANT_PENDING/REVOKING reservation is BUSY
	 * while the nested wait guard has closed, a barrier-aware SHARE must
	 * preserve BARRIER_CLOSED as a typed refusal.  It must not fall through
	 * to cluster_pcm_own_report_bump_failure(), whose BUSY mapping is the
	 * client-visible OBJECT_IN_USE error this contract forbids. */
	wait_fn = strstr(source, "\ncluster_bufmgr_pcm_begin_grant_reservation_wait(");
	wait_end = wait_fn != NULL ? strstr(wait_fn + 1, "\n/*\n * PGRAC (t/400 S_NEW") : NULL;
	UT_ASSERT_NOT_NULL(wait_fn);
	UT_ASSERT_NOT_NULL(wait_end);
	guard = wait_fn != NULL ? strstr(wait_fn, "if (guard_result != PCM_X_QUEUE_OK)") : NULL;
	typed = guard != NULL
		? strstr(guard,
				 "if (barrier_refused != NULL && guard_result == PCM_X_QUEUE_BARRIER_CLOSED)")
		: NULL;
	set_refused = typed != NULL ? strstr(typed, "*barrier_refused = true") : NULL;
	note_unwind = set_refused != NULL
		? strstr(set_refused, "cluster_pcm_x_stats_note_barrier_unwind()") : NULL;
	return_busy = note_unwind != NULL ? strstr(note_unwind, "return result") : NULL;
	UT_ASSERT(guard != NULL && wait_end != NULL && guard < wait_end);
	UT_ASSERT(typed != NULL && typed < wait_end);
	UT_ASSERT(set_refused != NULL && set_refused < wait_end);
	UT_ASSERT(note_unwind != NULL && note_unwind < wait_end);
	UT_ASSERT(return_busy != NULL && return_busy < wait_end);
	if (typed != NULL && set_refused != NULL && note_unwind != NULL && return_busy != NULL)
		UT_ASSERT(typed < set_refused && set_refused < note_unwind && note_unwind < return_busy);

	initial = strstr(source, "Legacy acquire path:");
	initial_call = initial != NULL
		? strstr(initial, "cluster_bufmgr_pcm_begin_grant_reservation_wait(") : NULL;
	initial_call_end = initial_call != NULL ? strstr(initial_call, ");") : NULL;
	initial_arg = initial_call != NULL ? strstr(initial_call, "pcm_barrier_refused") : NULL;
	initial_check = initial_call_end != NULL
		? strstr(initial_call_end, "pcm_barrier_refused != NULL && *pcm_barrier_refused") : NULL;
	initial_goto = initial_check != NULL
		? strstr(initial_check, "goto cluster_lockbuffer_barrier_refusal") : NULL;
	initial_report = initial_call_end != NULL
		? strstr(initial_call_end, "cluster_pcm_own_report_bump_failure(") : NULL;
	UT_ASSERT(initial_call != NULL && initial_call_end != NULL);
	UT_ASSERT(initial_arg != NULL && initial_arg < initial_call_end);
	UT_ASSERT(initial_check != NULL && initial_goto != NULL && initial_report != NULL);
	if (initial_check != NULL && initial_goto != NULL && initial_report != NULL)
		UT_ASSERT(initial_check < initial_goto && initial_goto < initial_report);

	rearm = strstr(source, "\ncluster_bufmgr_pcm_retry_denied_rearm(");
	rearm_call = rearm != NULL
		? strstr(rearm, "cluster_bufmgr_pcm_begin_grant_reservation_wait(") : NULL;
	rearm_call_end = rearm_call != NULL ? strstr(rearm_call, ");") : NULL;
	rearm_arg = rearm_call != NULL ? strstr(rearm_call, "barrier_refused") : NULL;
	rearm_check = rearm_call_end != NULL
		? strstr(rearm_call_end, "barrier_refused != NULL && *barrier_refused") : NULL;
	rearm_return = rearm_check != NULL
		? strstr(rearm_check, "return CLUSTER_BUFMGR_PCM_RETRY_BARRIER_REFUSED") : NULL;
	UT_ASSERT(rearm_call != NULL && rearm_call_end != NULL);
	UT_ASSERT(rearm_arg != NULL && rearm_arg < rearm_call_end);
	UT_ASSERT(rearm_check != NULL && rearm_return != NULL);

	revalidate = strstr(source, "cluster_pcm_note_writer_cover_stale_detected();");
	revalidate_call = revalidate != NULL
		? strstr(revalidate, "cluster_bufmgr_pcm_begin_grant_reservation_wait(") : NULL;
	revalidate_call_end = revalidate_call != NULL ? strstr(revalidate_call, ");") : NULL;
	revalidate_arg = revalidate_call != NULL ? strstr(revalidate_call, "pcm_barrier_refused") : NULL;
	revalidate_check = revalidate_call_end != NULL
		? strstr(revalidate_call_end, "pcm_barrier_refused != NULL && *pcm_barrier_refused") : NULL;
	revalidate_skip = revalidate_check != NULL
		? strstr(revalidate_check, "goto pcm_revalidate_acquire_done") : NULL;
	revalidate_report = revalidate_call_end != NULL
		? strstr(revalidate_call_end, "cluster_pcm_own_report_bump_failure(") : NULL;
	UT_ASSERT(revalidate_call != NULL && revalidate_call_end != NULL);
	UT_ASSERT(revalidate_arg != NULL && revalidate_arg < revalidate_call_end);
	UT_ASSERT(revalidate_check != NULL && revalidate_skip != NULL && revalidate_report != NULL);
	if (revalidate_check != NULL && revalidate_skip != NULL && revalidate_report != NULL)
		UT_ASSERT(revalidate_check < revalidate_skip && revalidate_skip < revalidate_report);
	free(source);
}

UT_TEST(test_stale_share_cover_holder_barrier_skips_content_lock_to_common_unwind)
{
	char *source = read_bufmgr_source();
	const char *stale;
	const char *admit;
	const char *admit_end;
	const char *typed_arg;
	const char *post_admit_guard;
	const char *content_lock;
	const char *reverify_done;
	const char *activation_guard;
	const char *holder_activate;
	const char *post_try;
	const char *common_goto;

	/* Regression B: stale-cover revalidation must keep the existing typed
	 * refusal channel through holder rearm.  A refusal produced inside the
	 * call is checked again before either content-lock acquire; after PG_TRY
	 * it reaches the sole common cleanup implementation. */
	stale = strstr(source, "cluster_pcm_note_writer_cover_stale_detected();");
	admit = stale != NULL
		? strstr(stale, "pcm_x_holder = cluster_bufmgr_pcm_x_holder_admit_owned_grant(") : NULL;
	admit_end = admit != NULL ? strstr(admit, ");") : NULL;
	typed_arg = admit != NULL ? strstr(admit, "buf, pcm_mode, pcm_barrier_refused,") : NULL;
	post_admit_guard = admit_end != NULL
		? strstr(admit_end, "pcm_barrier_refused == NULL || !*pcm_barrier_refused") : NULL;
	content_lock = admit_end != NULL
		? strstr(admit_end, "LWLockAcquire(BufferDescriptorGetContentLock(buf)") : NULL;
	reverify_done = admit_end != NULL
		? strstr(admit_end, "cluster_pcm_note_writer_reverify_reacquire();") : NULL;
	UT_ASSERT(admit != NULL && admit_end != NULL);
	UT_ASSERT(typed_arg != NULL && typed_arg < admit_end);
	UT_ASSERT(post_admit_guard != NULL && content_lock != NULL && reverify_done != NULL);
	if (post_admit_guard != NULL && content_lock != NULL && reverify_done != NULL)
		UT_ASSERT(post_admit_guard < content_lock && content_lock < reverify_done);
	activation_guard = reverify_done != NULL
		? strstr(reverify_done, "pcm_barrier_refused == NULL || !*pcm_barrier_refused") : NULL;
	holder_activate = reverify_done != NULL
		? strstr(reverify_done, "cluster_bufmgr_pcm_x_holder_activate(pcm_x_holder)") : NULL;
	UT_ASSERT(activation_guard != NULL && holder_activate != NULL);
	if (activation_guard != NULL && holder_activate != NULL)
		UT_ASSERT(activation_guard < holder_activate);

	post_try = reverify_done != NULL ? strstr(reverify_done, "\n\t\tPG_END_TRY();") : NULL;
	common_goto = post_try != NULL
		? strstr(post_try, "goto cluster_lockbuffer_barrier_refusal") : NULL;
	UT_ASSERT(post_try != NULL && common_goto != NULL);
	UT_ASSERT_EQ(count_occurrences(source, "cluster_lockbuffer_barrier_refusal:"), 1);
	free(source);
}

UT_TEST(test_pending_x_denied_retry_leaves_master_invalidate_gap)
{
	static const char *const retry_contract[]
		= { "cluster_pcm_own_abort_grant_reservation",
			"cluster_bufmgr_pcm_pending_x_retry_delay_ms(wait_index)", "WaitLatch",
			"cluster_bufmgr_pcm_begin_grant_reservation_wait" };
	static const char *const delay_contract[]
		= { "cluster_gcs_block_starvation_backoff_ms", "cluster_lmon_main_loop_interval",
			"retry_delay_ms <<= shift", "retry_delay_ms * 2" };
	char *source = read_bufmgr_source();

	/* A queue INVALIDATE that met mirror-N + GRANT_PENDING returns BUSY and
	 * the PCM-X master schedules its retry no sooner than the LMON interval.
	 * A DENIED_PENDING_X reader must therefore leave the exact reservation
	 * absent for strictly longer than that master retry delay.  Otherwise the
	 * two exponential schedules phase-lock forever: each reader request
	 * republishes GRANT_PENDING just as every INVALIDATE retry arrives. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_retry_denied_rearm(", "\nstatic ",
							   retry_contract, lengthof(retry_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_pending_x_retry_delay_ms(",
							   "\nstatic ", delay_contract, lengthof(delay_contract));
	free(source);
}

UT_TEST(test_bufmgr_finish_rejects_invalid_state_and_initializes_acquire_result)
{
	static const char *const finish_gate[]
		= { "new_pcm_state != (uint8)PCM_STATE_S", "new_pcm_state != (uint8)PCM_STATE_X",
			"return CLUSTER_PCM_OWN_INVALID", "LockBufHdr" };
	char *source = read_bufmgr_source();

	/* The only durable grant mirrors are S and X.  Validate that before any
	 * header/sidecar mutation, and never let PG_TRY leave an indeterminate
	 * acquire result for its catch/finalize paths. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_gate, lengthof(finish_gate));
	UT_ASSERT_NOT_NULL(strstr(source, "grant_acquired = false"));
	free(source);
}

UT_TEST(test_bufmgr_finish_and_abort_gate_on_exact_base_state)
{
	static const char *const finish_contract[]
		= { "LockBufHdr", "cluster_pcm_own_snapshot_locked", "cluster_pcm_x_grant_reservation_kind",
			"cluster_pcm_own_grant_commit_exact" };
	static const char *const abort_contract[]
		= { "LockBufHdr", "BufferTagsEqual", "buf->pcm_state != base->pcm_state",
			"cluster_pcm_own_reservation_abort_exact" };
	char *source = read_bufmgr_source();

	/* Tag/gen/token/flag identity is insufficient: a concurrent ownership
	 * transition that changed only the descriptor mirror must make both exact
	 * finish and abort return STALE before touching generation or flags. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_contract, lengthof(finish_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_reservation(",
							   "\nstatic void\ncluster_pcm_own_abort_grant_or_error(",
							   abort_contract, lengthof(abort_contract));
	free(source);
}

UT_TEST(test_retained_release_retag_respects_pin_contract)
{
	BufferDesc buf;
	uint32 buf_state;
	uint8 type_before;
	uint32 state_before;

	/* Unpinned: the released retained image is dropped -- !BM_VALID plus a
	 * BUF_TYPE_CURRENT retag makes the next ordinary read reload the current
	 * page bytes through the buffer-IO protocol. */
	memset(&buf, 0, sizeof(buf));
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	buf_state = BM_VALID | BM_TAG_VALID;
	UT_ASSERT(cluster_pcm_x_retained_release_retag(&buf.buffer_type, &buf_state));
	UT_ASSERT_EQ(buf_state & BM_VALID, 0);
	UT_ASSERT_EQ(buf.buffer_type, (uint8)BUF_TYPE_CURRENT);

	/* Pinned: a pre-existing PG pin freezes the page image -- the bytes may
	 * neither vanish (!BM_VALID under a pin breaks the pin contract and
	 * re-arms the legacy begin-over-invalid-base S_NEW mint) nor be reloaded
	 * in place (an image swap under the pin holder).  The release must keep
	 * the established PI+BM_VALID never-write/never-serve N mirror byte-exact
	 * (the passive-pin invalidate release shape): the next S acquire installs
	 * over it via an exact GRANT_PENDING and republishes CURRENT, an X
	 * convert rides the convert queue, and eviction retags after the last
	 * pin drains. */
	memset(&buf, 0, sizeof(buf));
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	buf_state = (BM_VALID | BM_TAG_VALID) + BUF_REFCOUNT_ONE * 2;
	type_before = buf.buffer_type;
	state_before = buf_state;
	UT_ASSERT(!cluster_pcm_x_retained_release_retag(&buf.buffer_type, &buf_state));
	UT_ASSERT_EQ(buf.buffer_type, type_before);
	UT_ASSERT_EQ(buf_state, state_before);

	/* One pin behaves like many. */
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	buf_state = (BM_VALID | BM_TAG_VALID) + BUF_REFCOUNT_ONE;
	UT_ASSERT(!cluster_pcm_x_retained_release_retag(&buf.buffer_type, &buf_state));
	UT_ASSERT(buf_state & BM_VALID);
	UT_ASSERT_EQ(buf.buffer_type, (uint8)BUF_TYPE_PI);

	/* The kept mirror is republished CURRENT only by a byte-currency proof
	 * inside the exact open legacy grant lifecycle (a shipped-image install,
	 * a storage refresh, or an SCN PASS proof): pcm N + live GRANT_PENDING +
	 * nonzero token + BM_VALID + PI.  Anything else must stay frozen so the
	 * finish valid-image gate keeps refusing an unproven stale cover. */
	UT_ASSERT(cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 7, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_S, PCM_OWN_FLAG_GRANT_PENDING, 7, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape((uint8)PCM_STATE_N, 0, 7, true,
														   (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_REVOKING, 7, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 0, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 7, false, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 7, true, (uint8)BUF_TYPE_CURRENT));
}

UT_TEST(test_passive_retained_pi_is_an_n_assertion_candidate_only)
{
	/* A DRAINed retained image kept under a pre-existing pin is a legal
	 * passive N requester shape.  It may originate ASSERT_X without a local
	 * image proof, but dirty/IO/malformed variants remain closed. */
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_N, (uint8)BUF_TYPE_CURRENT, BM_VALID),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, BM_VALID),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, BM_VALID | BM_DIRTY),
		CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI,
		BM_VALID | BM_IO_IN_PROGRESS),
		CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, 0),
		CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_N, (uint8)BUF_TYPE_XCUR, BM_VALID),
		CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape(
		(uint8)PCM_STATE_S, (uint8)BUF_TYPE_PI, BM_VALID),
		CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_retained_release_and_finish_never_cover_invalid_bytes)
{
	static const char *const release_retag_contract[]
		= { "cluster_pcm_own_revoke_retain_release_exact", "cluster_pcm_x_retained_release_retag",
			"UnlockBufHdr" };
	static const char *const finish_valid_gate[]
		= { "cluster_pcm_x_grant_reservation_kind", "BUF_TYPE_PI",
			"(buf_state & (BM_VALID | BM_IO_IN_PROGRESS)) == 0", "CLUSTER_PCM_OWN_CORRUPT",
			"cluster_pcm_own_grant_commit_exact" };
	char *source = read_bufmgr_source();

	/* The retained release must route its descriptor retag through the shared
	 * pin-aware decision helper under the same header-lock hold that released
	 * the exact write-fence token.  And a grant finish must never commit a
	 * durable S/X mirror over a page image that is not current (!BM_VALID or
	 * a PI mirror): that silent cover of stale bytes is how the pinned
	 * descriptor was previously stamped S over an invalid base, re-arming the
	 * refused legacy S_NEW convert (deterministic client ERROR) -- and, on
	 * the PI arm, a Rule 8.A stale read.  The one legal !BM_VALID commit
	 * shape is the direct-init window (EXTEND/READ_MISS), whose gate still
	 * owns BM_IO_IN_PROGRESS between StartBufferIO and
	 * TerminateBufferIO(BM_VALID).  Reloading the bytes in place under a
	 * foreign pin is equally forbidden. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
							   "\ncluster_bufmgr_pcm_own_self_handoff_probe(",
							   release_retag_contract, lengthof(release_retag_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_valid_gate, lengthof(finish_valid_gate));
	UT_ASSERT_NULL(strstr(source, "cluster_bufmgr_pcm_reload_invalid_pinned"));
	free(source);
}

UT_TEST(test_legacy_byte_proof_republishes_kept_pi_mirror)
{
	static const char *const republish_contract[]
		= { "cluster_pcm_x_grant_pending_republish_shape", "BUF_TYPE_CURRENT", "UnlockBufHdr" };
	char *bufmgr_source = read_bufmgr_source();

	/* A kept-pinned PI mirror regains CURRENT only where its bytes were just
	 * proven current inside the still-open legacy grant lifecycle (the
	 * gcs_block install / storage-fallback call sites are pinned in
	 * test_cluster_gcs_block).  The helper flips only the exact republish
	 * shape under header authority. */
	assert_ordered_in_function(
		bufmgr_source, "\ncluster_bufmgr_pcm_own_republish_grant_pending_image(",
		"\nstatic ClusterPcmOwnResult", republish_contract, lengthof(republish_contract));
	free(bufmgr_source);
}

UT_TEST(test_d5a_release_error_keeps_descriptor_out_of_freelist)
{
	static const char *const fail_closed_contract[]
		= { "BufTableDelete", "LWLockRelease(oldPartitionLock)",
			"PG_TRY",		  "cluster_pcm_lock_release_saved_tag_for_eviction",
			"PG_CATCH",		  "elog(LOG",
			"PG_RE_THROW",	  "StrategyFreeBuffer" };
	char *source = read_bufmgr_source();

	/* A remote release may throw only after the old mapping is gone.  Emit
	 * module evidence and rethrow before StrategyFreeBuffer, leaving the
	 * descriptor unmapped and non-reusable rather than losing a master holder
	 * through descriptor reuse. */
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferTry", fail_closed_contract,
							   lengthof(fail_closed_contract));
	free(source);
}

UT_TEST(test_queue_begin_requires_normalized_n_snapshot)
{
	static const char *const normalized_n_gate[]
		= { "expected->pcm_state != (uint8)PCM_STATE_N", "return CLUSTER_PCM_OWN_STALE" };
	char *source = read_bufmgr_source();

	/* Ordinary queued acquisition must use a fresh normalized N snapshot.
	 * Sole-requester S conversion has a separate exact handoff API that reuses
	 * REVOKING and therefore still cannot enter this new-token path. */
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_begin_x_reservation(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(",
		normalized_n_gate, lengthof(normalized_n_gate));
	free(source);
}

UT_TEST(test_queue_contract_exposes_prepare_only_begin_api)
{
	typedef ClusterPcmOwnResult (*BeginFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64 *);
	typedef ClusterPcmOwnResult (*HandoffFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64 *);
	typedef ClusterPcmOwnResult (*ReleaseSFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
											  ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64,
											uint64 *);
	typedef ClusterPcmOwnResult (*AbortFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64);

	/* The queue owns timing, but not the reservation lifecycle: JOIN/WAIT must
	 * never call this begin API.  ACTIVE_TRANSFER/PREPARE stores the returned
	 * token and all later finish/abort operations are exact. */
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_x_reservation),
										   BeginFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation), HandoffFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation), HandoffFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_s_release_to_n), ReleaseSFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_finish_x_commit),
										   FinishFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_x_reservation),
										   AbortFn));
}

UT_TEST(test_queue_contract_exposes_opaque_retained_revoke_api)
{
	typedef ClusterPcmOwnResult (*BeginRevokeFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
											 ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*BeginHeldXRevokeFn)(
		const BufferTag *, const ClusterPcmOwnSnapshot *,
		ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*AbortHeldXRevokeFn)(
		ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*TryDrainHeldXRevokeFn)(
		const ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*FinishHeldXRevokeFn)(
		ClusterPcmOwnHeldXRevoke *, XLogRecPtr, ClusterPcmOwnSnapshot *,
		ClusterPcmOwnFinishRefusal *);
	typedef ClusterPcmOwnResult (*AbandonHeldXRevokeFn)(
		ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*PrepareNSourceFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
													ClusterPcmOwnSnapshot *, char *, XLogRecPtr *,
													uint64 *);
	typedef ClusterPcmOwnResult (*PrepareSSourceFn)(
		BufferDesc *, const ClusterPcmOwnSnapshot *, SCN, ClusterPcmOwnSnapshot *, char *,
		XLogRecPtr *, uint64 *, ClusterPcmOwnSourcePrepareRefusal *);
	typedef ClusterPcmOwnResult (*AbortRevokeFn)(BufferDesc *, const ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishRetainFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												  XLogRecPtr, ClusterPcmOwnSnapshot *,
												  ClusterPcmOwnFinishRefusal *);
	typedef ClusterPcmOwnResult (*ReleaseRetainedFn)(const BufferTag *, uint64);
	typedef bool (*ContentWriteFn)(BufferDesc *);

	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_x_revoke),
										   BeginRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag),
		BeginHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_abort_held_x_revoke),
		AbortHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_try_drain_held_x_revoke),
		TryDrainHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_held_x_revoke_retain),
		FinishHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed),
		AbandonHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_x_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_s_revoke),
										   BeginRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_prepare_n_source_image), PrepareNSourceFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_prepare_s_source_image), PrepareSSourceFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_n_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_s_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_finish_revoke_retain),
										   FinishRetainFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_release_retained_image), ReleaseRetainedFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_x_content_write_permitted), ContentWriteFn));
}

UT_TEST(test_queue_n_source_refresh_is_exact_and_publishes_only_complete_image)
{
	static const char *const prepare_contract[]
		= { "PGIOAlignedBlock scratch",
			"expected_n->pcm_state != (uint8)PCM_STATE_N",
			"ReservePrivateRefCountEntry",
			"ResourceOwnerEnlargeBuffers(CurrentResourceOwner)",
			"cluster_pcm_own_snapshot_matches_locked",
			"BM_VALID",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"PinBuffer_Locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"FlushBuffer",
			"UnpinBuffer",
			"BM_IO_IN_PROGRESS",
			"cluster_pcm_own_reservation_begin_exact",
			"PCM_OWN_FLAG_REVOKING",
			"smgrread",
			"PageIsVerifiedExtended",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_bufmgr_pcm_own_copy_source_image_exact(" };
	static const char *const copy_contract[]
		= { "cluster_pcm_own_snapshot_matches_locked",
			"PCM_OWN_FLAG_REVOKING",
			"BM_VALID",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_IN_PROGRESS",
			"memcpy((char *)BufHdrGetBlock(buf), source_page, BLCKSZ)",
			"buf->buffer_type = (uint8)BUF_TYPE_CURRENT",
			"memcpy(block_data, source_page, BLCKSZ)",
			"PageGetLSN(source_page)",
			"pd_block_scn",
			"*out_revoking = live" };
	char *source = read_bufmgr_source();

	/* READY may be built only after one verified storage scratch has replaced
	 * the exact fenced N descriptor and supplied all image evidence.  A dirty
	 * pre-grant N page is legal (extension / redo dirt): the contract now pins
	 * the flush-then-BUSY convergence (reserve/pin under the header lock,
	 * WAL-first FlushBuffer, unpin) ahead of the reservation, with BM_IO_ERROR
	 * judged before the dirty branch on both passes. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_prepare_n_source_image(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_s_revoke(",
							   prepare_contract, lengthof(prepare_contract));
	assert_ordered_in_function(source,
							   "\ncluster_bufmgr_pcm_own_copy_source_image_exact(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_prepare_n_source_image(",
							   copy_contract, lengthof(copy_contract));
	free(source);
}

UT_TEST(test_queue_s_source_dirty_flush_makes_progress_and_reports_exact_refusal)
{
	static const char *const prepare_contract[]
		= { "ReservePrivateRefCountEntry",
			"ResourceOwnerEnlargeBuffers(CurrentResourceOwner)",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"PinBuffer_Locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_CONTENT_LOCK",
			"FlushBuffer",
			"CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_DIRTY_FLUSHED",
			"UnpinBuffer",
			"return CLUSTER_PCM_OWN_BUSY",
			"BM_IO_IN_PROGRESS",
			"CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_IO_IN_PROGRESS",
			"cluster_bufmgr_pcm_own_begin_s_revoke",
			"smgrread",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)" };
	char *source = read_bufmgr_source();

	/* A clean checkpoint can be dirtied again by a SELECT's commit hint before
	 * the S->X self handoff.  That is progress work, not a permanent refusal:
	 * pin and flush it before opening REVOKING, then retry.  Keep I/O and both
	 * content-lock refusals separately diagnosed so a native stall cannot be
	 * collapsed back into an opaque materialize-begin BUSY. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_prepare_s_source_image(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   prepare_contract, lengthof(prepare_contract));
	free(source);
}

UT_TEST(test_revoke_finish_mode_rejects_pinned_vm_fsm_and_retains_main)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 7), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 3), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_DROP);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 1), CLUSTER_PCM_X_REVOKE_FINISH_BUSY);
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_DROP);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, UINT32_MAX),
				 CLUSTER_PCM_X_REVOKE_FINISH_BUSY);
	tag.forkNum = InvalidForkNumber;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(NULL, 0), CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
}

UT_TEST(test_pcm_tracking_excludes_only_fsm_for_user_and_shared_catalog_relations)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.relNumber = FirstNormalObjectId;
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, false));
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, false));
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, false));
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(&tag, false));

	/* shared_catalog widens the relation-number domain, but must not put its
	 * advisory FSM fork back into the PCM/PCM-X authority domain. */
	tag.relNumber = FirstNormalObjectId - 1;
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(&tag, false));
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, true));
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, true));
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, true));
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(&tag, true));
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(NULL, true));
}

UT_TEST(test_pcm_tracking_uses_one_tag_gate_for_acquire_direct_init_and_eviction)
{
	char *source = read_bufmgr_source();
	const char *should_track;
	const char *should_track_end;
	const char *invalidate_commit;
	const char *invalidate_tail;
	const char *victim;
	const char *victim_end;
	const char *direct_init;
	const char *direct_init_end;
	const char *direct_gate;
	const char *arm;
	const char *consume;
	const char *content_lock;

	/* A saved tag must make exactly the same fork decision as a live
	 * BufferDesc.  A relnumber-only eviction gate would leak an FSM release
	 * back into a domain from which acquire was excluded. */
	UT_ASSERT_NULL(strstr(source, "cluster_bufmgr_reln_pcm_tracked"));
	should_track = strstr(source, "\ncluster_bufmgr_should_pcm_track(");
	should_track_end = should_track != NULL ? strstr(should_track, "\n}") : NULL;
	invalidate_commit = strstr(source, "\nInvalidateBufferCommitLocked(");
	invalidate_tail = strstr(source, "\nInvalidateBufferCommitTailLocked(");
	victim = strstr(source, "\nInvalidateVictimBuffer(");
	victim_end = strstr(source, "\nstatic Buffer\nGetVictimBuffer(");
	UT_ASSERT_NOT_NULL(should_track);
	UT_ASSERT_NOT_NULL(should_track_end);
	UT_ASSERT_NOT_NULL(invalidate_commit);
	UT_ASSERT_NOT_NULL(invalidate_tail);
	UT_ASSERT_NOT_NULL(victim);
	UT_ASSERT_NOT_NULL(victim_end);
	if (should_track != NULL && should_track_end != NULL)
		assert_source_range_contains(should_track, should_track_end,
									 "cluster_pcm_x_buffer_tag_tracked");
	if (invalidate_commit != NULL && invalidate_tail != NULL)
		assert_source_range_contains(invalidate_commit, invalidate_tail,
									 "cluster_pcm_x_buffer_tag_tracked");
	if (victim != NULL && victim_end != NULL)
		assert_source_range_contains(victim, victim_end, "cluster_pcm_x_buffer_tag_tracked");

	/* The dedicated VM/FSM initialization wrapper retains provenance.  Its
	 * common tag gate decides whether PCM proof is armed: FSM falls through to
	 * the local content lock, while VM still arms and consumes the exact proof. */
	direct_init = strstr(source, "\nLockBufferForAuxiliaryPageInit(");
	direct_init_end = strstr(source, "\nvoid\nLockBufferForVisibilityMapPageInit(");
	UT_ASSERT_NOT_NULL(direct_init);
	UT_ASSERT_NOT_NULL(direct_init_end);
	direct_gate
		= direct_init != NULL ? strstr(direct_init, "cluster_bufmgr_should_pcm_track(buf)") : NULL;
	arm = direct_init != NULL ? strstr(direct_init, "cluster_bufmgr_pcm_arm_direct_init") : NULL;
	consume
		= direct_init != NULL ? strstr(direct_init, "cluster_bufmgr_pcm_gate_direct_init") : NULL;
	content_lock = direct_init != NULL
					   ? strstr(direct_init,
								"LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)")
					   : NULL;
	UT_ASSERT_NOT_NULL(direct_gate);
	UT_ASSERT_NOT_NULL(arm);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(content_lock);
	if (direct_gate != NULL && arm != NULL && consume != NULL && content_lock != NULL)
		UT_ASSERT(direct_gate < arm && arm < consume && consume < content_lock
				  && content_lock < direct_init_end);

	free(source);
}

UT_TEST(test_queue_revoke_retains_main_but_drops_unpinned_vm_fsm)
{
	static const char *const begin_contract[] = { "expected_s->pcm_state != (uint8) PCM_STATE_S",
												  "LockBufHdr",
												  "cluster_pcm_own_gen_get",
												  "cluster_bufmgr_pcm_current_image_locked",
												  "cluster_pcm_own_classify_live_flags",
												  "cluster_pcm_own_reservation_begin_exact",
												  "PCM_OWN_FLAG_REVOKING",
												  "cluster_pcm_own_snapshot_locked",
												  "UnlockBufHdr" };
	static const char *const abort_contract[]
		= { "expected_revoking->pcm_state != (uint8) PCM_STATE_S",
			"LockBufHdr",
			"cluster_pcm_own_classify_live_flags",
			"cluster_pcm_own_reservation_abort_exact",
			"PCM_OWN_FLAG_REVOKING",
			"UnlockBufHdr" };
	static const char *const finish_contract[]
		= { "BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"cluster_pcm_own_snapshot_matches_locked",
			"cluster_bufmgr_pcm_current_image_locked",
			"BM_IO_IN_PROGRESS",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockRelease(partition_lock)",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"PG_TRY();",
			"cluster_pcm_own_snapshot_matches_locked",
			"PageGetLSN",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"LockBufHdr",
			"cluster_pcm_own_snapshot_matches_locked",
			"cluster_bufmgr_pcm_current_image_locked",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"result = CLUSTER_PCM_OWN_BUSY",
			"PageGetLSN",
			"cluster_pcm_own_revoke_retain_commit_exact",
			"buf->pcm_state = (uint8)PCM_STATE_N",
			"buf->buffer_type = (uint8)BUF_TYPE_PI",
			"BM_DIRTY | BM_JUST_DIRTIED",
			"BM_CHECKPOINT_NEEDED | BM_IO_ERROR",
			"cluster_pcm_own_snapshot_locked",
			"LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	static const char *const drop_contract[] = { "BufMappingPartitionLock",
												 "LWLockAcquire(partition_lock, LW_EXCLUSIVE)",
												 "BufTableLookup",
												 "LockBufHdr",
												 "BUF_STATE_GET_REFCOUNT",
												 "cluster_pcm_own_flags_get",
												 "BM_IO_IN_PROGRESS",
												 "CLUSTER_PCM_X_REVOKE_FINISH_BUSY",
												 "CLUSTER_PCM_OWN_FINISH_REFUSAL_VM_FSM_PINNED",
												 "CLUSTER_PCM_OWN_FINISH_REFUSAL_IO_IN_PROGRESS",
												 "CLUSTER_PCM_OWN_FINISH_REFUSAL_LIVE_FLAGS",
												 "PageGetLSN",
												 "cluster_pcm_own_revoke_commit_exact",
												 "buf->pcm_state = (uint8)PCM_STATE_N",
												 "cluster_pcm_own_snapshot_locked",
												 "InvalidateBufferCommitTailLocked" };
	static const char *const held_begin_contract[] = {
		"cluster_pcm_x_revoke_finish_mode(tag, 0)",
		"LWLockAcquire(partition_lock, LW_SHARED)",
		"BufTableLookup",
		"LockBufHdr",
		"cluster_pcm_own_snapshot_matches_locked",
		"cluster_bufmgr_pcm_current_image_locked",
		"cluster_bufmgr_pin_for_gcs_locked",
		"LWLockRelease(partition_lock)",
		"cluster_bufmgr_pcm_own_begin_x_revoke(",
		"cluster_bufmgr_unpin_for_gcs(buf)",
		"held_out->revoking = revoking",
		"held_out->flags = CLUSTER_PCM_OWN_HELD_X_REVOKE_KNOWN_MASK"
	};
	static const char *const held_abort_contract[] = {
		"cluster_bufmgr_pcm_own_abort_x_revoke(",
		"if (result != CLUSTER_PCM_OWN_OK)",
		"cluster_bufmgr_unpin_for_gcs(buf)",
		"memset(held, 0, sizeof(*held))"
	};
	static const char *const held_drain_contract[] = {
		"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
		"LockBufHdr(buf)",
		"cluster_pcm_own_snapshot_matches_locked",
		"cluster_bufmgr_pcm_current_image_locked",
		"BM_IO_IN_PROGRESS",
		"BM_IO_ERROR",
		"UnlockBufHdr(buf, buf_state)",
		"LWLockRelease(content_lock)"
	};
	static const char *const held_finish_contract[] = {
		"cluster_bufmgr_pcm_own_finish_revoke_retain(",
		"if (result != CLUSTER_PCM_OWN_OK)",
		"cluster_bufmgr_unpin_for_gcs(buf)",
		"memset(held, 0, sizeof(*held))"
	};
	static const char *const held_abandon_contract[] = {
		"LockBufHdr",
		"cluster_pcm_own_snapshot_matches_locked",
		"cluster_bufmgr_pcm_current_image_locked",
		"UnlockBufHdr",
		"cluster_bufmgr_unpin_for_gcs(buf)",
		"memset(held, 0, sizeof(*held))"
	};
	char *source = read_bufmgr_source();
	const char *begin_s;
	const char *abort_s;
	const char *begin_x;
	const char *abort_x;
	const char *drop_helper;
	const char *held_drain;
	const char *held_drain_end;
	const char *finish;
	const char *finish_end;

	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_begin_s_revoke(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   begin_contract, lengthof(begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_revoke(",
							   abort_contract, lengthof(abort_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(", finish_contract,
		lengthof(finish_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_revoke_retain(", drop_contract,
		lengthof(drop_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_held_x_revoke(",
		held_begin_contract, lengthof(held_begin_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_abort_held_x_revoke(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		held_abort_contract, lengthof(held_abort_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_try_drain_held_x_revoke(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		held_drain_contract, lengthof(held_drain_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(",
		held_finish_contract, lengthof(held_finish_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(",
		"\nstatic ClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(",
		held_abandon_contract, lengthof(held_abandon_contract));

	/* Main/init passive PG pins are not PCM holders, so their retained commit
	 * must not recreate the S3 pin ring.  VM/FSM take the separate exact-drop
	 * arm above, where a foreign pin returns BUSY before any ownership commit. */
	begin_s = strstr(source, "\ncluster_bufmgr_pcm_own_begin_s_revoke(");
	abort_s = strstr(source, "\ncluster_bufmgr_pcm_own_abort_s_revoke(");
	begin_x = strstr(source, "\ncluster_bufmgr_pcm_own_begin_x_revoke(");
	abort_x = strstr(source, "\ncluster_bufmgr_pcm_own_abort_x_revoke(");
	drop_helper = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(");
	finish = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(");
	finish_end = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_image(");
	held_drain = strstr(source,
		"\ncluster_bufmgr_pcm_own_try_drain_held_x_revoke(");
	held_drain_end = held_drain != NULL
		? strstr(held_drain,
			"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(")
		: NULL;
	UT_ASSERT_NOT_NULL(begin_s);
	UT_ASSERT_NOT_NULL(abort_s);
	UT_ASSERT_NOT_NULL(begin_x);
	UT_ASSERT_NOT_NULL(abort_x);
	UT_ASSERT_NOT_NULL(drop_helper);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(finish_end);
	UT_ASSERT_NOT_NULL(held_drain);
	UT_ASSERT_NOT_NULL(held_drain_end);
	if (held_drain != NULL && held_drain_end != NULL) {
		const char *forbidden;

		forbidden = strstr(held_drain, "LWLockAcquire(content_lock");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "LWLockAcquireOrWait(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "WaitLatch(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "pg_usleep(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "FlushBuffer(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain,
			"cluster_pcm_own_revoke_retain_commit_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
	}
	/* buffer_type is a monotone hint: every exact source lifecycle must
	 * accept a yielded S+XCUR through the centralized current-image gate. */
	assert_source_range_contains(begin_s, abort_s, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(abort_s, begin_x, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(begin_x, abort_x, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(abort_x, drop_helper, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(drop_helper, finish, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(finish, finish_end, "cluster_bufmgr_pcm_current_image_locked");
	if (begin_s != NULL && begin_x != NULL)
		UT_ASSERT(strstr(begin_s, "BUF_STATE_GET_REFCOUNT") == NULL
				  || strstr(begin_s, "BUF_STATE_GET_REFCOUNT") >= begin_x);
	if (begin_x != NULL && drop_helper != NULL)
		UT_ASSERT(strstr(begin_x, "BUF_STATE_GET_REFCOUNT") == NULL
				  || strstr(begin_x, "BUF_STATE_GET_REFCOUNT") >= drop_helper);
	if (finish != NULL && finish_end != NULL) {
		const char *refcount = strstr(finish, "BUF_STATE_GET_REFCOUNT");
		const char *drop = strstr(finish, "InvalidateBuffer");
		const char *legacy_pi = strstr(finish, "cluster_bufmgr_convert_to_pi_locked");
		const char *mapping = strstr(finish, "partition_lock");
		const char *conditional
			= strstr(finish, "LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)");

		UT_ASSERT(refcount == NULL || refcount >= finish_end);
		UT_ASSERT(drop == NULL || drop >= finish_end);
		UT_ASSERT(legacy_pi == NULL || legacy_pi >= finish_end);
		UT_ASSERT_NOT_NULL(mapping);
		UT_ASSERT_NOT_NULL(conditional);
		if (mapping != NULL && conditional != NULL)
			UT_ASSERT(mapping < conditional);
	}
	free(source);
}

UT_TEST(test_retained_image_release_and_writeback_gates_are_exact)
{
	static const char *const content_write_contract[]
		= { "LockBufHdr",
			"cluster_pcm_own_flags_get",
			"PCM_OWN_FLAG_REVOKING",
			"cluster_bufmgr_pcm_x_retained_image_locked",
			"PCM_OWN_FLAG_GRANT_PENDING",
			"UnlockBufHdr" };
	static const char *const release_contract[]
		= { "source_generation + 1",
			"BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"PCM_OWN_FLAG_REVOKING",
			"LWLockRelease(partition_lock)",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"LockBufHdr",
			"BufferTagsEqual",
			"BM_VALID",
			"BUF_TYPE_PI",
			"PCM_STATE_N",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR",
			"PCM_OWN_FLAG_REVOKING",
			"cluster_pcm_own_revoke_retain_release_exact" };
	char *source = read_bufmgr_source();
	const char *victim;
	const char *sync;
	const char *flush;
	const char *dirty;
	const char *hint;
	const char *lockbuffer;
	const char *conditional;
	const char *resident_stamp;
	const char *storage_refresh;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
		"\n/* ========================================================================\n * PGRAC "
		"MODIFICATIONS by SqlRush — spec-6.12h D-h2",
		release_contract, lengthof(release_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_content_write_permitted(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_snapshot_by_tag(",
							   content_write_contract, lengthof(content_write_contract));

	/* The content-lock ordering is the race proof: an already-started flush
	 * owns SHARE and finishes before retain; after retain owns EXCLUSIVE, all
	 * later Sync/Flush/dirty paths see the immutable retained shape. */
	victim = strstr(source, "\nInvalidateVictimBuffer(");
	sync = strstr(source, "\nSyncOneBuffer(");
	flush = strstr(source, "\nFlushBuffer(");
	dirty = strstr(source, "\nMarkBufferDirty(Buffer buffer)");
	hint = strstr(source, "\nMarkBufferDirtyHint(Buffer buffer, bool buffer_std)");
	lockbuffer = strstr(source, "\nLockBufferInternal(Buffer buffer, int mode");
	conditional = strstr(source, "\nConditionalLockBuffer(Buffer buffer)");
	resident_stamp = strstr(source, "\ncluster_bufmgr_lock_resident_for_stamp(");
	storage_refresh = strstr(source, "\ncluster_bufmgr_refresh_block_from_storage_for_gcs(");
	UT_ASSERT_NOT_NULL(victim);
	UT_ASSERT_NOT_NULL(sync);
	UT_ASSERT_NOT_NULL(flush);
	UT_ASSERT_NOT_NULL(dirty);
	UT_ASSERT_NOT_NULL(hint);
	UT_ASSERT_NOT_NULL(lockbuffer);
	UT_ASSERT_NOT_NULL(conditional);
	UT_ASSERT_NOT_NULL(resident_stamp);
	UT_ASSERT_NOT_NULL(storage_refresh);
	if (victim != NULL)
		UT_ASSERT(strstr(victim, "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked")
				  < strstr(victim, "ClearBufferTag"));
	if (sync != NULL) {
		const char *first_gate
			= strstr(sync, "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked");
		const char *content_share
			= strstr(sync, "LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED)");
		const char *second_gate
			= first_gate != NULL
				  ? strstr(first_gate + 1,
						   "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked")
				  : NULL;

		UT_ASSERT_NOT_NULL(first_gate);
		UT_ASSERT_NOT_NULL(content_share);
		UT_ASSERT_NOT_NULL(second_gate);
		if (first_gate != NULL)
			UT_ASSERT(first_gate < strstr(sync, "result |= BUF_REUSABLE"));
		if (content_share != NULL && second_gate != NULL)
			UT_ASSERT(content_share < second_gate);
	}
	if (flush != NULL)
		UT_ASSERT(strstr(flush, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(flush, "StartBufferIO(buf, false)"));
	if (dirty != NULL)
		UT_ASSERT(strstr(dirty, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(dirty, "buf_state |= BM_DIRTY"));
	if (hint != NULL) {
		const char *tracked = strstr(hint, "cluster_bufmgr_should_pcm_track(bufHdr)");
		const char *gate = strstr(hint, "cluster_pcm_x_ordinary_mutation_allowed(");
		const char *refuse = gate != NULL ? strstr(gate, "return;") : NULL;
		const char *dirty_flags = strstr(hint, "BM_DIRTY | BM_JUST_DIRTIED");

		/* Hint dirt is optional.  A kept pinned N/PI mirror may have its
		 * in-memory hint byte touched, but without live S/X current authority it
		 * must not regain writeback eligibility and block a later storage refresh. */
		UT_ASSERT_NOT_NULL(tracked);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(refuse);
		UT_ASSERT_NOT_NULL(dirty_flags);
		if (tracked != NULL && gate != NULL && refuse != NULL && dirty_flags != NULL)
			UT_ASSERT(gate < tracked && tracked < refuse && refuse < dirty_flags);
	}
	if (lockbuffer != NULL) {
		const char *reserve
			= strstr(lockbuffer, "cluster_bufmgr_pcm_begin_grant_reservation_wait(");
		const char *content
			= strstr(lockbuffer, "LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_SHARED)");
		const char *w1_reverify
			= strstr(lockbuffer, "cluster_pcm_x_cached_cover_reverify_accepts(");

		UT_ASSERT_NOT_NULL(reserve);
		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(w1_reverify);
		if (reserve != NULL && content != NULL)
			UT_ASSERT(reserve < content);
	}
	if (conditional != NULL) {
		const char *content
			= strstr(conditional, "LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf)");
		const char *ownership = strstr(conditional, "cluster_pcm_x_conditional_lock_allowed(");
		const char *release
			= strstr(conditional, "LWLockRelease(BufferDescriptorGetContentLock(buf))");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(ownership);
		UT_ASSERT_NOT_NULL(release);
		if (content != NULL && ownership != NULL && release != NULL)
			UT_ASSERT(content < ownership && ownership < release);
	}
	if (resident_stamp != NULL) {
		const char *content = strstr(
			resident_stamp, "LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)");
		const char *gate = strstr(resident_stamp, "cluster_bufmgr_pcm_x_retained_image_locked");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		if (content != NULL && gate != NULL)
			UT_ASSERT(content < gate);
	}
	if (storage_refresh != NULL) {
		const char *content = strstr(storage_refresh, "LWLockAcquire(content_lock, LW_EXCLUSIVE)");
		const char *gate = strstr(storage_refresh, "cluster_bufmgr_pcm_x_content_write_permitted");
		const char *copy = strstr(storage_refresh, "memcpy((char *) BufHdrGetBlock(buf)");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(copy);
		if (content != NULL && gate != NULL && copy != NULL)
			UT_ASSERT(content < gate && gate < copy);
	}
	free(source);
}

UT_TEST(test_retained_drain_retags_invalid_only_after_exact_token_release)
{
	static const char *const drain_contract[]
		= { "cluster_pcm_own_revoke_retain_release_exact", "result == CLUSTER_PCM_OWN_OK",
			"cluster_pcm_x_retained_release_retag", "&buf->buffer_type", "UnlockBufHdr" };
	char *source = read_bufmgr_source();
	const char *release;
	const char *release_end;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
		"\n/* ========================================================================\n * PGRAC "
		"MODIFICATIONS by SqlRush — spec-6.12h D-h2",
		drain_contract, lengthof(drain_contract));

	release = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_image(");
	release_end = release != NULL ? strstr(release + 1, "\n/* "
														"=========================================="
														"==============================\n * PGRAC ")
								  : NULL;
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(release_end);
	if (release != NULL && release_end != NULL)
		UT_ASSERT(strstr(release, "InvalidateBufferCommitTailLocked") == NULL
				  || strstr(release, "InvalidateBufferCommitTailLocked") >= release_end);

	free(source);
}

UT_TEST(test_source_settlement_releases_fence_without_discarding_pi)
{
	static const char *const preserve_contract[] = {
		"cluster_pcm_own_revoke_retain_release_exact",
		"result == CLUSTER_PCM_OWN_OK",
		"buf->buffer_type != (uint8) BUF_TYPE_PI",
		"(buf_state & BM_VALID) == 0",
		"UnlockBufHdr"
	};
	char *source = read_bufmgr_source();
	const char *preserve;
	const char *preserve_end;

	assert_ordered_in_function(
		source,
		"\ncluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(",
		preserve_contract, lengthof(preserve_contract));
	preserve = strstr(source,
		"\ncluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(");
	preserve_end = preserve != NULL
		? strstr(preserve + 1,
			"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(")
		: NULL;
	UT_ASSERT_NOT_NULL(preserve);
	UT_ASSERT_NOT_NULL(preserve_end);
	if (preserve != NULL && preserve_end != NULL) {
		const char *retag = strstr(preserve,
			"cluster_pcm_x_retained_release_retag");

		UT_ASSERT(retag == NULL || retag >= preserve_end);
	}
	free(source);
}

UT_TEST(test_queue_s_release_finish_is_header_exact_and_returns_fresh_n)
{
	static const char *const release_contract[] = { "expected_s->pcm_state != (uint8) PCM_STATE_S",
													"expected_s->flags != 0",
													"LockBufHdr",
													"cluster_pcm_own_snapshot_matches_locked",
													"cluster_pcm_own_bump_locked",
													"buf->pcm_state = (uint8) PCM_STATE_N",
													"cluster_pcm_own_snapshot_locked",
													"UnlockBufHdr" };
	char *source = read_bufmgr_source();

	/* The caller proves the exact remote RELEASE application ACK before this
	 * adapter.  The adapter then normalizes only the matching S tuple and
	 * returns the fresh N generation that PREPARE must reserve. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_s_release_to_n(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   release_contract, lengthof(release_contract));
	free(source);
}

UT_TEST(test_resource_x_remote_s_finish_requires_content_and_exact_revoke)
{
	typedef ClusterPcmOwnResult (*RemoteSFinishFn)(
		BufferDesc *, const ClusterPcmOwnSnapshot *, ClusterPcmOwnSnapshot *);
	static const char *const finish_contract[] = {
		"LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
		"expected_revoking->pcm_state != (uint8)PCM_STATE_S",
		"expected_revoking->flags != PCM_OWN_FLAG_REVOKING",
		"expected_revoking->reservation_token == 0",
		"LockBufHdr(buf)",
		"cluster_pcm_own_snapshot_matches_locked(buf, expected_revoking)",
		"cluster_bufmgr_pcm_current_image_locked(buf, buf_state)",
		"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
		"cluster_pcm_own_revoke_commit_exact(",
		"buf->pcm_state = (uint8)PCM_STATE_N",
		"buf->buffer_type = (uint8)BUF_TYPE_CURRENT",
		"cluster_pcm_own_snapshot_locked(buf, out_n_snapshot)",
		"UnlockBufHdr(buf, buf_state)"
	};
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_remote_s_block_to_n),
		RemoteSFinishFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_remote_s_block_to_n(",
		"\n/*\n * Release a passively pinned MAIN/INIT S mirror",
		finish_contract, lengthof(finish_contract));
	free(source);
}

UT_TEST(test_lockbuffer_pcm_x_holder_ledger_brackets_both_content_acquires)
{
	static const char *const unlock_contract[]
		= { "cluster_bufmgr_pcm_x_holder_mark_releasing",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_holder_unregister" };
	static const char *const acquire_contract[]
		= { "pcm_x_holder = cluster_bufmgr_pcm_x_holder_admit_owned_grant(",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"pcm_x_holder = cluster_bufmgr_pcm_x_holder_admit_owned_grant(",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"cluster_bufmgr_pcm_x_holder_activate(pcm_x_holder)" };
	char *source = read_bufmgr_source();

	/* One ACQUIRING entry brackets the first content-lock acquire and remains
	 * published across the W1 release/reacquire fallback.  Normal release
	 * publishes RELEASING before unlocking and unlinks only afterwards. */
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		unlock_contract, lengthof(unlock_contract));
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		acquire_contract, lengthof(acquire_contract));
	free(source);
}

UT_TEST(test_r11_lockbuffer_writer_selector_is_single_ingress_choice_and_exclusive)
{
	static const char *const selector_contract[] = {
		"pcm_writer_path = cluster_resource_x_writer_path_snapshot(",
		"switch (pcm_writer_path)",
		"case RESOURCE_X_WRITER_SOURCE:",
		"cluster_bufmgr_pcm_x_writer_prepare_source(",
		"case RESOURCE_X_WRITER_TARGET:",
		"cluster_bufmgr_pcm_x_writer_prepare_target(",
		"case RESOURCE_X_WRITER_CLOSED:",
		"cluster_lockbuffer_barrier_refusal"
	};
	char *source = read_bufmgr_source();

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	/* One LockBuffer ingress selection, two ordinary TARGET post-T3/content-lock
	 * revalidations, and two known-new direct-init proof revalidations.  None may
	 * resnapshot into the other implementation. */
	UT_ASSERT_EQ(count_occurrences(
		source, "cluster_resource_x_writer_path_snapshot("), 5);
	if (strstr(source, "cluster_resource_x_writer_path_snapshot(") == NULL)
	{
		free(source);
		return;
	}
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode", "\nvoid\nLockBuffer(",
		selector_contract, lengthof(selector_contract));
	/* The generation is passed into the selected wrapper; neither arm may
	 * silently resnapshot into the other implementation. */
	UT_ASSERT_NOT_NULL(strstr(source,
		"pcm_writer_r4_generation, pcm_barrier_refused"));
	free(source);
}

UT_TEST(test_bufmgr_pcm_x_holder_ledger_is_bounded_and_uses_private_identity)
{
	static const char *const identity_contract[]
		= { "cluster_bufmgr_pcm_x_holder_find",
			"entry != NULL",
			"return entry",
			"cluster_pcm_own_read",
			"cluster_epoch_get_current",
			"cluster_node_id",
			"MyProc->pgprocno",
			"cluster_bufmgr_pcm_x_holder_identity == UINT64_MAX",
			"ERRCODE_PROGRAM_LIMIT_EXCEEDED",
			"++cluster_bufmgr_pcm_x_holder_identity",
			"key.identity.request_id = identity",
			"key.identity.wait_seq = identity",
			"key.identity.base_own_generation = own_generation",
			"cluster_pcm_x_local_holder_register" };
	char *source = read_bufmgr_source();

	/* The ledger is backend-local and no larger than PG's own maximum held
	 * LWLock set.  Its checked identity populates only the protocol key; it
	 * must never borrow or mutate PGPROC's deadlock wait sequence. */
	UT_ASSERT_NOT_NULL(
		strstr(source, "cluster_bufmgr_pcm_x_holder_ledger[LWLOCK_MAX_HELD_BY_PROC]"));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   identity_contract, lengthof(identity_contract));
	UT_ASSERT_NULL(strstr(source, "cluster_lmd_wait.wait_seq"));
	free(source);
}

UT_TEST(test_unlockbuffers_exceptionally_detaches_released_pcm_x_holders)
{
	static const char *const cleanup_contract[]
		= { "LWLockHeldByMe(entry->content_lock)",
			"cluster_pcm_x_local_holder_exceptional_detach_exact(&entry->handle",
			"entry->content_lock" };
	static const char *const catch_contract[]
		= { "!LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_holder_abort_acquiring" };
	static const char *const eoxact_contract[]
		= { "AtEOXact_LocalBuffers(isCommit)",
			"cluster_bufmgr_pcm_x_holder_drain_deferred_nowait()",
			"Assert(PrivateRefCountOverflowed == 0)" };
	char *source = read_bufmgr_source();
	const char *cleanup_begin;
	const char *cleanup_end;
	const char *mirror_read;

	/* AbortTransaction releases all LWLocks before UnlockBuffers.  The
	 * process-local ledger must prove each content lock is gone before using
	 * the multi-state exceptional detach; an acquire that threw before owning
	 * the lock keeps the narrower ACQUIRING-only rollback. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_exception_cleanup_all(",
							   "\nstatic ", cleanup_contract, lengthof(cleanup_contract));
	cleanup_begin = strstr(source, "\ncluster_bufmgr_pcm_x_holder_exception_cleanup_all(");
	UT_ASSERT_NOT_NULL(cleanup_begin);
	cleanup_end = cleanup_begin == NULL ? NULL : strstr(cleanup_begin + 1, "\nstatic ");
	UT_ASSERT_NOT_NULL(cleanup_end);
	mirror_read = cleanup_begin == NULL ? NULL : strstr(cleanup_begin, "pcm_state");
	UT_ASSERT(mirror_read == NULL || (cleanup_end != NULL && mirror_read >= cleanup_end));
	UT_ASSERT(cleanup_begin == NULL || cleanup_end == NULL
			  || strstr(cleanup_begin, "WaitLatch(") == NULL
			  || strstr(cleanup_begin, "WaitLatch(") >= cleanup_end);
	UT_ASSERT(cleanup_begin == NULL || cleanup_end == NULL
			  || strstr(cleanup_begin, "CHECK_FOR_INTERRUPTS()") == NULL
			  || strstr(cleanup_begin, "CHECK_FOR_INTERRUPTS()") >= cleanup_end);
	assert_ordered_in_function(source,
							   "CLUSTER_INJECTION_POINT(\"cluster-pcm-writer-cached-x-stall\")",
							   "PG_END_TRY();", catch_contract, lengthof(catch_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_bufmgr_pcm_x_holder_exception_cleanup_all();"));
	assert_ordered_in_function(source, "\nAtEOXact_Buffers(bool isCommit)",
							   "\n/*\n * Initialize access to shared buffer pool", eoxact_contract,
							   lengthof(eoxact_contract));
	free(source);
}

UT_TEST(test_bufmgr_pcm_x_holder_gate_retry_is_bounded_outside_content_lock)
{
	static const char *const wait_contract[]
		= { "content_lock == NULL || LWLockHeldByMe(content_lock)",
			"ereport(ERROR",
			"cluster_pcm_x_nested_wait_guard_before_block()",
			"cluster_bufmgr_pcm_x_holder_report_failure(",
			"cluster_pcm_x_holder_retry_delay_ms(wait_index)",
			"CHECK_FOR_INTERRUPTS()",
			"WaitLatch(MyLatch",
			"WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT",
			"CHECK_FOR_INTERRUPTS()" };
	static const char *const register_contract[]
		= { "cluster_bufmgr_pcm_x_holder_drain_deferred_nowait()",
			"entry->phase == PCM_X_HOLDER_LEDGER_DEFERRED",
			"cluster_bufmgr_pcm_x_holder_drain_deferred(entry)",
			"cluster_pcm_x_local_holder_register(&key, &handle)",
			"cluster_pcm_x_holder_register_retry_action(",
			"cluster_bufmgr_pcm_x_holder_retry_wait(" };
	static const char *const unregister_contract[]
		= { "entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock)",
			"cluster_pcm_x_local_holder_unregister_exact(&entry->handle)",
			"cluster_pcm_x_holder_unregister_retry_action(result, waits_used)",
			"CLUSTER_PCM_X_HOLDER_RETRY_DEFER",
			"entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED",
			"return" };
	static const PcmXQueueResult register_script[]
		= { PCM_X_QUEUE_GATE_RETRY, PCM_X_QUEUE_GATE_RETRY, PCM_X_QUEUE_OK };
	char *source = read_bufmgr_source();
	const char *wait_begin;
	const char *wait_end;
	int i;

	for (i = 0; i < lengthof(register_script); i++)
		UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(register_script[i], true),
					 i + 1 == lengthof(register_script) ? CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE
														: CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_DUPLICATE, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_BARRIER_CLOSED, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_NOT_READY, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_NOT_READY, false),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_BUSY, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_BUSY, false),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_NO_CAPACITY, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	for (i = 0; i < CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS; i++) {
		UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(PCM_X_QUEUE_GATE_RETRY, i),
					 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
		UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_BUSY, i),
					 CLUSTER_PCM_X_WRITER_RETRY_WAIT);
		UT_ASSERT_EQ(cluster_pcm_x_holder_retry_delay_ms(i), 2L << i);
	}
	UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(
					 PCM_X_QUEUE_BUSY, CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS),
				 CLUSTER_PCM_X_HOLDER_RETRY_DEFER);
	UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(PCM_X_QUEUE_NOT_FOUND, 0),
				 CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(PCM_X_QUEUE_CORRUPT, 0),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_OK, 0),
				 CLUSTER_PCM_X_WRITER_RETRY_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_GATE_RETRY,
														   CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS),
				 CLUSTER_PCM_X_WRITER_RETRY_DEFER);
	UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_NOT_READY, 0),
				 CLUSTER_PCM_X_WRITER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_holder_retry_delay_ms(UINT32_C(99)), 32);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_OK, false, true),
				 CLUSTER_PCM_X_OWNER_EXIT_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_NOT_FOUND, true, true),
				 CLUSTER_PCM_X_OWNER_EXIT_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_BUSY, false, true),
				 CLUSTER_PCM_X_OWNER_EXIT_RETRY);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_GATE_RETRY, true, true),
				 CLUSTER_PCM_X_OWNER_EXIT_RETRY);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_BUSY, false, false),
				 CLUSTER_PCM_X_OWNER_EXIT_PRESERVE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_NOT_READY, true, true),
				 CLUSTER_PCM_X_OWNER_EXIT_PRESERVE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_CORRUPT, false, true),
				 CLUSTER_PCM_X_OWNER_EXIT_PRESERVE);

	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_retry_wait(", "\nstatic ",
							   wait_contract, lengthof(wait_contract));
	wait_begin = strstr(source, "\ncluster_bufmgr_pcm_x_holder_retry_wait(");
	wait_end = wait_begin == NULL ? NULL : strstr(wait_begin + 1, "\nstatic ");
	UT_ASSERT_NOT_NULL(wait_begin);
	UT_ASSERT_NOT_NULL(wait_end);
	UT_ASSERT(wait_begin == NULL || wait_end == NULL
			  || strstr(wait_begin, "ResetLatch(MyLatch)") == NULL
			  || strstr(wait_begin, "ResetLatch(MyLatch)") >= wait_end);
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   register_contract, lengthof(register_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_unregister(", "\nstatic ",
							   unregister_contract, lengthof(unregister_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_bufmgr_pcm_x_holder_drain_deferred_nowait();"));
	free(source);
}

UT_TEST(test_pending_grant_barrier_returns_to_owner_before_wait)
{
	static const char *const owner_return_contract[]
		= { "*return_to_owner = false",
			"owns_pending_grant && result == PCM_X_QUEUE_BARRIER_CLOSED",
			"*return_to_owner = true",
			"return NULL",
			"cluster_pcm_x_holder_register_retry_action(",
			"cluster_bufmgr_pcm_x_holder_retry_wait(" };
	static const char *const restart_contract[]
		= { "cluster_bufmgr_pcm_x_holder_prepare(",
			"*pending_set, &return_to_owner",
			"if (!return_to_owner)",
			"*pending_set = false",
			"*acquired = false",
			"cluster_bufmgr_pcm_retry_denied_rearm(",
			"CLUSTER_BUFMGR_PCM_RETRY_COVERED",
			"*covered = true",
			"cluster_pcm_lock_acquire_buffer(" };
	static const char *const lockbuffer_contract[]
		= { "pcm_x_holder = cluster_bufmgr_pcm_x_holder_admit_owned_grant(",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"pcm_x_holder = cluster_bufmgr_pcm_x_holder_admit_owned_grant(",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)" };
	char *source = read_bufmgr_source();
	uint64 first_token = 0;
	uint64 fresh_token = 0;

	/* The general holder policy remains WAIT for callers that do not own an
	 * in-flight grant, and for non-barrier retry results.  The bufmgr owner
	 * path below must intercept only its exact pending-grant/barrier pair. */
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(
					 PCM_X_QUEUE_BARRIER_CLOSED, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_GATE_RETRY, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);

	/* Exercise the real ownership object: returning to the exact owner clears
	 * only its live tuple, and the next reservation receives a fresh token. */
	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(
					 0, 0, PCM_OWN_FLAG_GRANT_PENDING, &first_token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(first_token, 1);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(
					 0, 0, first_token, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(0, first_token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(
					 0, 0, PCM_OWN_FLAG_GRANT_PENDING, &fresh_token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(fresh_token, first_token + 1);
	assert_entry(0, fresh_token, PCM_OWN_FLAG_GRANT_PENDING);

	/* Both holder-admission sites must return before the ordinary wait, exact
	 * abort/rearm outside content authority, preserve terminal cached cover,
	 * and issue a new acquire only for a genuinely rearmed reservation. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   owner_return_contract, lengthof(owner_return_contract));
	assert_ordered_in_function(source,
		"\ncluster_bufmgr_pcm_x_holder_admit_owned_grant(", "\nstatic ", restart_contract,
		lengthof(restart_contract));
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		lockbuffer_contract, lengthof(lockbuffer_contract));
	free(source);
}

UT_TEST(test_bufmgr_pcm_x_holder_reuse_and_deferred_failure_are_fail_closed)
{
	static const char *const exact_contract[]
		= { "entry == NULL || buf == NULL",
			"entry->buffer_id != buf->buf_id",
			"entry->content_lock != BufferDescriptorGetContentLock(buf)",
			"entry->handle.key.buffer_id != buf->buf_id",
			"!BufferTagsEqual(&entry->handle.key.identity.tag, &buf->tag)",
			"entry->handle.key.identity.node_id != cluster_node_id",
			"MyProc == NULL",
			"entry->handle.key.identity.procno != (uint32) MyProc->pgprocno",
			"entry->handle.key.identity.request_id == 0",
			"entry->handle.key.identity.wait_seq == 0",
			"cluster_epoch = cluster_epoch_get_current()",
			"runtime = cluster_pcm_x_runtime_snapshot()",
			"entry->handle.key.identity.cluster_epoch != cluster_epoch",
			"runtime.state != PCM_X_RUNTIME_ACTIVE",
			"runtime.master_session_incarnation == 0",
			"return true" };
	static const char *const prepare_contract[]
		= { "entry = cluster_bufmgr_pcm_x_holder_find(buf)",
			"if (entry != NULL)",
			"entry->phase != PCM_X_HOLDER_LEDGER_ACQUIRING",
			"!cluster_bufmgr_pcm_x_holder_entry_exact(entry, buf)",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)",
			"cluster_bufmgr_pcm_x_holder_report_failure",
			"LWLockHeldByMe(entry->content_lock)" };
	static const char *const deferred_contract[]
		= { "cluster_bufmgr_pcm_x_holder_retry_wait(",
			"wait_index++",
			"wait_index % CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS == 0",
			"runtime = cluster_pcm_x_runtime_snapshot()",
			"runtime.state != PCM_X_RUNTIME_ACTIVE",
			"runtime.master_session_incarnation == 0",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)",
			"cluster_bufmgr_pcm_x_holder_report_failure" };
	static const char *const nowait_contract[]
		= { "cluster_pcm_x_local_holder_exceptional_detach_exact", "PCM_X_QUEUE_GATE_RETRY",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)", "elog(LOG" };
	static const char *const abort_contract[]
		= { "cluster_pcm_x_local_holder_abort_acquiring_exact", "PCM_X_QUEUE_GATE_RETRY",
			"entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)", "elog(LOG" };
	static const char *const exception_contract[]
		= { "cluster_pcm_x_local_holder_exceptional_detach_exact", "PCM_X_QUEUE_GATE_RETRY",
			"entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)", "elog(LOG" };
	static const char *const unregister_contract[]
		= { "action != CLUSTER_PCM_X_HOLDER_RETRY_WAIT",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)",
			"cluster_bufmgr_pcm_x_holder_report_failure" };
	char *source = read_bufmgr_source();
	const char *entry_begin;
	const char *entry_end;

	/* A W1 fallback may reuse one ACQUIRING ledger entry, but buffer-id alone
	 * is not identity: descriptor retag, backend/epoch drift, or a stale lock
	 * pointer must close the runtime and retain the exact old handle. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_entry_exact(", "\nstatic ",
							   exact_contract, lengthof(exact_contract));
	entry_begin = strstr(source, "\ncluster_bufmgr_pcm_x_holder_entry_exact(");
	entry_end = entry_begin == NULL ? NULL : strstr(entry_begin + 1, "\nstatic ");
	UT_ASSERT_NOT_NULL(entry_begin);
	UT_ASSERT_NOT_NULL(entry_end);
	UT_ASSERT(entry_begin == NULL || entry_end == NULL
			  || strstr(entry_begin, "cluster_epoch == 0") == NULL
			  || strstr(entry_begin, "cluster_epoch == 0") >= entry_end);
	UT_ASSERT(entry_begin == NULL || entry_end == NULL
			  || strstr(entry_begin,
					"wait_seq != entry->handle.key.identity.request_id") == NULL
			  || strstr(entry_begin,
					"wait_seq != entry->handle.key.identity.request_id") >= entry_end);
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   prepare_contract, lengthof(prepare_contract));

	/* A live RETIRE gate can legitimately span one retry batch.  It cannot
	 * justify an infinite same-buffer wait after the formation runtime has
	 * left ACTIVE; all hard cleanup outcomes preserve exact evidence under a
	 * single explicit fail-closed transition. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_drain_deferred(", "\nstatic ",
							   deferred_contract, lengthof(deferred_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_drain_deferred_nowait(",
							   "\nstatic ", nowait_contract, lengthof(nowait_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_abort_acquiring(",
							   "\nstatic ", abort_contract, lengthof(abort_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_exception_cleanup_all(",
							   "\nstatic ", exception_contract, lengthof(exception_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_unregister(", "\nstatic ",
							   unregister_contract, lengthof(unregister_contract));
	free(source);
}

UT_TEST(test_queue_holder_snapshot_by_tag_is_mapping_and_header_exact)
{
	typedef ClusterPcmOwnResult (*SnapshotByTagFn)(const BufferTag *, int *,
												   ClusterPcmOwnSnapshot *);
	static const char *const snapshot_contract[]
		= { "BufTableHashCode", "LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",	"GetBufferDescriptor",
			"LockBufHdr",		"BufferTagsEqual",
			"BM_VALID",			"cluster_pcm_own_snapshot_locked",
			"UnlockBufHdr",		"LWLockRelease(partition_lock)" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_snapshot_by_tag),
										   SnapshotByTagFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_snapshot_by_tag(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_s_release_to_n(", snapshot_contract,
		lengthof(snapshot_contract));
	free(source);
}

UT_TEST(test_queue_passive_pinned_s_release_serializes_bytes_and_ownership)
{
	typedef ClusterPcmOwnResult (*PassiveReleaseFn)(const BufferTag *, XLogRecPtr *, uint64 *);
	static const char *const release_contract[]
		= { "BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"cluster_pcm_x_revoke_finish_mode(tag, shared_refcount)",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockRelease(partition_lock)",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_pcm_own_snapshot_matches_locked",
			"PageGetLSN",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"LockBufHdr",
			"cluster_pcm_own_snapshot_matches_locked",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"result = CLUSTER_PCM_OWN_BUSY",
			"cluster_pcm_own_bump_locked",
			"buf->pcm_state = (uint8) PCM_STATE_N",
			"buf->buffer_type = (uint8) BUF_TYPE_PI",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"BM_IO_ERROR",
			"cluster_bufmgr_unpin_for_gcs" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_release_pinned_s_for_gcs), PassiveReleaseFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_pinned_s_for_gcs(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_publish_installed_x_image(",
		release_contract, lengthof(release_contract));
	free(source);
}

UT_TEST(test_current_image_shape_accepts_monotone_xcur_after_x_to_s_yield)
{
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_SCUR, true));
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_XCUR, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_X, (uint8)BUF_TYPE_SCUR, true));
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_X, (uint8)BUF_TYPE_XCUR, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_PI, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_XCUR, false));
}

UT_TEST(test_conditional_lock_preserves_native_off_and_enforces_tracked_x)
{
	UT_ASSERT(cluster_pcm_x_conditional_lock_allowed(false, true, false, (uint8)PCM_STATE_N, 0,
										 0, 0));
	UT_ASSERT(cluster_pcm_x_conditional_lock_allowed(true, false, false, (uint8)PCM_STATE_N, 0,
										 0, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_N, 0,
										  0, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_S, 0,
										  0, 0));
	UT_ASSERT(cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0,
										 0, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0,
										  7, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0,
										  0, 41));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(false, false, true, (uint8)PCM_STATE_X, 0,
										  0, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(false, false, false, (uint8)PCM_STATE_X,
											  PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
}

UT_TEST(test_resource_x_ordinary_mutation_gate_dominates_dirty_hint_and_flush)
{
	static const char *const dirty_contract[]
		= { "LockBufHdr", "cluster_pcm_x_ordinary_mutation_allowed(", "UnlockBufHdr",
			"pg_atomic_read_u32(&bufHdr->state)" };
	static const char *const hint_contract[]
		= { "LockBufHdr", "cluster_pcm_x_ordinary_mutation_allowed(", "UnlockBufHdr",
			"XLogHintBitIsNeeded()" };
	static const char *const flush_contract[]
		= { "LockBufHdr", "cluster_pcm_x_flush_fence_consistent(", "UnlockBufHdr",
			"StartBufferIO(buf, false)" };
	char *source;

	UT_ASSERT(cluster_pcm_x_ordinary_mutation_allowed(false, true, false,
		(uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_ordinary_mutation_allowed(true, false, false,
		(uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_ordinary_mutation_allowed(true, true, false,
		(uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_ordinary_mutation_allowed(true, true, false,
		(uint8)PCM_STATE_X, 0, 12, 0));
	UT_ASSERT(!cluster_pcm_x_ordinary_mutation_allowed(true, true, false,
		(uint8)PCM_STATE_X, 0, 0, 41));
	UT_ASSERT(!cluster_pcm_x_ordinary_mutation_allowed(true, true, false,
		(uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_flush_fence_consistent(false, 12, 41));
	UT_ASSERT(!cluster_pcm_x_flush_fence_consistent(true, 12, 0));
	UT_ASSERT(cluster_pcm_x_flush_fence_consistent(true, 0, 0));

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\nMarkBufferDirty(", "\n/*\n * ReleaseAndReadBuffer",
								   dirty_contract, lengthof(dirty_contract));
	assert_ordered_in_function(source, "\nMarkBufferDirtyHint(",
								   "\n/*\n * Release buffer content locks",
								   hint_contract, lengthof(hint_contract));
	assert_ordered_in_function(source, "\nFlushBuffer(", "\n/*\n * RelationGetNumberOfBlocksInFork",
								   flush_contract, lengthof(flush_contract));
	free(source);
}

UT_TEST(test_resource_x_t2_t3_buffer_owner_is_generation_exact_and_ordered)
{
	static const char *const capture_contract[]
		= { "BufTableLookup", "cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_pcm_x_resource_x_t2_snapshot_exact",
			"cluster_gcs_block_compute_checksum((const char *)page)", "PageGetLSN(page)",
			"memcpy(page_bytes, page, BLCKSZ)", "LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	static const char *const t2_contract[]
		= { "BufTableLookup", "cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_gcs_block_compute_checksum(image->page_bytes)",
			"memcpy(page, image->page_bytes, BLCKSZ)", "PageSetLSN",
			"cluster_pcm_own_resource_x_activation_bind_exact(",
			"cluster_pcm_own_snapshot_locked", "LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	static const char *const t3_contract[]
		= { "BufTableLookup", "cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_pcm_x_resource_x_t3_snapshot_exact",
			"cluster_pcm_own_resource_x_activation_clear_exact(",
			"cluster_pcm_own_snapshot_locked", "LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	ResourceXAcquisitionRef ref;
	ClusterPcmOwnSnapshot live;
	char *source;

	memset(&ref, 0, sizeof(ref));
	ref.assertion.resource.spcOid = 1663;
	ref.assertion.resource.dbOid = 1;
	ref.assertion.resource.relNumber = 100;
	ref.assertion.resource.forkNum = MAIN_FORKNUM;
	ref.assertion.resource.blockNum = 71;
	ref.assertion.requester_node = 2;
	ref.formation = 17;
	ref.acquisition_generation = 41;
	memset(&live, 0, sizeof(live));
	live.tag = ref.assertion.resource;
	live.generation = 9;
	live.reservation_token = 12;
	live.writer_activation_token = 12;
	live.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT_EQ(sizeof(ResourceXCurrentImage), 32);
	UT_ASSERT(cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	live.resource_x_activation_generation = ref.acquisition_generation;
	UT_ASSERT(cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	UT_ASSERT(cluster_pcm_x_resource_x_t3_snapshot_exact(&ref, &live));
	live.resource_x_activation_generation++;
	UT_ASSERT(!cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_t3_snapshot_exact(&ref, &live));
	live.resource_x_activation_generation = ref.acquisition_generation;
	live.writer_activation_token = 0;
	UT_ASSERT(!cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_t3_snapshot_exact(&ref, &live));

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_capture_current_x_by_tag(",
							   "\nResourceXBufferActivationResult\n"
							   "cluster_bufmgr_pcm_own_activate_x_by_tag(",
							   capture_contract, lengthof(capture_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_activate_x_by_tag(",
							   "\nResourceXBufferActivationResult\n"
							   "cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(",
							   t2_contract, lengthof(t2_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_snapshot_by_tag(", t3_contract,
		lengthof(t3_contract));
	free(source);
}

UT_TEST(test_queue_passive_n_mirror_is_never_gcs_ship_authority)
{
	static const char *const probe_contract[]
		= { "LockBufHdr", "cluster_bufmgr_pcm_current_image_locked", "UnlockBufHdr" };
	static const char *const copy_contract[]
		= { "LockBufHdr",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_bufmgr_pcm_current_image_locked",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"memcpy(dst, page, BLCKSZ)" };
	static const char *const live_sge_contract[]
		= { "LockBufHdr",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_bufmgr_pcm_current_image_locked",
			"*out_page_addr = page" };
	static const char *const smart_contract[]
		= { "LockBufHdr",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_bufmgr_pcm_current_image_locked",
			"memcpy(dst, page, BLCKSZ)" };
	char *source = read_bufmgr_source();

	assert_ordered_in_function(source, "\ncluster_bufmgr_probe_block_for_gcs(",
							   "\n/*\n * Read the shared-storage version", probe_contract,
							   lengthof(probe_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_copy_block_for_gcs(",
							   "\n/*\n * Borrow a live shared_buffers page", copy_contract,
							   lengthof(copy_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_borrow_block_for_gcs_live_sge(",
							   "\nvoid\ncluster_bufmgr_release_block_for_gcs_live_sge(",
							   live_sge_contract, lengthof(live_sge_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_copy_block_for_gcs_smart_fusion(",
							   "\n/*\n * cluster_bufmgr_redeclare_scan_chunk", smart_contract,
							   lengthof(smart_contract));
	free(source);
}

UT_TEST(test_gcs_ship_copy_reports_exact_nonblocking_refusal_stage)
{
	static const char *const refusal_contract[]
		= { "CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_HC89_LSN_DRIFT" };
	char *source = read_bufmgr_source();
	const char *copy
		= source != NULL ? strstr(source, "\ncluster_bufmgr_copy_block_for_gcs(") : NULL;
	const char *copy_end
		= copy != NULL ? strstr(copy, "\n/*\n * Borrow a live shared_buffers page") : NULL;
	int i;

	/* P0-21 observation contract: every nonblocking false return that can
	 * become holder-side DENIED_MASTER_NOT_HOLDER identifies the precise
	 * residency/current-image/content-lock/HC89 refusal stage. */
	UT_ASSERT_NOT_NULL(copy);
	UT_ASSERT_NOT_NULL(copy_end);
	if (copy != NULL && copy_end != NULL) {
		for (i = 0; i < lengthof(refusal_contract); i++) {
			const char *hit = strstr(copy, refusal_contract[i]);

			UT_ASSERT_NOT_NULL(hit);
			if (hit != NULL)
				UT_ASSERT(hit < copy_end);
		}
	}
	free(source);
}

UT_TEST(test_queue_installed_image_publication_is_exact_and_content_locked)
{
	typedef ClusterPcmOwnResult (*PublishImageFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												  uint64);
	static const char *const publish_contract[]
		= { "LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"LockBufHdr",
			"BufferTagsEqual",
			"cluster_pcm_own_gen_get",
			"cluster_pcm_own_reservation_token_get",
			"PCM_OWN_FLAG_GRANT_PENDING",
			"buf->pcm_state != (uint8) PCM_STATE_N",
			"buf_state |= BM_VALID",
			"UnlockBufHdr" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_publish_installed_x_image), PublishImageFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_publish_installed_x_image(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_begin_grant_reservation(", publish_contract,
		lengthof(publish_contract));
	free(source);
}

UT_TEST(test_queue_self_source_handoff_is_single_lifecycle_and_readonly_drain)
{
	static const char *const handoff_contract[] = { "LWLockHeldByMe(content_lock)",
													"LockBufHdr",
													"cluster_pcm_own_classify_live_flags",
													"cluster_bufmgr_pcm_current_image_locked",
													"cluster_pcm_own_revoke_to_grant_handoff_exact",
													"UnlockBufHdr" };
	static const char *const drain_proof_contract[] = { "BufMappingPartitionLock",
														"BufTableLookup",
														"CLUSTER_PCM_OWN_OK",
														"LockBufHdr",
														"cluster_pcm_own_classify_live_flags",
														"UnlockBufHdr",
														"CLUSTER_PCM_OWN_CORRUPT" };
	char *source = read_bufmgr_source();
	const char *handoff;
	const char *handoff_end;
	const char *forbidden;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(", handoff_contract,
		lengthof(handoff_contract));
	handoff = strstr(source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(");
	handoff_end
		= handoff != NULL
			  ? strstr(handoff,
					   "\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(")
			  : NULL;
	UT_ASSERT_NOT_NULL(handoff);
	UT_ASSERT_NOT_NULL(handoff_end);
	if (handoff != NULL && handoff_end != NULL) {
		forbidden = strstr(handoff, "cluster_pcm_own_reservation_begin_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= handoff_end);
		forbidden = strstr(handoff, "cluster_pcm_own_reservation_abort_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= handoff_end);
	}
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_self_handoff_probe(",
							   "\n/* ==========", drain_proof_contract,
							   lengthof(drain_proof_contract));
	free(source);
}

UT_TEST(test_queue_writer_grant_snapshot_is_claim_and_generation_exact)
{
	PcmXLocalWriterClaim claim;
	ClusterPcmOwnSnapshot granted;
	ClusterPcmOwnSnapshot live;

	memset(&claim, 0, sizeof(claim));
	claim.writer.identity.base_own_generation = 10;
	claim.writer.membership_slot.slot_index = 3;
	claim.writer.membership_slot.slot_generation = 5;
	claim.writer.local_round = 7;
	claim.writer.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	claim.active_slot = claim.writer.membership_slot;
	claim.claim_generation = 11;
	claim.local_round = claim.writer.local_round;
	claim.role = claim.writer.role;
	memset(&granted, 0, sizeof(granted));
	granted.generation = 11;
	granted.reservation_token = 13;
	granted.writer_activation_token = granted.reservation_token;
	granted.pcm_state = (uint8)PCM_STATE_X;
	live = granted;

	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live.generation++;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	live.reservation_token++;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	live.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	/* A' rebase: a published effective grant base supersedes the enqueue-time
	 * identity base in the exact granted-generation binding. */
	claim.grant_base_own_generation = 14;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	granted.generation = 15;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.grant_base_own_generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.grant_base_own_generation = 0;
	granted.generation = 11;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.semantic_generation = 99;
	granted.writer_activation_token = 0;
	granted.resource_x_activation_generation = 0;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.semantic_generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.semantic_generation = 99;
	granted.writer_activation_token = granted.reservation_token;
	live = granted;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.semantic_generation = 0;
	granted.writer_activation_token = granted.reservation_token;
	live = granted;
	claim.active_slot.slot_generation++;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));

	UT_ASSERT(cluster_pcm_x_should_release_legacy_on_unlock(false, false));
	UT_ASSERT(!cluster_pcm_x_should_release_legacy_on_unlock(false, true));
	UT_ASSERT(!cluster_pcm_x_should_release_legacy_on_unlock(true, false));
	UT_ASSERT(!cluster_pcm_x_should_release_legacy_on_unlock(true, true));
	UT_ASSERT(cluster_pcm_x_legacy_holder_registration_required(false));
	UT_ASSERT(!cluster_pcm_x_legacy_holder_registration_required(true));

	UT_ASSERT(cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(false, true, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, false, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X,
												 PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X,
										 PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X, 0,
												 UINT64_C(91), 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X, 0,
												 UINT64_C(91), UINT64_C(22)));
}

UT_TEST(test_queue_writer_activation_fence_is_leader_owned_per_grant)
{
	static const char *const prepare_contract[]
		= { "entry->granted = granted",
			"entry->activation_fence_armed = granted.writer_activation_token != 0",
			"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING" };
	static const char *const activate_contract[]
		= { "entry == NULL || buf == NULL",
			"cluster_pcm_own_snapshot_locked(buf, &live)",
			"cluster_pcm_x_writer_grant_snapshot_exact(",
			"&entry->authority.source, &entry->granted, &live)",
			"entry->activation_fence_armed != (entry->granted.writer_activation_token != 0)",
			"cluster_pcm_own_writer_activation_token_get(buf->buf_id)",
			"!= entry->granted.writer_activation_token",
			"if (entry->activation_fence_armed)",
			"cluster_pcm_own_writer_activation_clear_exact(",
			"else",
			"result = CLUSTER_PCM_OWN_OK" };
	PcmXLocalWriterClaim leader;
	PcmXLocalWriterClaim follower;
	ClusterPcmOwnSnapshot granted;
	ClusterPcmOwnSnapshot live;
	char *source;
	uint64 committed_generation = 0;
	uint64 token = 0;

	/* A real commit publishes the one grant-to-content activation token. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_C(8));
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(
					 0, UINT64_C(8), PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_writer_grant_commit_exact(
					 0, UINT64_C(8), token, &committed_generation),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed_generation, UINT64_C(9));
	assert_writer_activation(token);

	memset(&leader, 0, sizeof(leader));
	leader.writer.identity.base_own_generation = UINT64_C(8);
	leader.writer.membership_slot.slot_index = 2;
	leader.writer.membership_slot.slot_generation = UINT64_C(631);
	leader.writer.local_round = 3;
	leader.writer.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	leader.active_slot = leader.writer.membership_slot;
	leader.claim_generation = 2;
	leader.local_round = leader.writer.local_round;
	leader.role = leader.writer.role;

	memset(&granted, 0, sizeof(granted));
	granted.generation = committed_generation;
	granted.reservation_token = token;
	granted.writer_activation_token = token;
	granted.pcm_state = (uint8)PCM_STATE_X;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&leader, &granted, &live));

	/* The leader consumes the shared fence once.  A later FIFO follower names
	 * the same node grant but observes the already-zero activation token. */
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(
					 0, committed_generation, token),
				 CLUSTER_PCM_OWN_OK);
	assert_writer_activation(0);
	follower = leader;
	follower.writer.membership_slot.slot_index = 3;
	follower.writer.membership_slot.slot_generation = UINT64_C(632);
	follower.writer.role = PCM_X_LOCAL_ROLE_FOLLOWER;
	follower.active_slot = follower.writer.membership_slot;
	follower.claim_generation = 3;
	follower.role = follower.writer.role;
	granted.writer_activation_token = 0;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&follower, &granted, &live));
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(
					 0, committed_generation, token),
				 CLUSTER_PCM_OWN_STALE);

	/* Role/token crossover and a torn live activation sample are not an exact
	 * writer grant, even when generation and reservation token still match. */
	granted.writer_activation_token = token;
	live = granted;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&follower, &granted, &live));
	granted.writer_activation_token = 0;
	live = granted;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&leader, &granted, &live));
	granted.writer_activation_token = token;
	live = granted;
	live.writer_activation_token = 0;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&leader, &granted, &live));

	/* Bufmgr derives local fence responsibility from the exact grant.  An
	 * unarmed follower revalidates and succeeds without a second clear. */
	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_prepare(", "\nstatic ",
							   prepare_contract, lengthof(prepare_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_activation_clear(",
							   "\nstatic bool\ncluster_bufmgr_pcm_x_writer_finish_claim_cleanup(",
							   activate_contract, lengthof(activate_contract));
	free(source);
}

UT_TEST(test_lockbuffer_pcm_x_writer_ledger_is_distinct_and_brackets_content_authority)
{
	static const char *const prepare_contract[]
		= { "entry->phase = PCM_X_WRITER_LEDGER_HANDOFF",
			"cluster_gcs_pcm_x_acquire_writer(",
			"buf, r4_generation, &entry->authority.source,",
			"&entry->claim_handed_off)",
			"entry->claim_handed_off",
			"cluster_bufmgr_pcm_own_snapshot(buf, &granted)",
			"cluster_pcm_x_writer_grant_snapshot_exact(",
			"&entry->authority.source, &granted, &granted)",
			"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING" };
	static const char *const unlock_contract[]
		= { "pcm_x_writer_managed = pcm_x_writer != NULL",
			"cluster_bufmgr_pcm_x_writer_mark_releasing(pcm_x_writer)",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_writer_release(pcm_x_writer)",
			"cluster_bufmgr_pcm_x_holder_unregister(pcm_x_holder)",
			"cluster_pcm_x_should_release_legacy_on_unlock(",
			"cluster_pcm_lock_unlock_content_buffer(buf, old_mode)" };
	static const char *const acquire_contract[]
		= { "pcm_covered = cluster_pcm_x_cached_cover_bypasses_queue(",
			"switch (pcm_writer_path)",
			"pcm_x_writer = cluster_bufmgr_pcm_x_writer_prepare_source(",
			"pcm_x_holder = cluster_bufmgr_pcm_x_holder_admit_owned_grant(",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"cluster_bufmgr_pcm_x_writer_activate(pcm_x_writer)" };
	static const char *const cleanup_contract[]
		= { "cluster_bufmgr_pcm_x_writer_exception_cleanup_all()",
			"cluster_bufmgr_pcm_x_holder_exception_cleanup_all()" };
	static const char *const owner_exit_contract[]
		= { "UnlockBuffers()", "cluster_bufmgr_pcm_x_owner_exit_drain()", "CheckForBufferLeaks()" };
	static const char *const owner_drain_contract[]
		= { "cluster_bufmgr_pcm_x_writer_owner_exit_drain_once(runtime_active)",
			"cluster_bufmgr_pcm_x_holder_owner_exit_drain_once(runtime_active)",
			"if (!writer_retry && !holder_retry)", "pg_usleep(1000L)" };
	static const char *const snapshot_failure_contract[] = {
		"release_result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(",
		"&entry->authority.source)",
		"entry->claim_cleanup_complete = true", "entry->phase = PCM_X_WRITER_LEDGER_DEFERRED",
		"cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf"
	};
	static const char *const writer_holder_publish_contract[]
		= { "cluster_pcm_x_local_writer_holder_register_exact(", "entry->handle = handle",
			"entry->phase = PCM_X_HOLDER_LEDGER_ACQUIRING",
			"committed_own_generation != writer_entry->granted.generation",
			"cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf" };
	static const char *const writer_holder_runtime_contract[]
		= { "runtime = cluster_pcm_x_runtime_snapshot()", "runtime.state != PCM_X_RUNTIME_ACTIVE",
			"if (writer_authorized)",
			"cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_NOT_READY, buf",
			"return NULL" };
	static const char *const deferred_release_contract[]
		= { "cluster_bufmgr_pcm_x_writer_entry_exact(entry, buf)",
			"entry->writer_path == RESOURCE_X_WRITER_TARGET",
			"cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(",
			"&entry->authority.source)" };
	static const char *const deferred_cleanup_contract[]
		= { "result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(",
			"&entry->authority.source)",
			"entry->claim_cleanup_complete = true",
			"cluster_bufmgr_pcm_x_writer_finish_claim_cleanup(entry" };
	static const char *const abort_cleanup_contract[]
		= { "entry->writer_path == RESOURCE_X_WRITER_TARGET",
			"result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(",
			"&entry->authority.source)",
			"entry->phase = PCM_X_WRITER_LEDGER_DEFERRED" };
	static const char *const exception_cleanup_contract[]
		= { "entry->writer_path == RESOURCE_X_WRITER_TARGET",
			"result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(",
			"&entry->authority.source)",
			"entry->phase = PCM_X_WRITER_LEDGER_DEFERRED" };
	static const char *const owner_exit_cleanup_contract[]
		= { "entry->writer_path == RESOURCE_X_WRITER_TARGET",
			"result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(",
			"&entry->authority.source)",
			"action = cluster_pcm_x_owner_exit_action(result, false, runtime_active)" };
	char *source = read_bufmgr_source();
	const char *writer_prepare;
	const char *writer_prepare_end;
	const char *protocol_external_wait;
	const char *claim_exact;
	const char *claim_exact_end;
	const char *grant_exact;
	const char *grant_exact_end;
	const char *owner_drain;
	const char *owner_drain_end;

	UT_ASSERT_NOT_NULL(
		strstr(source, "cluster_bufmgr_pcm_x_writer_ledger[LWLOCK_MAX_HELD_BY_PROC]"));
	UT_ASSERT_NOT_NULL(
		strstr(source, "cluster_bufmgr_pcm_x_holder_ledger[LWLOCK_MAX_HELD_BY_PROC]"));
	writer_prepare = strstr(source, "\ncluster_bufmgr_pcm_x_writer_prepare(");
	writer_prepare_end = writer_prepare != NULL ? strstr(writer_prepare, "\nstatic ") : NULL;
	protocol_external_wait = writer_prepare != NULL
								 ? strstr(writer_prepare, "cluster_bufmgr_pcm_x_holder_retry_wait(")
								 : NULL;
	UT_ASSERT_NOT_NULL(writer_prepare);
	UT_ASSERT_NOT_NULL(writer_prepare_end);
	UT_ASSERT(protocol_external_wait == NULL || protocol_external_wait >= writer_prepare_end);
	claim_exact = strstr(source, "\ncluster_bufmgr_pcm_x_writer_claim_entry_exact(");
	claim_exact_end = claim_exact != NULL ? strstr(claim_exact, "\n}\n") : NULL;
	grant_exact = strstr(source, "\ncluster_bufmgr_pcm_x_writer_entry_exact(");
	grant_exact_end = grant_exact != NULL ? strstr(grant_exact, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(claim_exact);
	UT_ASSERT_NOT_NULL(claim_exact_end);
	UT_ASSERT_NOT_NULL(grant_exact);
	UT_ASSERT_NOT_NULL(grant_exact_end);
	if (claim_exact != NULL && claim_exact_end != NULL)
		UT_ASSERT(strstr(claim_exact, "BufferTagsEqual(") == NULL
				  || strstr(claim_exact, "BufferTagsEqual(") >= claim_exact_end);
	if (grant_exact != NULL && grant_exact_end != NULL)
		UT_ASSERT(strstr(grant_exact, "BufferTagsEqual(") != NULL
				  && strstr(grant_exact, "BufferTagsEqual(") < grant_exact_end);
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_prepare(", "\nstatic ",
							   prepare_contract, lengthof(prepare_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_prepare(", "\nstatic ",
							   snapshot_failure_contract, lengthof(snapshot_failure_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_release(", "\nstatic ",
							   deferred_release_contract, lengthof(deferred_release_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_drain_deferred_nowait(",
							   "\nstatic ", deferred_cleanup_contract,
							   lengthof(deferred_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_abort_acquiring(",
							   "\nstatic ", abort_cleanup_contract,
							   lengthof(abort_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_exception_cleanup_all(",
							   "\nstatic ", exception_cleanup_contract,
							   lengthof(exception_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_owner_exit_drain_once(",
							   "\nstatic ", owner_exit_cleanup_contract,
							   lengthof(owner_exit_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   writer_holder_publish_contract,
							   lengthof(writer_holder_publish_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   writer_holder_runtime_contract,
							   lengthof(writer_holder_runtime_contract));
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		acquire_contract, lengthof(acquire_contract));
	assert_ordered_in_function(
		source, "\nLockBufferInternal(Buffer buffer, int mode",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		unlock_contract, lengthof(unlock_contract));
	assert_ordered_in_function(source, "\nUnlockBuffers(void)",
							   "\n/*\n * Acquire or release the content_lock for the buffer.",
							   cleanup_contract, lengthof(cleanup_contract));
	assert_ordered_in_function(source, "\nAtProcExit_Buffers(int code, Datum arg)",
							   "\n/*\n *\t\tCheckForBufferLeaks", owner_exit_contract,
							   lengthof(owner_exit_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_owner_exit_drain(", "\nstatic ",
							   owner_drain_contract, lengthof(owner_drain_contract));
	owner_drain = strstr(source, "\ncluster_bufmgr_pcm_x_owner_exit_drain(");
	owner_drain_end = owner_drain != NULL ? strstr(owner_drain, "\nstatic ") : NULL;
	UT_ASSERT_NOT_NULL(owner_drain);
	UT_ASSERT_NOT_NULL(owner_drain_end);
	if (owner_drain != NULL && owner_drain_end != NULL) {
		UT_ASSERT(strstr(owner_drain, "CHECK_FOR_INTERRUPTS") == NULL
				  || strstr(owner_drain, "CHECK_FOR_INTERRUPTS") >= owner_drain_end);
		UT_ASSERT(strstr(owner_drain, "WaitLatch(") == NULL
				  || strstr(owner_drain, "WaitLatch(") >= owner_drain_end);
		UT_ASSERT(strstr(owner_drain, "ereport(") == NULL
				  || strstr(owner_drain, "ereport(") >= owner_drain_end);
	}
	free(source);
}

/* review P1-4: deterministic execution of the preflight transient-BUSY arm
 * over the REAL ownership object.  A live GRANT_PENDING (then REVOKING)
 * lifecycle preflights BUSY and the requester site classifies WAIT -- never
 * fail-closed; once the lifecycle clears, the SAME identity's re-snapshot
 * preflights OK and the real grant reservation begins on the idle tuple.
 * Formation/session drift observed across the wait fails the exact runtime
 * proof, so the driver exits before touching the reservation. */
UT_TEST(test_preflight_busy_waits_then_clean_resnapshot_begins_reservation)
{
	ClusterPcmOwnSnapshot live;
	PcmXWaitIdentity identity;
	PcmXRuntimeSnapshot current;
	PcmXRuntimeSnapshot start;
	uint64 blocker_token = 0;
	uint64 token = 0;

	reset_fixture();
	memset(&identity, 0, sizeof(identity));
	identity.tag.relNumber = 20001;
	identity.tag.blockNum = 3;
	identity.base_own_generation = 0;
	memset(&live, 0, sizeof(live));
	live.tag = identity.tag;
	live.pcm_state = (uint8)PCM_STATE_N;

	/* A concurrent grant lifecycle holds GRANT_PENDING: BUSY -> WAIT. */
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &blocker_token),
		CLUSTER_PCM_OWN_OK);
	live.generation = pg_atomic_read_u64(&ClusterPcmOwnArray[0].generation);
	live.reservation_token = pg_atomic_read_u64(&ClusterPcmOwnArray[0].reservation_token);
	live.flags = pg_atomic_read_u32(&ClusterPcmOwnArray[0].flags);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_retry_action(
					 GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_BUSY),
				 GCS_BLOCK_PCM_X_RETRY_WAIT);
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_abort_exact(0, 0, blocker_token, PCM_OWN_FLAG_GRANT_PENDING),
		CLUSTER_PCM_OWN_OK);

	/* A live revoke lifecycle classifies exactly the same way. */
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_REVOKING, &blocker_token),
		CLUSTER_PCM_OWN_OK);
	live.reservation_token = pg_atomic_read_u64(&ClusterPcmOwnArray[0].reservation_token);
	live.flags = pg_atomic_read_u32(&ClusterPcmOwnArray[0].flags);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_BUSY);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_retry_action(
					 GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_BUSY),
				 GCS_BLOCK_PCM_X_RETRY_WAIT);
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_abort_exact(0, 0, blocker_token, PCM_OWN_FLAG_REVOKING),
		CLUSTER_PCM_OWN_OK);

	/* The wait ends: the clean-N re-snapshot (idle nonzero token is legal)
	 * preflights OK and the REAL reservation begins on the same tuple. */
	live.reservation_token = pg_atomic_read_u64(&ClusterPcmOwnArray[0].reservation_token);
	live.flags = pg_atomic_read_u32(&ClusterPcmOwnArray[0].flags);
	UT_ASSERT_EQ(live.flags, (uint32)0);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT(token > blocker_token);
	assert_entry(0, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* Formation/session drift across the wait refuses the re-entry. */
	memset(&start, 0, sizeof(start));
	start.state = PCM_X_RUNTIME_ACTIVE;
	start.gate_generation = 3;
	start.master_session_incarnation = 9;
	current = start;
	UT_ASSERT(cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current.gate_generation = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current = start;
	current.master_session_incarnation = 10;
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current = start;
	current.state = PCM_X_RUNTIME_RECOVERY_BLOCKED;
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
}

UT_TEST(test_own_lifecycle_counters_land_on_exact_begin_and_x_commit)
{
	static const char *const exact_begin_contract[]
		= { "cluster_pcm_own_reservation_begin_exact", "PCM_OWN_FLAG_GRANT_PENDING", "UnlockBufHdr",
			"result == CLUSTER_PCM_OWN_OK", "cluster_pcm_x_stats_note_own_begin();" };
	static const char *const handoff_begin_contract[]
		= { "flags == PCM_OWN_FLAG_GRANT_PENDING",
			"result = CLUSTER_PCM_OWN_OK",
			"cluster_pcm_own_revoke_to_grant_handoff_exact",
			"handoff_transitioned = result == CLUSTER_PCM_OWN_OK",
			"UnlockBufHdr",
			"if (handoff_transitioned)",
			"cluster_pcm_x_stats_note_own_begin();" };
	static const char *const direct_init_begin_contract[]
		= { "cluster_pcm_own_reservation_begin_exact", "PCM_OWN_FLAG_GRANT_PENDING", "UnlockBufHdr",
			"pending_result == CLUSTER_PCM_OWN_OK", "cluster_pcm_x_stats_note_own_begin();" };
	static const char *const x_commit_contract[]
		= { "cluster_pcm_own_grant_commit_exact", "UnlockBufHdr",
			"result == CLUSTER_PCM_OWN_OK && new_pcm_state == (uint8)PCM_STATE_X",
			"cluster_pcm_x_stats_note_own_commit();" };
	char *source = read_bufmgr_source();

	/* t/400 own-lifecycle counters: every counted begin sits after the header
	 * spinlock proved an exact GRANT_PENDING linearization (a fresh token, a
	 * REVOKING->GRANT_PENDING handoff, or a consumed direct-init proof), and
	 * a replayed/refused begin never counts.  The idempotent duplicate
	 * PREPARE that merely observes GRANT_PENDING must stay uncounted, so the
	 * handoff increments only behind its transition evidence. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_begin_grant_reservation(",
							   "\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   exact_begin_contract, lengthof(exact_begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(",
							   exact_begin_contract, lengthof(exact_begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(",
							   "\ncluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation(",
							   handoff_begin_contract, lengthof(handoff_begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_gate_direct_init(",
							   "#define BUF_DROP_FULL_SCAN_THRESHOLD", direct_init_begin_contract,
							   lengthof(direct_init_begin_contract));

	/* The X ownership commit counts exactly once, at the single exact-commit
	 * funnel shared by the queue finish, the LockBuffer finalize, and the
	 * direct-init grant; an S-grant finish and every refusal stay silent. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\ncluster_bufmgr_pcm_own_finish_x_commit(", x_commit_contract,
							   lengthof(x_commit_contract));

	/* Pin the production wiring against silent duplication or removal: four
	 * begin sites, one commit funnel, no counter call under LockBufHdr. */
	UT_ASSERT_EQ(count_occurrences(source, "cluster_pcm_x_stats_note_own_begin();"), 4);
	UT_ASSERT_EQ(count_occurrences(source, "cluster_pcm_x_stats_note_own_commit();"), 1);
	free(source);
}

UT_TEST(test_pcm_x_retain_flush_error_injection_is_exact_and_pre_write)
{
	static const char *const finish_flush_contract[]
		= { "cluster_injection_is_armed(\"cluster-pcm-x-retain-flush-error\")",
			"buf_state |= BM_DIRTY | BM_JUST_DIRTIED",
			"needs_flush =",
			"cluster_pcm_x_finish_retain_flush_active = true",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"cluster_pcm_x_finish_retain_flush_active = false",
			"cluster PCM-X retained-image finish FlushBuffer succeeded" };
	static const char *const flush_error_contract[]
		= { "if (!StartBufferIO(buf, false))",
			"cluster_pcm_x_finish_retain_flush_io_active = true",
			"cluster_pcm_x_finish_retain_flush_active",
			"cluster_pcm_own_flags_get(buf->buf_id) == PCM_OWN_FLAG_REVOKING",
			"CLUSTER_INJECTION_POINT(\"cluster-pcm-x-retain-flush-error\")",
			"cluster_injection_should_skip(\"cluster-pcm-x-retain-flush-error\")",
			"errmsg(\"injected PCM-X retained-image FlushBuffer failure\")",
			"smgrwrite(",
			"TerminateBufferIO(buf, true, 0)",
			"cluster_pcm_x_finish_retain_flush_io_active = false" };
	static const char *const catch_contract[]
		= { "PG_CATCH();",
			"cluster_pcm_x_finish_retain_flush_active = false",
			"if (cluster_pcm_x_finish_retain_flush_error_context_pushed)",
			"error_context_stack = cluster_pcm_x_finish_retain_flush_error_context_previous",
			"if (content_locked && LWLockHeldByMe(content_lock))",
			"HOLD_INTERRUPTS();",
			"LWLockRelease(content_lock)",
			"if (cluster_pcm_x_finish_retain_flush_io_active)",
			"cluster_pcm_x_finish_retain_flush_io_active = false",
			"AbortBufferIO(BufferDescriptorGetBuffer(buf))",
			"cluster_bufmgr_unpin_for_gcs(buf)",
			"PG_RE_THROW();" };
	char *source = read_bufmgr_source();
	const char *finish;
	const char *catch;
	const char *rethrow;
	const char *resume;

	/* The point is armed only at the finish-revoke-retain seam.  Test arming
	 * makes an otherwise already-flushed copy dirty without changing bytes,
	 * so the caller-pin/content-EXCLUSIVE FlushBuffer leg is deterministic.
	 * The generic flush path dispatches the point only while that exact call
	 * is active and the ownership token is still REVOKING.  ERROR clears the
	 * process interrupt holdoff count before longjmp, so the cleanup must
	 * restore FlushBuffer's stack-local error callback, release content
	 * authority with a replacement hold, abort the exact ResourceOwner-tracked
	 * BufferIO while its raw pin is still live, and only then unpin.  Otherwise
	 * an aux worker that absorbs the ERROR reaches commit-style resource-owner
	 * cleanup and PANICs with "lost track of buffer IO".  LWLockRelease consumes
	 * the replacement hold itself; a second RESUME would underflow in cassert
	 * builds and an unconditional HOLD would leak on the no-lock path. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
							   "\ncluster_bufmgr_pcm_own_release_retained_image(",
							   finish_flush_contract, lengthof(finish_flush_contract));
	assert_ordered_in_function(source, "\nFlushBuffer(", "\n/*\n * RelationGetNumberOfBlocksInFork",
							   flush_error_contract, lengthof(flush_error_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
							   "\ncluster_bufmgr_pcm_own_release_retained_image(", catch_contract,
							   lengthof(catch_contract));
	finish = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(");
	UT_ASSERT_NOT_NULL(finish);
	catch = strstr(finish, "PG_CATCH();");
	UT_ASSERT_NOT_NULL(catch);
	rethrow = strstr(catch, "PG_RE_THROW();");
	UT_ASSERT_NOT_NULL(rethrow);
	resume = strstr(catch, "RESUME_INTERRUPTS();");
	UT_ASSERT(resume == NULL || resume > rethrow);
	free(source);
}

UT_TEST(test_writer_activation_diagnostic_covers_commit_clear_and_unguarded_n_boundaries)
{
	static const char *const commit_contract[]
		= { "cluster_pcm_own_writer_grant_commit_exact(", "buf->pcm_state = new_pcm_state",
			"cluster_pcm_own_snapshot_locked(buf, &activation_diag)",
			"UnlockBufHdr(buf, buf_state)",
			"cluster_pcm_own_activation_diag_emit(\"writer-commit\"" };
	static const char *const clear_contract[]
		= { "cluster_pcm_own_snapshot_locked(buf, &live)",
			"cluster_pcm_own_writer_activation_clear_exact(", "UnlockBufHdr(buf, buf_state)",
			"cluster_pcm_own_activation_diag_emit(\"writer-activation-clear\"" };
	char *source = read_bufmgr_source();

	UT_ASSERT_EQ(sizeof(ClusterPcmOwnSnapshot), 64);
	UT_ASSERT_NOT_NULL(strstr(source, "out->writer_activation_token"));
	UT_ASSERT_NOT_NULL(strstr(source, "out->resource_x_activation_generation"));
	UT_ASSERT_NOT_NULL(strstr(source,
		"writer_activation_token = cluster_pcm_own_writer_activation_token_get"));
	UT_ASSERT_NOT_NULL(strstr(source,
		"resource_x_activation_generation = "
		"cluster_pcm_own_resource_x_activation_generation_get"));
	UT_ASSERT_NOT_NULL(strstr(source, "writer_activation_token == 0"));
	UT_ASSERT_NOT_NULL(strstr(source, "resource_x_activation_generation == 0"));
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   commit_contract, lengthof(commit_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_activation_clear(",
							   "\nstatic bool\ncluster_bufmgr_pcm_x_writer_finish_claim_cleanup(",
							   clear_contract, lengthof(clear_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "\"invalidate-stage-n-pi\""));
	UT_ASSERT_NOT_NULL(strstr(source, "\"invalidate-stage-n-drop\""));
	UT_ASSERT_NOT_NULL(strstr(source, "\"drop-no-wire-stage-n-pi\""));
	UT_ASSERT_NOT_NULL(strstr(source, "\"drop-no-wire-stage-n-drop\""));
	UT_ASSERT_NOT_NULL(strstr(source, "\"discard-pi-stage-n\""));
	free(source);
}

UT_TEST(test_resource_x_target_writer_context_is_post_t3_and_local_cleanup_only)
{
	char *source = read_bufmgr_source();
	const char *direct_init;
	const char *direct_init_validate;
	const char *direct_init_track;
	const char *direct_init_return;
	const char *tracker;
	const char *tracker_end;
	const char *target;
	const char *target_end;
	const char *activate;
	const char *activate_end;
	const char *release;
	const char *release_end;
	const char *abort;
	const char *abort_end;
	const char *exception;
	const char *exception_end;
	const char *owner_exit;
	const char *owner_exit_end;
	const char *recycle_arm;
	const char *recycle_arm_end;
	const char *recycle_unlock;
	const char *recycle_unlock_end;
	const char *recycle_relock;
	const char *recycle_relock_end;
	static const char *const target_contract[] = {
		"entry->writer_path = RESOURCE_X_WRITER_TARGET",
		"cluster_gcs_resource_x_target_acquire_exact(",
		"cluster_pcm_lock_resource_x_gate_snapshot(&gate)",
		"cluster_bufmgr_pcm_own_snapshot(buf, &granted)",
		"granted.writer_activation_token != 0",
		"granted.resource_x_activation_generation != 0",
		"cluster_gcs_resource_x_target_context_recheck_exact(&context)",
		"entry->authority.target = context",
		"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING"
	};
	static const char *const activate_contract[] = {
		"entry->writer_path == RESOURCE_X_WRITER_TARGET",
		"cluster_resource_x_writer_path_snapshot(&current_r4_generation)",
		"cluster_pcm_lock_resource_x_gate_snapshot(&gate)",
		"cluster_bufmgr_pcm_own_snapshot(buf, &live)",
		"live.writer_activation_token != 0",
		"live.resource_x_activation_generation != 0",
		"cluster_gcs_resource_x_target_context_recheck_exact(",
		"entry->phase = PCM_X_WRITER_LEDGER_ACTIVE"
	};
	static const char *const cleanup_forbidden[] = {
		"cluster_pcm_lock_release(",
		"cluster_pcm_lock_resource_x_release_x_exact(",
		"RESOURCE_X_WIRE_RELEASE_X"
	};
	size_t i;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	UT_ASSERT_NOT_NULL(strstr(source, "ResourceXWriterPath writer_path"));
	UT_ASSERT_NOT_NULL(strstr(source, "ResourceXWriterUseContext target"));
	UT_ASSERT_NOT_NULL(strstr(source, "union"));
	UT_ASSERT_EQ(sizeof(ResourceXWriterUseContext), 72);
	UT_ASSERT_NOT_NULL(strstr(source,
		"&& pcm_writer_path != RESOURCE_X_WRITER_TARGET"));
	UT_ASSERT_NOT_NULL(strstr(source,
		"if (writer_entry->writer_path == RESOURCE_X_WRITER_TARGET)"));
	direct_init = strstr(source, "\ncluster_bufmgr_pcm_gate_direct_init(");
	direct_init_validate = direct_init != NULL
		? strstr(direct_init,
			"cluster_pcm_direct_init_target_commit_validate(") : NULL;
	direct_init_track = direct_init_validate != NULL
		? strstr(direct_init_validate,
			"cluster_bufmgr_pcm_x_writer_track_target_direct_init(") : NULL;
	direct_init_return = direct_init_track != NULL
		? strstr(direct_init_track, "return;") : NULL;
	tracker = strstr(source,
		"\ncluster_bufmgr_pcm_x_writer_track_target_direct_init(");
	tracker_end = tracker != NULL ? strstr(tracker, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(direct_init);
	UT_ASSERT_NOT_NULL(direct_init_validate);
	UT_ASSERT_NOT_NULL(direct_init_track);
	UT_ASSERT_NOT_NULL(direct_init_return);
	UT_ASSERT_NOT_NULL(tracker);
	UT_ASSERT_NOT_NULL(tracker_end);
	if (direct_init_validate != NULL && direct_init_track != NULL
		&& direct_init_return != NULL) {
		UT_ASSERT(direct_init_validate < direct_init_track);
		UT_ASSERT(direct_init_track < direct_init_return);
	}
	if (tracker != NULL && tracker_end != NULL)
		assert_source_range_contains(tracker, tracker_end,
			"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING");
	UT_ASSERT_EQ(count_occurrences(source,
		"cluster_bufmgr_pcm_x_writer_activate_target_direct_init("), 4);

	target = strstr(source,
		"\ncluster_bufmgr_pcm_x_writer_prepare_target(");
	target_end = target != NULL ? strstr(target, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(target);
	UT_ASSERT_NOT_NULL(target_end);
	if (target != NULL && target_end != NULL) {
		const char *cursor = target;

		for (i = 0; i < lengthof(target_contract); i++) {
			cursor = strstr(cursor, target_contract[i]);
			UT_ASSERT_NOT_NULL(cursor);
			if (cursor == NULL)
				break;
			UT_ASSERT(cursor < target_end);
		}
		UT_ASSERT(strstr(target, "cluster_gcs_pcm_x_acquire_writer(") == NULL
			|| strstr(target, "cluster_gcs_pcm_x_acquire_writer(") >= target_end);
	}

	activate = strstr(source, "\ncluster_bufmgr_pcm_x_writer_activate(");
	activate_end = activate != NULL ? strstr(activate, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(activate);
	UT_ASSERT_NOT_NULL(activate_end);
	if (activate != NULL && activate_end != NULL) {
		const char *cursor = activate;

		for (i = 0; i < lengthof(activate_contract); i++) {
			cursor = strstr(cursor, activate_contract[i]);
			UT_ASSERT_NOT_NULL(cursor);
			if (cursor == NULL)
				break;
			UT_ASSERT(cursor < activate_end);
		}
	}

	release = strstr(source, "\ncluster_bufmgr_pcm_x_writer_release(");
	release_end = release != NULL ? strstr(release, "\n}\n") : NULL;
	abort = strstr(source, "\ncluster_bufmgr_pcm_x_writer_abort_acquiring(");
	abort_end = abort != NULL ? strstr(abort, "\n}\n") : NULL;
	exception = strstr(source,
		"\ncluster_bufmgr_pcm_x_writer_exception_cleanup_all(");
	exception_end = exception != NULL ? strstr(exception, "\n}\n") : NULL;
	owner_exit = strstr(source,
		"\ncluster_bufmgr_pcm_x_writer_owner_exit_drain_once(");
	owner_exit_end = owner_exit != NULL ? strstr(owner_exit, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(release_end);
	UT_ASSERT_NOT_NULL(abort_end);
	UT_ASSERT_NOT_NULL(exception_end);
	UT_ASSERT_NOT_NULL(owner_exit_end);
	if (release != NULL && release_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(release,
			"entry->writer_path == RESOURCE_X_WRITER_TARGET"));
	if (abort != NULL && abort_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(abort,
			"entry->writer_path == RESOURCE_X_WRITER_TARGET"));
	if (exception != NULL && exception_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(exception,
			"entry->writer_path == RESOURCE_X_WRITER_TARGET"));
	if (owner_exit != NULL && owner_exit_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(owner_exit,
			"entry->writer_path == RESOURCE_X_WRITER_TARGET"));
	recycle_arm = strstr(source, "\ncluster_bufmgr_itl_recycle_guard_arm(");
	recycle_arm_end = recycle_arm != NULL
		? strstr(recycle_arm,
			"\nvoid\ncluster_bufmgr_itl_recycle_guard_unlock(") : NULL;
	recycle_unlock = strstr(source, "\ncluster_bufmgr_itl_recycle_guard_unlock(");
	recycle_unlock_end = recycle_unlock != NULL
		? strstr(recycle_unlock,
			"\nbool\ncluster_bufmgr_itl_recycle_guard_relock(") : NULL;
	recycle_relock = strstr(source, "\ncluster_bufmgr_itl_recycle_guard_relock(");
	recycle_relock_end = recycle_relock != NULL
		? strstr(recycle_relock,
			"\nvoid\ncluster_bufmgr_itl_recycle_guard_cancel(") : NULL;
	UT_ASSERT_NOT_NULL(recycle_arm_end);
	UT_ASSERT_NOT_NULL(recycle_unlock_end);
	UT_ASSERT_NOT_NULL(recycle_relock_end);
	if (recycle_arm != NULL && recycle_arm_end != NULL) {
		assert_source_range_contains(recycle_arm, recycle_arm_end,
			"writer->writer_path != RESOURCE_X_WRITER_TARGET");
		assert_source_range_contains(recycle_arm, recycle_arm_end,
			"cluster_gcs_resource_x_target_itl_recycle_begin_exact(");
		assert_source_range_contains(recycle_arm, recycle_arm_end,
			"writer->phase = PCM_X_WRITER_LEDGER_RECYCLING");
	}
	if (recycle_unlock != NULL && recycle_unlock_end != NULL) {
		assert_source_range_contains(recycle_unlock, recycle_unlock_end,
			"PCM_X_WRITER_LEDGER_RECYCLING");
		UT_ASSERT_NOT_NULL(strstr(recycle_unlock,
			"content lock it cannot authorize page writes"));
		UT_ASSERT(strstr(recycle_unlock,
			"content lock it cannot authorize page writes")
			< strstr(recycle_unlock,
				"cluster_bufmgr_pcm_x_writer_release(writer)"));
	}
	if (recycle_relock != NULL && recycle_relock_end != NULL) {
		assert_source_range_contains(recycle_relock, recycle_relock_end,
			"cluster_gcs_resource_x_target_itl_recycle_finish_exact(");
		assert_source_range_contains(recycle_relock, recycle_relock_end,
			"writer->phase = PCM_X_WRITER_LEDGER_ACTIVE");
	}
	for (i = 0; i < lengthof(cleanup_forbidden); i++) {
		const char *site;

		site = strstr(target, cleanup_forbidden[i]);
		UT_ASSERT(site == NULL || site >= target_end);
	}
	free(source);
}

int
main(void)
{
	UT_PLAN(74);
	UT_RUN(test_shmem_initializes_complete_entry);
	UT_RUN(test_resource_x_activation_binding_is_exact_and_legacy_closed);
	UT_RUN(test_resource_x_reconfig_neutralize_is_generation_exact_and_nonblocking);
	UT_RUN(test_writer_activation_fence_blocks_revoke_until_exact_clear);
	UT_RUN(test_begin_abort_is_exact_and_monotonic);
	UT_RUN(test_invalid_live_flag_shapes_are_corrupt_not_busy);
	UT_RUN(test_grant_commit_is_exact_and_bumps_once);
	UT_RUN(test_s_revoke_handoff_reuses_exact_token_and_bumps_once);
	UT_RUN(test_revoke_handoff_kinds_cover_n_s_x_with_one_lifecycle);
	UT_RUN(test_s_new_fresh_token_finish_shape_stays_invalid);
	UT_RUN(test_parallel_s_cover_is_rechecked_before_legacy_token_mint);
	UT_RUN(test_share_cover_reverify_accepts_stable_successor_grant);
	UT_RUN(test_retained_release_retag_respects_pin_contract);
	UT_RUN(test_passive_retained_pi_is_an_n_assertion_candidate_only);
	UT_RUN(test_retained_release_and_finish_never_cover_invalid_bytes);
	UT_RUN(test_legacy_byte_proof_republishes_kept_pi_mirror);
	UT_RUN(test_revoke_commit_is_exact_and_classifies_live_races);
	UT_RUN(test_revoke_retain_commit_keeps_exact_token_until_release);
	UT_RUN(test_revoke_commit_exhaustion_is_side_effect_free);
	UT_RUN(test_token_and_generation_never_wrap);
	UT_RUN(test_ordinary_generation_bump_rejects_live_reservation);
	UT_RUN(test_eviction_rejects_live_reservation_and_exhaustion);
	UT_RUN(test_bufmgr_d5a_commitlocked_uses_locked_commit_and_saved_tag_release);
	UT_RUN(test_bufmgr_abort_cleanup_is_never_silent);
	UT_RUN(test_bufmgr_finish_failure_rolls_back_acquired_master_grant);
	UT_RUN(test_bufmgr_s_base_rollback_normalizes_to_n_under_header_authority);
	UT_RUN(test_lockbuffer_content_error_uses_post_master_rollback_contract);
	UT_RUN(test_bufmgr_generation_bump_failure_is_classified_under_header_lock);
	UT_RUN(test_lockbuffer_reservation_failures_use_busy_corrupt_classifier);
	UT_RUN(test_lockbuffer_reservation_busy_barrier_is_typed_before_failure_report);
	UT_RUN(test_stale_share_cover_holder_barrier_skips_content_lock_to_common_unwind);
	UT_RUN(test_pending_x_denied_retry_leaves_master_invalidate_gap);
	UT_RUN(test_bufmgr_finish_rejects_invalid_state_and_initializes_acquire_result);
	UT_RUN(test_bufmgr_finish_and_abort_gate_on_exact_base_state);
	UT_RUN(test_d5a_release_error_keeps_descriptor_out_of_freelist);
	UT_RUN(test_queue_begin_requires_normalized_n_snapshot);
	UT_RUN(test_queue_contract_exposes_prepare_only_begin_api);
	UT_RUN(test_queue_contract_exposes_opaque_retained_revoke_api);
	UT_RUN(test_queue_n_source_refresh_is_exact_and_publishes_only_complete_image);
	UT_RUN(test_queue_s_source_dirty_flush_makes_progress_and_reports_exact_refusal);
	UT_RUN(test_revoke_finish_mode_rejects_pinned_vm_fsm_and_retains_main);
	UT_RUN(test_pcm_tracking_excludes_only_fsm_for_user_and_shared_catalog_relations);
	UT_RUN(test_pcm_tracking_uses_one_tag_gate_for_acquire_direct_init_and_eviction);
	UT_RUN(test_queue_revoke_retains_main_but_drops_unpinned_vm_fsm);
	UT_RUN(test_retained_image_release_and_writeback_gates_are_exact);
	UT_RUN(test_retained_drain_retags_invalid_only_after_exact_token_release);
	UT_RUN(test_source_settlement_releases_fence_without_discarding_pi);
	UT_RUN(test_queue_s_release_finish_is_header_exact_and_returns_fresh_n);
	UT_RUN(test_resource_x_remote_s_finish_requires_content_and_exact_revoke);
	UT_RUN(test_lockbuffer_pcm_x_holder_ledger_brackets_both_content_acquires);
	UT_RUN(test_r11_lockbuffer_writer_selector_is_single_ingress_choice_and_exclusive);
	UT_RUN(test_bufmgr_pcm_x_holder_ledger_is_bounded_and_uses_private_identity);
	UT_RUN(test_unlockbuffers_exceptionally_detaches_released_pcm_x_holders);
	UT_RUN(test_bufmgr_pcm_x_holder_gate_retry_is_bounded_outside_content_lock);
	UT_RUN(test_pending_grant_barrier_returns_to_owner_before_wait);
	UT_RUN(test_bufmgr_pcm_x_holder_reuse_and_deferred_failure_are_fail_closed);
	UT_RUN(test_queue_holder_snapshot_by_tag_is_mapping_and_header_exact);
	UT_RUN(test_queue_passive_pinned_s_release_serializes_bytes_and_ownership);
	UT_RUN(test_current_image_shape_accepts_monotone_xcur_after_x_to_s_yield);
	UT_RUN(test_conditional_lock_preserves_native_off_and_enforces_tracked_x);
	UT_RUN(test_resource_x_ordinary_mutation_gate_dominates_dirty_hint_and_flush);
	UT_RUN(test_resource_x_t2_t3_buffer_owner_is_generation_exact_and_ordered);
	UT_RUN(test_queue_installed_image_publication_is_exact_and_content_locked);
	UT_RUN(test_queue_self_source_handoff_is_single_lifecycle_and_readonly_drain);
	UT_RUN(test_queue_passive_n_mirror_is_never_gcs_ship_authority);
	UT_RUN(test_gcs_ship_copy_reports_exact_nonblocking_refusal_stage);
	UT_RUN(test_queue_writer_grant_snapshot_is_claim_and_generation_exact);
	UT_RUN(test_queue_writer_activation_fence_is_leader_owned_per_grant);
	UT_RUN(test_lockbuffer_pcm_x_writer_ledger_is_distinct_and_brackets_content_authority);
	UT_RUN(test_preflight_busy_waits_then_clean_resnapshot_begins_reservation);
	UT_RUN(test_own_lifecycle_counters_land_on_exact_begin_and_x_commit);
	UT_RUN(test_pcm_x_retain_flush_error_injection_is_exact_and_pre_write);
	UT_RUN(test_writer_activation_diagnostic_covers_commit_clear_and_unguarded_n_boundaries);
	UT_RUN(test_resource_x_target_writer_context_is_post_t3_and_local_cleanup_only);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
