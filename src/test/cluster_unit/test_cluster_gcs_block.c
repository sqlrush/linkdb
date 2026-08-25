/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block.c
 *	  Compile-time invariants for the spec-2.33 GCS block-shipping
 *	  substrate (cluster_gcs_block.h wire ABI + math invariants).
 *
 *	  spec-2.33 ships cross-node 8KB block shipping on top of the spec-2.32
 *	  control plane.  This unit binary checks compile-time invariants
 *	  (struct sizes, field offsets, enum values, hash math) that are
 *	  verifiable from headers alone — no linking of cluster_gcs_block.o.
 *	  The R1 acquisition-observation group links cluster_pcm_x_convert.o
 *	  solely to exercise its real accounting and snapshot API.
 *	  Symbol linkability + behavioral coverage (XLogFlush invocation order,
 *	  checksum fail-closed, PageSetLSN install, HC89 single-retry
 *	  revalidation, sparse-topology end-to-end ship) lives in cluster_tap
 *	  t/111_gcs_block_ship_2node.pl which exercises a real PG instance.
 *
 *	  Tests in this binary (L1-L15):
 *	    L1  msg_type enum values (BLOCK_REQUEST=14, BLOCK_REPLY=15;
 *	         spec-2.32 GCS_REQUEST=12 / REPLY=13 preserved;
 *	         CSSD_HEARTBEAT=11 preserved)
 *	    L2  payload struct sizes + GCS_BLOCK_DATA_SIZE locked
 *	    L3  GcsBlockRequestPayload field offsets (64B layout — Sprint A
 *	         SA-F1 PG-fact:  natural 8-aligned struct size, reserved_0[19]
 *	         absorbs the trailing pad to 64B)
 *	    L4  GcsBlockReplyHeader field offsets (48B layout; spec-2.35
 *	         reuses 4 bytes of the original reserved budget for
 *	         forwarding_master_node_bytes)
 *	    L5  GcsBlockReplyStatus enum values through spec-2.35
 *	    L6  sparse node_id topology — hash mod-N over declared array
 *	         {0,2,5} only returns those three node_ids (HC81 math)
 *	    L7  deterministic hash determinism — same tag returns same master
 *	         on 32 repeated invocations
 *	    L8  LWTRANCHE_CLUSTER_GCS_BLOCK enum value distinct from GCS / PCM
 *	    L9  4 NEW wait events distinct from each other + GCS_REPLY_WAIT
 *	    L10 GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE math = header (48) + 8192
 *	    L11 reply key compound (backend_id, request_id) — HC80 layout
 *	    L12 reserved_0 padding bytes zero after memset(0)
 *	    L13 BLCKSZ == GCS_BLOCK_DATA_SIZE invariant (defensive cross-check
 *	         of the StaticAssertDecl in cluster_gcs_block.h)
 *	    L14 ClusterICMsgType enum extends without gap (spec-2.32 12/13
 *	         consecutive with spec-2.33 14/15)
 *	    L15 GcsBlockRequestPayload.tag is the standard PG BufferTag 20B
 *	         (defensive cross-check that PG-fact BufferTag size is unchanged)
 *
 *	  Sprint A finding (SA-F2):  spec §4.1 calls for 26 unit tests
 *	  (L1-L26).  Many of L18-L26 are behavioral (HC82 XLogFlush invocation
 *	  order via test hook, HC83 checksum fail-closed buffer no-pollute,
 *	  HC84 PageSetLSN install on GRANTED, HC88 master-not-holder paths,
 *	  HC89 single-retry revalidation) which require a live backend +
 *	  cluster_gcs_block_test_xlog_flush_hook + lsn_drift_hook execution.
 *	  Those move to cluster_tap t/111;  this unit binary keeps 15 tests
 *	  covering compile-time invariants.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <setjmp.h>

#include "cluster/cluster_cssd.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_pcm_x_convert.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_thread_recovery.h"
#include "common/hashfn.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* Backend dependencies of cluster_pcm_x_convert.o are unreachable from the
 * observation-only test below.  Keep their test stubs fail-fast so this unit
 * cannot silently expand into a second PCM-X behavioral harness. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
volatile uint32 CritSectionCount = 0;
int MaxBackends = 4;
int NBuffers = 1;
int cluster_lmd_max_wait_edges = 1;
int cluster_node_id = 0;

static BufferDescPadded observation_buffer_descriptors[1];
BufferDescPadded *BufferDescriptors = observation_buffer_descriptors;

static PROC_HDR observation_proc_global;
PROC_HDR *ProcGlobal = &observation_proc_global;

bool
cluster_pcm_lock_resource_x_cutover_proofs_exact(
	ResourceXReconfigToken *token_out pg_attribute_unused(),
	ResourceXZeroResidualProof *zero_proof_out pg_attribute_unused(),
	ResourceXCleanCompletionProof *clean_proof_out pg_attribute_unused())
{
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
	ResourceXReconfigToken *token_out pg_attribute_unused(),
	ResourceXZeroResidualProof *zero_proof_out pg_attribute_unused(),
	ResourceXCleanCompletionProof *clean_proof_out pg_attribute_unused())
{
	return false;
}

void
pg_re_throw(void)
{
	abort();
}

void
FlushErrorState(void)
{}

Size
add_size(Size left, Size right)
{
	if (left > SIZE_MAX - right)
		abort();
	return left + right;
}

Size
mul_size(Size left, Size right)
{
	if (left != 0 && right > SIZE_MAX / left)
		abort();
	return left * right;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *found_ptr pg_attribute_unused())
{
	abort();
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	abort();
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	abort();
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	abort();
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{
	abort();
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	abort();
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	abort();
}

bool
LWLockHeldByMe(LWLock *lock pg_attribute_unused())
{
	abort();
}

bool
LWLockHeldByMeInMode(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	abort();
}

bool
LWLockAnyHeldByMe(LWLock *lock pg_attribute_unused(), int nlocks pg_attribute_unused(),
				  size_t stride pg_attribute_unused())
{
	abort();
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *) pg_attribute_unused(),
					  void *context pg_attribute_unused())
{
	abort();
}


static char *
read_source_path(const char *path)
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
read_gcs_block_source(void)
{
	return read_source_path(GCS_BLOCK_SOURCE_PATH);
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


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


UT_TEST(test_gcs_block_msg_type_enum_values_no_collision)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST, 14);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REPLY, 15);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REQUEST, 12);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REPLY, 13);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_CSSD_HEARTBEAT, 11);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_CF_BLOCK_SHIP, 6);
}


UT_TEST(test_gcs_block_payload_sizes_locked)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockRequestPayload), 64);
	UT_ASSERT_EQ((int)sizeof(GcsBlockReplyHeader), 48);
	UT_ASSERT_EQ((int)GCS_BLOCK_DATA_SIZE, 8192);
}


UT_TEST(test_gcs_block_request_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, epoch), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, tag), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, sender_node), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, requester_backend_id), 40);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, transition_id), 44);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, reserved_0), 45);
}


UT_TEST(test_gcs_block_reply_header_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, page_lsn), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, epoch), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, checksum), 24);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, sender_node), 28);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, requester_backend_id), 32);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, transition_id), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, status), 37);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, forwarding_master_node_bytes), 38);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, reserved_0), 42);
}


UT_TEST(test_gcs_block_reply_status_enum_values_through_spec_2_35)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED, 0);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK, 1);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE, 2);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, 3);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_EPOCH_STALE, 4);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL, 5);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, 6);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_DEDUP_FULL, 7);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, 8);
}


UT_TEST(test_gcs_block_sparse_hash_mod_n_distribution)
{
	const int declared[3] = { 0, 2, 5 };
	const int declared_count = 3;
	int i;

	for (i = 0; i < 100; i++) {
		BufferTag tag = { 0 };
		uint32 h;
		int master;

		tag.spcOid = (Oid)(i * 13 + 1);
		tag.dbOid = (Oid)(i * 17 + 1);
		tag.blockNum = (BlockNumber)(i * 19);

		h = hash_bytes((const unsigned char *)&tag, sizeof(tag));
		master = declared[h % (uint32)declared_count];

		UT_ASSERT(master == 0 || master == 2 || master == 5);
	}
}


UT_TEST(test_gcs_block_hash_deterministic_same_tag_same_master)
{
	const int declared[4] = { 0, 1, 2, 3 };
	const int declared_count = 4;
	BufferTag tag = { 0 };
	uint32 h1;
	int m1;
	int i;

	tag.spcOid = (Oid)0x12345;
	tag.dbOid = (Oid)0x6789a;
	tag.blockNum = (BlockNumber)42;

	h1 = hash_bytes((const unsigned char *)&tag, sizeof(tag));
	m1 = declared[h1 % (uint32)declared_count];
	for (i = 0; i < 32; i++) {
		uint32 h2 = hash_bytes((const unsigned char *)&tag, sizeof(tag));
		int m2 = declared[h2 % (uint32)declared_count];

		UT_ASSERT_EQ(m1, m2);
	}
}


UT_TEST(test_gcs_block_lwlock_tranche_distinct)
{
	UT_ASSERT((int)LWTRANCHE_CLUSTER_GCS_BLOCK != (int)LWTRANCHE_CLUSTER_GCS);
	UT_ASSERT((int)LWTRANCHE_CLUSTER_GCS_BLOCK != (int)LWTRANCHE_CLUSTER_PCM);
}


UT_TEST(test_gcs_block_wait_events_distinct)
{
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_SHIP_WAIT != (int)WAIT_EVENT_GCS_REPLY_WAIT);
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_REQUEST_DISPATCH != (int)WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_REPLY_DISPATCH
			  != (int)WAIT_EVENT_GCS_BLOCK_REQUEST_DISPATCH);
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_CHECKSUM_FAIL != (int)WAIT_EVENT_GCS_BLOCK_REPLY_DISPATCH);
}


UT_TEST(test_gcs_block_reply_total_size_is_8240)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE, 8240);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE,
				 (int)(sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE));
}


UT_TEST(test_pcm_x_session_auth_sample_classifies_epoch_zero_and_torn_reads)
{
	ClusterGcsPcmXAuthSample sample;

	memset(&sample, 0, sizeof(sample));
	sample.connection_before_valid = true;
	sample.connection_after_valid = true;
	sample.slot_before_valid = true;
	sample.slot_after_valid = true;
	sample.fresh_before = true;
	sample.fresh_after = true;
	sample.session_before = 41;
	sample.session_after = 41;
	sample.slot_generation_before = 7;
	sample.slot_generation_after = 7;
	/* Both INITIAL epoch 0 and the registered RDMA connection generation 0
	 * are live values, not empty sentinels. */
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0), PCM_X_SESSION_AUTH_OK);

	sample.slot_generation_after++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0), PCM_X_SESSION_AUTH_SLOT_TORN);
	sample.slot_generation_after = sample.slot_generation_before;
	sample.fresh_after = false;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0),
				 PCM_X_SESSION_AUTH_FRESH_NOT_READY);
	sample.fresh_after = true;
	sample.connection_generation_after = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0),
				 PCM_X_SESSION_AUTH_CONNECTION_TORN);
}


/*
 * Exercise the real PCM-X acquisition-accounting object from this GCS unit.
 * The production wrapper's preflight, ordinary-return, and error paths are
 * covered by cluster_tap t/418; this test only pins the accounting API and
 * snapshot contract without introducing a wrapper double or test seam.
 */
UT_TEST(test_pcm_x_acquire_observation_real_api_counts_exactly_once)
{
	PcmXShmemHeader header;
	PcmXAcquireObservationSnapshot baseline;
	PcmXAcquireObservationSnapshot after_begin;
	PcmXAcquireObservationSnapshot after_success;
	PcmXAcquireObservationSnapshot after_non_success;
	PcmXAcquireObservationSnapshot after_exception;
	PcmXAcquireObservationSnapshot after_duplicate_close;
	int i;

	UT_ASSERT_EQ(PCM_X_QUEUE_RESULT_COUNT, 14);
	UT_ASSERT_EQ(PCM_X_ACQUIRE_HIST_BUCKETS, 32);
	memset(&header, 0, sizeof(header));
	pg_atomic_init_u64(&header.acquire_observation.started_count, 0);
	pg_atomic_init_u64(&header.acquire_observation.active_count, 0);
	pg_atomic_init_u64(&header.acquire_observation.exception_count, 0);
	for (i = 0; i < PCM_X_QUEUE_RESULT_COUNT; i++)
		pg_atomic_init_u64(&header.acquire_observation.terminal_result_count[i], 0);
	for (i = 0; i < PCM_X_ACQUIRE_HIST_BUCKETS; i++)
		pg_atomic_init_u64(&header.acquire_observation.success_latency_bucket[i], 0);
	pg_atomic_init_u64(&header.acquire_observation.success_latency_overflow_count, 0);
	ClusterPcmXConvertShmem = &header;

	cluster_pcm_x_acquire_observation_snapshot(&baseline);
	UT_ASSERT_EQ(baseline.started_count, 0);
	UT_ASSERT_EQ(baseline.active_count, 0);
	UT_ASSERT_EQ(baseline.exception_count, 0);

	cluster_pcm_x_acquire_observation_begin(UINT64_C(100));
	cluster_pcm_x_acquire_observation_snapshot(&after_begin);
	UT_ASSERT_EQ(after_begin.started_count, 1);
	UT_ASSERT_EQ(after_begin.active_count, 1);
	UT_ASSERT_EQ(after_begin.exception_count, 0);
	cluster_pcm_x_acquire_observation_finish(PCM_X_QUEUE_OK, UINT64_C(101));
	cluster_pcm_x_acquire_observation_snapshot(&after_success);
	UT_ASSERT_EQ(after_success.started_count, 1);
	UT_ASSERT_EQ(after_success.active_count, 0);
	UT_ASSERT_EQ(after_success.exception_count, 0);
	for (i = 0; i < PCM_X_QUEUE_RESULT_COUNT; i++)
		UT_ASSERT_EQ(after_success.terminal_result_count[i], i == PCM_X_QUEUE_OK ? 1 : 0);
	for (i = 0; i < PCM_X_ACQUIRE_HIST_BUCKETS; i++)
		UT_ASSERT_EQ(after_success.success_latency_bucket[i], i == 0 ? 1 : 0);
	UT_ASSERT_EQ(after_success.success_latency_overflow_count, 0);

	cluster_pcm_x_acquire_observation_begin(UINT64_C(200));
	cluster_pcm_x_acquire_observation_finish(PCM_X_QUEUE_BUSY, UINT64_C(250));
	cluster_pcm_x_acquire_observation_snapshot(&after_non_success);
	UT_ASSERT_EQ(after_non_success.started_count, 2);
	UT_ASSERT_EQ(after_non_success.active_count, 0);
	UT_ASSERT_EQ(after_non_success.exception_count, 0);
	for (i = 0; i < PCM_X_QUEUE_RESULT_COUNT; i++)
		UT_ASSERT_EQ(after_non_success.terminal_result_count[i],
					 i == PCM_X_QUEUE_OK || i == PCM_X_QUEUE_BUSY ? 1 : 0);
	for (i = 0; i < PCM_X_ACQUIRE_HIST_BUCKETS; i++)
		UT_ASSERT_EQ(after_non_success.success_latency_bucket[i],
					 after_success.success_latency_bucket[i]);
	UT_ASSERT_EQ(after_non_success.success_latency_overflow_count,
				 after_success.success_latency_overflow_count);

	cluster_pcm_x_acquire_observation_begin(UINT64_C(300));
	cluster_pcm_x_acquire_observation_exception();
	cluster_pcm_x_acquire_observation_snapshot(&after_exception);
	UT_ASSERT_EQ(after_exception.started_count, 3);
	UT_ASSERT_EQ(after_exception.active_count, 0);
	UT_ASSERT_EQ(after_exception.exception_count, 1);
	for (i = 0; i < PCM_X_QUEUE_RESULT_COUNT; i++)
		UT_ASSERT_EQ(after_exception.terminal_result_count[i],
					 after_non_success.terminal_result_count[i]);
	for (i = 0; i < PCM_X_ACQUIRE_HIST_BUCKETS; i++)
		UT_ASSERT_EQ(after_exception.success_latency_bucket[i],
					 after_non_success.success_latency_bucket[i]);
	UT_ASSERT_EQ(after_exception.success_latency_overflow_count,
				 after_non_success.success_latency_overflow_count);

	cluster_pcm_x_acquire_observation_finish(PCM_X_QUEUE_OK, UINT64_C(400));
	cluster_pcm_x_acquire_observation_snapshot(&after_duplicate_close);
	UT_ASSERT_EQ(after_duplicate_close.started_count, after_exception.started_count);
	UT_ASSERT_EQ(after_duplicate_close.active_count, after_exception.active_count);
	UT_ASSERT_EQ(after_duplicate_close.exception_count, after_exception.exception_count);
	for (i = 0; i < PCM_X_QUEUE_RESULT_COUNT; i++)
		UT_ASSERT_EQ(after_duplicate_close.terminal_result_count[i],
					 after_exception.terminal_result_count[i]);
	for (i = 0; i < PCM_X_ACQUIRE_HIST_BUCKETS; i++)
		UT_ASSERT_EQ(after_duplicate_close.success_latency_bucket[i],
					 after_exception.success_latency_bucket[i]);
	UT_ASSERT_EQ(after_duplicate_close.success_latency_overflow_count,
				 after_exception.success_latency_overflow_count);

	ClusterPcmXConvertShmem = NULL;
}


UT_TEST(test_gcs_block_reply_key_is_compound)
{
	GcsBlockReplyHeader hdr;

	memset(&hdr, 0xab, sizeof(hdr));
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, requester_backend_id), 32);
	UT_ASSERT_EQ((int)sizeof(hdr.request_id), 8);
	UT_ASSERT_EQ((int)sizeof(hdr.requester_backend_id), 4);
}


UT_TEST(test_gcs_block_reserved_padding_present)
{
	GcsBlockRequestPayload req;
	GcsBlockReplyHeader rep;

	UT_ASSERT_EQ((int)sizeof(req.reserved_0), 19);
	UT_ASSERT_EQ((int)sizeof(rep.forwarding_master_node_bytes), 4);
	UT_ASSERT_EQ((int)sizeof(rep.reserved_0), 6);
	memset(&req, 0, sizeof(req));
	memset(&rep, 0, sizeof(rep));
	UT_ASSERT_EQ((int)req.reserved_0[0], 0);
	UT_ASSERT_EQ((int)req.reserved_0[18], 0);
	UT_ASSERT_EQ((int)rep.forwarding_master_node_bytes[0], 0);
	UT_ASSERT_EQ((int)rep.forwarding_master_node_bytes[3], 0);
	UT_ASSERT_EQ((int)rep.reserved_0[0], 0);
	UT_ASSERT_EQ((int)rep.reserved_0[5], 0);
}


UT_TEST(test_gcs_block_data_size_equals_blcksz)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_DATA_SIZE, (int)BLCKSZ);
}


UT_TEST(test_gcs_block_msg_type_enum_extends_without_gap)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REPLY + 1, (int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST + 1, (int)PGRAC_IC_MSG_GCS_BLOCK_REPLY);
}


UT_TEST(test_gcs_block_tag_is_standard_buffer_tag_20b)
{
	GcsBlockRequestPayload req;

	/* BufferTag in PG 16 is 5×uint32 = 20B (spec-2.30 v0.2 F1 PG-fact). */
	UT_ASSERT_EQ((int)sizeof(req.tag), 20);
	UT_ASSERT_EQ((int)sizeof(BufferTag), 20);
}


/* spec-5.2 D2 (U3): pure master-side decision for an X-held N→S read.
 * node0 = holder/master in DIRECT, node1 = requester. */
UT_TEST(test_xheld_read_ship_decision_truth_table)
{
	/* N→S read, block held X, master(0) is the resident holder → direct ship. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													0 /*holder*/, 1 /*requester*/, 0 /*master*/,
													true /*resident*/),
				 GCS_XHELD_READ_DIRECT_FROM_MASTER);

	/* Holder(0) is a different node from the master(1) → forward to holder. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													0 /*holder*/, 1 /*requester*/,
													1 /*master==requester*/, false),
				 GCS_XHELD_READ_FORWARD_TO_HOLDER);

	/* Master is the recorded holder but the buffer is NOT resident (evict
	 * race) → fail-closed (never a silent stale read). */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													0, 1, 0, false /*not resident*/),
				 GCS_XHELD_READ_DENY);

	/* Holder == requester (read-ship to self) → deny. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													1 /*holder==requester*/, 1, 0, true),
				 GCS_XHELD_READ_DENY);

	/* No valid holder → deny. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													-1 /*no holder*/, 1, 0, true),
				 GCS_XHELD_READ_DENY);

	/* Block held S (not X) → not applicable; the existing 2-way share path
	 * handles it. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_S,
													0, 1, 0, true),
				 GCS_XHELD_READ_NOT_APPLICABLE);

	/* A write request (N→X) on an X-held block is never a read-image case. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_X, (int)PCM_LOCK_MODE_X,
													0, 1, 0, true),
				 GCS_XHELD_READ_NOT_APPLICABLE);
}


/* A native Resource-X head blocks durable N->S admission, but an exact,
 * authority-stable current-X holder may still supply a one-shot read image.
 * Any authority or identity drift must keep the barrier fail closed. */
UT_TEST(test_xheld_read_image_bypasses_only_exact_stable_s_barrier)
{
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;

	memset(&before, 0, sizeof(before));
	before.master_holder.node_id = 1;
	before.master_holder.procno = 17;
	before.master_holder.cluster_epoch = UINT64_C(41);
	before.master_holder.request_id = UINT64_C(73);
	before.transition_count = UINT64_C(9);
	before.state = PCM_STATE_X;
	before.x_holder_node = 1;
	before.pending_x_requester_node = -1;
	after = before;

	UT_ASSERT(gcs_block_xheld_read_barrier_bypass_exact(&before, &after,
		2 /* requester */));
	UT_ASSERT_EQ(gcs_block_s_barrier_read_action_exact(
		false, false, false, false, &before, true, &after, true, 2),
		GCS_BLOCK_S_BARRIER_NONE);
	UT_ASSERT_EQ(gcs_block_s_barrier_read_action_exact(
		true, false, true, true, &before, true, &after, true, 2),
		GCS_BLOCK_S_BARRIER_DENY);
	UT_ASSERT_EQ(gcs_block_s_barrier_read_action_exact(
		false, false, true, true, &before, true, &after, true, 2),
		GCS_BLOCK_S_BARRIER_IMAGE_ONLY);
	UT_ASSERT_EQ(gcs_block_s_barrier_read_action_exact(
		false, false, true, true, &before, false, &after, true, 2),
		GCS_BLOCK_S_BARRIER_DENY);

	after.transition_count++;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &after, 2));
	UT_ASSERT_EQ(gcs_block_s_barrier_read_action_exact(
		false, false, true, true, &before, true, &after, true, 2),
		GCS_BLOCK_S_BARRIER_DENY);
	after = before;
	after.master_holder.request_id++;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &after, 2));
	after = before;
	after.pending_x_requester_node = 3;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &after, 2));
	after = before;
	after.s_holders_bitmap = UINT32_C(1) << 3;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &after, 2));

	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &before,
		1 /* requester is holder */));
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &before,
		PCM_X_PROTOCOL_NODE_LIMIT));

	after = before;
	after.state = PCM_STATE_S;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&before, &after, 2));
	after = before;
	after.x_holder_node = -1;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&after, &after, 2));
	after = before;
	after.master_holder.node_id = 0;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&after, &after, 2));
	after = before;
	after.transition_count = 0;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&after, &after, 2));
	after.transition_count = UINT64_MAX;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&after, &after, 2));
	after = before;
	after.reserved[0] = 1;
	UT_ASSERT(!gcs_block_xheld_read_barrier_bypass_exact(&after, &after, 2));
}


/* A forwarded-in-flight dedup marker is only a routing cache.  Once the
 * canonical holder changes, replay must return to the master decision tree;
 * otherwise the old holder is retried forever after an exact X handoff. */
UT_TEST(test_forward_replay_requires_current_exact_authority)
{
	PcmAuthoritySnapshot authority;

	memset(&authority, 0, sizeof(authority));
	authority.state = PCM_STATE_X;
	authority.x_holder_node = 0;
	authority.master_holder.node_id = 0;
	authority.pending_x_requester_node = -1;

	UT_ASSERT(gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, (uint8)PCM_TRANS_N_TO_S,
		0 /* cached holder */, 2 /* requester */, &authority));

	/* Resource-X committed a new current X holder while this request was in
	 * flight.  Replaying the old holder marker must now fail closed. */
	authority.x_holder_node = 1;
	authority.master_holder.node_id = 1;
	UT_ASSERT(!gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, (uint8)PCM_TRANS_N_TO_S,
		0 /* stale cached holder */, 2, &authority));

	/* An ordered X round also invalidates an older read-image route before the
	 * holder switch commits. */
	authority.x_holder_node = 0;
	authority.master_holder.node_id = 0;
	authority.pending_x_requester_node = 1;
	authority.pending_x_since_lsn = 9;
	UT_ASSERT(!gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, (uint8)PCM_TRANS_N_TO_S,
		0, 2, &authority));

	/* A normal S forward remains reusable only for the exact canonical S
	 * holder and with no newer writer barrier. */
	authority.state = PCM_STATE_S;
	authority.x_holder_node = -1;
	authority.s_holders_bitmap = UINT32_C(1) << 0;
	authority.master_holder.node_id = 0;
	authority.pending_x_requester_node = -1;
	authority.pending_x_since_lsn = 0;
	UT_ASSERT(gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, (uint8)PCM_TRANS_N_TO_S, 0, 2,
		&authority));
	authority.master_holder.node_id = 1;
	UT_ASSERT(!gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, (uint8)PCM_TRANS_N_TO_S, 0, 2,
		&authority));

	/* Writer transfer replay is exact to the old X holder and this requester's
	 * pending-X episode. */
	authority.state = PCM_STATE_X;
	authority.x_holder_node = 0;
	authority.s_holders_bitmap = 0;
	authority.master_holder.node_id = 0;
	authority.pending_x_requester_node = 2;
	authority.pending_x_since_lsn = 11;
	UT_ASSERT(gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, (uint8)PCM_TRANS_N_TO_X, 0, 2,
		&authority));
	authority.pending_x_requester_node = 3;
	UT_ASSERT(!gcs_block_forward_replay_authority_exact(
		GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, (uint8)PCM_TRANS_N_TO_X, 0, 2,
		&authority));
}


/* spec-5.2 D2: read-image forward flag overlays reserved_0[0] without
 * growing the 64B forward wire. */
UT_TEST(test_forward_payload_read_image_flag_roundtrip)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 0);

	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 1);

	GcsBlockForwardPayloadSetReadImage(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 0);

	/* Setting the flag must not perturb the HC127 watermark bytes. */
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)0x1122334455667788ULL);
	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	UT_ASSERT_EQ((long long)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd),
				 (long long)0x1122334455667788ULL);

	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* spec-5.2a D1 (U2): clean-page X-transfer eligibility flag.  The request
 * payload carries it in reserved_0[0] (free) and the forward payload in
 * reserved_0[2] (v0.3 P0 FIX — reserved_0[0]/[1] are already the spec-5.2
 * read-image / X-transfer flags, so the forward eligibility flag MUST NOT
 * reuse them).  This test pins the roundtrip AND the three-way orthogonality:
 * setting clean-eligible must never perturb read-image or X-transfer, and
 * vice versa.  ABI stays 64B for both payloads. */
UT_TEST(test_clean_page_xfer_eligible_flag_roundtrip_and_orthogonal)
{
	GcsBlockRequestPayload req;
	GcsBlockForwardPayload fwd;

	/* request-side roundtrip (reserved_0[0]). */
	memset(&req, 0, sizeof(req));
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);
	GcsBlockRequestPayloadSetCleanEligible(&req, true);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 1);
	GcsBlockRequestPayloadSetCleanEligible(&req, false);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);

	/* forward-side roundtrip (reserved_0[2]). */
	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 0);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 1);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 0);

	/* Orthogonality: clean-eligible vs read-image[0] vs X-transfer[1]. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	GcsBlockForwardPayloadSetXTransfer(&fwd, true);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsXTransfer(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 1);

	/* Clearing clean-eligible leaves read-image / X-transfer untouched. */
	GcsBlockForwardPayloadSetCleanEligible(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsXTransfer(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 0);

	/* Clearing read-image / X-transfer leaves clean-eligible untouched. */
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	GcsBlockForwardPayloadSetReadImage(&fwd, false);
	GcsBlockForwardPayloadSetXTransfer(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsXTransfer(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 1);

	/* Setting the forward clean flag must not perturb the HC127 watermark. */
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)0x1122334455667788ULL);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	UT_ASSERT_EQ((long long)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd),
				 (long long)0x1122334455667788ULL);

	/* Both payloads stay 64B. */
	UT_ASSERT_EQ((int)sizeof(GcsBlockRequestPayload), 64);
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* spec-6.13 D6: request-side direct-land flag rides reserved_0[1] and must
 * stay independent from the existing clean-page eligibility flag at [0]. */
UT_TEST(test_request_payload_direct_land_flag_roundtrip_and_orthogonal)
{
	GcsBlockRequestPayload req;

	memset(&req, 0, sizeof(req));
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 0);

	GcsBlockRequestPayloadSetCleanEligible(&req, true);
	GcsBlockRequestPayloadSetDirectLandArmed(&req, true);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 1);

	GcsBlockRequestPayloadSetDirectLandArmed(&req, false);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 1);

	GcsBlockRequestPayloadSetCleanEligible(&req, false);
	GcsBlockRequestPayloadSetDirectLandArmed(&req, true);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)sizeof(GcsBlockRequestPayload), 64);
}


/* spec-5.2a D3 (U3): pure master-side clean-page X-transfer decision, all 5
 * branches.  Master == self runs the handler; args are (x_holder, requester,
 * master). */
UT_TEST(test_clean_xfer_master_decision_5_branches)
{
	/* ① x_holder == requester → idempotent (already holds X). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(1 /*holder*/, 1 /*req*/, 0 /*master*/),
				 GCS_CLEAN_XFER_IDEMPOTENT);
	/* ② no holder → storage-fallback. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(-1, 1, 0), GCS_CLEAN_XFER_STORAGE_FALLBACK);
	/* ③ x_holder == master(self) → path-B self-ship. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(0 /*holder==master*/, 1, 0),
				 GCS_CLEAN_XFER_SELF_SHIP);
	/* ④ other live holder, master == requester → forward to holder. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(2 /*other holder*/, 0 /*req==master*/, 0),
				 GCS_CLEAN_XFER_FORWARD_TO_HOLDER);
	/* ⑤ other live holder, master ∉ {req,holder} (3-node third party) → DENY. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(2 /*holder*/, 1 /*req*/, 0 /*master*/),
				 GCS_CLEAN_XFER_THIRD_PARTY_DENY);
	/* idempotent wins even when requester would otherwise be a third party
	 * (holder == requester is checked first). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(1, 1, 2), GCS_CLEAN_XFER_IDEMPOTENT);
}


/* spec-5.2a D3 (U4): pure stale-holder-break predicate.  Only an eligible
 * request that got a holder DENIED_MASTER_NOT_HOLDER reply breaks the loop via
 * storage-fallback; a non-eligible reply, a timeout (no reply), or any other
 * status does NOT (Rule 8.A: never storage-fallback unless the holder proved it
 * dropped). */
UT_TEST(test_clean_xfer_stale_break_predicate)
{
	/* eligible + got_reply + DENIED_MASTER_NOT_HOLDER → break. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, true, (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					 ? 1
					 : 0,
				 1);
	/* NOT eligible → never break (heap transient retransmit path). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 false, true, (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					 ? 1
					 : 0,
				 0);
	/* timeout (no reply) → never break (cannot prove holder dropped). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, false, (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					 ? 1
					 : 0,
				 0);
	/* a different reply (e.g. X_GRANTED / READ_IMAGE) → never break. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, true, (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER)
					 ? 1
					 : 0,
				 0);
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, true, (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER)
					 ? 1
					 : 0,
				 0);
}

UT_TEST(test_forwarded_holder_refusal_allows_one_master_cleanup_retry)
{
	bool awaiting_master_cleanup = false;

	UT_ASSERT(gcs_block_holder_refusal_retry_exact(
		1, &awaiting_master_cleanup, 0, 8));
	UT_ASSERT(awaiting_master_cleanup);
	UT_ASSERT(gcs_block_holder_refusal_retry_exact(
		GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		&awaiting_master_cleanup, 1, 8));
	UT_ASSERT(!awaiting_master_cleanup);
	UT_ASSERT(!gcs_block_holder_refusal_retry_exact(
		GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		&awaiting_master_cleanup, 2, 8));

	UT_ASSERT(!gcs_block_holder_refusal_retry_exact(
		1, &awaiting_master_cleanup, 8, 8));
	UT_ASSERT(!awaiting_master_cleanup);
}


/* spec-5.22d D4-6: reserved_0[6] VALUE 4 = dead-owner AUTHORITY verdict
 * request.  Pins the value-multiplex against the other kinds (1 = undo-TT
 * fetch, 2 = derived verdict, 3 = MULTI verdict, 5 = authoritative verdict):
 * the kind-4 predicate must never match 1/2/3/5 and vice versa, and setting
 * kind 4 must not perturb the widened-xid watermark carrier.  ABI stays 64B. */
UT_TEST(test_forward_payload_undo_authority_verdict_kind4)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);

	GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(&fwd);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 1);
	/* kind 4 is NOT one of the owner-served verdict kinds (2/3), NOT the
	 * undo-TT fetch (1) */
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoTtFetchRequest(&fwd) ? 1 : 0, 0);

	/* and the owner-served kinds are NOT the authority kind */
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, true /* value 5 */);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, false /* value 2 */);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	GcsBlockForwardPayloadSetUndoTtFetchRequest(&fwd, true /* value 1 */);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);

	/* kind 4 must not perturb the widened-xid carrier bytes */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)0x00000000AABBCCDDULL);
	GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(&fwd);
	UT_ASSERT_EQ((long long)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd),
				 (long long)0x00000000AABBCCDDULL);

	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* spec-5.22f Hardening (RC#1 integration review): the AUTHORITATIVE single
 * verdict sub-kind (spec-5.22f D6-7) must NOT share reserved_0[6] value 3 with
 * the spec-7.1 D3-b MULTI verdict request.  It originally did, so
 * IsUndoVerdictRequest matched a multi request and the forward handler's
 * single-verdict branch stole it before the multi branch -> a cross-node
 * multixact member serve refused and the requester fail-closed 53R97
 * (t/359_mxid G5 red on the branch, green on main).  Lock the full byte legend
 * (1 fetch / 2 derived / 3 MULTI / 4 authority / 5 authoritative) so the five
 * request kinds stay mutually exclusive across every Is* predicate. */
UT_TEST(test_forward_payload_undo_verdict_kinds_no_collision)
{
	GcsBlockForwardPayload fwd;

	/* MULTI verdict (value 3): matched ONLY by the multi predicate. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetUndoMultiVerdictRequest(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoMultiVerdictRequest(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 0); /* the collision */
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoTtFetchRequest(&fwd) ? 1 : 0, 0);

	/* AUTHORITATIVE single verdict (value 5): a verdict request, authoritative,
	 * but NOT a multi and NOT the dead-owner authority kind. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoMultiVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoTtFetchRequest(&fwd) ? 1 : 0, 0);

	/* DERIVED single verdict (value 2): a verdict request, NOT authoritative,
	 * NOT a multi. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoMultiVerdictRequest(&fwd) ? 1 : 0, 0);
}


/* spec-5.22d D4-6: the authority fetch tag carries the dead OWNER in the
 * previously-empty tag.relNumber as owner+1 (0 stays "absent" so the three
 * owner-served kinds keep their strict empty-relNumber shape).  The serve
 * side NEVER blind-trusts this field — it re-derives the authority and
 * only answers when the triple check passes — but the encode/decode
 * roundtrip and the 0-absent boundary are pinned here. */
UT_TEST(test_undo_authority_fetch_tag_owner_roundtrip)
{
	BufferTag legacy = GcsBlockUndoFetchTagMake(7, 0);
	BufferTag tag;
	uint32 segment_id = 0;
	uint32 block_no = 99;
	int32 owner = -1;

	/* legacy owner-served tag: relNumber empty, owner decode refuses */
	UT_ASSERT_EQ((int)legacy.relNumber, 0);
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(legacy, &owner) ? 1 : 0, 0);

	/* authority tag: owner 2 rides as relNumber 3; base fields unchanged */
	tag = GcsBlockUndoAuthorityFetchTagMake(7, 0, 2 /* owner */);
	UT_ASSERT_EQ((int)tag.relNumber, 3);
	UT_ASSERT_EQ(GcsBlockUndoFetchTagDecode(tag, &segment_id, &block_no) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)segment_id, 7);
	UT_ASSERT_EQ((int)block_no, 0);
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(tag, &owner) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)owner, 2);

	/* owner 0 (node id 0) must survive the +1 bias roundtrip */
	tag = GcsBlockUndoAuthorityFetchTagMake(1, 0, 0);
	UT_ASSERT_EQ((int)tag.relNumber, 1);
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(tag, &owner) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)owner, 0);

	/* wrong magic: never decodes, wherever the relNumber points */
	tag.spcOid = (Oid)0xDEADBEEF;
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(tag, &owner) ? 1 : 0, 0);
}


/* spec-5.22d D4-6: the authority-served verdict page version is a DISTINCT
 * provenance value — an old requester's strict ==1 gate refuses it (fail
 * closed) and the new authority leg accepts ONLY it (an owner-served v1
 * page can never masquerade as an authority serve). */
UT_TEST(test_undo_verdict_version_authority_distinct)
{
	UT_ASSERT_EQ((int)CLUSTER_GCS_UNDO_VERDICT_VERSION, 1);
	UT_ASSERT_EQ((int)CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY, 2);
}


UT_TEST(test_pcm_x_enqueue_ingress_binds_transport_epoch_and_master)
{
	PcmXEnqueuePayload request;

	memset(&request, 0, sizeof(request));
	request.identity.node_id = 1;
	request.identity.procno = 7;
	request.identity.cluster_epoch = 9;
	request.identity.request_id = 11;
	request.identity.wait_seq = 13;
	request.prehandle.sender_session_incarnation = 17;
	request.prehandle.prehandle_sequence = 19;

	UT_ASSERT(cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request) - 1, 1, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 0, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 10, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 9, 3, 2));
	request.prehandle.prehandle_sequence = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 9, 2, 2));
}

UT_TEST(test_pcm_x_admit_confirm_ingress_binds_requester_and_master)
{
	PcmXPhasePayload phase;

	memset(&phase, 0, sizeof(phase));
	phase.ref.identity.node_id = 1;
	phase.ref.identity.procno = 7;
	phase.ref.identity.cluster_epoch = 9;
	phase.ref.identity.request_id = 11;
	phase.ref.identity.wait_seq = 13;
	phase.ref.handle.ticket_id = 17;
	phase.ref.handle.queue_generation = 19;
	phase.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;

	UT_ASSERT(cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase), 1, 9, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase) - 1, 1, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase), 0, 9, 2, 2));
	phase.reason = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase), 1, 9, 2, 2));
}

UT_TEST(test_pcm_x_admit_confirm_ack_binds_exact_master_source)
{
	PcmXPhasePayload phase;

	memset(&phase, 0, sizeof(phase));
	phase.ref.identity.node_id = 1;
	phase.ref.identity.procno = 7;
	phase.ref.identity.cluster_epoch = 9;
	phase.ref.identity.request_id = 11;
	phase.ref.identity.wait_seq = 13;
	phase.ref.handle.ticket_id = 17;
	phase.ref.handle.queue_generation = 19;
	phase.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;

	UT_ASSERT(cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(&phase, sizeof(phase), 2, 9, 2, 1));
	UT_ASSERT(
		!cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(&phase, sizeof(phase), 3, 9, 2, 1));
	phase.ref.grant_generation = 1;
	UT_ASSERT(
		!cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(&phase, sizeof(phase), 2, 9, 2, 1));
}

UT_TEST(test_pcm_x_cancel_requests_bind_exact_source_epoch_master_and_phase)
{
	PcmXPrehandleCancelPayload prehandle;
	PcmXPhasePayload cancel;

	memset(&prehandle, 0, sizeof(prehandle));
	prehandle.identity.node_id = 1;
	prehandle.identity.procno = 7;
	prehandle.identity.cluster_epoch = 9;
	prehandle.identity.request_id = 11;
	prehandle.identity.wait_seq = 13;
	prehandle.prehandle.sender_session_incarnation = 17;
	prehandle.prehandle.prehandle_sequence = 19;
	UT_ASSERT(cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(&prehandle, sizeof(prehandle), 1, 9,
															   2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(&prehandle, sizeof(prehandle), 0, 9,
																2, 2));
	prehandle.prehandle.prehandle_sequence = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(&prehandle, sizeof(prehandle), 1, 9,
																2, 2));

	memset(&cancel, 0, sizeof(cancel));
	cancel.ref.identity = prehandle.identity;
	cancel.ref.identity.node_id = 1;
	cancel.ref.handle.ticket_id = 23;
	cancel.ref.handle.queue_generation = 29;
	cancel.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	UT_ASSERT(cluster_gcs_pcm_x_cancel_ingress_valid(&cancel, sizeof(cancel), 1, 9, 2, 2));
	cancel.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;
	UT_ASSERT(!cluster_gcs_pcm_x_cancel_ingress_valid(&cancel, sizeof(cancel), 1, 9, 2, 2));
}

UT_TEST(test_pcm_x_cancel_acks_bind_exact_master_and_canonical_payload)
{
	PcmXAdmitAckPayload prehandle_ack;
	PcmXPhasePayload cancel_ack;

	memset(&prehandle_ack, 0, sizeof(prehandle_ack));
	prehandle_ack.ref.identity.node_id = 1;
	prehandle_ack.ref.identity.procno = 7;
	prehandle_ack.ref.identity.cluster_epoch = 9;
	prehandle_ack.ref.identity.request_id = 11;
	prehandle_ack.ref.identity.wait_seq = 13;
	prehandle_ack.ref.handle.ticket_id = 17;
	prehandle_ack.ref.handle.queue_generation = 19;
	prehandle_ack.prehandle.sender_session_incarnation = 23;
	prehandle_ack.prehandle.prehandle_sequence = 29;
	prehandle_ack.result = PCM_X_QUEUE_OK;
	prehandle_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL;
	UT_ASSERT(cluster_gcs_pcm_x_prehandle_cancel_ack_ingress_valid(
		&prehandle_ack, sizeof(prehandle_ack), 2, 9, 2, 1));
	prehandle_ack.result = PCM_X_QUEUE_DUPLICATE;
	UT_ASSERT(!cluster_gcs_pcm_x_prehandle_cancel_ack_ingress_valid(
		&prehandle_ack, sizeof(prehandle_ack), 2, 9, 2, 1));

	memset(&cancel_ack, 0, sizeof(cancel_ack));
	cancel_ack.ref = prehandle_ack.ref;
	cancel_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	UT_ASSERT(
		cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	cancel_ack.reason = ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED;
	UT_ASSERT(
		cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	cancel_ack.reason = ERRCODE_CLUSTER_GCS_BLOCK_INVALIDATE_TIMEOUT;
	UT_ASSERT(
		cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	cancel_ack.reason = ERRCODE_CLUSTER_LOST_WRITE_DETECTED;
	UT_ASSERT(
		cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	cancel_ack.reason = ERRCODE_DATA_CORRUPTED;
	UT_ASSERT(
		!cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	UT_ASSERT(cluster_gcs_pcm_x_cancel_ack_base_ingress_valid(
		&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	cancel_ack.reason = 0;
	cancel_ack.ref.grant_generation = 1;
	UT_ASSERT(
		!cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
}

UT_TEST(test_pcm_x_wait_identity_maps_to_real_wfg_vertex)
{
	PcmXWaitIdentity identity;
	ClusterLmdVertex vertex;

	memset(&identity, 0, sizeof(identity));
	identity.node_id = 2;
	identity.procno = 7;
	identity.xid = 11;
	identity.cluster_epoch = 13;
	identity.request_id = 17;
	identity.wait_seq = 19;
	memset(&vertex, 0xA5, sizeof(vertex));

	cluster_gcs_pcm_x_vertex_from_identity(&identity, &vertex);
	UT_ASSERT_EQ(vertex.node_id, 2);
	UT_ASSERT_EQ(vertex.procno, (uint32)7);
	UT_ASSERT_EQ(vertex.xid, (TransactionId)11);
	UT_ASSERT_EQ(vertex.cluster_epoch, (uint64)13);
	UT_ASSERT_EQ(vertex.request_id, (uint64)17);
	UT_ASSERT_EQ(vertex.wait_seq, (uint64)19);
	UT_ASSERT_EQ(vertex.local_start_ts_ms, (int64)0);
}


UT_TEST(test_pcm_x_initial_epoch_zero_is_exact_across_wire_classes)
{
	ClusterLmdWaitStateSnapshot wait_snapshot;
	PcmXEnqueuePayload enqueue;
	PcmXRetirePayload retire;
	PcmXTicketRef ref;

	memset(&enqueue, 0, sizeof(enqueue));
	enqueue.identity.node_id = 1;
	enqueue.identity.procno = 7;
	enqueue.identity.cluster_epoch = 0;
	enqueue.identity.request_id = 11;
	enqueue.identity.wait_seq = 13;
	enqueue.prehandle.sender_session_incarnation = 17;
	enqueue.prehandle.prehandle_sequence = 19;
	UT_ASSERT(cluster_gcs_pcm_x_enqueue_ingress_valid(&enqueue, sizeof(enqueue), 1, 0, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&enqueue, sizeof(enqueue), 1, 1, 2, 2));

	memset(&ref, 0, sizeof(ref));
	ref.identity = enqueue.identity;
	ref.handle.ticket_id = 23;
	ref.handle.queue_generation = 29;
	UT_ASSERT(cluster_gcs_pcm_x_ticket_ref_wire_valid(&ref, 0));
	UT_ASSERT(!cluster_gcs_pcm_x_ticket_ref_wire_valid(&ref, 1));
	ref.grant_generation = 31;
	UT_ASSERT(cluster_gcs_pcm_x_transfer_ref_wire_valid(&ref, 0));
	UT_ASSERT(cluster_gcs_pcm_x_terminal_ref_wire_valid(&ref, 0));
	UT_ASSERT(!cluster_gcs_pcm_x_transfer_ref_wire_valid(&ref, 1));

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = 0;
	retire.master_session_incarnation = 37;
	retire.retire_through_ticket_id = 41;
	retire.sender_node = 3;
	UT_ASSERT(cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 37, 0, 3));
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 37, 1, 3));
	UT_ASSERT(cluster_gcs_pcm_x_retire_ack_ingress_valid(&retire, sizeof(retire), 3, 0, 37, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&retire, sizeof(retire), 3, 1, 37, 2));

	memset(&wait_snapshot, 0, sizeof(wait_snapshot));
	wait_snapshot.active = true;
	wait_snapshot.kind = CLUSTER_LMD_WAIT_PCM_CONVERT;
	wait_snapshot.request_id = enqueue.identity.request_id;
	wait_snapshot.cluster_epoch = 0;
	wait_snapshot.wait_seq = enqueue.identity.wait_seq;
	UT_ASSERT(cluster_gcs_pcm_x_wait_identity_matches(
		&enqueue.identity, 1, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &wait_snapshot));
	UT_ASSERT(cluster_gcs_block_epoch_advance_stales_slot(true, 0, 1));
	UT_ASSERT(!cluster_gcs_block_epoch_advance_stales_slot(false, 0, 1));
	UT_ASSERT(!cluster_gcs_block_epoch_advance_stales_slot(true, 1, 1));
}

UT_TEST(test_pcm_x_blocker_header_ingress_binds_master_not_requester_source)
{
	PcmXBlockerSetHeaderPayload header;

	memset(&header, 0, sizeof(header));
	header.ref.identity.node_id = 1;
	header.ref.identity.procno = 7;
	header.ref.identity.cluster_epoch = 9;
	header.ref.identity.request_id = 11;
	header.ref.identity.wait_seq = 13;
	header.ref.handle.ticket_id = 17;
	header.ref.handle.queue_generation = 19;
	header.set_generation = UINT64_C(0x100000001);
	header.nblockers = 2;
	header.set_crc32c = UINT32_C(0x12345678);

	/* Holder node 3 reports blockers for requester node 1 to tag master 2. */
	UT_ASSERT(cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 9, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header) - 1, 3, 9, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 10, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 9, 1, 2));
	header.set_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 9, 2, 2));
}

UT_TEST(test_pcm_x_blocker_edge_ingress_binds_blocker_to_holder_source)
{
	PcmXBlockerChunkPayload edge;

	memset(&edge, 0, sizeof(edge));
	edge.requester_node = 1;
	edge.requester_procno = 7;
	edge.cluster_epoch = 9;
	edge.request_id = 11;
	edge.handle.ticket_id = 17;
	edge.handle.queue_generation = 19;
	edge.set_generation = UINT64_C(0x100000001);
	edge.blocker.node_id = 3;
	edge.blocker.procno = 23;
	edge.blocker.cluster_epoch = 9;
	edge.blocker.request_id = 29;
	edge.blocker.wait_seq = 31;

	UT_ASSERT(cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.node_id = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.node_id = 3;
	edge.blocker.cluster_epoch = 10;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.cluster_epoch = 9;
	edge.grant_generation = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.grant_generation = 0;
	edge.blocker.request_id = 0;
	edge.blocker.xid = (TransactionId)37;
	UT_ASSERT(cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.xid = InvalidTransactionId;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.request_id = 29;
	edge.blocker.wait_seq = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
}

UT_TEST(test_pcm_x_blocker_ack_carries_full_generation_and_binds_master_source)
{
	PcmXPhasePayload ack;
	PcmXPhasePayload probe;
	PcmXTicketRef ref;
	const uint64 generation = UINT64_C(0xFEDCBA9876543210);

	memset(&ack, 0, sizeof(ack));
	ack.ref.identity.node_id = 1;
	ack.ref.identity.procno = 7;
	ack.ref.identity.cluster_epoch = 9;
	ack.ref.identity.request_id = 11;
	ack.ref.identity.wait_seq = 13;
	ack.ref.handle.ticket_id = 17;
	ack.ref.handle.queue_generation = 19;
	cluster_gcs_pcm_x_blocker_ack_set_generation(&ack, generation);

	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), generation);
	UT_ASSERT(cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 4, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 2, 10, 2, 3));
	cluster_gcs_pcm_x_blocker_ack_set_generation(&ack, 0);
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 2, 9, 2, 3));

	/* Type 48 uses the same authenticated master->holder direction for both
	 * arms.  An all-zero generation is the PROBE request; it is never a
	 * generation-exact ACK. */
	probe = ack;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&probe), 0);
	UT_ASSERT(cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 4, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 2, 10, 2, 3));
	cluster_gcs_pcm_x_blocker_ack_set_generation(&probe, 1);
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 2, 9, 2, 3));
	UT_ASSERT(cluster_gcs_pcm_x_blocker_ack_ingress_valid(&probe, sizeof(probe), 2, 9, 2, 3));

	/* The ACK producer has no representation for the reserved PROBE value. */
	ref = ack.ref;
	memset(&ack, 0xA5, sizeof(ack));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_build(&ref, 0, &ack));
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), 0);
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_build(&ref, UINT64_MAX, &ack));
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), 0);
	UT_ASSERT(cluster_gcs_pcm_x_blocker_ack_build(&ref, generation, &ack));
	UT_ASSERT(memcmp(&ack.ref, &ref, sizeof(ref)) == 0);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), generation);
}

UT_TEST(test_pcm_x_drain_poll_binds_exact_master_and_generation)
{
	PcmXDrainPollPayload poll;

	memset(&poll, 0, sizeof(poll));
	poll.ref.identity.node_id = 1;
	poll.ref.identity.procno = 7;
	poll.ref.identity.cluster_epoch = 9;
	poll.ref.identity.request_id = 11;
	poll.ref.identity.wait_seq = 13;
	poll.ref.handle.ticket_id = 17;
	poll.ref.handle.queue_generation = 19;
	poll.ref.grant_generation = 23;
	poll.drain_generation = 29;

	/* The tag master is node 2; node 3 is one terminal participant. */
	UT_ASSERT(cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll) - 1, 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 4, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 10, 2, 3));
	poll.drain_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 9, 2, 3));
	poll.drain_generation = 29;
	poll.ref.grant_generation = UINT64_MAX;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 9, 2, 3));
}

UT_TEST(test_pcm_x_drain_ack_binds_participant_and_canonical_payload)
{
	PcmXPhasePayload ack;

	memset(&ack, 0, sizeof(ack));
	ack.ref.identity.node_id = 1;
	ack.ref.identity.procno = 7;
	ack.ref.identity.cluster_epoch = 9;
	ack.ref.identity.request_id = 11;
	ack.ref.identity.wait_seq = 13;
	ack.ref.handle.ticket_id = 17;
	ack.ref.handle.queue_generation = 19;
	ack.ref.grant_generation = 23;

	/* Participant node 3 ACKs to the local tag master node 2. */
	UT_ASSERT(cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 4, 2));
	ack.reason = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 2, 2));
	ack.reason = 0;
	ack.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 2, 2));
}

UT_TEST(test_pcm_x_retire_request_binds_master_session_and_target)
{
	PcmXRetirePayload retire;

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = 9;
	retire.master_session_incarnation = 17;
	retire.retire_through_ticket_id = 23;
	retire.sender_node = 3;

	UT_ASSERT(cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 17, 9, 3));
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 19, 9, 3));
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 17, 10, 3));
	retire.sender_node = 4;
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 17, 9, 3));
}

UT_TEST(test_pcm_x_retire_ack_binds_responder_and_master_authority)
{
	PcmXRetirePayload ack;

	memset(&ack, 0, sizeof(ack));
	ack.cluster_epoch = 9;
	ack.master_session_incarnation = 17;
	ack.retire_through_ticket_id = 23;
	ack.sender_node = 3;

	UT_ASSERT(cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 17, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 4, 9, 17, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 19, 2));
	ack.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 17, 2));
}


UT_TEST(test_pcm_x_formation_identical_complete_samples_may_revalidate)
{
	PcmXPeerBinding after[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXPeerBinding before[PCM_X_PROTOCOL_NODE_LIMIT];

	memset(before, 0, sizeof(before));
	before[0].cluster_epoch = 9;
	before[0].peer_session_incarnation = 101;
	before[1].cluster_epoch = 9;
	before[1].peer_session_incarnation = 102;
	memcpy(after, before, sizeof(after));
	UT_ASSERT(cluster_gcs_pcm_x_formation_samples_stable(true, before, true, after));
}


UT_TEST(test_pcm_x_formation_transient_or_inconsistent_sample_is_tick_noop)
{
	PcmXPeerBinding after[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXPeerBinding before[PCM_X_PROTOCOL_NODE_LIMIT];

	memset(before, 0, sizeof(before));
	before[0].cluster_epoch = 9;
	before[0].peer_session_incarnation = 101;
	memcpy(after, before, sizeof(after));
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(false, before, true, after));
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(true, before, false, after));
	after[0].peer_session_incarnation++;
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(true, before, true, after));
	memcpy(after, before, sizeof(after));
	after[1].cluster_epoch = 10;
	after[1].peer_session_incarnation = 102;
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(true, before, true, after));
}


/* P0-20: a post-handoff COMMIT_X ticket can remain freshly armed while the
 * production LMON path silently exits before retry mutation.  Every
 * pre-mutation boundary must therefore leave one bounded, stage-specific
 * breadcrumb; otherwise t/400 cannot distinguish formation rejection from a
 * missed work scan or a drive precheck refusal. */
UT_TEST(test_pcm_x_periodic_retry_reports_pre_mutation_exit_stage)
{
	char *source = read_gcs_block_source();
	const char *formation;
	const char *formation_end;
	const char *retry_tick;
	const char *retry_tick_end;
	const char *drive;
	const char *drive_end;
	const char *transfer;
	const char *transfer_end;

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		formation = strstr(source, "\ncluster_gcs_block_pcm_x_formation_tick(");
		formation_end = formation != NULL ? strstr(formation, "\nfail_closed:") : NULL;
		UT_ASSERT_NOT_NULL(formation);
		UT_ASSERT_NOT_NULL(formation_end);
		if (formation != NULL && formation_end != NULL) {
			UT_ASSERT_NOT_NULL(strstr(formation, "\"collect-before\""));
			UT_ASSERT_NOT_NULL(strstr(formation, "\"collect-after\""));
			UT_ASSERT_NOT_NULL(strstr(formation, "\"stability\""));
			UT_ASSERT_NOT_NULL(strstr(formation, "\"peer-revalidate\""));
			UT_ASSERT_NOT_NULL(strstr(formation, "\"runtime-resample\""));
			UT_ASSERT_NOT_NULL(strstr(formation,
							  "gcs_block_pcm_x_resource_retry_tick(bindings_before)"));
		}

		retry_tick = strstr(source, "\ngcs_block_pcm_x_resource_retry_tick(");
		retry_tick_end = retry_tick != NULL ? strstr(retry_tick, "\n}\n") : NULL;
		UT_ASSERT_NOT_NULL(retry_tick);
		UT_ASSERT_NOT_NULL(retry_tick_end);
		if (retry_tick != NULL && retry_tick_end != NULL) {
			const char *backoff_guc = strstr(
				retry_tick, "cluster_gcs_block_retransmit_initial_backoff_ms");
			const char *budget_guc = strstr(
				retry_tick, "cluster_gcs_block_retransmit_max_retries");
			const char *sampled_next = strstr(retry_tick,
										  "resource_x_retry_next_due_exact");

			UT_ASSERT_NOT_NULL(strstr(retry_tick, "\"work-next\""));
			UT_ASSERT_NOT_NULL(strstr(retry_tick, "cursor_before"));
			UT_ASSERT_NOT_NULL(sampled_next);
			UT_ASSERT(sampled_next == NULL || sampled_next < retry_tick_end);
			UT_ASSERT(backoff_guc == NULL || backoff_guc >= retry_tick_end);
			UT_ASSERT(budget_guc == NULL || budget_guc >= retry_tick_end);
		}
		UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_x_retry_work_next"));
		UT_ASSERT_NOT_NULL(strstr(source, "#define PCM_X_MASTER_DRIVE_SCAN_BUDGET 1024"));
		UT_ASSERT_NOT_NULL(strstr(retry_tick,
							  "cluster_pcm_x_retry_work_next(&cursor, "
							  "PCM_X_MASTER_DRIVE_SCAN_BUDGET"));
		UT_ASSERT_NOT_NULL(strstr(source, "resource_x_retry_classify_exact"));
		UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_x_local_retry_admitted_exact"));
		UT_ASSERT_NOT_NULL(strstr(retry_tick,
							  "RESOURCE_X_RETRY_TERMINAL_EXHAUSTED"));
		UT_ASSERT_NOT_NULL(strstr(retry_tick,
							  "cluster_pcm_x_local_retry_exhausted_exact"));
		UT_ASSERT_NOT_NULL(strstr(retry_tick, "RESOURCE_X_RETRY_ROLL_FORWARD"));
		UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_x_live_ticket_count(&live_tickets)"));
		UT_ASSERT_NOT_NULL(strstr(source, "live_tickets == 0"));

		drive = strstr(source, "\ngcs_block_pcm_x_master_drive_tag(");
		drive_end = drive != NULL ? strstr(drive, "\n}\n") : NULL;
		UT_ASSERT_NOT_NULL(drive);
		UT_ASSERT_NOT_NULL(drive_end);
		if (drive != NULL && drive_end != NULL) {
			UT_ASSERT_NOT_NULL(strstr(drive, "\"drive-precheck-"));
		}

		transfer = strstr(source, "\ngcs_block_pcm_x_master_drive_transfer(");
		transfer_end = transfer != NULL ? strstr(transfer, "\n}\n") : NULL;
		UT_ASSERT_NOT_NULL(transfer);
		UT_ASSERT_NOT_NULL(transfer_end);
		if (transfer != NULL && transfer_end != NULL) {
			UT_ASSERT_NOT_NULL(strstr(transfer, "\"commit-retry\""));
		}
		free(source);
	}
}


/* D10-13: capability publication does not remove the per-peer/runtime gate;
 * a drifted or unavailable peer must still keep the Resource-X domain dark. */
UT_TEST(test_resource_x_retry_domain_remains_capability_gated)
{
	char *source = read_gcs_block_source();
	const char *driver;
	const char *driver_end;
	const char *driver_gate;
	const char *submission;
	const char *granted;
	const char *granted_gate;
	const char *target_executor;
	const char *retry_tick;
	const char *retry_end;
	const char *tick_gate;
	const char *target_work;
	const char *work_next;
	const char *master_drive;
	const char *local_gate;
	const char *local_drive;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	driver = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	driver_gate = driver != NULL
		? strstr(driver, "gcs_block_pcm_x_resource_x_retry_enabled()")
		: NULL;
	submission = driver != NULL
		? strstr(driver, "gcs_block_pcm_x_note_local_submission(&handle, &token)")
		: NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	UT_ASSERT_NOT_NULL(driver_gate);
	UT_ASSERT_NOT_NULL(submission);
	if (driver_gate != NULL && submission != NULL && driver_end != NULL)
		UT_ASSERT(driver_gate < submission && submission < driver_end);
	granted = driver != NULL
		? strstr(driver, "else if (progress.member_state == PCM_XL_GRANTED)")
		: NULL;
	granted_gate = granted != NULL
		? strstr(granted, "gcs_block_pcm_x_resource_x_retry_enabled()")
		: NULL;
	target_executor = granted != NULL
		? strstr(granted, "gcs_block_pcm_x_resource_x_terminal_try(")
		: NULL;
	UT_ASSERT_NOT_NULL(granted);
	UT_ASSERT_NOT_NULL(granted_gate);
	UT_ASSERT_NOT_NULL(target_executor);
	if (granted_gate != NULL && target_executor != NULL && driver_end != NULL)
		UT_ASSERT(granted_gate < target_executor && target_executor < driver_end);

	retry_tick = strstr(source, "\ngcs_block_pcm_x_resource_retry_tick(");
	retry_end = retry_tick != NULL ? strstr(retry_tick, "\n}\n") : NULL;
	tick_gate = retry_tick != NULL
		? strstr(retry_tick,
			"resource_x_enabled = gcs_block_pcm_x_resource_x_retry_enabled()")
		: NULL;
	target_work = retry_tick != NULL
		? strstr(retry_tick, "cluster_pcm_x_local_target_activation_work_next(")
		: NULL;
	work_next = retry_tick != NULL
		? strstr(retry_tick, "cluster_pcm_x_retry_work_next(")
		: NULL;
	master_drive = work_next != NULL
		? strstr(work_next, "if (work.kind == PCM_X_RETRY_WORK_MASTER)")
		: NULL;
	local_gate = master_drive != NULL
		? strstr(master_drive, "if (!resource_x_enabled)")
		: NULL;
	local_drive = local_gate != NULL
		? strstr(local_gate, "if (work.kind != PCM_X_RETRY_WORK_LOCAL")
		: NULL;
	UT_ASSERT_NOT_NULL(retry_tick);
	UT_ASSERT_NOT_NULL(retry_end);
	UT_ASSERT_NOT_NULL(tick_gate);
	UT_ASSERT_NOT_NULL(target_work);
	UT_ASSERT_NOT_NULL(work_next);
	UT_ASSERT_NOT_NULL(master_drive);
	UT_ASSERT_NOT_NULL(local_gate);
	UT_ASSERT_NOT_NULL(local_drive);
	if (tick_gate != NULL && target_work != NULL && work_next != NULL
		&& retry_end != NULL)
		UT_ASSERT(tick_gate < target_work && target_work < work_next
				  && work_next < retry_end);
	if (work_next != NULL && master_drive != NULL && local_gate != NULL
		&& local_drive != NULL && retry_end != NULL)
		UT_ASSERT(work_next < master_drive && master_drive < local_gate
				  && local_gate < local_drive && local_drive < retry_end);
	UT_ASSERT_NULL(strstr(retry_tick,
		"if (!gcs_block_pcm_x_resource_x_retry_enabled())\n\t\treturn;"));
	free(source);
}


/* D7-08: the periodic producer may only publish an immutable terminal and
 * wake its exact requester.  The requester validates and raises that record;
 * ERROR cleanup releases the writer claim before completing/detaching the
 * failed attempt, and only then wakes the promoted successor.  Type-60 zero
 * remains the legacy cancel path while an allowlisted nonzero reason enters
 * the same terminal record consumer. */
UT_TEST(test_pcm_x_resource_terminal_result_replays_through_requester_cleanup)
{
	char	   *source = read_gcs_block_source();
	const char *retry_tick;
	const char *retry_end;
	const char *exhausted;
	const char *producer_wake;
	const char *driver;
	const char *driver_end;
	const char *raise_call;
	const char *leader_raise_call;
	const char *follower_claim;
	const char *progress;
	const char *raise_helper;
	const char *raise_end;
	const char *state_read;
	const char *reason_decode;
	const char *consume_ready;
	const char *exact_errcode;
	const char *cleanup;
	const char *cleanup_end;
	const char *claim_release;
	const char *terminal_complete;
	const char *terminal_detach;
	const char *successor_wake;
	const char *cancel_ack;
	const char *cancel_ack_end;
	const char *base_ingress;
	const char *auth_revalidate;
	const char *invalid_reason;
	const char *invalid_fail_closed;
	const char *legacy_branch;
	const char *terminal_ack;
	const char *denial_wake;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;

	retry_tick = strstr(source, "\ngcs_block_pcm_x_resource_retry_tick(");
	retry_end = retry_tick != NULL ? strstr(retry_tick, "\n}\n") : NULL;
	exhausted = retry_tick != NULL
		? strstr(retry_tick, "cluster_pcm_x_local_retry_exhausted_exact(")
		: NULL;
	producer_wake = exhausted != NULL
		? strstr(exhausted, "gcs_block_pcm_x_wake_requester(&work.local_handle.identity)")
		: NULL;
	UT_ASSERT_NOT_NULL(retry_tick);
	UT_ASSERT_NOT_NULL(retry_end);
	UT_ASSERT_NOT_NULL(exhausted);
	UT_ASSERT_NOT_NULL(producer_wake);
	if (exhausted != NULL && producer_wake != NULL && retry_end != NULL)
		UT_ASSERT(exhausted < producer_wake && producer_wake < retry_end);

	driver = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	raise_call = driver != NULL
		? strstr(driver, "gcs_block_pcm_x_raise_terminal_if_exact(&handle)")
		: NULL;
	follower_claim = raise_call != NULL
		? strstr(raise_call, "cluster_pcm_x_local_writer_claim_exact(&handle, claim_out)")
		: NULL;
	leader_raise_call = raise_call != NULL
		? strstr(raise_call + 1, "gcs_block_pcm_x_raise_terminal_if_exact(&handle)")
		: NULL;
	progress = leader_raise_call != NULL
		? strstr(leader_raise_call, "cluster_pcm_x_local_progress_exact(&handle, &progress)")
		: NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	UT_ASSERT_NOT_NULL(raise_call);
	UT_ASSERT_NOT_NULL(follower_claim);
	UT_ASSERT_NOT_NULL(leader_raise_call);
	UT_ASSERT_NOT_NULL(progress);
	if (raise_call != NULL && follower_claim != NULL && leader_raise_call != NULL
		&& progress != NULL && driver_end != NULL)
		UT_ASSERT(raise_call < follower_claim && follower_claim < leader_raise_call
				  && leader_raise_call < progress && progress < driver_end);

	raise_helper = strstr(source, "\ngcs_block_pcm_x_raise_terminal_if_exact(");
	raise_end = raise_helper != NULL ? strstr(raise_helper, "\n}\n") : NULL;
	state_read = raise_helper != NULL
		? strstr(raise_helper, "cluster_pcm_x_local_retry_state_exact")
		: NULL;
	reason_decode = state_read != NULL
		? strstr(state_read, "resource_x_terminal_reason_decode")
		: NULL;
	consume_ready = reason_decode != NULL
		? strstr(reason_decode, "cluster_pcm_x_local_retry_terminal_ready_exact")
		: NULL;
	exact_errcode = consume_ready != NULL
		? strstr(consume_ready, "errcode((int)terminal.terminal_errcode)")
		: NULL;
	UT_ASSERT_NOT_NULL(raise_helper);
	UT_ASSERT_NOT_NULL(raise_end);
	UT_ASSERT_NOT_NULL(state_read);
	UT_ASSERT_NOT_NULL(reason_decode);
	UT_ASSERT_NOT_NULL(consume_ready);
	UT_ASSERT_NOT_NULL(exact_errcode);
	if (state_read != NULL && reason_decode != NULL && consume_ready != NULL
		&& exact_errcode != NULL && raise_end != NULL)
		UT_ASSERT(state_read < reason_decode && reason_decode < consume_ready
				  && consume_ready < exact_errcode && exact_errcode < raise_end);

	cleanup = strstr(source, "\ngcs_block_pcm_x_requester_cleanup_impl(");
	cleanup_end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	claim_release = cleanup != NULL
		? strstr(cleanup, "cluster_gcs_pcm_x_writer_claim_release_and_wake_exact")
		: NULL;
	terminal_complete = claim_release != NULL
		? strstr(claim_release, "cluster_pcm_x_local_retry_terminal_complete_exact")
		: NULL;
	terminal_detach = terminal_complete != NULL
		? strstr(terminal_complete, "cluster_pcm_x_local_detach_terminal_exact")
		: NULL;
	successor_wake = terminal_detach != NULL
		? strstr(terminal_detach, "gcs_block_pcm_x_wake_requester(&promoted.identity)")
		: NULL;
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	UT_ASSERT_NOT_NULL(claim_release);
	UT_ASSERT_NOT_NULL(terminal_complete);
	UT_ASSERT_NOT_NULL(terminal_detach);
	UT_ASSERT_NOT_NULL(successor_wake);
	if (claim_release != NULL && terminal_complete != NULL && terminal_detach != NULL
		&& successor_wake != NULL && cleanup_end != NULL)
		UT_ASSERT(claim_release < terminal_complete && terminal_complete < terminal_detach
				  && terminal_detach < successor_wake && successor_wake < cleanup_end);

	cancel_ack = strstr(source, "\ncluster_gcs_handle_pcm_x_cancel_ack_envelope(");
	cancel_ack_end = cancel_ack != NULL ? strstr(cancel_ack, "\n}\n") : NULL;
	base_ingress = cancel_ack != NULL
		? strstr(cancel_ack, "cluster_gcs_pcm_x_cancel_ack_base_ingress_valid(")
		: NULL;
	auth_revalidate = base_ingress != NULL
		? strstr(base_ingress, "gcs_block_pcm_x_revalidate_peer_binding")
		: NULL;
	invalid_reason = auth_revalidate != NULL
		? strstr(auth_revalidate, "resource_x_terminal_reason_decode(ack->reason)")
		: NULL;
	invalid_fail_closed = invalid_reason != NULL
		? strstr(invalid_reason, "cluster_pcm_x_runtime_fail_closed()")
		: NULL;
	legacy_branch = invalid_fail_closed != NULL
		? strstr(invalid_fail_closed, "if (ack->reason == 0)")
		: NULL;
	terminal_ack = legacy_branch != NULL
		? strstr(legacy_branch, "cluster_pcm_x_local_retry_terminal_ack_exact(")
		: NULL;
	denial_wake = terminal_ack != NULL
		? strstr(terminal_ack, "gcs_block_pcm_x_wake_requester(&ack->ref.identity)")
		: NULL;
	UT_ASSERT_NOT_NULL(cancel_ack);
	UT_ASSERT_NOT_NULL(cancel_ack_end);
	UT_ASSERT_NOT_NULL(base_ingress);
	UT_ASSERT_NOT_NULL(auth_revalidate);
	UT_ASSERT_NOT_NULL(invalid_reason);
	UT_ASSERT_NOT_NULL(invalid_fail_closed);
	UT_ASSERT_NOT_NULL(legacy_branch);
	UT_ASSERT_NOT_NULL(terminal_ack);
	UT_ASSERT_NOT_NULL(denial_wake);
	if (base_ingress != NULL && auth_revalidate != NULL && invalid_reason != NULL
		&& invalid_fail_closed != NULL && legacy_branch != NULL && terminal_ack != NULL
		&& denial_wake != NULL
		&& cancel_ack_end != NULL)
		UT_ASSERT(base_ingress < auth_revalidate && auth_revalidate < invalid_reason
				  && invalid_reason < invalid_fail_closed && invalid_fail_closed < legacy_branch
				  && legacy_branch < terminal_ack && terminal_ack < denial_wake
				  && denial_wake < cancel_ack_end);

	free(source);
}


/* review R2 P0-1: a SELF-loopback V2 INSTALL_READY (master == requester
 * node) has no HELLO capability record; the handler must treat self as
 * rebase-capable (the local binary) while still requiring the
 * activation-bound coverage flag.  The regression silently dropped the
 * self V2 frame, froze the PREPARE->INSTALL chain on the master ticket and
 * leaked the remote retained source. */
UT_TEST(test_pcm_x_install_ready_v2_self_loopback_is_admissible)
{
	char *source = read_gcs_block_source();
	const char *handler;
	const char *self_arm;
	const char *peer_arm;
	const char *valid_call;

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		handler = strstr(source, "\ncluster_gcs_handle_pcm_x_install_ready_envelope(");
		UT_ASSERT_NOT_NULL(handler);
		self_arm = handler != NULL ? strstr(handler, "source_node == cluster_node_id") : NULL;
		UT_ASSERT_NOT_NULL(self_arm);
		peer_arm = self_arm != NULL
					   ? strstr(self_arm, "cluster_sf_peer_supports_pcm_x_rebase(source_node)")
					   : NULL;
		UT_ASSERT_NOT_NULL(peer_arm);
		valid_call = peer_arm != NULL
						 ? strstr(peer_arm, "cluster_gcs_pcm_x_install_ready_ingress_valid(")
						 : NULL;
		UT_ASSERT_NOT_NULL(valid_call);
		free(source);
	}
}


/* review P0-2: the collector binds capability bits to the peer session only
 * through one lock-coherent (bits, generation) sample taken on BOTH sides of
 * the authority pass; a reconnect between the samples changes the record
 * generation and rejects the tick, so a stale connection's REBASE bit can
 * never activate V2 against a fresh session. */
UT_TEST(test_pcm_x_formation_samples_capability_family_atomically)
{
	char *source = read_gcs_block_source();
	const char *collect;
	const char *first_sample;
	const char *barrier;
	const char *second_sample;
	const char *end;
	const char *stray;

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		collect = strstr(source, "\ngcs_block_pcm_x_collect_formation(");
		UT_ASSERT_NOT_NULL(collect);
		end = collect != NULL ? strstr(collect, "\n#define PCM_X_MASTER_DRIVE_SCAN_BUDGET") : NULL;
		UT_ASSERT_NOT_NULL(end);
		first_sample
			= collect != NULL ? strstr(collect, "cluster_sf_peer_pcm_x_capability_sample(") : NULL;
		UT_ASSERT_NOT_NULL(first_sample);
		barrier = first_sample != NULL ? strstr(first_sample, "pg_read_barrier();") : NULL;
		UT_ASSERT_NOT_NULL(barrier);
		second_sample
			= barrier != NULL ? strstr(barrier, "cluster_sf_peer_pcm_x_capability_sample(") : NULL;
		UT_ASSERT_NOT_NULL(second_sample);
		if (second_sample != NULL && end != NULL)
			UT_ASSERT(second_sample < end);
		/* The separate single-shot capability reads are gone from the
		 * collector: only the atomic family sample may feed it. */
		if (collect != NULL && end != NULL) {
			stray = strstr(collect, "cluster_sf_peer_supports_pcm_x_convert(");
			UT_ASSERT(stray == NULL || stray > end);
			stray = strstr(collect, "cluster_sf_peer_supports_pcm_x_rebase(");
			UT_ASSERT(stray == NULL || stray > end);
		}
		free(source);
	}
}


UT_TEST(test_pcm_x_confirm_publish_then_stale_requires_exact_graph_close)
{
	UT_ASSERT(cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_STALE));
	UT_ASSERT(
		cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_NOT_READY));
	UT_ASSERT(!cluster_gcs_pcm_x_confirm_compensation_required(0, PCM_X_QUEUE_STALE));
	UT_ASSERT(!cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_OK));
	UT_ASSERT(
		!cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_DUPLICATE));
}


static void
pcm_x_test_init_transfer_ref(PcmXTicketRef *ref)
{
	memset(ref, 0, sizeof(*ref));
	ref->identity.node_id = 1;
	ref->identity.procno = 7;
	ref->identity.xid = 9;
	ref->identity.cluster_epoch = 11;
	ref->identity.request_id = 13;
	ref->identity.wait_seq = 17;
	ref->identity.base_own_generation = 19;
	ref->handle.ticket_id = 23;
	ref->handle.queue_generation = 29;
	ref->grant_generation = 31;
}


UT_TEST(test_pcm_x_revoke_ingress_binds_master_and_exact_transfer_key)
{
	PcmXRevokePayload revoke;
	uint64 image_id;

	memset(&revoke, 0, sizeof(revoke));
	pcm_x_test_init_transfer_ref(&revoke.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	revoke.image_id = image_id;

	/* Master node 2 revokes current holder node 3 for requester node 1. */
	UT_ASSERT(cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke) - 1, 2, 11, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 3, 11, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 12, 2, 3));
	revoke.ref.grant_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	revoke.ref.grant_generation = 31;
	revoke.image_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	revoke.image_id = UINT64CONST(0xe000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &revoke.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
}


UT_TEST(test_pcm_x_image_ready_ingress_binds_holder_image_to_master)
{
	PcmXGrantPayload ready;
	uint64 image_id;

	memset(&ready, 0, sizeof(ready));
	pcm_x_test_init_transfer_ref(&ready.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	ready.image.image_id = image_id;
	ready.image.source_node = 3;
	ready.image.page_scn = 41;
	ready.image.page_lsn = 43;
	ready.image.page_checksum = UINT32_C(0x12345678);

	/* source_own_generation=0 is the legal first ownership generation. */
	UT_ASSERT(cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready) - 1, 3, 11, 2, 2));
	ready.image.source_node = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	ready.image.source_node = 3;
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 1, 2));
	ready.image.image_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	ready.image.image_id = UINT64CONST(0xf000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &ready.image.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
}


UT_TEST(test_pcm_x_prepare_grant_ingress_binds_master_to_requester)
{
	PcmXGrantPayload grant;
	uint64 image_id;

	memset(&grant, 0, sizeof(grant));
	pcm_x_test_init_transfer_ref(&grant.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	grant.image.image_id = image_id;
	grant.image.source_node = 3;

	UT_ASSERT(cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 4));
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 3, 11, 2, 1));
	grant.image.source_node = PCM_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	grant.image.source_node = 3;
	grant.ref.identity.request_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	grant.ref.identity.request_id = 13;
	grant.image.image_id = UINT64CONST(0xe000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &grant.image.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
}


UT_TEST(test_pcm_x_install_ready_ingress_is_canonical_requester_ack)
{
	PcmXInstallReadyPayload ready;
	uint64 image_id;

	memset(&ready, 0, sizeof(ready));
	pcm_x_test_init_transfer_ref(&ready.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	ready.image_id = image_id;
	ready.result = PCM_X_QUEUE_OK;
	ready.phase = PGRAC_IC_MSG_PCM_X_INSTALL_READY;

	UT_ASSERT(cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															true, true));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2,
															 true, true));
	ready.result = PCM_X_QUEUE_DUPLICATE;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));
	ready.result = PCM_X_QUEUE_OK;
	ready.phase = PGRAC_IC_MSG_PCM_X_PREPARE_GRANT;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));
	ready.phase = PGRAC_IC_MSG_PCM_X_INSTALL_READY;
	ready.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));
	ready.flags = 0;
	ready.image_id = UINT64CONST(0xf000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &ready.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));

	/* A' rebase: both exact frame lengths are legal, nothing in between; a
	 * V1-length frame must carry a zero rebase and a V2 rebase must be
	 * strictly newer than the immutable identity base and not the exhausted
	 * sentinel. */
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &ready.image_id));
	UT_ASSERT(cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, PCM_X_INSTALL_READY_V1_LEN, 1,
															11, 2, 2, true, true));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, PCM_X_INSTALL_READY_V1_LEN + 4,
															 1, 11, 2, 2, true, true));
	ready.ref.identity.base_own_generation = 5;
	ready.rebased_own_generation = 8;
	UT_ASSERT(cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															true, true));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, PCM_X_INSTALL_READY_V1_LEN, 1,
															 11, 2, 2, true, true));
	ready.rebased_own_generation = 5;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));
	ready.rebased_own_generation = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));
	ready.rebased_own_generation = UINT64_MAX;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, true));

	/* Receiver-side V2 admission (review P1-3): a 112-byte frame is refused
	 * unless BOTH this node's activated formation has full V2 coverage and
	 * the source's current connection advertised the REBASE capability.  The
	 * sender-side coverage gate is not a receiver invariant. */
	ready.rebased_own_generation = 8;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 false, true));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 true, false));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2,
															 false, false));
	ready.rebased_own_generation = 0;
	ready.ref.identity.base_own_generation = 0;
	/* V1 frames stay independent of the rebase capability pair. */
	UT_ASSERT(cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, PCM_X_INSTALL_READY_V1_LEN, 1,
															11, 2, 2, false, false));
}


UT_TEST(test_pcm_x_commit_x_ingress_is_canonical_master_phase)
{
	PcmXPhasePayload commit;

	memset(&commit, 0, sizeof(commit));
	pcm_x_test_init_transfer_ref(&commit.ref);
	commit.phase = PGRAC_IC_MSG_PCM_X_COMMIT_X;

	UT_ASSERT(cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 3, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 4));
	commit.reason = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 1));
	commit.reason = 0;
	commit.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 1));
}


UT_TEST(test_pcm_x_final_ack_ingress_binds_monotonic_committed_floor)
{
	PcmXFinalAckPayload ack;
	uint64 image_id;

	memset(&ack, 0, sizeof(ack));
	pcm_x_test_init_transfer_ref(&ack.ref);
	ack.ref.identity.base_own_generation = 0;
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	ack.image_id = image_id;
	ack.committed_own_generation = 1;

	UT_ASSERT(cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 3, 11, 2, 2));
	/* A' rebase: the wire check is only a monotonic floor (committed > base);
	 * a published rebase legally lifts committed past base+1 and the exact
	 * "+1" proof runs under the master ticket lock against the effective
	 * grant base.  At or below the base stays refused. */
	ack.committed_own_generation = 2;
	UT_ASSERT(cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.ref.identity.base_own_generation = 5;
	ack.committed_own_generation = 5;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.committed_own_generation = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.committed_own_generation = 9;
	UT_ASSERT(cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.ref.identity.base_own_generation = UINT64_MAX;
	ack.committed_own_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.ref.identity.base_own_generation = 0;
	ack.committed_own_generation = 1;
	ack.image_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.image_id = UINT64CONST(0xe000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &ack.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
}


UT_TEST(test_pcm_x_final_commit_ack_ingress_is_canonical_master_phase)
{
	PcmXPhasePayload ack;

	memset(&ack, 0, sizeof(ack));
	pcm_x_test_init_transfer_ref(&ack.ref);
	ack.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;

	UT_ASSERT(cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 2, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 3, 11, 2, 1));
	ack.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
	UT_ASSERT(!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 2, 11, 2, 1));
	ack.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	ack.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 2, 11, 2, 1));
}


UT_TEST(test_pcm_x_final_confirm_ingress_is_canonical_requester_phase)
{
	PcmXPhasePayload confirm;

	memset(&confirm, 0, sizeof(confirm));
	pcm_x_test_init_transfer_ref(&confirm.ref);
	confirm.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;

	UT_ASSERT(
		cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 3, 11, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 3, 2));
	confirm.reason = 1;
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 2, 2));
	confirm.reason = 0;
	confirm.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 2, 2));
}

UT_TEST(test_pcm_x_master_drive_selects_exact_authority_and_next_holder)
{
	PcmAuthoritySnapshot authority;
	const uint64 ticket_id = 73;
	uint32 holders;
	int32 holder;
	int32 source;

	memset(&authority, 0, sizeof(authority));
	authority.state = PCM_STATE_X;
	authority.x_holder_node = 2;
	authority.master_holder.node_id = 2;
	authority.pending_x_requester_node = 3;
	authority.pending_x_since_lsn = UINT64_C(0x8000000000000000) | ticket_id;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 3, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, UINT32_C(1) << 2);
	UT_ASSERT_EQ(source, 2);
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 3, ticket_id + 1, &holders, &source),
		PCM_X_QUEUE_STALE);
	authority.pending_x_requester_node = 2;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 2, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, UINT32_C(1) << 2);
	UT_ASSERT_EQ(source, 2);

	authority.state = PCM_STATE_S;
	authority.x_holder_node = -1;
	authority.s_holders_bitmap = (UINT32_C(1) << 1) | (UINT32_C(1) << 3);
	authority.master_holder.node_id = 1;
	authority.pending_x_requester_node = 3;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 3, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, authority.s_holders_bitmap);
	/* A requester S mirror must be the source even when another holder is
	 * available.  Invalidating requester S first bumps its local ownership
	 * generation; the subsequent X commit would then be a second bump while
	 * FINAL_ACK is generation-exact at identity.base+1. */
	UT_ASSERT_EQ(source, 3);
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_STALE);
	authority.pending_x_requester_node = 0;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(source, 1);
	/* A canonical S master-holder that is also the requester retains that
	 * source role; the exact self handoff keeps the lifecycle finite. */
	authority.s_holders_bitmap = (UINT32_C(1) << 0) | (UINT32_C(1) << 1);
	authority.master_holder.node_id = 0;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(source, 0);
	/* A sole requester S copy reuses its exact REVOKING lifecycle as the
	 * requester grant reservation; selecting self is therefore finite and
	 * preserves the single ownership-generation bump. */
	authority.s_holders_bitmap = UINT32_C(1) << 0;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(source, 0);
	authority.s_holders_bitmap = (UINT32_C(1) << 1) | (UINT32_C(1) << 3);
	authority.master_holder.node_id = 2;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_CORRUPT);

	memset(&authority, 0, sizeof(authority));
	authority.state = PCM_STATE_N;
	authority.x_holder_node = -1;
	authority.master_holder.node_id = UINT32_MAX;
	authority.pending_x_requester_node = 0;
	authority.pending_x_since_lsn = UINT64_C(0x8000000000000000) | ticket_id;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, UINT32_C(1) << 0);
	UT_ASSERT_EQ(source, 0);
	authority.master_holder.node_id = 1;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_CORRUPT);
	authority.master_holder.node_id = PCM_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_CORRUPT);

	UT_ASSERT_EQ(cluster_gcs_pcm_x_next_unacked_holder((UINT32_C(1) << 1) | (UINT32_C(1) << 3),
													   UINT32_C(1) << 1, &holder),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder, 3);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_next_unacked_holder(UINT32_C(1) << 1, UINT32_C(1) << 1, &holder),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(holder, -1);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_next_unacked_holder(UINT32_C(1) << 1, UINT32_C(1) << 2, &holder),
				 PCM_X_QUEUE_CORRUPT);
}

UT_TEST(test_pcm_x_master_drive_wiring_binds_grd_barrier_to_exact_ticket)
{
	char *source = read_gcs_block_source();
	char *claim;
	char *cancel;
	char *end;
	char *finalize;
	char *fail_closed;
	char *graph;
	char *handler;
	char *note_stale;
	char *prepare;
	char *stale;
	char *state;
	char *reserve;
	char *publish;
	char *post_verify_state;
	char *normalize;
	char *revalidate;
	char *clear;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	claim = strstr(source, "\ngcs_block_pcm_x_ensure_pending_x_claim(");
	end = claim != NULL ? strstr(claim, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(claim);
	UT_ASSERT_NOT_NULL(end);
	if (claim != NULL && end != NULL) {
		state = strstr(claim, "cluster_pcm_x_master_pending_x_claim_state_exact(");
		reserve = strstr(claim, "cluster_pcm_lock_try_reserve_pending_x(");
		publish = strstr(claim, "cluster_pcm_x_master_pending_x_claim_exact(");
		revalidate
			= publish != NULL ? strstr(publish, "cluster_pcm_lock_queue_pending_x_exact(") : NULL;
		post_verify_state
			= revalidate != NULL
				  ? strstr(revalidate, "cluster_pcm_x_master_pending_x_claim_state_exact(")
				  : NULL;
		normalize
			= post_verify_state != NULL ? strstr(post_verify_state, "PCM_X_QUEUE_NOT_READY") : NULL;
		UT_ASSERT_NOT_NULL(state);
		UT_ASSERT_NOT_NULL(reserve);
		UT_ASSERT_NOT_NULL(publish);
		UT_ASSERT_NOT_NULL(revalidate);
		UT_ASSERT_NOT_NULL(post_verify_state);
		UT_ASSERT_NOT_NULL(normalize);
		if (state != NULL && reserve != NULL && publish != NULL && revalidate != NULL
			&& post_verify_state != NULL && normalize != NULL)
			UT_ASSERT(state < reserve && reserve < publish && publish < revalidate
					  && revalidate < post_verify_state && post_verify_state < normalize
					  && normalize < end);
		clear = strstr(claim, "cluster_pcm_lock_clear_pending_x_if(");
		UT_ASSERT(clear == NULL || clear > end);
	}
	cancel = strstr(source, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	if (cancel != NULL)
		cancel = strstr(cancel + 1, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	end = cancel != NULL ? strstr(cancel, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(cancel);
	UT_ASSERT_NOT_NULL(end);
	if (cancel != NULL && end != NULL) {
		graph = strstr(cancel, "cluster_lmd_graph_remove_edge_by_waiter_exact_result(");
		stale = graph != NULL ? strstr(graph, "CLUSTER_LMD_GRAPH_REMOVE_STALE") : NULL;
		note_stale = stale != NULL
						 ? strstr(stale, "cluster_lmd_pcm_convert_wfg_note_exact_remove_stale(")
						 : NULL;
		fail_closed
			= note_stale != NULL ? strstr(note_stale, "cluster_pcm_x_runtime_fail_closed(") : NULL;
		clear = strstr(cancel, "cluster_pcm_lock_clear_queue_pending_x_exact(");
		finalize = strstr(cancel, "cluster_pcm_x_master_pending_x_cancel_finalize_exact(");
		UT_ASSERT_NOT_NULL(graph);
		UT_ASSERT_NOT_NULL(stale);
		UT_ASSERT_NOT_NULL(note_stale);
		UT_ASSERT_NOT_NULL(fail_closed);
		UT_ASSERT_NOT_NULL(clear);
		UT_ASSERT_NOT_NULL(finalize);
		if (graph != NULL && stale != NULL && note_stale != NULL && fail_closed != NULL
			&& clear != NULL && finalize != NULL)
			UT_ASSERT(graph < stale && stale < note_stale && note_stale < fail_closed
					  && fail_closed < clear && clear < finalize && finalize < end);
		prepare = strstr(cancel, "cluster_pcm_x_master_pending_x_cancel_prepare_exact(");
		UT_ASSERT(prepare == NULL || prepare > end);
	}
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_cancel_envelope(");
	end = handler != NULL ? strstr(handler, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(end);
	if (handler != NULL && end != NULL) {
		prepare = strstr(handler, "cluster_pcm_x_master_pending_x_cancel_prepare_exact(");
		cancel = prepare != NULL ? strstr(prepare, "gcs_block_pcm_x_cancel_claimed_probe_exact(")
								 : NULL;
		UT_ASSERT_NOT_NULL(prepare);
		UT_ASSERT_NOT_NULL(cancel);
		if (prepare != NULL && cancel != NULL)
			UT_ASSERT(prepare < cancel && cancel < end);
		UT_ASSERT(strstr(handler, "cluster_pcm_x_master_cancel_reversible_exact(") == NULL);
	}
	free(source);
}

UT_TEST(test_pcm_x_cancel_cleanup_classifies_exact_wfg_and_post_clear_failure)
{
	char *source = read_gcs_block_source();
	char *ordinary;
	char *claimed;
	char *end;
	char *exact;
	char *removed;
	char *stale;
	char *note_stale;
	char *fail_closed;
	char *finalize;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	ordinary = strstr(source, "\ncluster_gcs_pcm_x_remove_cancelled_waiter(");
	end = ordinary != NULL ? strstr(ordinary, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(ordinary);
	UT_ASSERT_NOT_NULL(end);
	if (ordinary != NULL && end != NULL) {
		exact = strstr(ordinary, "cluster_lmd_graph_remove_edge_by_waiter_exact_result(");
		removed = exact != NULL ? strstr(exact, "CLUSTER_LMD_GRAPH_REMOVE_REMOVED") : NULL;
		stale = removed != NULL ? strstr(removed, "CLUSTER_LMD_GRAPH_REMOVE_STALE") : NULL;
		note_stale = stale != NULL
						 ? strstr(stale, "cluster_lmd_pcm_convert_wfg_note_exact_remove_stale(")
						 : NULL;
		fail_closed
			= note_stale != NULL ? strstr(note_stale, "cluster_pcm_x_runtime_fail_closed(") : NULL;
		UT_ASSERT_NOT_NULL(exact);
		UT_ASSERT_NOT_NULL(removed);
		UT_ASSERT_NOT_NULL(stale);
		UT_ASSERT_NOT_NULL(note_stale);
		UT_ASSERT_NOT_NULL(fail_closed);
		if (exact != NULL && removed != NULL && stale != NULL && note_stale != NULL
			&& fail_closed != NULL)
			UT_ASSERT(exact < removed && removed < stale && stale < note_stale
					  && note_stale < fail_closed && fail_closed < end);
	}
	claimed = strstr(source, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	if (claimed != NULL)
		claimed = strstr(claimed + 1, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	end = claimed != NULL ? strstr(claimed, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(claimed);
	UT_ASSERT_NOT_NULL(end);
	if (claimed != NULL && end != NULL) {
		finalize = strstr(claimed,
						  "result = cluster_pcm_x_master_pending_x_cancel_finalize_exact(token)");
		fail_closed
			= finalize != NULL ? strstr(finalize, "cluster_pcm_x_runtime_fail_closed(") : NULL;
		UT_ASSERT_NOT_NULL(finalize);
		UT_ASSERT_NOT_NULL(fail_closed);
		if (finalize != NULL && fail_closed != NULL)
			UT_ASSERT(finalize < fail_closed && fail_closed < end);
	}
	free(source);
}

UT_TEST(test_pcm_x_terminal_retry_reclaims_cancel_cleanup_after_owner_death)
{
	char *source = read_gcs_block_source();
	char *cleanup;
	char *kick;
	char *end;
	char *prepare;
	char *claimed;
	char *ordinary;
	char *cancel_gate;
	char *cancel_ack;
	char *prehandle_ack;
	char *snapshot;
	char *stage;
	char *stage_second;
	char *detach;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	cleanup = strstr(source, "\ngcs_block_pcm_x_cancel_terminal_cleanup_exact(");
	end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(end);
	if (cleanup != NULL && end != NULL) {
		prepare = strstr(cleanup, "cluster_pcm_x_master_pending_x_cancel_prepare_exact(");
		claimed = prepare != NULL ? strstr(prepare, "gcs_block_pcm_x_cancel_claimed_probe_exact(")
								  : NULL;
		ordinary = prepare != NULL ? strstr(prepare, "cluster_gcs_pcm_x_remove_cancelled_waiter(")
								   : NULL;
		UT_ASSERT_NOT_NULL(prepare);
		UT_ASSERT_NOT_NULL(claimed);
		UT_ASSERT_NOT_NULL(ordinary);
		if (prepare != NULL && claimed != NULL && ordinary != NULL)
			UT_ASSERT(prepare < claimed && prepare < ordinary && claimed < end && ordinary < end);
	}
	kick = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	end = kick != NULL ? strstr(kick, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(kick);
	UT_ASSERT_NOT_NULL(end);
	if (kick != NULL && end != NULL) {
		cancel_gate = strstr(kick, "ref->grant_generation == 0");
		cleanup = cancel_gate != NULL
					  ? strstr(cancel_gate, "gcs_block_pcm_x_cancel_terminal_cleanup_exact(")
					  : NULL;
		snapshot = cleanup != NULL
					   ? strstr(cleanup, "cluster_pcm_x_master_cancel_ack_snapshot_exact(")
					   : NULL;
		prehandle_ack
			= snapshot != NULL ? strstr(snapshot, "PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK") : NULL;
		cancel_ack
			= prehandle_ack != NULL ? strstr(prehandle_ack, "PGRAC_IC_MSG_PCM_X_CANCEL_ACK") : NULL;
		stage = snapshot != NULL ? strstr(snapshot, "cluster_gcs_pcm_x_stage_frame(") : NULL;
		stage_second = stage != NULL ? strstr(stage + 1, "cluster_gcs_pcm_x_stage_frame(") : NULL;
		detach = strstr(kick, "cluster_pcm_x_master_detach_terminal_exact(");
		UT_ASSERT_NOT_NULL(cancel_gate);
		UT_ASSERT_NOT_NULL(cleanup);
		UT_ASSERT_NOT_NULL(snapshot);
		UT_ASSERT_NOT_NULL(prehandle_ack);
		UT_ASSERT_NOT_NULL(cancel_ack);
		UT_ASSERT_NOT_NULL(stage);
		UT_ASSERT_NOT_NULL(stage_second);
		UT_ASSERT_NOT_NULL(detach);
		if (cancel_gate != NULL && cleanup != NULL && snapshot != NULL && prehandle_ack != NULL
			&& cancel_ack != NULL && stage != NULL && stage_second != NULL && detach != NULL)
			UT_ASSERT(cancel_gate < cleanup && cleanup < snapshot && snapshot < prehandle_ack
					  && prehandle_ack < cancel_ack && snapshot < stage && stage < stage_second
					  && stage_second < detach && detach < end);
	}
	free(source);
}


UT_TEST(test_pcm_x_terminal_driver_selects_one_frozen_role_not_membership)
{
	char *source = read_gcs_block_source();
	const char *kick;
	const char *end;
	const char *select;
	const char *authenticate;
	const char *arm;
	const char *for_kind;
	const char *for_node;
	const char *membership_node;
	const char *capability_node;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	kick = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	end = kick != NULL ? strstr(kick, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(kick);
	UT_ASSERT_NOT_NULL(end);
	if (kick != NULL && end != NULL) {
		select = strstr(kick, "cluster_pcm_x_master_terminal_work_exact(");
		authenticate = select != NULL
			? strstr(select, "gcs_block_pcm_x_authenticated_session_result(") : NULL;
		arm = authenticate != NULL
			? strstr(authenticate, "cluster_pcm_x_master_terminal_leg_arm_exact(") : NULL;
		UT_ASSERT_NOT_NULL(select);
		UT_ASSERT_NOT_NULL(authenticate);
		UT_ASSERT_NOT_NULL(arm);
		if (select != NULL && authenticate != NULL && arm != NULL)
			UT_ASSERT(select < authenticate && authenticate < arm && arm < end);
		for_kind = strstr(kick, "for (kind =");
		for_node = strstr(kick, "for (node =");
		membership_node = strstr(kick, "cluster_membership_is_member(node)");
		capability_node = strstr(kick, "gcs_block_pcm_x_source_capable(node)");
		UT_ASSERT(for_kind == NULL || for_kind > end);
		UT_ASSERT(for_node == NULL || for_node > end);
		UT_ASSERT(membership_node == NULL || membership_node > end);
		UT_ASSERT(capability_node == NULL || capability_node > end);
	}
	free(source);
}


UT_TEST(test_pcm_x_invalidate_ack_matches_only_exact_unacked_holder)
{
	GcsBlockInvalidateAckPayload ack;
	PcmXMasterDriveSnapshot snapshot;

	memset(&ack, 0, sizeof(ack));
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.ref.identity.tag.spcOid = 11;
	snapshot.ref.identity.tag.dbOid = 12;
	snapshot.ref.identity.tag.relNumber = 13;
	snapshot.ref.identity.tag.blockNum = 14;
	snapshot.ref.identity.cluster_epoch = 0;
	snapshot.ref.identity.request_id = 19;
	snapshot.ticket_state = PCM_XT_ACTIVE_TRANSFER;
	snapshot.pending_s_holders_bitmap = (UINT32_C(1) << 1) | (UINT32_C(1) << 3);
	snapshot.acked_s_holders_bitmap = UINT32_C(1) << 1;
	ack.tag = snapshot.ref.identity.tag;
	ack.epoch = snapshot.ref.identity.cluster_epoch;
	ack.request_id = snapshot.ref.identity.request_id;
	ack.sender_node = 3;

	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_OK);
	ack.ack_status = GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_BUSY);
	ack.ack_status = 0;
	ack.sender_node = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 1),
				 PCM_X_QUEUE_DUPLICATE);
	ack.sender_node = 2;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 2),
				 PCM_X_QUEUE_STALE);
	ack.sender_node = 3;
	ack.ack_status = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_BAD_STATE);
	ack.ack_status = 0;
	ack.request_id++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_NOT_FOUND);
}


UT_TEST(test_pcm_x_invalidate_busy_routes_to_exact_ticket_backoff)
{
	char *source = read_gcs_block_source();
	const char *handler;
	const char *queue_busy;
	const char *backoff;
	const char *received;
	const char *queue_return;
	const char *legacy_busy;
	const char *transfer;
	const char *deadline;
	const char *revoke;
	const char *delay_helper;
	const char *delay_end;
	const char *leg_retry_delay;
	const char *execute;
	const char *self_capable_first;
	const char *self_capable_second;
	const char *execute_end;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_block_invalidate_ack_envelope(");
	queue_busy = handler != NULL ? strstr(handler, "queue_result == PCM_X_QUEUE_BUSY") : NULL;
	backoff = queue_busy != NULL
				  ? strstr(queue_busy, "cluster_pcm_x_master_invalidate_busy_backoff_exact(")
				  : NULL;
	received = backoff != NULL ? strstr(backoff, "invalidate_busy_received_count") : NULL;
	queue_return = received != NULL ? strstr(received, "if (!queue_positive)") : NULL;
	legacy_busy = queue_return != NULL
					  ? strstr(queue_return,
							   "ack->ack_status == GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY")
					  : NULL;
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(queue_busy);
	UT_ASSERT_NOT_NULL(backoff);
	UT_ASSERT_NOT_NULL(received);
	UT_ASSERT_NOT_NULL(queue_return);
	UT_ASSERT_NOT_NULL(legacy_busy);
	if (handler != NULL && queue_busy != NULL && backoff != NULL && received != NULL
		&& queue_return != NULL && legacy_busy != NULL)
		UT_ASSERT(handler < queue_busy && queue_busy < backoff && backoff < received
				  && received < queue_return && queue_return < legacy_busy);

	transfer = strstr(source, "\ngcs_block_pcm_x_master_drive_transfer(");
	deadline = transfer != NULL ? strstr(transfer, "snapshot->retry_deadline_ms") : NULL;
	revoke = transfer != NULL ? strstr(transfer, "cluster_pcm_x_master_revoke_arm_exact(") : NULL;
	UT_ASSERT_NOT_NULL(transfer);
	UT_ASSERT_NOT_NULL(deadline);
	UT_ASSERT_NOT_NULL(revoke);
	if (transfer != NULL && deadline != NULL && revoke != NULL)
		UT_ASSERT(transfer < deadline && deadline < revoke);

	/* reliable.retry_count counts repeated REVOKE arming while an earlier
	 * holder INVALIDATE is still missing; it is not an INVALIDATE-BUSY retry
	 * counter.  Feeding it into this delay can saturate the very first BUSY
	 * retry at 25s and phase-lock the denied reader's GRANT_PENDING window. */
	delay_helper = strstr(source, "\ngcs_block_pcm_x_invalidate_busy_retry_delay_ms(");
	delay_end = delay_helper != NULL ? strstr(delay_helper, "\n}") : NULL;
	leg_retry_delay = delay_helper != NULL ? strstr(delay_helper, "snapshot->retry_count") : NULL;
	UT_ASSERT_NOT_NULL(delay_helper);
	UT_ASSERT_NOT_NULL(delay_end);
	if (delay_helper != NULL && delay_end != NULL)
		UT_ASSERT(leg_retry_delay == NULL || leg_retry_delay > delay_end);

	/* Self has no HELLO capability record.  The local binary is nevertheless
	 * BUSY-capable on both GRANT_PENDING and pinned-S refusal arms. */
	execute = strstr(source, "\ngcs_block_invalidate_execute(");
	execute_end = execute != NULL ? strstr(execute, "\n}\n") : NULL;
	self_capable_first
		= execute != NULL ? strstr(execute, "inv->master_node == cluster_node_id") : NULL;
	self_capable_second = self_capable_first != NULL ? strstr(self_capable_first + 1,
															  "inv->master_node == cluster_node_id")
													 : NULL;
	UT_ASSERT_NOT_NULL(execute);
	UT_ASSERT_NOT_NULL(execute_end);
	UT_ASSERT_NOT_NULL(self_capable_first);
	UT_ASSERT_NOT_NULL(self_capable_second);
	if (execute != NULL && self_capable_first != NULL && self_capable_second != NULL
		&& execute_end != NULL)
		UT_ASSERT(execute < self_capable_first && self_capable_first < self_capable_second
				  && self_capable_second < execute_end);
	free(source);
}


UT_TEST(test_pcm_x_direct_invalidate_refusal_diagnosis_is_deterministic)
{
	char *source = read_gcs_block_source();
	const char *execute
		= source != NULL ? strstr(source, "\ngcs_block_invalidate_execute(") : NULL;
	const char *execute_end = execute != NULL ? strstr(execute, "\n}\n") : NULL;
	const char *queue_default
		= execute != NULL
			  ? strstr(execute, "PcmXQueueResult queue_result = PCM_X_QUEUE_INVALID;")
			  : NULL;
	const char *invalidate
		= execute != NULL ? strstr(execute, "switch (cluster_bufmgr_invalidate_block_for_gcs(") : NULL;
	const char *pinned
		= invalidate != NULL ? strstr(invalidate, "case CLUSTER_BUFMGR_GCS_DROP_PINNED:") : NULL;
	const char *stale
		= pinned != NULL ? strstr(pinned, "case CLUSTER_BUFMGR_GCS_DROP_STALE:") : NULL;
	const char *diagnosis
		= stale != NULL ? strstr(stale, "PCM-X queue INVALIDATE passive-S release refused") : NULL;
	const char *queue_detail
		= diagnosis != NULL ? strstr(diagnosis, "(int)queue_result") : NULL;
	const char *busy_peer
		= queue_detail != NULL ? strstr(queue_detail, "cluster_sf_peer_supports_gcs_inval_busy(") : NULL;
	const char *busy_counter
		= busy_peer != NULL ? strstr(busy_peer, "invalidate_busy_sent_count") : NULL;
	const char *busy_status
		= busy_counter != NULL
			  ? strstr(busy_counter, "GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY")
			  : NULL;
	const char *busy_ack = busy_status != NULL ? strstr(busy_status, "goto send_ack;") : NULL;
	const char *park = busy_ack != NULL ? strstr(busy_ack, "return false;") : NULL;
	const char *send_ack = park != NULL ? strstr(park, "\nsend_ack:") : NULL;

	UT_ASSERT_NOT_NULL(execute);
	UT_ASSERT_NOT_NULL(execute_end);
	UT_ASSERT_NOT_NULL(queue_default);
	UT_ASSERT_NOT_NULL(invalidate);
	UT_ASSERT_NOT_NULL(pinned);
	UT_ASSERT_NOT_NULL(stale);
	UT_ASSERT_NOT_NULL(diagnosis);
	UT_ASSERT_NOT_NULL(queue_detail);
	UT_ASSERT_NOT_NULL(busy_peer);
	UT_ASSERT_NOT_NULL(busy_counter);
	UT_ASSERT_NOT_NULL(busy_status);
	UT_ASSERT_NOT_NULL(busy_ack);
	UT_ASSERT_NOT_NULL(park);
	UT_ASSERT_NOT_NULL(send_ack);
	if (execute != NULL && execute_end != NULL && queue_default != NULL && invalidate != NULL
		&& pinned != NULL && stale != NULL && diagnosis != NULL && queue_detail != NULL
		&& busy_peer != NULL && busy_counter != NULL && busy_status != NULL && busy_ack != NULL
		&& park != NULL && send_ack != NULL)
		UT_ASSERT(execute < queue_default && queue_default < invalidate && invalidate < pinned
				  && pinned < stale && stale < diagnosis && diagnosis < queue_detail
				  && queue_detail < busy_peer && busy_peer < busy_counter
				  && busy_counter < busy_status && busy_status < busy_ack && busy_ack < park
				  && park < send_ack && send_ack < execute_end);
	free(source);
}


UT_TEST(test_pcm_x_local_pending_s_denial_match_is_attempt_exact)
{
	BufferTag slot_tag = { 0 };
	BufferTag inv_tag;

	slot_tag.spcOid = 31;
	slot_tag.dbOid = 32;
	slot_tag.relNumber = 33;
	slot_tag.forkNum = MAIN_FORKNUM;
	slot_tag.blockNum = 34;
	inv_tag = slot_tag;

	UT_ASSERT(GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_ABORTED, false, &inv_tag, UINT64_C(41), 2));

	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		false, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, true, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, true, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_X, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	inv_tag.blockNum++;
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	inv_tag = slot_tag;
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(40), 2,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 1,
		GCS_BLOCK_DIRECT_UNARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_ARMED, false, &inv_tag, UINT64_C(41), 2));
	UT_ASSERT(!GcsBlockLocalPendingSDenialMatches(
		true, false, false, (uint8)PCM_TRANS_N_TO_S, &slot_tag, UINT64_C(41), 2,
		GCS_BLOCK_DIRECT_UNARMED, true, &inv_tag, UINT64_C(41), 2));
}


UT_TEST(test_pcm_x_grant_pending_invalidate_wakes_local_s_before_busy)
{
	char *source = read_gcs_block_source();
	const char *direct_prepare;
	const char *direct_prepare_end;
	const char *direct_first_reply_check;
	const char *direct_target_prepare;
	const char *direct_second_reply_check;
	const char *direct_target_cleanup;
	const char *helper;
	const char *execute;
	const char *pending;
	const char *wake;
	const char *busy;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	direct_prepare = strstr(source, "\ngcs_block_direct_prepare_attempt(");
	direct_prepare_end = direct_prepare != NULL ? strstr(direct_prepare, "\n}\n\n\n") : NULL;
	direct_first_reply_check
		= direct_prepare != NULL ? strstr(direct_prepare, "slot->reply_received") : NULL;
	direct_target_prepare = direct_first_reply_check != NULL
								? strstr(direct_first_reply_check,
										 "cluster_bufmgr_prepare_direct_land_target_for_gcs(")
								: NULL;
	direct_second_reply_check = direct_target_prepare != NULL
									? strstr(direct_target_prepare, "slot->reply_received")
									: NULL;
	direct_target_cleanup
		= direct_second_reply_check != NULL
			  ? strstr(direct_second_reply_check, "gcs_block_direct_finish_target(buf, true, false")
			  : NULL;
	helper = strstr(source, "\ngcs_block_wake_local_pending_s_request(");
	execute = strstr(source, "\ngcs_block_invalidate_execute(");
	pending
		= execute != NULL ? strstr(execute, "cluster_bufmgr_block_grant_pending(inv->tag)") : NULL;
	wake = pending != NULL ? strstr(pending, "gcs_block_wake_local_pending_s_request(inv)") : NULL;
	busy = wake != NULL ? strstr(wake, "invalidate_busy_sent_count") : NULL;
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(direct_prepare);
	UT_ASSERT_NOT_NULL(direct_prepare_end);
	UT_ASSERT_NOT_NULL(direct_first_reply_check);
	UT_ASSERT_NOT_NULL(direct_target_prepare);
	UT_ASSERT_NOT_NULL(direct_second_reply_check);
	UT_ASSERT_NOT_NULL(direct_target_cleanup);
	UT_ASSERT_NOT_NULL(execute);
	UT_ASSERT_NOT_NULL(pending);
	UT_ASSERT_NOT_NULL(wake);
	UT_ASSERT_NOT_NULL(busy);
	if (direct_prepare != NULL && direct_prepare_end != NULL && direct_first_reply_check != NULL
		&& direct_target_prepare != NULL && direct_second_reply_check != NULL
		&& direct_target_cleanup != NULL)
		UT_ASSERT(direct_prepare < direct_first_reply_check
				  && direct_first_reply_check < direct_target_prepare
				  && direct_target_prepare < direct_second_reply_check
				  && direct_second_reply_check < direct_target_cleanup
				  && direct_target_cleanup < direct_prepare_end);
	if (helper != NULL && execute != NULL && pending != NULL && wake != NULL && busy != NULL)
		UT_ASSERT(helper < execute && execute < pending && pending < wake && wake < busy);
	free(source);
}


UT_TEST(test_pcm_x_grant_pending_orphan_observation_is_identity_exact)
{
	char *gcs_source = read_gcs_block_source();
	char *bufmgr_source = read_source_path(BUFMGR_SOURCE_PATH);
	const char *execute;
	const char *pending;
	const char *wake;
	const char *observe;
	const char *busy;
	const char *retry;
	const char *abort;
	const char *abort_observe;
	const char *release;
	const char *release_end;
	const char *release_live;
	const char *direct_fail;
	const char *direct_fail_end;
	const char *direct_finish;
	const char *direct_abort_observe;

	UT_ASSERT_NOT_NULL(gcs_source);
	UT_ASSERT_NOT_NULL(bufmgr_source);
	if (gcs_source == NULL || bufmgr_source == NULL) {
		free(gcs_source);
		free(bufmgr_source);
		return;
	}

	execute = strstr(gcs_source, "\ngcs_block_invalidate_execute(");
	pending
		= execute != NULL ? strstr(execute, "cluster_bufmgr_block_grant_pending(inv->tag)") : NULL;
	wake = pending != NULL
			   ? strstr(pending, "woke_local = gcs_block_wake_local_pending_s_request(inv)")
			   : NULL;
	observe = wake != NULL
				  ? strstr(wake, "gcs_block_observe_grant_pending_invalidate(inv, woke_local)")
				  : NULL;
	busy = observe != NULL ? strstr(observe, "invalidate_busy_sent_count") : NULL;
	UT_ASSERT_NOT_NULL(execute);
	UT_ASSERT_NOT_NULL(pending);
	UT_ASSERT_NOT_NULL(wake);
	UT_ASSERT_NOT_NULL(observe);
	UT_ASSERT_NOT_NULL(busy);
	if (execute != NULL && pending != NULL && wake != NULL && observe != NULL && busy != NULL)
		UT_ASSERT(execute < pending && pending < wake && wake < observe && observe < busy);

	/* The diagnostic must report why a candidate was rejected without replacing
	 * or weakening the established attempt-exact protocol predicate. */
	UT_ASSERT_NOT_NULL(strstr(gcs_source, "GcsBlockLocalPendingSDenialMatches("));
	UT_ASSERT_NOT_NULL(strstr(gcs_source, "grant-pending invalidate observation:"));
	UT_ASSERT_NOT_NULL(strstr(gcs_source, "slot_backend=%d slot_index=%d slot_request_id="));
	UT_ASSERT_NOT_NULL(strstr(gcs_source, "direct_state=%d direct_prepared=%d reject_mask=0x%x"));

	retry = strstr(bufmgr_source, "\ncluster_bufmgr_pcm_retry_denied_rearm(");
	abort = retry != NULL
				? strstr(retry,
						 "cluster_pcm_own_abort_grant_reservation(buf, base, *reservation_token)")
				: NULL;
	abort_observe
		= abort != NULL ? strstr(abort, "cluster PCM pending-X exact abort observation:") : NULL;
	UT_ASSERT_NOT_NULL(retry);
	UT_ASSERT_NOT_NULL(abort);
	UT_ASSERT_NOT_NULL(abort_observe);
	if (retry != NULL && abort != NULL && abort_observe != NULL)
		UT_ASSERT(retry < abort && abort < abort_observe);

	release = strstr(gcs_source, "\ngcs_block_release_slot(");
	release_end = release != NULL ? strstr(release, "\n}\n") : NULL;
	release_live
		= release != NULL
			  ? strstr(release,
					   "cluster GCS block slot released with live direct target observation:")
			  : NULL;
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(release_end);
	UT_ASSERT_NOT_NULL(release_live);
	if (release != NULL && release_end != NULL && release_live != NULL)
		UT_ASSERT(release < release_live && release_live < release_end);

	direct_fail = strstr(gcs_source, "\ngcs_block_direct_fail_slot(");
	direct_fail_end = direct_fail != NULL ? strstr(direct_fail, "\n}\n") : NULL;
	direct_finish
		= direct_fail != NULL
			  ? strstr(direct_fail, "gcs_block_direct_finish_target(target_buf, prepared, false")
			  : NULL;
	direct_abort_observe
		= direct_finish != NULL
			  ? strstr(direct_finish, "cluster GCS block direct abort observation:")
			  : NULL;
	UT_ASSERT_NOT_NULL(direct_fail);
	UT_ASSERT_NOT_NULL(direct_fail_end);
	UT_ASSERT_NOT_NULL(direct_finish);
	UT_ASSERT_NOT_NULL(direct_abort_observe);
	if (direct_fail != NULL && direct_fail_end != NULL && direct_finish != NULL
		&& direct_abort_observe != NULL)
		UT_ASSERT(direct_fail < direct_finish && direct_finish < direct_abort_observe
				  && direct_abort_observe < direct_fail_end);

	free(gcs_source);
	free(bufmgr_source);
}


UT_TEST(test_pcm_x_final_ack_builds_exact_grd_handoff_token)
{
	PcmAuthoritySnapshot authority;
	PcmXGrdHandoffToken handoff;
	PcmXMasterFinalAckToken final;

	memset(&authority, 0, sizeof(authority));
	memset(&final, 0, sizeof(final));
	final.final_ack.ref.identity.tag.spcOid = 21;
	final.final_ack.ref.identity.tag.dbOid = 22;
	final.final_ack.ref.identity.tag.relNumber = 23;
	final.final_ack.ref.identity.tag.blockNum = 24;
	final.final_ack.ref.identity.node_id = 3;
	final.final_ack.ref.identity.procno = 25;
	final.final_ack.ref.identity.cluster_epoch = 0;
	final.final_ack.ref.identity.request_id = 27;
	final.final_ack.ref.handle.ticket_id = 35;
	final.final_ack.ref.grant_generation = 28;
	final.final_ack.image_id = 29;
	final.final_ack.committed_own_generation = 30;
	final.image.image_id = final.final_ack.image_id;
	/* Generation zero is the legal first ownership generation. */
	final.image.source_own_generation = 0;
	final.image.page_scn = 32;
	final.image.page_lsn = 33;
	final.image.source_node = 2;
	final.image.page_checksum = 34;

	UT_ASSERT(cluster_gcs_pcm_x_grd_handoff_token_build(&final, &authority, &handoff));
	UT_ASSERT(BufferTagsEqual(&handoff.tag, &final.final_ack.ref.identity.tag));
	UT_ASSERT_EQ(handoff.cluster_epoch, 0);
	UT_ASSERT_EQ(handoff.request_id, 27);
	UT_ASSERT_EQ(handoff.ticket_id, 35);
	UT_ASSERT_EQ(handoff.grant_generation, 28);
	UT_ASSERT_EQ(handoff.image_id, 29);
	UT_ASSERT_EQ(handoff.source_own_generation, 0);
	UT_ASSERT_EQ(handoff.requester_node, 3);
	UT_ASSERT_EQ(handoff.requester_procno, 25);
	UT_ASSERT_EQ(handoff.source_node, 2);
	UT_ASSERT_EQ(handoff.page_checksum, 34);
	final.image.image_id++;
	UT_ASSERT(!cluster_gcs_pcm_x_grd_handoff_token_build(&final, &authority, &handoff));
}


UT_TEST(test_pcm_x_final_ack_fail_closed_names_exact_handoff_stage)
{
	char *source = read_gcs_block_source();
	const char *handler
		= source != NULL ? strstr(source, "\ncluster_gcs_handle_pcm_x_final_ack_envelope(") : NULL;
	const char *handler_end
		= handler != NULL ? strstr(handler, "\ncluster_gcs_handle_pcm_x_final_commit_ack_envelope(")
						  : NULL;
	const char *stage_log
		= handler != NULL ? strstr(handler, "PCM-X FINAL_ACK fail-closed at %s") : NULL;
	const char *canonical
		= handler != NULL ? strstr(handler, "cluster_pcm_x_runtime_fail_closed()") : NULL;
	const char *direct
		= handler != NULL ? strstr(handler, "cluster_pcm_x_runtime_transition(") : NULL;
	const char *master_holder = handler != NULL ? strstr(handler, "master_holder=%u") : NULL;
	const char *image_page_scn = handler != NULL ? strstr(handler, "image_page_scn=%llu") : NULL;
	const char *watermark_scn = handler != NULL ? strstr(handler, "watermark_scn=%llu") : NULL;
	const char *watermark_source = handler != NULL ? strstr(handler, "wm_src=%s") : NULL;
	const char *watermark_sender = handler != NULL ? strstr(handler, "wm_sender=%d") : NULL;
	const char *watermark_request = handler != NULL ? strstr(handler, "wm_request_id=%llu") : NULL;
	const char *watermark_old = handler != NULL ? strstr(handler, "wm_old_scn=%llu") : NULL;
	const char *watermark_new = handler != NULL ? strstr(handler, "wm_new_scn=%llu") : NULL;

	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(handler_end);
	UT_ASSERT_NOT_NULL(stage_log);
	UT_ASSERT_NOT_NULL(canonical);
	UT_ASSERT_NOT_NULL(master_holder);
	UT_ASSERT_NOT_NULL(image_page_scn);
	UT_ASSERT_NOT_NULL(watermark_scn);
	UT_ASSERT_NOT_NULL(watermark_source);
	UT_ASSERT_NOT_NULL(watermark_sender);
	UT_ASSERT_NOT_NULL(watermark_request);
	UT_ASSERT_NOT_NULL(watermark_old);
	UT_ASSERT_NOT_NULL(watermark_new);
	UT_ASSERT_NULL(source != NULL ? strstr(source, "cluster_pcm_x_runtime_transition(") : NULL);
	/* A blocked runtime must always publish the canonical counter and file:line arm. */
	if (handler != NULL && handler_end != NULL && stage_log != NULL && canonical != NULL)
		UT_ASSERT(handler < stage_log && stage_log < canonical && canonical < handler_end);
	if (handler != NULL && handler_end != NULL)
		UT_ASSERT(direct == NULL || direct >= handler_end);
	free(source);
}


UT_TEST(test_pcm_x_holder_image_evidence_never_uses_generation_as_presence)
{
	PcmXLocalHolderProgress progress;
	PcmXTicketRef ref;

	memset(&progress, 0, sizeof(progress));
	memset(&ref, 0, sizeof(ref));
	ref.identity.node_id = 1;
	ref.identity.cluster_epoch = 2;
	ref.identity.request_id = 3;
	ref.handle.ticket_id = 4;
	ref.handle.queue_generation = 5;
	ref.grant_generation = 6;
	progress.ref = ref;
	progress.image.image_id = 7;
	progress.image.source_node = 2;
	progress.image.source_own_generation = 0;

	/* REVOKE has already published image_id, but that alone is not READY. */
	UT_ASSERT(!cluster_gcs_pcm_x_holder_image_ready_exact(&progress, &ref, 7, 2));
	progress.last_response_opcode = PGRAC_IC_MSG_PCM_X_REVOKE;
	progress.pending_opcode = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	progress.phase = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	UT_ASSERT(cluster_gcs_pcm_x_holder_image_ready_exact(&progress, &ref, 7, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_holder_image_ready_exact(&progress, &ref, 7, 3));

	progress.pending_opcode = 0;
	progress.phase = 0;
	progress.last_response_opcode = PGRAC_IC_MSG_PCM_X_DRAIN_POLL;
	progress.flags = PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	UT_ASSERT(cluster_gcs_pcm_x_holder_image_drained_exact(&progress, &ref, 7, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_holder_image_drained_exact(&progress, &ref, 8, 2));
}


UT_TEST(test_pcm_x_pending_x_marker_is_only_a_pre_handoff_gate)
{
	UT_ASSERT(cluster_gcs_pcm_x_transfer_pre_handoff_phase(0));
	UT_ASSERT(cluster_gcs_pcm_x_transfer_pre_handoff_phase(PGRAC_IC_MSG_PCM_X_REVOKE));
	UT_ASSERT(!cluster_gcs_pcm_x_transfer_pre_handoff_phase(PGRAC_IC_MSG_PCM_X_PREPARE_GRANT));
	UT_ASSERT(!cluster_gcs_pcm_x_transfer_pre_handoff_phase(PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK));
}


UT_TEST(test_pcm_x_ready_publication_follows_exact_retained_commit)
{
	char *source = read_gcs_block_source();
	const char *begin;
	const char *materialize;
	const char *finish;
	const char *publish;
	const char *send;
	const char *end;
	const char *wrapper;
	const char *wrapper_end;
	const char *wrapper_catch;
	const char *copy_error;
	const char *flush_error;
	const char *preserve;
	const char *fail_closed;
	const char *return_corrupt;
	const char *rollback;

	if (source == NULL)
		return;
	begin = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL) {
		free(source);
		return;
	}
	end = strstr(begin + 1, "\n}\n\n\n");
	materialize = strstr(begin, "cluster_gcs_block_dedup_pcm_x_materialize(");
	finish = strstr(begin, "gcs_block_pcm_x_finish_revoke_retain(");
	publish = strstr(begin, "cluster_gcs_block_dedup_pcm_x_publish_ready_exact(");
	send = strstr(begin, "gcs_block_pcm_x_stage_ready_work(");
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(materialize);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(publish);
	UT_ASSERT_NOT_NULL(send);
	if (end != NULL && materialize != NULL && finish != NULL && publish != NULL && send != NULL)
		UT_ASSERT(materialize < finish && finish < publish && publish < send && send < end);
	wrapper = strstr(source, "\ngcs_block_pcm_x_finish_revoke_retain(");
	wrapper_end = wrapper != NULL ? strstr(wrapper, "\n}\n") : NULL;
	wrapper_catch = wrapper != NULL ? strstr(wrapper, "PG_CATCH();") : NULL;
	copy_error = wrapper_catch != NULL ? strstr(wrapper_catch, "CopyErrorData();") : NULL;
	flush_error = copy_error != NULL ? strstr(copy_error, "FlushErrorState();") : NULL;
	preserve
		= flush_error != NULL
			  ? strstr(flush_error, "cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(")
			  : NULL;
	fail_closed
		= preserve != NULL ? strstr(preserve, "cluster_pcm_x_runtime_fail_closed();") : NULL;
	return_corrupt
		= fail_closed != NULL ? strstr(fail_closed, "result = CLUSTER_PCM_OWN_CORRUPT;") : NULL;
	rollback = wrapper_catch != NULL
				   ? strstr(wrapper_catch, "gcs_block_pcm_x_abort_image_before_finish(")
				   : NULL;
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper_end);
	UT_ASSERT_NOT_NULL(wrapper_catch);
	UT_ASSERT_NOT_NULL(copy_error);
	UT_ASSERT_NOT_NULL(flush_error);
	UT_ASSERT_NOT_NULL(preserve);
	UT_ASSERT_NOT_NULL(strstr(wrapper_catch, "PCM-X finish-error evidence exact"));
	UT_ASSERT_NOT_NULL(strstr(wrapper_catch, "preserve_result"));
	UT_ASSERT_NOT_NULL(strstr(wrapper_catch, "work->key"));
	UT_ASSERT_NOT_NULL(strstr(wrapper_catch, "work->binding.identity.ref"));
	UT_ASSERT_NOT_NULL(strstr(wrapper_catch, "revoking->reservation_token"));
	UT_ASSERT_NOT_NULL(fail_closed);
	UT_ASSERT_NOT_NULL(return_corrupt);
	if (wrapper != NULL && wrapper_end != NULL && wrapper_catch != NULL && copy_error != NULL
		&& flush_error != NULL && preserve != NULL && fail_closed != NULL
		&& return_corrupt != NULL) {
		UT_ASSERT(wrapper < wrapper_catch && wrapper_catch < copy_error && copy_error < flush_error
				  && flush_error < preserve && preserve < fail_closed
				  && fail_closed < return_corrupt && return_corrupt < wrapper_end);
		UT_ASSERT(rollback == NULL || rollback >= wrapper_end);
		UT_ASSERT(strstr(wrapper_catch, "PG_RE_THROW();") == NULL
				  || strstr(wrapper_catch, "PG_RE_THROW();") >= wrapper_end);
	}
	free(source);
}


UT_TEST(test_pcm_x_ready_materializes_exact_n_s_or_x_source_without_wire_change)
{
	char *source = read_gcs_block_source();
	const char *abort;
	const char *begin;
	const char *copy;
	const char *finish;
	const char *materialize;
	const char *publish;
	const char *revoke_handler;
	const char *drain;
	const char *generic_install;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	begin = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	abort = strstr(source, "\ngcs_block_pcm_x_abort_image_before_finish(");
	revoke_handler = strstr(source, "\ncluster_gcs_handle_pcm_x_revoke_envelope(");
	drain = strstr(source, "\ngcs_block_pcm_x_local_drain_apply_exact(");
	generic_install = strstr(source, "\ngcs_block_install_block(");
	UT_ASSERT_NOT_NULL(begin);
	UT_ASSERT_NOT_NULL(abort);
	UT_ASSERT_NOT_NULL(revoke_handler);
	UT_ASSERT_NOT_NULL(drain);
	UT_ASSERT_NOT_NULL(generic_install);
	if (revoke_handler != NULL) {
		UT_ASSERT_NOT_NULL(strstr(revoke_handler, "own_snapshot.pcm_state == (uint8)PCM_STATE_N"));
		UT_ASSERT_NOT_NULL(strstr(revoke_handler, "own_snapshot.pcm_state == (uint8)PCM_STATE_S"));
		UT_ASSERT_NOT_NULL(strstr(revoke_handler, "own_snapshot.pcm_state == (uint8)PCM_STATE_X"));
	}
	if (begin != NULL) {
		UT_ASSERT_NOT_NULL(strstr(begin, "current.pcm_state == (uint8)PCM_STATE_N"));
		UT_ASSERT_NOT_NULL(strstr(begin, "current.pcm_state == (uint8)PCM_STATE_S"));
		UT_ASSERT_NOT_NULL(strstr(begin, "current.pcm_state == (uint8)PCM_STATE_X"));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_bufmgr_pcm_own_prepare_n_source_image("));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_bufmgr_pcm_own_prepare_s_source_image("));
		UT_ASSERT_NOT_NULL(strstr(begin, "binding.required_page_scn"));
		UT_ASSERT_NOT_NULL(strstr(begin, "&source_prepare_refusal"));
		UT_ASSERT_NOT_NULL(strstr(source, "materialize-begin-s-content-lock"));
		UT_ASSERT_NOT_NULL(strstr(source, "materialize-begin-s-dirty-flushed"));
		UT_ASSERT_NOT_NULL(strstr(source, "materialize-begin-s-dirty-raced"));
		UT_ASSERT_NOT_NULL(strstr(source, "materialize-begin-s-io-in-progress"));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_bufmgr_pcm_own_begin_x_revoke("));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_pcm_x_revoke_finish_mode("));
		UT_ASSERT_NOT_NULL(strstr(begin, "CLUSTER_PCM_X_REVOKE_FINISH_DROP"));
		copy = strstr(begin, "cluster_bufmgr_copy_block_for_gcs(");
		materialize = strstr(begin, "cluster_gcs_block_dedup_pcm_x_materialize(");
		finish = strstr(begin, "gcs_block_pcm_x_finish_revoke_retain(");
		publish = strstr(begin, "cluster_gcs_block_dedup_pcm_x_publish_ready_exact(");
		UT_ASSERT_NOT_NULL(copy);
		UT_ASSERT_NOT_NULL(materialize);
		UT_ASSERT_NOT_NULL(finish);
		UT_ASSERT_NOT_NULL(publish);
		if (copy != NULL && materialize != NULL && finish != NULL && publish != NULL)
			UT_ASSERT(copy < materialize && materialize < finish && finish < publish);
	}
	if (abort != NULL) {
		UT_ASSERT_NOT_NULL(strstr(abort, "cluster_bufmgr_pcm_own_abort_n_revoke("));
		UT_ASSERT_NOT_NULL(strstr(abort, "cluster_bufmgr_pcm_own_abort_s_revoke("));
		UT_ASSERT_NOT_NULL(strstr(abort, "cluster_bufmgr_pcm_own_abort_x_revoke("));
	}
	if (drain != NULL) {
		const char *pair_prepare = strstr(drain,
			"cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(");
		const char *pair_release = pair_prepare != NULL
			? strstr(pair_prepare, "cluster_bufmgr_pcm_own_release_retained_image(")
			: NULL;
		const char *pair_commit = pair_release != NULL
			? strstr(pair_release,
				"cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(")
			: NULL;
		const char *local_drain
			= strstr(drain, "cluster_pcm_x_local_drain_poll_certificate_exact(");
		const char *duplicate_guard
			= local_drain != NULL
				  ? strstr(local_drain,
						   "if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)")
				  : NULL;
		const char *drain_status
			= duplicate_guard != NULL
				  ? strstr(duplicate_guard, "cluster_gcs_block_dedup_pcm_x_drain_status_exact(")
				  : NULL;
		const char *drained_replay
			= drain_status != NULL ? strstr(drain_status, "GCS_BLOCK_PCM_X_IMAGE_DUPLICATE") : NULL;
		/* The self-source release authority is the exact completion
		 * certificate.  Once local DRAIN is durable, an inexact ledger cannot
		 * be retried from a fabricated certificate and must preserve evidence
		 * under the runtime fuse . */
		const char *certificate_policy = strstr(drain, "source_own_generation + 1");
		const char *certificate_refusal
			= certificate_policy != NULL ? strstr(certificate_policy, "PCM_X_QUEUE_CORRUPT") : NULL;
		const char *release_record = strstr(drain, "cluster_gcs_block_dedup_pcm_x_release_exact(");
		const char *finish_mode_gate = strstr(drain, "cluster_pcm_x_revoke_finish_mode(");
		const char *drop_arm = strstr(drain, "CLUSTER_PCM_X_REVOKE_FINISH_DROP");
		const char *release_retained
			= finish_mode_gate != NULL
				  ? strstr(finish_mode_gate,
						   "cluster_bufmgr_pcm_own_release_retained_image(")
				  : NULL;

		UT_ASSERT_NOT_NULL(pair_prepare);
		UT_ASSERT_NOT_NULL(pair_release);
		UT_ASSERT_NOT_NULL(pair_commit);
		UT_ASSERT_NOT_NULL(local_drain);
		UT_ASSERT_NOT_NULL(duplicate_guard);
		UT_ASSERT_NOT_NULL(drain_status);
		UT_ASSERT_NOT_NULL(drained_replay);
		UT_ASSERT_NOT_NULL(certificate_policy);
		UT_ASSERT_NOT_NULL(certificate_refusal);
		UT_ASSERT_NOT_NULL(release_record);
		UT_ASSERT_NOT_NULL(finish_mode_gate);
		UT_ASSERT_NOT_NULL(drop_arm);
		UT_ASSERT_NOT_NULL(release_retained);
		if (pair_prepare != NULL && pair_release != NULL && pair_commit != NULL
			&& local_drain != NULL)
			UT_ASSERT(pair_prepare < pair_release && pair_release < pair_commit
					  && pair_commit < local_drain);
		if (local_drain != NULL && duplicate_guard != NULL && drain_status != NULL
			&& drained_replay != NULL && certificate_policy != NULL && certificate_refusal != NULL
			&& release_record != NULL && finish_mode_gate != NULL && drop_arm != NULL
			&& release_retained != NULL)
			UT_ASSERT(local_drain < duplicate_guard && duplicate_guard < drain_status
					  && drain_status < drained_replay && drained_replay < certificate_policy
					  && certificate_policy < certificate_refusal
					  && certificate_refusal < finish_mode_gate
					  && finish_mode_gate < release_retained && release_retained < drop_arm
					  && drop_arm < release_record);
	}
	if (generic_install != NULL) {
		const char *content = strstr(generic_install, "LWLockAcquire(content_lock, LW_EXCLUSIVE)");
		const char *gate
			= strstr(generic_install, "cluster_bufmgr_pcm_x_content_write_permitted(buf)");
		const char *copy = strstr(generic_install, "memcpy(page, block_data");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(copy);
		if (content != NULL && gate != NULL && copy != NULL)
			UT_ASSERT(content < gate && gate < copy);
	}
	free(source);
}


UT_TEST(test_pcm_x_s_source_hard_failure_observation_is_reason_exact)
{
	char *source = read_source_path(BUFMGR_SOURCE_PATH);
	const char *observe;
	const char *prepare;
	const char *prepare_end;
	static const char *const reasons[] = { "CLUSTER_PCM_S_SOURCE_HARD_INITIAL_CURRENT_IMAGE",
										   "CLUSTER_PCM_S_SOURCE_HARD_INITIAL_IO_ERROR",
										   "CLUSTER_PCM_S_SOURCE_HARD_BEGIN_REVOKE_CORRUPT",
										   "CLUSTER_PCM_S_SOURCE_HARD_STORAGE_VERIFY",
										   "CLUSTER_PCM_S_SOURCE_HARD_POST_LOCK_CURRENT_IMAGE",
										   "CLUSTER_PCM_S_SOURCE_HARD_POST_LOCK_IO_ERROR",
										   "CLUSTER_PCM_S_SOURCE_HARD_NO_COVER",
										   "CLUSTER_PCM_S_SOURCE_HARD_ABORT_FAILURE" };
	int i;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	observe = strstr(source, "\ncluster_bufmgr_pcm_own_observe_s_source_hard_failure(");
	prepare = strstr(source, "\ncluster_bufmgr_pcm_own_prepare_s_source_image(");
	prepare_end
		= prepare != NULL
			  ? strstr(prepare, "\n/* Abort only the matching S-source staging reservation. */")
			  : NULL;
	UT_ASSERT_NOT_NULL(observe);
	UT_ASSERT_NOT_NULL(prepare);
	UT_ASSERT_NOT_NULL(prepare_end);
	for (i = 0; i < lengthof(reasons); i++)
		UT_ASSERT_NOT_NULL(strstr(source, reasons[i]));

	/* Hard-failure evidence is local-only and state-change suppressed.  It
	 * identifies the exact ownership tuple and records both sides of the SCN
	 * floor comparison without changing the prepare function's public API. */
	UT_ASSERT_NOT_NULL(strstr(source, "ClusterPcmOwnSSourceHardFailureObservation cache[8]"));
	UT_ASSERT_NOT_NULL(strstr(source, "memcmp(&cache[cache_idx], &obs, sizeof(obs)) == 0"));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster PCM S-source hard failure observation:"));
	UT_ASSERT_NOT_NULL(strstr(source, "reason=%s cause=%s result=%d abort_result=%d"));
	UT_ASSERT_NOT_NULL(strstr(source, "buffer=%d spc=%u db=%u rel=%u fork=%d blk=%u"));
	UT_ASSERT_NOT_NULL(strstr(source, "state=%u generation=%llu token=%llu flags=0x%x"));
	UT_ASSERT_NOT_NULL(strstr(source, "required_scn=%llu local_scn=%llu storage_scn=%llu"));
	UT_ASSERT_NOT_NULL(strstr(source, "buffer_state=0x%x buffer_type=%u"));
	if (observe != NULL && prepare != NULL)
		UT_ASSERT(observe < prepare);
	if (prepare != NULL && prepare_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(prepare, "hard_failure_reason ="));
		UT_ASSERT_NOT_NULL(
			strstr(prepare, "cluster_bufmgr_pcm_own_observe_s_source_hard_failure("));
		UT_ASSERT(strstr(prepare, "cluster_bufmgr_pcm_own_observe_s_source_hard_failure(")
				  < prepare_end);
	}
	free(source);
}


UT_TEST(test_pcm_x_self_and_remote_drain_share_full_image_release_wrapper)
{
	char *source = read_gcs_block_source();
	const char *apply;
	const char *handler;
	const char *handler_end;
	const char *raw_drain;
	const char *stage;
	const char *terminal;
	const char *terminal_end;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_drain_poll_envelope(");
	terminal = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(terminal);
	if (handler != NULL) {
		handler_end = strstr(handler + 1, "\n}\n\n\n");
		apply = strstr(handler, "gcs_block_pcm_x_local_drain_apply_exact(");
		raw_drain = strstr(handler, "cluster_pcm_x_local_drain_poll_exact(");
		UT_ASSERT_NOT_NULL(handler_end);
		UT_ASSERT_NOT_NULL(apply);
		if (handler_end != NULL && apply != NULL)
			UT_ASSERT(apply < handler_end);
		if (handler_end != NULL && raw_drain != NULL)
			UT_ASSERT(raw_drain > handler_end);
	}
	if (terminal != NULL) {
		terminal_end = strstr(terminal + 1, "\n}\n\n\n");
		stage = strstr(terminal, "cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_DRAIN_POLL");
		UT_ASSERT_NOT_NULL(terminal_end);
		UT_ASSERT_NOT_NULL(stage);
		if (terminal_end != NULL && stage != NULL)
			UT_ASSERT(stage < terminal_end);
		/* Self is not a special raw-drain arm: every participant, local or
		 * remote, enters the same authenticated envelope handler above. */
		if (terminal_end != NULL) {
			raw_drain = strstr(terminal, "cluster_pcm_x_local_drain_poll_exact(");
			apply = strstr(terminal, "gcs_block_pcm_x_local_drain_apply_exact(");
			if (raw_drain != NULL)
				UT_ASSERT(raw_drain > terminal_end);
			if (apply != NULL)
				UT_ASSERT(apply > terminal_end);
		}
	}
	free(source);
}


UT_TEST(test_pcm_x_ready_admission_marks_before_send_and_rolls_back_refusal)
{
	char *source = read_gcs_block_source();
	char *outbound_source = read_source_path(LMS_OUTBOUND_SOURCE_PATH);
	const char *begin;
	const char *end;
	const char *arm;
	const char *arm_refusal;
	const char *mark;
	const char *send;
	const char *rollback;
	const char *handler;
	const char *handler_end;
	const char *prepare_handler;
	const char *prepare_handler_end;

	if (source == NULL || outbound_source == NULL) {
		free(source);
		free(outbound_source);
		return;
	}
	begin = strstr(source, "\ngcs_block_pcm_x_stage_ready_work(");
	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL) {
		free(source);
		return;
	}
	end = strstr(begin + 1, "\n}\n\n\n");
	arm = strstr(begin, "cluster_pcm_x_local_holder_image_ready_arm_exact_diagnosed(");
	arm_refusal = strstr(begin, "gcs_block_pcm_x_image_ready_arm_refusal_note_work(");
	mark = strstr(begin, "cluster_gcs_block_dedup_pcm_x_mark_staged_exact(");
	send = strstr(begin, "cluster_gcs_pcm_x_stage_frame(");
	rollback = strstr(begin, "cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(");
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(arm);
	UT_ASSERT_NOT_NULL(arm_refusal);
	UT_ASSERT_NOT_NULL(mark);
	UT_ASSERT_NOT_NULL(send);
	UT_ASSERT_NOT_NULL(rollback);
	if (end != NULL && arm != NULL && arm_refusal != NULL && mark != NULL && send != NULL
		&& rollback != NULL)
		UT_ASSERT(arm < arm_refusal && arm_refusal < mark && mark < send && send < rollback
				  && rollback < end);
	UT_ASSERT_NOT_NULL(strstr(begin, "PCM_X_LOCAL_IMAGE_READY_REFUSAL_ACTIVE_HOLDER"));
	UT_ASSERT_NOT_NULL(strstr(begin, "PCM_X_LOCAL_IMAGE_READY_REFUSAL_RELIABLE_LEG"));
	UT_ASSERT_NOT_NULL(strstr(begin, "PCM-X IMAGE_READY stage boundary"));
	UT_ASSERT_NOT_NULL(strstr(begin, "mark_result"));
	UT_ASSERT_NOT_NULL(strstr(begin, "stage_result"));
	UT_ASSERT_NOT_NULL(strstr(outbound_source, "cluster_lms_note_pcm_x_image_ready_boundary("));
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_image_ready_envelope(");
	UT_ASSERT_NOT_NULL(handler);
	if (handler != NULL) {
		handler_end = strstr(handler + 1, "\n}\n\n\n");
		UT_ASSERT_NOT_NULL(handler_end);
		UT_ASSERT_NOT_NULL(strstr(handler, "image-ready-master-consume"));
		UT_ASSERT_NOT_NULL(strstr(handler, "PCM-X IMAGE_READY master boundary: ingress"));
		UT_ASSERT_NOT_NULL(strstr(handler, "PCM-X IMAGE_READY master boundary: consume"));
		UT_ASSERT_NOT_NULL(strstr(handler, "wire_valid"));
		UT_ASSERT_NOT_NULL(strstr(handler, "authorized"));
		UT_ASSERT_NOT_NULL(strstr(handler, "prepare_stage_result"));
		if (handler_end != NULL)
			UT_ASSERT(strstr(handler, "image-ready-master-consume") < handler_end);
	}
	prepare_handler = strstr(source, "\ncluster_gcs_handle_pcm_x_prepare_grant_envelope(");
	UT_ASSERT_NOT_NULL(prepare_handler);
	if (prepare_handler != NULL) {
		prepare_handler_end = strstr(prepare_handler + 1, "\n}\n\n\n");
		UT_ASSERT_NOT_NULL(prepare_handler_end);
		UT_ASSERT_NOT_NULL(
			strstr(prepare_handler, "PCM-X PREPARE_GRANT requester boundary: ingress"));
		UT_ASSERT_NOT_NULL(
			strstr(prepare_handler, "PCM-X PREPARE_GRANT requester boundary: apply"));
		UT_ASSERT_NOT_NULL(strstr(prepare_handler, "wire_valid"));
		UT_ASSERT_NOT_NULL(strstr(prepare_handler, "authorized"));
		UT_ASSERT_NOT_NULL(strstr(prepare_handler, "lookup_result"));
		if (prepare_handler_end != NULL)
			UT_ASSERT(strstr(prepare_handler, "PCM-X PREPARE_GRANT requester boundary: apply")
					  < prepare_handler_end);
	}
	free(source);
	free(outbound_source);
}


UT_TEST(test_pcm_x_lms_owner_death_and_restart_audit_fail_closed)
{
	char *gcs_source = read_gcs_block_source();
	char *lms_source = read_source_path(LMS_SOURCE_PATH);
	const char *owner_start;
	const char *owner_exit;
	const char *main_start;
	const char *worker_start;
	const char *main_call;
	const char *worker_call;

	if (gcs_source == NULL || lms_source == NULL) {
		free(gcs_source);
		free(lms_source);
		return;
	}
	owner_exit = strstr(gcs_source, "\ngcs_block_pcm_x_owner_exit(");
	owner_start = strstr(gcs_source, "\ncluster_gcs_block_pcm_x_owner_start(");
	UT_ASSERT_NOT_NULL(owner_exit);
	UT_ASSERT_NOT_NULL(owner_start);
	if (owner_exit != NULL) {
		UT_ASSERT_NOT_NULL(strstr(owner_exit, "code != 0"));
		UT_ASSERT_NOT_NULL(strstr(owner_exit, "cluster_pcm_x_runtime_fail_closed()"));
	}
	if (owner_start != NULL) {
		UT_ASSERT_NOT_NULL(strstr(owner_start, "before_shmem_exit("));
		UT_ASSERT_NOT_NULL(
			strstr(owner_start, "cluster_gcs_block_dedup_pcm_x_restart_audit(worker_id)"));
	}

	main_start = strstr(lms_source, "\nLmsMain(void)");
	worker_start = strstr(lms_source, "\nLmsWorkerMain(int worker_id)");
	UT_ASSERT_NOT_NULL(main_start);
	UT_ASSERT_NOT_NULL(worker_start);
	UT_ASSERT_NOT_NULL(strstr(lms_source, "PCM-X IMAGE_READY transport boundary"));
	main_call
		= main_start != NULL ? strstr(main_start, "cluster_gcs_block_pcm_x_owner_start(0)") : NULL;
	worker_call = worker_start != NULL
					  ? strstr(worker_start, "cluster_gcs_block_pcm_x_owner_start(worker_id)")
					  : NULL;
	UT_ASSERT_NOT_NULL(main_call);
	UT_ASSERT_NOT_NULL(worker_call);
	if (main_start != NULL && main_call != NULL)
		UT_ASSERT(main_call < strstr(main_start, "for (;;)"));
	if (worker_start != NULL && worker_call != NULL)
		UT_ASSERT(worker_call < strstr(worker_start, "for (;;)"));
	UT_ASSERT_EQ(count_occurrences(lms_source, "cluster_gcs_block_pcm_x_owner_start("), 2);
	free(gcs_source);
	free(lms_source);
}


UT_TEST(test_resource_x_scan_more_wakes_the_current_lms_latch)
{
	char *lms_source = read_source_path(LMS_SOURCE_PATH);
	const char *wakeup;
	const char *wakeup_end;
	const char *self_branch;
	const char *set_latch;
	const char *signal_peer;

	if (lms_source == NULL)
		return;
	wakeup = strstr(lms_source, "\ncluster_lms_wakeup(int worker_id)");
	wakeup_end = wakeup != NULL ? strstr(wakeup + 1, "\n}\n") : NULL;
	self_branch = wakeup != NULL ? strstr(wakeup, "if (pid == MyProcPid)") : NULL;
	set_latch = self_branch != NULL ? strstr(self_branch, "SetLatch(MyLatch)") : NULL;
	signal_peer = wakeup != NULL ? strstr(wakeup, "kill(pid, SIGUSR1)") : NULL;
	UT_ASSERT_NOT_NULL(wakeup);
	UT_ASSERT_NOT_NULL(wakeup_end);
	UT_ASSERT_NOT_NULL(self_branch);
	UT_ASSERT_NOT_NULL(set_latch);
	UT_ASSERT_NOT_NULL(signal_peer);
	if (wakeup_end != NULL && self_branch != NULL && set_latch != NULL
		&& signal_peer != NULL)
		UT_ASSERT(self_branch < set_latch && set_latch < signal_peer
			&& signal_peer < wakeup_end);
	free(lms_source);
}


UT_TEST(test_pcm_x_lms_reload_acknowledges_finish_flush_injection)
{
	char *lms_source = read_source_path(LMS_SOURCE_PATH);
	const char *helper;
	const char *main_start;
	const char *main_reload;
	const char *main_ack;
	const char *worker_start;
	const char *worker_reload;
	const char *worker_ack;

	if (lms_source == NULL)
		return;
	helper = strstr(lms_source, "\nlms_note_pcm_x_finish_flush_injection_reload(");
	main_start = strstr(lms_source, "\nLmsMain(void)");
	worker_start = strstr(lms_source, "\nLmsWorkerMain(int worker_id)");
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(main_start);
	UT_ASSERT_NOT_NULL(worker_start);
	if (helper != NULL) {
		UT_ASSERT_NOT_NULL(
			strstr(helper, "cluster_injection_is_armed(\"cluster-pcm-x-retain-flush-error\")"));
		UT_ASSERT_NOT_NULL(strstr(helper, "MyProcPid"));
		UT_ASSERT_NOT_NULL(strstr(helper, "cluster_injection_points"));
	}
	main_reload = main_start != NULL ? strstr(main_start, "ProcessConfigFile(PGC_SIGHUP);") : NULL;
	main_ack = main_reload != NULL
				   ? strstr(main_reload, "lms_note_pcm_x_finish_flush_injection_reload(0);")
				   : NULL;
	worker_reload
		= worker_start != NULL ? strstr(worker_start, "ProcessConfigFile(PGC_SIGHUP);") : NULL;
	worker_ack
		= worker_reload != NULL
			  ? strstr(worker_reload, "lms_note_pcm_x_finish_flush_injection_reload(worker_id);")
			  : NULL;
	UT_ASSERT_NOT_NULL(main_reload);
	UT_ASSERT_NOT_NULL(main_ack);
	UT_ASSERT_NOT_NULL(worker_reload);
	UT_ASSERT_NOT_NULL(worker_ack);
	if (main_reload != NULL && main_ack != NULL && worker_start != NULL)
		UT_ASSERT(main_reload < main_ack && main_ack < worker_start);
	if (worker_reload != NULL && worker_ack != NULL)
		UT_ASSERT(worker_reload < worker_ack);
	free(lms_source);
}


UT_TEST(test_pcm_x_destructive_finish_fault_times_out_in_sql_before_harness)
{
	char *source = read_source_path(T400_SOURCE_PATH);
	const char *leg;
	const char *statement_timeout;
	const char *insert;
	const char *harness_timeout;
	const char *assertion;
	const char *destructive_leg;
	const char *exact_oracle;
	const char *global_occupancy;
	const char *relfilenode;

	if (source == NULL)
		return;
	leg = strstr(source, "my ($flush_error_rc, $flush_error_out, $flush_error_err)");
	destructive_leg = strstr(source, "# The destructive leg is deliberately last");
	UT_ASSERT_NOT_NULL(leg);
	UT_ASSERT_NOT_NULL(destructive_leg);
	statement_timeout = leg != NULL ? strstr(leg, "statement_timeout") : NULL;
	insert
		= leg != NULL ? strstr(leg, "INSERT INTO pcm_xq_flush_error(id, v) VALUES (2, 1)") : NULL;
	harness_timeout = leg != NULL ? strstr(leg, "timeout => 30") : NULL;
	assertion = leg != NULL ? strstr(leg, "L5F remote writer failed") : NULL;
	relfilenode
		= destructive_leg != NULL ? strstr(destructive_leg, "flush_error_relfilenode") : NULL;
	exact_oracle = destructive_leg != NULL
					   ? strstr(destructive_leg,
								"PCM-X Resource-X finish-error evidence exact")
					   : NULL;
	global_occupancy
		= destructive_leg != NULL ? strstr(destructive_leg, "dedup_entry_count") : NULL;
	UT_ASSERT_NOT_NULL(statement_timeout);
	UT_ASSERT_NOT_NULL(insert);
	UT_ASSERT_NOT_NULL(harness_timeout);
	UT_ASSERT_NOT_NULL(assertion);
	UT_ASSERT_NOT_NULL(relfilenode);
	UT_ASSERT_NOT_NULL(exact_oracle);
	UT_ASSERT_NULL(global_occupancy);
	if (statement_timeout != NULL && insert != NULL && harness_timeout != NULL && assertion != NULL)
		UT_ASSERT(statement_timeout < insert && insert < harness_timeout
				  && harness_timeout < assertion);
	free(source);
}


UT_TEST(test_pcm_x_image_fetch_intercepts_canonical_id_before_generic_dedup)
{
	char *source = read_gcs_block_source();
	char *handler;
	char *intercept;
	char *generic_lookup;
	char *serve;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_block_request_envelope(");
	serve = strstr(source, "\ngcs_block_pcm_x_serve_image_fetch(");
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(serve);
	if (handler != NULL) {
		intercept = strstr(handler, "gcs_block_pcm_x_serve_image_fetch(env, req, dedup_worker_id)");
		generic_lookup = strstr(handler, "cluster_gcs_block_dedup_lookup_or_register(");
		UT_ASSERT_NOT_NULL(intercept);
		UT_ASSERT_NOT_NULL(generic_lookup);
		if (intercept != NULL && generic_lookup != NULL)
			UT_ASSERT(intercept < generic_lookup);
	}
	if (serve != NULL) {
		UT_ASSERT_NOT_NULL(strstr(serve, "PCM-X image fetch source boundary: ingress"));
		UT_ASSERT_NOT_NULL(strstr(serve, "PCM-X image fetch source boundary: validate"));
		UT_ASSERT_NOT_NULL(strstr(serve, "PCM-X image fetch source boundary: lookup"));
		UT_ASSERT_NOT_NULL(strstr(serve, "PCM-X image fetch source boundary: reply"));
		UT_ASSERT_NOT_NULL(strstr(serve, "cluster_pcm_x_image_fetch_request_exact("));
		UT_ASSERT(count_occurrences(serve, "cluster_pcm_x_local_holder_progress_exact(") >= 2);
		UT_ASSERT_NOT_NULL(strstr(serve, "gcs_block_pcm_x_authenticated_session("));
		UT_ASSERT_NOT_NULL(strstr(serve, "gcs_block_pcm_x_revalidate_peer_binding("));
		UT_ASSERT_NOT_NULL(strstr(serve, "cluster_gcs_block_dedup_pcm_x_lookup("));
		UT_ASSERT_NOT_NULL(strstr(serve, "gcs_block_resend_cached_reply("));
	}
	free(source);
}


UT_TEST(test_pcm_x_requester_fetch_revalidates_queue_and_reservation_before_install)
{
	char *source = read_gcs_block_source();
	const char *fetch;
	const char *fetch_end;
	const char *backoff_branch;
	const char *backoff_wait;
	const char *backoff_before;
	const char *backoff_after;
	const char *cv_wait;
	const char *cv_timeout;
	const char *cv_before;
	const char *cv_after;
	const char *install_call;
	const char *install;
	const char *install_end;
	const char *install_runtime_before;
	const char *install_publish;
	const char *install_runtime_after;
	const char *recovering_retry;
	const char *recovering_retry_counter;
	const char *reply_validation;
	char *reply_handler;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	fetch = strstr(source, "\ncluster_gcs_pcm_x_fetch_image_and_install(");
	fetch_end = fetch != NULL ? strstr(fetch, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(fetch);
	UT_ASSERT_NOT_NULL(fetch_end);
	if (fetch != NULL && fetch_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(fetch, "PCM-X image fetch requester boundary: entry"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "PCM-X image fetch requester boundary: pre-slot reject"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "branch=identity-base"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "branch=reservation-live"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "progress_grant_base=%llu effective_grant_base=%llu"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "live_writer_activation_token=%llu"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "PCM-X image fetch requester boundary: send"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "PCM-X image fetch requester boundary: receive"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "PCM-X image fetch requester boundary: complete"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "gcs_block_try_reserve_exact_slot("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "cluster_pcm_x_image_fetch_build_request("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "const PcmXRuntimeSnapshot *request_runtime"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "cluster_pcm_x_image_fetch_reply_exact("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "cluster_pcm_x_image_fetch_reservation_exact("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "gcs_block_pcm_x_install_reserved_image_exact("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "gcs_block_release_slot(slot)"));
		recovering_retry = strstr(fetch, "GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING");
		recovering_retry_counter = strstr(fetch, "pcm_x_image_fetch_recovering_retry_count");
		reply_validation = strstr(fetch, "cluster_pcm_x_image_fetch_reply_exact(");
		UT_ASSERT_NOT_NULL(recovering_retry);
		UT_ASSERT_NOT_NULL(recovering_retry_counter);
		UT_ASSERT(recovering_retry == NULL || recovering_retry < fetch_end);
		UT_ASSERT(recovering_retry_counter == NULL || recovering_retry_counter < fetch_end);
		UT_ASSERT(recovering_retry == NULL || reply_validation == NULL
				  || recovering_retry < reply_validation);
		UT_ASSERT(recovering_retry_counter == NULL || reply_validation == NULL
				  || recovering_retry_counter < reply_validation);
		backoff_branch = strstr(fetch, "if (retry_attempt > 0)");
		backoff_wait
			= backoff_branch != NULL ? strstr(backoff_branch, "(void)WaitLatch(MyLatch") : NULL;
		backoff_before
			= backoff_wait != NULL
				  ? strstr(backoff_branch, "gcs_block_pcm_x_fetch_requester_authority_exact(")
				  : NULL;
		backoff_after
			= backoff_wait != NULL
				  ? strstr(backoff_wait, "gcs_block_pcm_x_fetch_requester_authority_exact(")
				  : NULL;
		cv_wait = strstr(fetch, "ConditionVariableTimedSleep(");
		cv_timeout = strstr(fetch, "timeout_ms = (long)");
		cv_before = cv_timeout != NULL && cv_wait != NULL
						? strstr(cv_timeout, "gcs_block_pcm_x_fetch_requester_authority_exact(")
						: NULL;
		cv_after = cv_wait != NULL
					   ? strstr(cv_wait, "gcs_block_pcm_x_fetch_requester_authority_exact(")
					   : NULL;
		install_call = cv_after != NULL
						   ? strstr(cv_after, "gcs_block_pcm_x_install_reserved_image_exact(")
						   : NULL;
		UT_ASSERT_NOT_NULL(backoff_branch);
		UT_ASSERT_NOT_NULL(backoff_wait);
		UT_ASSERT_NOT_NULL(backoff_before);
		UT_ASSERT_NOT_NULL(backoff_after);
		UT_ASSERT_NOT_NULL(cv_wait);
		UT_ASSERT_NOT_NULL(cv_timeout);
		UT_ASSERT_NOT_NULL(cv_before);
		UT_ASSERT_NOT_NULL(cv_after);
		UT_ASSERT_NOT_NULL(install_call);
		if (backoff_branch != NULL && backoff_before != NULL && backoff_wait != NULL
			&& backoff_after != NULL && cv_timeout != NULL && cv_before != NULL && cv_wait != NULL
			&& cv_after != NULL && install_call != NULL)
			UT_ASSERT(backoff_branch < backoff_before && backoff_before < backoff_wait
					  && backoff_wait < backoff_after && cv_timeout < cv_before
					  && cv_before < cv_wait && cv_wait < cv_after && cv_after < install_call
					  && install_call < fetch_end);
	}
	install = strstr(source, "\ngcs_block_pcm_x_install_reserved_image_exact(");
	install_end = install != NULL ? strstr(install, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(install);
	UT_ASSERT_NOT_NULL(install_end);
	if (install != NULL && install_end != NULL) {
		const char *reserved_write_proof
			= strstr(install, "gcs_block_pcm_x_reserved_image_write_exact(");

		UT_ASSERT_NOT_NULL(strstr(install, "const PcmXRuntimeSnapshot *request_runtime"));
		UT_ASSERT_NOT_NULL(reserved_write_proof);
		UT_ASSERT_NULL(strstr(install, "cluster_bufmgr_pcm_x_content_write_permitted("));
		install_runtime_before = strstr(install, "cluster_gcs_pcm_x_requester_runtime_exact(");
		install_publish = strstr(install, "cluster_bufmgr_pcm_own_publish_installed_x_image(");
		install_runtime_after
			= install_publish != NULL
				  ? strstr(install_publish, "cluster_gcs_pcm_x_requester_runtime_exact(")
				  : NULL;
		UT_ASSERT_NOT_NULL(install_runtime_before);
		UT_ASSERT_NOT_NULL(install_publish);
		UT_ASSERT_NOT_NULL(install_runtime_after);
		if (install_runtime_before != NULL && install_publish != NULL
			&& install_runtime_after != NULL)
			UT_ASSERT(install_runtime_before < install_publish
					  && install_publish < install_runtime_after
					  && install_runtime_after < install_end);
	}
	{
		const char *proof = strstr(source, "\ngcs_block_pcm_x_reserved_image_write_exact(");
		const char *proof_end = proof != NULL ? strstr(proof, "\n}\n") : NULL;

		UT_ASSERT_NOT_NULL(proof);
		UT_ASSERT_NOT_NULL(proof_end);
		if (proof != NULL && proof_end != NULL) {
			UT_ASSERT_NOT_NULL(strstr(proof, "cluster_pcm_x_image_fetch_reservation_exact("));
			UT_ASSERT_NOT_NULL(strstr(proof, "live->writer_activation_token == 0"));
			UT_ASSERT_NOT_NULL(strstr(proof, "live->resource_x_activation_generation == 0"));
			UT_ASSERT_NOT_NULL(strstr(proof, "base->writer_activation_token == 0"));
			UT_ASSERT_NOT_NULL(strstr(proof, "base->resource_x_activation_generation == 0"));
		}
	}
	reply_handler = strstr(source, "\ncluster_gcs_handle_block_reply_envelope(");
	UT_ASSERT_NOT_NULL(reply_handler);
	if (reply_handler != NULL) {
		UT_ASSERT_NOT_NULL(
			strstr(reply_handler, "env->source_node_id != (uint32)hdr->sender_node"));
		UT_ASSERT_NOT_NULL(strstr(reply_handler, "env->dest_node_id != (uint32)cluster_node_id"));
	}
	free(source);
}


UT_TEST(test_pcm_x_self_source_handoff_is_no_copy_and_drain_preserves_x)
{
	char *source = read_gcs_block_source();
	const char *adopt;
	const char *adopt_end;
	const char *adopt_page_check;
	const char *adopt_handoff;
	const char *finish;
	const char *finish_end;
	const char *finish_preflight;
	const char *finish_progress;
	const char *finish_try;
	const char *finish_fail_closed;
	const char *finish_commit;
	const char *finish_catch;
	const char *finish_catch_guard;
	const char *finish_catch_fail_closed;
	const char *finish_catch_release;
	const char *finish_catch_rethrow;
	const char *finish_end_try;
	const char *copy;
	const char *materialize;
	const char *materialize_end;
	const char *self_arm;
	const char *remote_finish;
	const char *drain;
	const char *drain_end;
	const char *verify_x;
	const char *release_record;
	const char *self_return;
	const char *release_retained;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	adopt = strstr(source, "\ncluster_gcs_pcm_x_adopt_self_image(");
	finish = strstr(source, "\ncluster_gcs_pcm_x_finish_self_image_x(");
	materialize = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	drain = strstr(source, "\ngcs_block_pcm_x_local_drain_apply_exact(");
	UT_ASSERT_NOT_NULL(adopt);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(materialize);
	UT_ASSERT_NOT_NULL(drain);
	if (adopt == NULL || finish == NULL || materialize == NULL || drain == NULL) {
		free(source);
		return;
	}
	adopt_end = finish;
	finish_end = strstr(finish + 1, "\n\n/*");
	materialize_end = strstr(materialize + 1, "\n\n\n");
	drain_end = strstr(drain + 1, "\n\n\n");
	UT_ASSERT_NOT_NULL(finish_end);
	UT_ASSERT_NOT_NULL(materialize_end);
	UT_ASSERT_NOT_NULL(drain_end);
	copy = strstr(adopt, "memcpy(page");
	UT_ASSERT(copy == NULL || copy >= adopt_end);
	adopt_page_check = strstr(adopt, "gcs_block_pcm_x_self_page_exact(");
	adopt_handoff = strstr(adopt, "cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(");
	UT_ASSERT_NOT_NULL(adopt_page_check);
	UT_ASSERT_NOT_NULL(adopt_handoff);
	if (adopt_page_check != NULL && adopt_handoff != NULL)
		UT_ASSERT(adopt_page_check < adopt_end && adopt_handoff < adopt_end);
	if (finish_end != NULL) {
		finish_preflight = strstr(finish, "cluster_bufmgr_pcm_own_snapshot(");
		finish_progress = strstr(finish, "cluster_pcm_x_local_progress_exact(");
		finish_try = strstr(finish, "PG_TRY();");
		finish_fail_closed = strstr(finish, "cluster_pcm_x_runtime_fail_closed();");
		finish_commit = strstr(finish, "cluster_bufmgr_pcm_own_finish_x_commit(");
		UT_ASSERT_NOT_NULL(finish_preflight);
		UT_ASSERT_NOT_NULL(finish_progress);
		UT_ASSERT_NOT_NULL(finish_try);
		UT_ASSERT_NOT_NULL(finish_fail_closed);
		UT_ASSERT_NOT_NULL(finish_commit);
		if (finish_preflight != NULL && finish_progress != NULL && finish_try != NULL
			&& finish_fail_closed != NULL && finish_commit != NULL)
			UT_ASSERT(finish_preflight < finish_progress && finish_progress < finish_try
					  && finish_try < finish_commit && finish_commit < finish_end
					  && finish_fail_closed < finish_end);
		finish_catch = finish_try != NULL ? strstr(finish_try, "PG_CATCH();") : NULL;
		finish_catch_guard
			= finish_catch != NULL ? strstr(finish_catch, "if (handoff_live || committed)") : NULL;
		finish_catch_fail_closed
			= finish_catch_guard != NULL
				  ? strstr(finish_catch_guard, "cluster_pcm_x_runtime_fail_closed();")
				  : NULL;
		finish_catch_release = finish_catch_fail_closed != NULL
								   ? strstr(finish_catch_fail_closed,
											"if (content_locked && LWLockHeldByMe(content_lock))")
								   : NULL;
		finish_catch_rethrow
			= finish_catch_release != NULL ? strstr(finish_catch_release, "PG_RE_THROW();") : NULL;
		finish_end_try
			= finish_catch_rethrow != NULL ? strstr(finish_catch_rethrow, "PG_END_TRY();") : NULL;
		UT_ASSERT_NOT_NULL(finish_catch);
		UT_ASSERT_NOT_NULL(finish_catch_guard);
		UT_ASSERT_NOT_NULL(finish_catch_fail_closed);
		UT_ASSERT_NOT_NULL(finish_catch_release);
		UT_ASSERT_NOT_NULL(finish_catch_rethrow);
		UT_ASSERT_NOT_NULL(finish_end_try);
		if (finish_catch != NULL && finish_catch_guard != NULL && finish_catch_fail_closed != NULL
			&& finish_catch_release != NULL && finish_catch_rethrow != NULL
			&& finish_end_try != NULL)
			UT_ASSERT(finish_catch < finish_catch_guard
					  && finish_catch_guard < finish_catch_fail_closed
					  && finish_catch_fail_closed < finish_catch_release
					  && finish_catch_release < finish_catch_rethrow
					  && finish_catch_rethrow < finish_end_try && finish_end_try < finish_end);
	}
	self_arm = strstr(materialize, "if (self_source_handoff)");
	remote_finish = strstr(materialize, "gcs_block_pcm_x_finish_revoke_retain(");
	UT_ASSERT_NOT_NULL(self_arm);
	UT_ASSERT_NOT_NULL(remote_finish);
	if (self_arm != NULL && remote_finish != NULL && materialize_end != NULL)
		UT_ASSERT(self_arm < remote_finish && remote_finish < materialize_end);

	verify_x = strstr(drain, "cluster_bufmgr_pcm_own_self_handoff_probe(");
	release_record = strstr(drain, "cluster_gcs_block_dedup_pcm_x_release_exact(");
	self_return
		= release_record != NULL ? strstr(release_record, "if (self_source_handoff)") : NULL;
	release_retained = verify_x != NULL
		? strstr(verify_x, "cluster_bufmgr_pcm_own_release_retained_image(")
		: NULL;
	UT_ASSERT_NOT_NULL(verify_x);
	UT_ASSERT_NOT_NULL(release_record);
	UT_ASSERT_NOT_NULL(self_return);
	UT_ASSERT_NOT_NULL(release_retained);
	if (verify_x != NULL && release_record != NULL && self_return != NULL
		&& release_retained != NULL && drain_end != NULL)
		UT_ASSERT(verify_x < release_retained && release_retained < release_record
				  && release_record < self_return && self_return < drain_end);
	free(source);
}


UT_TEST(test_pcm_x_retire_wake_identity_is_wait_generation_exact)
{
	PcmXWaitIdentity identity;
	ClusterLmdWaitStateSnapshot snapshot;

	memset(&identity, 0, sizeof(identity));
	identity.node_id = 2;
	identity.procno = 7;
	identity.xid = (TransactionId)42;
	identity.cluster_epoch = UINT64_C(9);
	identity.request_id = UINT64_C(23);
	identity.wait_seq = UINT64_C(31);
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.active = true;
	snapshot.kind = CLUSTER_LMD_WAIT_PCM_CONVERT;
	snapshot.request_id = identity.request_id;
	snapshot.cluster_epoch = identity.cluster_epoch;
	snapshot.xid = identity.xid;
	snapshot.wait_seq = identity.wait_seq;

	UT_ASSERT(cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 1, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 7, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_BUSY, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_INACTIVE, &snapshot));
	snapshot.active = false;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.active = true;
	snapshot.kind = CLUSTER_LMD_WAIT_GES;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.kind = CLUSTER_LMD_WAIT_PCM_CONVERT;
	snapshot.request_id++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.request_id = identity.request_id;
	snapshot.cluster_epoch++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.cluster_epoch = identity.cluster_epoch;
	snapshot.wait_seq++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.wait_seq = identity.wait_seq;
	snapshot.xid++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.xid = identity.xid;
	identity.request_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
}


UT_TEST(test_pcm_x_requester_driver_owns_fifo_and_transfer_lifecycles)
{
	char *source = read_gcs_block_source();
	const char *driver;
	const char *driver_end;
	const char *rekey_helper;
	const char *rekey_helper_end;
	const char *snapshot_before;
	const char *rekey_exact;
	const char *snapshot_after;
	const char *wrapper;
	const char *wrapper_end;
	const char *cleanup;
	const char *cleanup_end;
	const char *handoff_pointer;
	const char *handoff_active;
	const char *handoff_publish;
	const char *requester_clear;
	const char *cleanup_handoff;
	const char *cleanup_wfg;
	const char *cleanup_release;
	const char *cleanup_abort;
	const char *cleanup_clear_helper;
	const char *cleanup_clear_helper_end;
	const char *cleanup_stale;
	const char *cleanup_stale_clear;
	const char *cleanup_stale_return;
	const char *cleanup_owner_retry;
	const char *cleanup_owner_retry_clear;
	const char *cleanup_owner_retry_return;
	const char *cleanup_fail_closed;
	const char *cleanup_fail_closed_clear;
	const char *cleanup_fail_closed_return;
	const char *formation;
	const char *formation_wait;
	const char *authenticated_session;
	const char *authority_preflight;
	const char *request_id;
	const char *wait_publish;
	const char *nested_guard;
	const char *ownership_snapshot;
	const char *first_wait;
	const char *join;
	const char *leader_rekey;
	const char *claim;
	const char *enqueue;
	const char *follower_snapshot;
	const char *graph_replace;
	const char *graph_clear;
	const char *self_adopt;
	const char *remote_fetch;
	const char *install_ready;
	const char *self_finish;
	const char *remote_finish;
	const char *final_ack;
	const char *final_confirm;
	const char *clear_wait;
	const char *authority_helper;
	const char *authority_helper_end;
	const char *authority_runtime;
	const char *authority_runtime_exact;
	const char *authority_peer;
	const char *wait_exact;
	const char *wait_exact_end;
	const char *wait_exact_first_authority;
	const char *wait_exact_physical;
	const char *wait_exact_second_revalidate;
	const char *wait_revalidate;
	const char *wait_revalidate_end;
	const char *wait_revalidate_authority;
	const char *wait_revalidate_nested_guard;
	const char *wait_scan;
	int raw_wait_count = 0;
	int exact_wait_count = 0;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	driver = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	rekey_helper = strstr(source, "\ngcs_block_pcm_x_rekey_leader_base_exact(");
	rekey_helper_end = rekey_helper != NULL ? strstr(rekey_helper, "\n}\n") : NULL;
	wrapper = strstr(source, "\ncluster_gcs_pcm_x_acquire_writer(");
	wrapper_end = wrapper != NULL ? strstr(wrapper, "\n}\n") : NULL;
	cleanup = strstr(source, "\ngcs_block_pcm_x_requester_cleanup_impl(");
	cleanup_end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	cleanup_clear_helper = strstr(source, "\ngcs_block_pcm_x_requester_clear_wait(");
	cleanup_clear_helper_end
		= cleanup_clear_helper != NULL ? strstr(cleanup_clear_helper, "\n}\n") : NULL;
	authority_helper = strstr(source, "\ngcs_block_pcm_x_requester_authority_exact(");
	authority_helper_end = authority_helper != NULL ? strstr(authority_helper, "\n}\n") : NULL;
	wait_exact = strstr(source, "\ngcs_block_pcm_x_requester_wait_exact(");
	wait_exact_end = wait_exact != NULL ? strstr(wait_exact, "\n}\n") : NULL;
	wait_revalidate = strstr(source, "\ngcs_block_pcm_x_requester_pre_sleep_revalidate(");
	wait_revalidate_end = wait_revalidate != NULL ? strstr(wait_revalidate, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	UT_ASSERT_NOT_NULL(rekey_helper);
	UT_ASSERT_NOT_NULL(rekey_helper_end);
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper_end);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	UT_ASSERT_NOT_NULL(cleanup_clear_helper);
	UT_ASSERT_NOT_NULL(cleanup_clear_helper_end);
	UT_ASSERT_NOT_NULL(authority_helper);
	UT_ASSERT_NOT_NULL(authority_helper_end);
	UT_ASSERT_NOT_NULL(wait_exact);
	UT_ASSERT_NOT_NULL(wait_exact_end);
	UT_ASSERT_NOT_NULL(wait_revalidate);
	UT_ASSERT_NOT_NULL(wait_revalidate_end);
	if (driver == NULL || driver_end == NULL || wrapper == NULL || wrapper_end == NULL) {
		free(source);
		return;
	}
	UT_ASSERT_NOT_NULL(strstr(driver, "ClusterPcmXConvertShmem == NULL"));
	UT_ASSERT_NOT_NULL(strstr(wrapper, "PG_TRY();"));
	UT_ASSERT_NOT_NULL(strstr(wrapper, "gcs_block_pcm_x_requester_cleanup_noexcept(cleanup)"));
	UT_ASSERT_NOT_NULL(strstr(wrapper, "ReThrowError(original_error)"));
	handoff_pointer = strstr(wrapper, "cleanup->claim_handoff_out = claim_handed_off");
	handoff_active
		= handoff_pointer != NULL ? strstr(handoff_pointer, "cleanup->active = true") : NULL;
	handoff_publish = strstr(wrapper, "*claim_handed_off = true");
	requester_clear = handoff_publish != NULL ? strstr(handoff_publish, "memset(cleanup, 0") : NULL;
	UT_ASSERT_NOT_NULL(handoff_pointer);
	UT_ASSERT_NOT_NULL(handoff_active);
	UT_ASSERT_NOT_NULL(handoff_publish);
	UT_ASSERT_NOT_NULL(requester_clear);
	if (handoff_pointer != NULL && handoff_active != NULL && handoff_publish != NULL
		&& requester_clear != NULL)
		UT_ASSERT(handoff_pointer < handoff_active && handoff_publish < requester_clear
				  && requester_clear < wrapper_end);
	cleanup_handoff = cleanup != NULL ? strstr(cleanup, "cleanup->claim_handoff_out") : NULL;
	cleanup_wfg = cleanup != NULL ? strstr(cleanup, "if (cleanup->wfg_live)") : NULL;
	cleanup_release = cleanup != NULL
						  ? strstr(cleanup, "cluster_gcs_pcm_x_writer_claim_release_and_wake_exact")
						  : NULL;
	cleanup_abort
		= cleanup != NULL ? strstr(cleanup, "cluster_pcm_x_local_writer_claim_abort_exact") : NULL;
	UT_ASSERT_NOT_NULL(cleanup_handoff);
	UT_ASSERT_NOT_NULL(cleanup_wfg);
	UT_ASSERT_NOT_NULL(cleanup_release);
	UT_ASSERT(cleanup_abort == NULL || cleanup_abort >= cleanup_end);
	if (cleanup_handoff != NULL && cleanup_wfg != NULL && cleanup_end != NULL)
		UT_ASSERT(cleanup_handoff < cleanup_wfg && cleanup_wfg < cleanup_end);
	if (cleanup_clear_helper != NULL && cleanup_clear_helper_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(cleanup_clear_helper, "cleanup->wait_published"));
		UT_ASSERT_NOT_NULL(strstr(cleanup_clear_helper, "cluster_lmd_wait_state_clear("));
		UT_ASSERT_NOT_NULL(strstr(cleanup_clear_helper, "cleanup->wait_published = false"));
	}
	cleanup_stale = cleanup != NULL ? strstr(cleanup, "CLUSTER_LMD_GRAPH_REMOVE_STALE") : NULL;
	cleanup_stale_clear
		= cleanup_stale != NULL
			  ? strstr(cleanup_stale, "gcs_block_pcm_x_requester_clear_wait(cleanup)")
			  : NULL;
	cleanup_stale_return = cleanup_stale_clear != NULL
							   ? strstr(cleanup_stale_clear, "return PCM_X_QUEUE_CORRUPT")
							   : NULL;
	cleanup_owner_retry
		= cleanup != NULL ? strstr(cleanup, "== CLUSTER_PCM_X_OWNER_EXIT_RETRY") : NULL;
	cleanup_owner_retry_clear
		= cleanup_owner_retry != NULL
			  ? strstr(cleanup_owner_retry, "gcs_block_pcm_x_requester_clear_wait(cleanup)")
			  : NULL;
	cleanup_owner_retry_return = cleanup_owner_retry_clear != NULL
									 ? strstr(cleanup_owner_retry_clear, "return result;")
									 : NULL;
	cleanup_fail_closed
		= cleanup_owner_retry_return != NULL
			  ? strstr(cleanup_owner_retry_return, "cluster_pcm_x_runtime_fail_closed()")
			  : NULL;
	cleanup_fail_closed_clear
		= cleanup_fail_closed != NULL
			  ? strstr(cleanup_fail_closed, "gcs_block_pcm_x_requester_clear_wait(cleanup)")
			  : NULL;
	cleanup_fail_closed_return = cleanup_fail_closed_clear != NULL
									 ? strstr(cleanup_fail_closed_clear, "return result;")
									 : NULL;
	UT_ASSERT_NOT_NULL(cleanup_stale_clear);
	UT_ASSERT_NOT_NULL(cleanup_stale_return);
	UT_ASSERT_NOT_NULL(cleanup_owner_retry_clear);
	UT_ASSERT_NOT_NULL(cleanup_owner_retry_return);
	UT_ASSERT_NOT_NULL(cleanup_fail_closed_clear);
	UT_ASSERT_NOT_NULL(cleanup_fail_closed_return);
	if (cleanup_stale_clear != NULL && cleanup_stale_return != NULL)
		UT_ASSERT(cleanup_stale_clear < cleanup_stale_return && cleanup_stale_return < cleanup_end);
	if (cleanup_owner_retry_clear != NULL && cleanup_owner_retry_return != NULL)
		UT_ASSERT(cleanup_owner_retry_clear < cleanup_owner_retry_return
				  && cleanup_owner_retry_return < cleanup_end);
	if (cleanup_fail_closed_clear != NULL && cleanup_fail_closed_return != NULL)
		UT_ASSERT(cleanup_fail_closed_clear < cleanup_fail_closed_return
				  && cleanup_fail_closed_return < cleanup_end);
	authority_runtime = authority_helper != NULL
							? strstr(authority_helper, "cluster_pcm_x_runtime_snapshot()")
							: NULL;
	authority_runtime_exact
		= authority_runtime != NULL
			  ? strstr(authority_runtime, "cluster_gcs_pcm_x_requester_runtime_exact(")
			  : NULL;
	authority_peer
		= authority_runtime_exact != NULL
			  ? strstr(authority_runtime_exact, "gcs_block_pcm_x_revalidate_peer_binding(")
			  : NULL;
	wait_exact_first_authority
		= wait_exact != NULL ? strstr(wait_exact, "gcs_block_pcm_x_requester_authority_exact(")
							 : NULL;
	wait_exact_physical
		= wait_exact_first_authority != NULL
			  ? strstr(wait_exact_first_authority, "cluster_pcm_x_requester_wait_once_result(")
			  : NULL;
	wait_exact_second_revalidate
		= wait_exact_physical != NULL
			  ? strstr(wait_exact_physical + 1,
					   "gcs_block_pcm_x_requester_pre_sleep_revalidate(&context)")
			  : NULL;
	wait_revalidate_authority
		= wait_revalidate != NULL
			  ? strstr(wait_revalidate, "gcs_block_pcm_x_requester_authority_exact(")
			  : NULL;
	wait_revalidate_nested_guard
		= wait_revalidate_authority != NULL
			  ? strstr(wait_revalidate_authority, "cluster_pcm_x_nested_wait_guard_before_block()")
			  : NULL;
	UT_ASSERT_NOT_NULL(authority_runtime);
	UT_ASSERT_NOT_NULL(authority_runtime_exact);
	UT_ASSERT_NOT_NULL(authority_peer);
	UT_ASSERT_NOT_NULL(wait_exact_first_authority);
	UT_ASSERT_NOT_NULL(wait_exact_physical);
	UT_ASSERT_NOT_NULL(wait_exact_second_revalidate);
	UT_ASSERT_NOT_NULL(wait_revalidate_authority);
	UT_ASSERT_NOT_NULL(wait_revalidate_nested_guard);
	if (authority_runtime != NULL && authority_runtime_exact != NULL && authority_peer != NULL
		&& authority_helper_end != NULL)
		UT_ASSERT(authority_runtime < authority_runtime_exact
				  && authority_runtime_exact < authority_peer
				  && authority_peer < authority_helper_end);
	if (wait_exact_first_authority != NULL && wait_exact_physical != NULL
		&& wait_exact_second_revalidate != NULL && wait_exact_end != NULL)
		UT_ASSERT(wait_exact_first_authority < wait_exact_physical
				  && wait_exact_physical < wait_exact_second_revalidate
				  && wait_exact_second_revalidate < wait_exact_end);
	if (wait_revalidate_authority != NULL && wait_revalidate_nested_guard != NULL
		&& wait_revalidate_end != NULL)
		UT_ASSERT(wait_revalidate_authority < wait_revalidate_nested_guard
				  && wait_revalidate_nested_guard < wait_revalidate_end);
	formation = strstr(driver, "cluster_gcs_pcm_x_requester_formation_action(");
	formation_wait = formation != NULL
						 ? strstr(formation, "gcs_block_pcm_x_requester_wait(&wait_index)")
						 : NULL;
	authenticated_session = formation_wait != NULL
								? strstr(formation_wait, "gcs_block_pcm_x_authenticated_session(")
								: NULL;
	authority_preflight
		= authenticated_session != NULL
			  ? strstr(authenticated_session, "gcs_block_pcm_x_requester_authority_exact(")
			  : NULL;
	request_id = authority_preflight != NULL
					 ? strstr(authority_preflight, "gcs_block_pcm_x_next_request_id(")
					 : NULL;
	wait_publish
		= request_id != NULL ? strstr(request_id, "cluster_lmd_wait_state_publish(") : NULL;
	nested_guard = wait_publish != NULL
					   ? strstr(wait_publish, "cluster_pcm_x_nested_wait_guard_before_block()")
					   : NULL;
	ownership_snapshot
		= nested_guard != NULL ? strstr(nested_guard, "cluster_bufmgr_pcm_own_snapshot(buf") : NULL;
	first_wait = ownership_snapshot != NULL
					 ? strstr(ownership_snapshot, "gcs_block_pcm_x_requester_wait_exact(")
					 : NULL;
	UT_ASSERT_NOT_NULL(formation);
	UT_ASSERT_NOT_NULL(formation_wait);
	UT_ASSERT_NOT_NULL(authenticated_session);
	UT_ASSERT_NOT_NULL(authority_preflight);
	UT_ASSERT_NOT_NULL(request_id);
	UT_ASSERT_NOT_NULL(wait_publish);
	UT_ASSERT_NOT_NULL(nested_guard);
	UT_ASSERT_NOT_NULL(ownership_snapshot);
	UT_ASSERT_NOT_NULL(first_wait);
	if (formation != NULL && formation_wait != NULL && authenticated_session != NULL
		&& authority_preflight != NULL && request_id != NULL && wait_publish != NULL
		&& nested_guard != NULL && ownership_snapshot != NULL && first_wait != NULL
		&& driver_end != NULL)
		UT_ASSERT(formation < formation_wait && formation_wait < authenticated_session
				  && authenticated_session < authority_preflight && authority_preflight < request_id
				  && request_id < wait_publish && wait_publish < nested_guard
				  && nested_guard < ownership_snapshot && ownership_snapshot < first_wait
				  && first_wait < driver_end);
	wait_scan = driver;
	while (wait_scan != NULL
		   && (wait_scan = strstr(wait_scan, "gcs_block_pcm_x_requester_wait(&wait_index)")) != NULL
		   && wait_scan < driver_end) {
		raw_wait_count++;
		wait_scan++;
	}
	wait_scan = driver;
	while (wait_scan != NULL
		   && (wait_scan = strstr(wait_scan, "gcs_block_pcm_x_requester_wait_exact(")) != NULL
		   && wait_scan < driver_end) {
		exact_wait_count++;
		wait_scan++;
	}
	UT_ASSERT_EQ(raw_wait_count, 1);
	UT_ASSERT_EQ(exact_wait_count, 9);
	snapshot_before
		= rekey_helper != NULL ? strstr(rekey_helper, "cluster_bufmgr_pcm_own_snapshot(") : NULL;
	rekey_exact
		= snapshot_before != NULL
			  ? strstr(snapshot_before, "cluster_pcm_x_local_leader_rekey_generation_exact(")
			  : NULL;
	snapshot_after
		= rekey_exact != NULL ? strstr(rekey_exact, "cluster_bufmgr_pcm_own_snapshot(") : NULL;
	UT_ASSERT_NOT_NULL(snapshot_before);
	UT_ASSERT_NOT_NULL(rekey_exact);
	UT_ASSERT_NOT_NULL(snapshot_after);
	if (snapshot_before != NULL && rekey_exact != NULL && snapshot_after != NULL
		&& rekey_helper_end != NULL)
		UT_ASSERT(snapshot_before < rekey_exact && rekey_exact < snapshot_after
				  && snapshot_after < rekey_helper_end);
	join = strstr(driver, "cluster_pcm_x_local_join_begin_semantic(");
	leader_rekey = join != NULL ? strstr(join, "gcs_block_pcm_x_rekey_leader_base_exact(") : NULL;
	claim = leader_rekey != NULL ? strstr(leader_rekey, "cluster_pcm_x_local_writer_claim_exact(")
								 : NULL;
	enqueue = claim != NULL ? strstr(claim, "cluster_pcm_x_local_enqueue_arm_exact(") : NULL;
	graph_clear = strstr(driver, "cluster_pcm_x_local_follower_wfg_clear_exact(");
	follower_snapshot
		= graph_clear != NULL
			  ? strstr(graph_clear, "cluster_pcm_x_local_follower_wfg_snapshot_exact(")
			  : NULL;
	graph_replace = follower_snapshot != NULL
						? strstr(follower_snapshot, "cluster_lmd_graph_replace_waiter_edges_exact(")
						: NULL;
	self_adopt = strstr(driver, "cluster_gcs_pcm_x_adopt_self_image(");
	remote_fetch = strstr(driver, "cluster_gcs_pcm_x_fetch_image_and_install(");
	install_ready = strstr(driver, "cluster_pcm_x_local_install_ready_arm_exact(");
	self_finish = strstr(driver, "cluster_gcs_pcm_x_finish_self_image_x(");
	remote_finish = strstr(driver, "cluster_bufmgr_pcm_own_finish_x_commit(");
	final_ack = strstr(driver, "cluster_pcm_x_local_final_ack_arm_exact(");
	final_confirm = strstr(driver, "PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM");
	clear_wait = strstr(driver, "cluster_lmd_wait_state_clear(");

	UT_ASSERT_NOT_NULL(join);
	UT_ASSERT_NOT_NULL(strstr(driver, "join_result == RESOURCE_X_LOCAL_LEADER_MUST_SUBMIT"));
	UT_ASSERT_NOT_NULL(strstr(driver, "join_result == RESOURCE_X_LOCAL_JOINED_LOCAL_ASSERTION"));
	UT_ASSERT_NOT_NULL(strstr(driver, "join_result == RESOURCE_X_LOCAL_WAIT_SUCCESSOR_ROUND"));
	UT_ASSERT_NULL(strstr(driver, "cluster_pcm_x_local_join_begin(&identity"));
	UT_ASSERT_NOT_NULL(leader_rekey);
	UT_ASSERT_NOT_NULL(claim);
	UT_ASSERT_NOT_NULL(enqueue);
	UT_ASSERT_NOT_NULL(follower_snapshot);
	UT_ASSERT_NOT_NULL(graph_replace);
	UT_ASSERT_NOT_NULL(graph_clear);
	UT_ASSERT_NOT_NULL(self_adopt);
	UT_ASSERT_NOT_NULL(remote_fetch);
	UT_ASSERT_NOT_NULL(install_ready);
	UT_ASSERT_NOT_NULL(self_finish);
	UT_ASSERT_NOT_NULL(remote_finish);
	UT_ASSERT_NOT_NULL(final_ack);
	UT_ASSERT_NOT_NULL(final_confirm);
	UT_ASSERT_NOT_NULL(clear_wait);
	if (join != NULL && leader_rekey != NULL && claim != NULL && enqueue != NULL)
		UT_ASSERT(join < leader_rekey && leader_rekey < claim && claim < enqueue
				  && enqueue < driver_end);
	UT_ASSERT_NOT_NULL(strstr(driver, "cluster_pcm_x_local_lookup_exact(&handle.identity"));
	UT_ASSERT_NOT_NULL(
		strstr(driver, "cluster_gcs_pcm_x_role_refresh_exact(&handle, &fresh_handle)"));
	UT_ASSERT_NOT_NULL(strstr(driver, "initial_own.pcm_state != (uint8)PCM_STATE_X"));
	{
		const char *preflight;
		const char *preflight_retry;

		preflight = strstr(driver, "cluster_gcs_pcm_x_remote_reservation_preflight(");
		preflight_retry
			= preflight != NULL
				  ? strstr(preflight, "GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT")
				  : NULL;
		UT_ASSERT_NOT_NULL(preflight);
		UT_ASSERT_NOT_NULL(preflight_retry);
		/* The transient-BUSY wait dispatch must sit between the preflight and
		 * the image fetch;  a non-wait verdict is the only fail-closed exit. */
		if (preflight != NULL && preflight_retry != NULL && remote_fetch != NULL)
			UT_ASSERT(preflight < preflight_retry && preflight_retry < remote_fetch);
	}
	if (follower_snapshot != NULL && graph_replace != NULL && graph_clear != NULL)
		UT_ASSERT(graph_clear < follower_snapshot && follower_snapshot < graph_replace
				  && graph_replace < driver_end);
	if (self_adopt != NULL && remote_fetch != NULL && install_ready != NULL)
		UT_ASSERT(self_adopt < install_ready && remote_fetch < install_ready);
	if (self_finish != NULL && remote_finish != NULL && final_ack != NULL)
		UT_ASSERT(self_finish < final_ack && remote_finish < final_ack);
	if (final_confirm != NULL && clear_wait != NULL)
		UT_ASSERT(final_confirm < clear_wait && clear_wait < driver_end);
	free(source);
}


UT_TEST(test_pcm_x_requester_retry_policy_is_operation_exact)
{
	PcmXRuntimeSnapshot current;
	PcmXRuntimeSnapshot start;
	const struct {
		GcsBlockPcmXRequesterSite site;
		PcmXQueueResult result;
		GcsBlockPcmXRetryAction action;
	} cases[] = {
		{ GCS_BLOCK_PCM_X_RETRY_SITE_JOIN, PCM_X_QUEUE_NO_CAPACITY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_JOIN, PCM_X_QUEUE_STALE, GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_GATE_RETRY,
		  GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_NOT_FOUND,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM, PCM_X_QUEUE_NO_CAPACITY,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF, PCM_X_QUEUE_GATE_RETRY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, PCM_X_QUEUE_BARRIER_CLOSED,
		  GCS_BLOCK_PCM_X_RETRY_WAIT },
		/* STALE at the follower WFG snapshot is the same slot-churn signal
		 * (promotion / round advance) the claim site recovers from with a
		 * refresh lookup;  it must re-dispatch, not close the runtime. */
		{ GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_REFRESH_ROLE },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, PCM_X_QUEUE_CORRUPT,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_RESNAPSHOT_WFG },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT, PCM_X_QUEUE_CORRUPT,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_WFG_CLEAR, PCM_X_QUEUE_NOT_READY,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, PCM_X_QUEUE_NOT_READY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH, PCM_X_QUEUE_BARRIER_CLOSED,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_ARM, PCM_X_QUEUE_NOT_READY,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_ARM, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		/* Remote-reservation preflight: BUSY is a transient own-slot lifecycle
		 * (a revoke/grant flag mid-flight on this node) and must wait, never
		 * close the runtime.  STALE stays fail-closed for now: it means the
		 * enqueue-time base_own_generation was consumed by a revoke while the
		 * request was queued, which the grant/final-ack chain cannot absorb
		 * without a master-visible rebase (pending protocol amendment). */
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_BUSY,
		  GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_CORRUPT,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_COUNTER_EXHAUSTED,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
	};
	size_t i;

	for (i = 0; i < lengthof(cases); i++)
		UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_retry_action(cases[i].site, cases[i].result),
					 cases[i].action);

	UT_ASSERT(cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_BUSY));
	UT_ASSERT(cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_GATE_RETRY));
	UT_ASSERT(cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_STALE));
	UT_ASSERT(!cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_BARRIER_CLOSED));
	UT_ASSERT(!cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_NOT_READY));
	UT_ASSERT(!cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_CORRUPT));

	memset(&start, 0, sizeof(start));
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&start),
				 GCS_BLOCK_PCM_X_FORMATION_WAIT);
	start.state = PCM_X_RUNTIME_ACTIVE;
	start.gate_generation = 7;
	start.master_session_incarnation = 11;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&start),
				 GCS_BLOCK_PCM_X_FORMATION_PROCEED);
	current = start;
	UT_ASSERT(cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current = start;
	current.gate_generation = 0;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	current = start;
	current.master_session_incarnation = 0;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	current = start;
	current.gate_generation++;
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current = start;
	current.state = PCM_X_RUNTIME_RECOVERY_BLOCKED;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	memset(&current, 0, sizeof(current));
	current.gate_generation = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	memset(&current, 0, sizeof(current));
	current.master_session_incarnation = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	current = start;
	current.state = PCM_X_RUNTIME_SHUTTING_DOWN;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
}


/* One CLAIMED-without-cookie observation is a legal cancel/serve two-phase
 * window;  only a per-ticket streak that survives consecutive periodic drive
 * ticks may close the runtime.  Any settled drive for the tag resets it. */
UT_TEST(test_pcm_x_drive_anomaly_streak_gates_fail_closed)
{
	GcsBlockPcmXDriveAnomaly table[4];
	BufferTag tag_a;
	BufferTag tag_b;
	uint32 i;

	memset(table, 0, sizeof(table));
	memset(&tag_a, 0, sizeof(tag_a));
	memset(&tag_b, 0, sizeof(tag_b));
	tag_a.blockNum = 1;
	tag_b.blockNum = 2;

	for (i = 1; i < GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 7));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 7));

	/* A different ticket on the same tag counts independently. */
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 8));
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_b, 7));

	/* Settling the tag clears every streak bound to it, and only it. */
	cluster_gcs_pcm_x_drive_anomaly_settle(table, 4, &tag_a);
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 7));
	for (i = 2; i < GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_b, 7));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_b, 7));

	/* A missing tracking substrate stays fail-closed. */
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(NULL, 4, &tag_a, 7));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 0, &tag_a, 7));

	/* Table pressure recycles the lowest streak:  stale one-shot entries are
	 * evicted while a persisting anomaly keeps its count uninterrupted. */
	cluster_gcs_pcm_x_drive_anomaly_settle(table, 4, &tag_a);
	cluster_gcs_pcm_x_drive_anomaly_settle(table, 4, &tag_b);
	tag_a.blockNum = 50;
	for (i = 1; i <= 3; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 9));
	tag_a.blockNum = 100;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 101;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 102;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 103;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 50;
	for (i = 4; i < GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 9));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 9));
}


/* Pin the four transient-churn recovery arms in the source:  benign slot
 * churn and two-phase cookie windows must route to per-request refresh or
 * per-ticket damping instead of an immediate runtime fuse. */
UT_TEST(test_pcm_x_transient_churn_recovers_without_runtime_fuse)
{
	char *source = read_gcs_block_source();
	const char *driver;
	const char *driver_end;
	const char *cleanup;
	const char *cleanup_end;
	const char *ensure;
	const char *ensure_end;
	const char *drive;
	const char *drive_end;
	const char *scan;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;

	/* (a) follower WFG snapshot STALE takes the claim-site refresh lookup:
	 * the driver must contain a second handle-identity lookup site. */
	driver = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	scan = driver != NULL ? strstr(driver, "cluster_pcm_x_local_lookup_exact(&handle.identity")
						  : NULL;
	UT_ASSERT_NOT_NULL(scan);
	scan = scan != NULL ? strstr(scan + 1, "cluster_pcm_x_local_lookup_exact(&handle.identity")
						: NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && driver_end != NULL)
		UT_ASSERT(scan < driver_end);

	/* (b) cleanup CANCEL_LOCAL retries a STALE cancel through a refreshed
	 * handle for the exact same identity before any fail-closed verdict. */
	cleanup = strstr(source, "\ngcs_block_pcm_x_requester_cleanup_impl(");
	cleanup_end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	scan = cleanup != NULL
			   ? strstr(cleanup, "cluster_pcm_x_local_lookup_exact(&cleanup->handle.identity")
			   : NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && cleanup_end != NULL)
		UT_ASSERT(scan < cleanup_end);

	/* (c) the already-claimed ensure arm rechecks the ticket under its own
	 * domain lock before calling a missing cookie BAD_STATE. */
	ensure = strstr(source, "\ngcs_block_pcm_x_ensure_pending_x_claim(");
	ensure_end = ensure != NULL ? strstr(ensure, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(ensure);
	UT_ASSERT_NOT_NULL(ensure_end);
	scan = ensure != NULL ? strstr(ensure, "cluster_pcm_x_master_pending_x_claim_state_exact(")
						  : NULL;
	UT_ASSERT_NOT_NULL(scan);
	scan = scan != NULL ? strstr(scan + 1, "cluster_pcm_x_master_pending_x_claim_state_exact(")
						: NULL;
	UT_ASSERT_NOT_NULL(scan);
	scan = scan != NULL ? strstr(scan + 1, "cluster_pcm_x_master_pending_x_claim_state_exact(")
						: NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && ensure_end != NULL)
		UT_ASSERT(scan < ensure_end);

	/* (d) the drive dispatch damps BAD_STATE through the per-ticket streak
	 * and settles it on any non-anomalous completion. */
	drive = strstr(source, "\ngcs_block_pcm_x_master_drive_tag(");
	drive_end = drive != NULL ? strstr(drive, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(drive);
	UT_ASSERT_NOT_NULL(drive_end);
	scan = drive != NULL ? strstr(drive, "cluster_gcs_pcm_x_drive_anomaly_note(") : NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && drive_end != NULL)
		UT_ASSERT(scan < drive_end);
	scan = drive != NULL ? strstr(drive, "cluster_gcs_pcm_x_drive_anomaly_settle(") : NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && drive_end != NULL)
		UT_ASSERT(scan < drive_end);

	free(source);
}


UT_TEST(test_pcm_x_remote_reservation_preflight_binds_identity_base)
{
	ClusterPcmOwnSnapshot live;
	PcmXWaitIdentity identity;

	memset(&live, 0, sizeof(live));
	memset(&identity, 0, sizeof(identity));
	identity.tag.spcOid = 7;
	identity.tag.dbOid = 8;
	identity.tag.relNumber = 9;
	identity.tag.forkNum = MAIN_FORKNUM;
	identity.tag.blockNum = 10;
	live.tag = identity.tag;
	live.generation = 41;
	live.reservation_token = 12;
	live.pcm_state = (uint8)PCM_STATE_N;
	identity.base_own_generation = live.generation;

	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity), PCM_X_QUEUE_OK);
	live.generation++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_STALE);
	live.generation = identity.base_own_generation;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_BUSY);
	live.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_BUSY);
	live.flags = PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_CORRUPT);
	live.flags = UINT32_C(0x80);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_CORRUPT);
	live.flags = 0;
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_STALE);
	live.pcm_state = (uint8)PCM_STATE_N;
	live.tag.blockNum++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_STALE);
	live.tag = identity.tag;
	identity.base_own_generation = UINT64_MAX;
	live.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
}


UT_TEST(test_pcm_x_requester_wait_backoff_saturates)
{
	uint32 wait_index = 0;
	uint32 i;

	for (i = 0; i < CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS + 4; i++)
		wait_index = cluster_gcs_pcm_x_requester_wait_index_advance(wait_index);
	UT_ASSERT_EQ(wait_index, CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS - 1);
	UT_ASSERT_EQ(cluster_pcm_x_holder_retry_delay_ms(wait_index),
				 cluster_pcm_x_holder_retry_delay_ms(CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS + 20));
}


UT_TEST(test_pcm_x_requester_cleanup_never_guesses_after_irreversible_boundary)
{
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(false, false, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_NONE);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, false, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, true, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(false, true, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, true, true, false),
				 GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, true, false, true),
				 GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED);
}


UT_TEST(test_pcm_x_retire_commit_wakes_exact_waiters_before_ack_or_resolve)
{
	char *source = read_gcs_block_source();
	const char *common;
	const char *common_end;
	const char *allocate;
	const char *allocate_after;
	const char *collect;
	const char *dedup_retire;
	const char *exact_wake;
	const char *wake;
	const char *wake_end;
	const char *read;
	const char *match;
	const char *set_latch;
	const char *writer_release;
	const char *writer_release_end;
	const char *writer_collect;
	const char *writer_wake;
	const char *writer_allocate;
	const char *writer_cleanup;
	const char *writer_cleanup_end;
	const char *writer_cleanup_try;
	const char *writer_cleanup_exact;
	const char *writer_cleanup_catch;
	const char *writer_cleanup_flush;
	const char *writer_cleanup_fail_closed;
	const char *remote;
	const char *remote_end;
	const char *remote_apply;
	const char *remote_ack;
	const char *terminal;
	const char *terminal_end;
	const char *self_apply;
	const char *self_resolve;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	wake = strstr(source, "\ngcs_block_pcm_x_wake_requester_exact(");
	wake_end = wake != NULL ? strstr(wake, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(wake);
	UT_ASSERT_NOT_NULL(wake_end);
	if (wake != NULL && wake_end != NULL) {
		read = strstr(wake, "cluster_lmd_wait_state_read_exact(");
		match = read != NULL ? strstr(read, "cluster_gcs_pcm_x_wait_identity_matches(") : NULL;
		set_latch = match != NULL ? strstr(match, "SetLatch(") : NULL;
		UT_ASSERT_NOT_NULL(read);
		UT_ASSERT_NOT_NULL(match);
		UT_ASSERT_NOT_NULL(set_latch);
		if (read != NULL && match != NULL && set_latch != NULL)
			UT_ASSERT(read < match && match < set_latch && set_latch < wake_end);
	}
	writer_release = strstr(source, "\ncluster_gcs_pcm_x_writer_claim_release_and_wake_exact(");
	writer_release_end = writer_release != NULL ? strstr(writer_release, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(writer_release);
	UT_ASSERT_NOT_NULL(writer_release_end);
	if (writer_release != NULL && writer_release_end != NULL) {
		writer_collect
			= strstr(writer_release, "cluster_pcm_x_local_writer_claim_release_collect_exact(");
		writer_wake = writer_collect != NULL
						  ? strstr(writer_collect, "gcs_block_pcm_x_wake_requester_exact(")
						  : NULL;
		writer_allocate = strstr(writer_release, "palloc");
		UT_ASSERT_NOT_NULL(writer_collect);
		UT_ASSERT_NOT_NULL(writer_wake);
		if (writer_collect != NULL && writer_wake != NULL)
			UT_ASSERT(writer_collect < writer_wake && writer_wake < writer_release_end);
		UT_ASSERT(writer_allocate == NULL || writer_allocate > writer_release_end);
	}
	writer_cleanup = strstr(source, "\ncluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(");
	writer_cleanup_end = writer_cleanup != NULL ? strstr(writer_cleanup, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(writer_cleanup);
	UT_ASSERT_NOT_NULL(writer_cleanup_end);
	if (writer_cleanup != NULL && writer_cleanup_end != NULL) {
		writer_cleanup_try = strstr(writer_cleanup, "PG_TRY();");
		writer_cleanup_exact
			= writer_cleanup_try != NULL
				  ? strstr(writer_cleanup_try,
						   "cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(claim)")
				  : NULL;
		writer_cleanup_catch
			= writer_cleanup_exact != NULL ? strstr(writer_cleanup_exact, "PG_CATCH();") : NULL;
		writer_cleanup_flush = writer_cleanup_catch != NULL
								   ? strstr(writer_cleanup_catch, "FlushErrorState();")
								   : NULL;
		writer_cleanup_fail_closed
			= writer_cleanup_flush != NULL
				  ? strstr(writer_cleanup_flush, "cluster_pcm_x_runtime_fail_closed();")
				  : NULL;
		UT_ASSERT_NOT_NULL(writer_cleanup_try);
		UT_ASSERT_NOT_NULL(writer_cleanup_exact);
		UT_ASSERT_NOT_NULL(writer_cleanup_catch);
		UT_ASSERT_NOT_NULL(writer_cleanup_flush);
		UT_ASSERT_NOT_NULL(writer_cleanup_fail_closed);
		if (writer_cleanup_try != NULL && writer_cleanup_exact != NULL
			&& writer_cleanup_catch != NULL && writer_cleanup_flush != NULL
			&& writer_cleanup_fail_closed != NULL)
			UT_ASSERT(writer_cleanup_try < writer_cleanup_exact
					  && writer_cleanup_exact < writer_cleanup_catch
					  && writer_cleanup_catch < writer_cleanup_flush
					  && writer_cleanup_flush < writer_cleanup_fail_closed
					  && writer_cleanup_fail_closed < writer_cleanup_end);
	}
	common = strstr(source, "\ngcs_block_pcm_x_local_retire_apply_and_wake(");
	common_end = common != NULL ? strstr(common, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(common);
	UT_ASSERT_NOT_NULL(common_end);
	if (common != NULL && common_end != NULL) {
		allocate = strstr(common, "palloc0(");
		collect = allocate != NULL
					  ? strstr(allocate, "cluster_pcm_x_local_retire_up_to_collect_exact(")
					  : NULL;
		dedup_retire = collect != NULL
						   ? strstr(collect, "cluster_gcs_block_dedup_pcm_x_retire_up_to(")
						   : NULL;
		exact_wake
			= collect != NULL ? strstr(collect, "gcs_block_pcm_x_wake_requester_exact(") : NULL;
		allocate_after = allocate != NULL ? strstr(allocate + 1, "palloc0(") : NULL;
		UT_ASSERT_NOT_NULL(allocate);
		UT_ASSERT_NOT_NULL(collect);
		UT_ASSERT_NOT_NULL(dedup_retire);
		UT_ASSERT_NOT_NULL(exact_wake);
		if (allocate != NULL && collect != NULL && dedup_retire != NULL && exact_wake != NULL)
			UT_ASSERT(allocate < collect && collect < dedup_retire && dedup_retire < exact_wake
					  && exact_wake < common_end);
		UT_ASSERT(allocate_after == NULL || allocate_after > common_end);
	}
	remote = strstr(source, "\ncluster_gcs_handle_pcm_x_retire_up_to_envelope(");
	remote_end = remote != NULL ? strstr(remote, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(remote);
	UT_ASSERT_NOT_NULL(remote_end);
	if (remote != NULL && remote_end != NULL) {
		remote_apply = strstr(remote, "gcs_block_pcm_x_local_retire_apply_and_wake(");
		remote_ack = remote_apply != NULL ? strstr(remote_apply, "gcs_block_pcm_x_send_retire_ack(")
										  : NULL;
		UT_ASSERT_NOT_NULL(remote_apply);
		UT_ASSERT_NOT_NULL(remote_ack);
		if (remote_apply != NULL && remote_ack != NULL)
			UT_ASSERT(remote_apply < remote_ack && remote_ack < remote_end);
	}
	terminal = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	terminal_end = terminal != NULL ? strstr(terminal, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(terminal);
	UT_ASSERT_NOT_NULL(terminal_end);
	if (terminal != NULL && terminal_end != NULL) {
		self_apply = strstr(terminal, "gcs_block_pcm_x_local_retire_apply_and_wake(");
		self_resolve = self_apply != NULL
						   ? strstr(self_apply, "cluster_pcm_x_master_retire_ack_resolve_exact(")
						   : NULL;
		UT_ASSERT_NOT_NULL(self_apply);
		UT_ASSERT_NOT_NULL(self_resolve);
		if (self_apply != NULL && self_resolve != NULL)
			UT_ASSERT(self_apply < self_resolve && self_resolve < terminal_end);
	}
	UT_ASSERT_EQ(count_occurrences(source, "gcs_block_pcm_x_local_retire_apply_and_wake("), 3);
	UT_ASSERT_EQ(count_occurrences(source, "cluster_pcm_x_local_retire_up_to_collect_exact("), 1);
	free(source);
}


UT_TEST(test_pcm_x_tagless_retire_uses_explicit_data_plane_handoff)
{
	char *source = read_gcs_block_source();
	const char *handler;
	const char *handler_end;
	const char *kick;
	const char *kick_end;
	const char *ack_send;
	const char *request_stage;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_retire_up_to_envelope(");
	handler_end = handler != NULL ? strstr(handler + 1, "\n}\n\n\n") : NULL;
	kick = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	kick_end = kick != NULL ? strstr(kick + 1, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(handler_end);
	UT_ASSERT_NOT_NULL(kick);
	UT_ASSERT_NOT_NULL(kick_end);
	ack_send = handler != NULL ? strstr(handler, "gcs_block_pcm_x_send_retire_ack(") : NULL;
	request_stage = kick != NULL ? strstr(kick, "gcs_block_pcm_x_stage_retire_up_to(") : NULL;
	UT_ASSERT_NOT_NULL(ack_send);
	UT_ASSERT_NOT_NULL(request_stage);
	if (handler_end != NULL && ack_send != NULL)
		UT_ASSERT(ack_send < handler_end);
	if (kick_end != NULL && request_stage != NULL)
		UT_ASSERT(request_stage < kick_end);
	free(source);
}


UT_TEST(test_pcm_x_role_refresh_accepts_only_same_member_promotion)
{
	PcmXLocalHandle follower;
	PcmXLocalHandle promoted;

	memset(&follower, 0, sizeof(follower));
	follower.identity.node_id = 1;
	follower.identity.procno = 7;
	follower.identity.xid = 11;
	follower.identity.cluster_epoch = 13;
	follower.identity.request_id = 17;
	follower.identity.wait_seq = 19;
	follower.identity.base_own_generation = 23;
	follower.tag_slot.slot_index = 29;
	follower.tag_slot.slot_generation = 31;
	follower.membership_slot.slot_index = 37;
	follower.membership_slot.slot_generation = 41;
	follower.local_sequence = 43;
	follower.local_round = 47;
	follower.role = PCM_X_LOCAL_ROLE_FOLLOWER;
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;

	UT_ASSERT(cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted.role = PCM_X_LOCAL_ROLE_FOLLOWER;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.membership_slot.slot_generation++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.local_sequence++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.local_round++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.identity.base_own_generation++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
}

UT_TEST(test_legacy_byte_proof_sites_republish_kept_pi_mirror)
{
	static const char *const install_contract[]
		= { "cluster_bufmgr_pcm_x_content_write_permitted", "memcpy", "PageSetLSN",
			"cluster_bufmgr_pcm_own_republish_grant_pending_image", "LWLockRelease" };
	static const char *const fallback_contract[]
		= { "GCS_LOST_WRITE_PASS", "cluster_bufmgr_pcm_own_republish_grant_pending_image",
			"cluster_bufmgr_refresh_block_from_storage_for_gcs", "GCS_LOST_WRITE_PASS",
			"cluster_bufmgr_pcm_own_republish_grant_pending_image" };
	char *source = read_gcs_block_source();

	/* A retained-image release now keeps a frozen PI+BM_VALID N mirror for
	 * pre-existing pins.  Its bytes regain CURRENT only where a legacy grant
	 * just proved them: the shipped-image install memcpy (still under the
	 * same content EXCLUSIVE hold) and both storage-fallback PASS proofs
	 * (direct SCN PASS, and refresh-then-PASS).  The SKIP arms must leave PI
	 * frozen so the finish valid-image gate keeps failing closed on unproven
	 * bytes. */
	assert_ordered_in_function(source, "\ngcs_block_install_block(", "\nstatic ", install_contract,
							   lengthof(install_contract));
	assert_ordered_in_function(source, "\ncluster_gcs_block_fallback_verify_refresh(", "\nstatic ",
							   fallback_contract, lengthof(fallback_contract));
	free(source);
}


UT_TEST(test_revoke_handler_silent_refusal_arms_all_note)
{
	char *source = read_gcs_block_source();
	const char *handler = strstr(source, "\ncluster_gcs_handle_pcm_x_revoke_envelope(");
	const char *materialize = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	const char *publish;
	const char *reset;

	/* The master re-sends REVOKE forever, so every refusal arm in the source
	 * handler must name itself through the log-once streak note (t/400 form-B
	 * wedges previously refused silently at un-instrumented arms): ingress
	 * auth, holder-ledger stale, holder-progress error, image reserve, apply,
	 * and ingress snapshot.  Merely accepting/re-arming work is not progress;
	 * the streak resets only after exact READY publication. */
	UT_ASSERT_NOT_NULL(handler);
	if (handler != NULL)
		UT_ASSERT(count_occurrences(handler, "gcs_block_pcm_x_revoke_refusal_note_exact(") >= 6);
	UT_ASSERT_NOT_NULL(materialize);
	if (materialize != NULL) {
		UT_ASSERT_NOT_NULL(strstr(materialize, "\"materialize-copy\""));
		UT_ASSERT_NOT_NULL(strstr(materialize, "\"materialize-finish\""));
	}
	publish = materialize != NULL
				  ? strstr(materialize, "cluster_gcs_block_dedup_pcm_x_publish_ready_exact(")
				  : NULL;
	reset = publish != NULL ? strstr(publish, "gcs_block_pcm_x_revoke_refusal_note(NULL, 0, NULL)")
							: NULL;
	if (reset == NULL && publish != NULL)
		reset = strstr(publish,
					   "gcs_block_pcm_x_revoke_refusal_note_exact(NULL, 0, NULL, NULL, 0, 0)");
	UT_ASSERT_NOT_NULL(publish);
	UT_ASSERT_NOT_NULL(reset);
	UT_ASSERT_NOT_NULL(strstr(source, "request_id=%llu wait_seq=%llu"));
	UT_ASSERT_NOT_NULL(strstr(source, "image_id=%llu source_generation=%llu"));
	UT_ASSERT_NOT_NULL(strstr(source, "own_generation=%llu token=%llu"));
	UT_ASSERT_NOT_NULL(strstr(source, "materialized-finish-vm-fsm-pinned"));
	UT_ASSERT_NOT_NULL(strstr(source, "materialized-finish-io-in-progress"));
	UT_ASSERT_NOT_NULL(strstr(source, "materialized-finish-live-flags"));
	UT_ASSERT_NOT_NULL(strstr(source, "finish_refusal=%u shared_refcount=%u"));
	UT_ASSERT_NOT_NULL(strstr(source, "bm_io_in_progress=%s live_flags=%u live_token=%llu"));
	free(source);
}


UT_TEST(test_local_master_read_image_retries_holder_busy_with_fresh_identity)
{
	char *source = read_gcs_block_source();
	const char *read_image
		= source != NULL ? strstr(source, "\ncluster_gcs_local_master_read_image_and_wait(") : NULL;
	const char *read_image_end = read_image != NULL ? strstr(read_image, "\n}\n") : NULL;
	const char *budget;
	const char *loop;
	const char *backoff;
	const char *fresh_id;
	const char *slot_id;
	const char *forward_id;
	const char *send;
	const char *retryable_deny;
	const char *retry;
	const char *terminal_error;

	/* P0-21 completion: a DATA worker must never wait behind BufferContent.
	 * Its conditional-copy refusal is HC105 transient, so the local-master
	 * requester retries with bounded backoff.  Each re-arm mints a new request
	 * id, preventing a delayed denial from an earlier attempt from winning the
	 * next attempt's slot. */
	UT_ASSERT_NOT_NULL(read_image);
	UT_ASSERT_NOT_NULL(read_image_end);
	if (read_image == NULL || read_image_end == NULL) {
		free(source);
		return;
	}
	budget = strstr(read_image, "cluster_gcs_block_retransmit_max_retries");
	loop = budget != NULL ? strstr(budget, "for (retry_attempt = 0;") : NULL;
	backoff = loop != NULL ? strstr(loop, "gcs_block_backoff_ms_for_retry(retry_attempt)") : NULL;
	fresh_id
		= backoff != NULL ? strstr(backoff, "gcs_block_pcm_x_next_request_id(&request_id)") : NULL;
	slot_id = fresh_id != NULL ? strstr(fresh_id, "slot->request_id = request_id") : NULL;
	forward_id = slot_id != NULL ? strstr(slot_id, "fwd.request_id = request_id") : NULL;
	send = forward_id != NULL ? strstr(forward_id, "cluster_grd_outbound_enqueue_backend_msg(")
							  : NULL;
	retryable_deny = send != NULL ? strstr(send, "GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER") : NULL;
	retry = retryable_deny != NULL ? strstr(retryable_deny, "continue;") : NULL;
	terminal_error
		= retry != NULL ? strstr(retry, "could not obtain read image from X holder") : NULL;
	UT_ASSERT_NOT_NULL(budget);
	UT_ASSERT_NOT_NULL(loop);
	UT_ASSERT_NOT_NULL(backoff);
	UT_ASSERT_NOT_NULL(fresh_id);
	UT_ASSERT_NOT_NULL(slot_id);
	UT_ASSERT_NOT_NULL(forward_id);
	UT_ASSERT_NOT_NULL(send);
	UT_ASSERT_NOT_NULL(retryable_deny);
	UT_ASSERT_NOT_NULL(retry);
	UT_ASSERT_NOT_NULL(terminal_error);
	if (budget != NULL && loop != NULL && backoff != NULL && fresh_id != NULL && slot_id != NULL
		&& forward_id != NULL && send != NULL && retryable_deny != NULL && retry != NULL
		&& terminal_error != NULL)
		UT_ASSERT(read_image < budget && budget < loop && loop < backoff && backoff < fresh_id
				  && fresh_id < slot_id && slot_id < forward_id && forward_id < send
				  && send < retryable_deny && retryable_deny < retry && retry < terminal_error
				  && terminal_error < read_image_end);
	free(source);
}

UT_TEST(test_local_master_read_image_stops_retrying_displaced_holder_exactly)
{
	char *source = read_gcs_block_source();
	const char *read_image
		= source != NULL ? strstr(source, "\ncluster_gcs_local_master_read_image_and_wait(") : NULL;
	const char *read_image_end = read_image != NULL ? strstr(read_image, "\n}\n") : NULL;
	const char *expected_arg;
	const char *retry_arg;
	const char *precheck;
	const char *reserve;
	const char *backoff;
	const char *backoff_check;
	const char *fresh_id;
	const char *denial;
	const char *denial_check;
	const char *drift_retry;
	const char *release;
	const char *drift_return;
	const char *terminal_error;

	/* P0-21 residual: a conditional refusal is retryable only while the
	 * complete remote-X authority token still names the same holder.  Once a
	 * queue handoff displaces that token, this helper must stop spending its
	 * bounded budget on the old node and return the retry boundary to bufmgr;
	 * the outer GRANT_PENDING abort/rearm owns fresh authority selection. */
	UT_ASSERT_NOT_NULL(read_image);
	UT_ASSERT_NOT_NULL(read_image_end);
	if (read_image == NULL || read_image_end == NULL) {
		free(source);
		return;
	}
	expected_arg = strstr(read_image, "const PcmAuthoritySnapshot *expected");
	retry_arg = expected_arg != NULL ? strstr(expected_arg, "bool *out_retry_denied") : NULL;
	precheck = retry_arg != NULL
				   ? strstr(retry_arg, "if (!cluster_pcm_lock_authority_matches(tag, expected))")
				   : NULL;
	reserve = precheck != NULL ? strstr(precheck, "gcs_block_reserve_slot(") : NULL;
	backoff
		= reserve != NULL ? strstr(reserve, "gcs_block_backoff_ms_for_retry(retry_attempt)") : NULL;
	backoff_check = backoff != NULL
						? strstr(backoff, "if (!cluster_pcm_lock_authority_matches(tag, expected))")
						: NULL;
	fresh_id = backoff_check != NULL
				   ? strstr(backoff_check, "gcs_block_pcm_x_next_request_id(&request_id)")
				   : NULL;
	denial = fresh_id != NULL ? strstr(fresh_id, "GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER") : NULL;
	denial_check = denial != NULL
					   ? strstr(denial, "if (!cluster_pcm_lock_authority_matches(tag, expected))")
					   : NULL;
	drift_retry = denial_check != NULL ? strstr(denial_check, "*out_retry_denied = true") : NULL;
	release = drift_retry != NULL ? strstr(drift_retry, "gcs_block_release_slot(slot)") : NULL;
	drift_return = release != NULL ? strstr(release, "if (*out_retry_denied)") : NULL;
	terminal_error = drift_return != NULL
						 ? strstr(drift_return, "could not obtain read image from X holder")
						 : NULL;

	UT_ASSERT_NOT_NULL(expected_arg);
	UT_ASSERT_NOT_NULL(retry_arg);
	UT_ASSERT_NOT_NULL(precheck);
	UT_ASSERT_NOT_NULL(reserve);
	UT_ASSERT_NOT_NULL(backoff);
	UT_ASSERT_NOT_NULL(backoff_check);
	UT_ASSERT_NOT_NULL(fresh_id);
	UT_ASSERT_NOT_NULL(denial);
	UT_ASSERT_NOT_NULL(denial_check);
	UT_ASSERT_NOT_NULL(drift_retry);
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(drift_return);
	UT_ASSERT_NOT_NULL(terminal_error);
	if (expected_arg != NULL && retry_arg != NULL && precheck != NULL && reserve != NULL
		&& backoff != NULL && backoff_check != NULL && fresh_id != NULL && denial != NULL
		&& denial_check != NULL && drift_retry != NULL && release != NULL && drift_return != NULL
		&& terminal_error != NULL)
		UT_ASSERT(read_image < expected_arg && expected_arg < retry_arg && retry_arg < precheck
				  && precheck < reserve && reserve < backoff && backoff < backoff_check
				  && backoff_check < fresh_id && fresh_id < denial && denial < denial_check
				  && denial_check < drift_retry && drift_retry < release && release < drift_return
				  && drift_return < terminal_error && terminal_error < read_image_end);
	free(source);
}

UT_TEST(test_local_master_read_image_refusal_evidence_is_attempt_exact)
{
	char *source = read_gcs_block_source();
	char *t400 = read_source_path(T400_SOURCE_PATH);
	const char *read_image
		= source != NULL ? strstr(source, "\ncluster_gcs_local_master_read_image_and_wait(") : NULL;
	const char *read_image_end = read_image != NULL ? strstr(read_image, "\n}\n") : NULL;
	const char *attempts = read_image != NULL ? strstr(read_image, "attempts=%d") : NULL;
	const char *last_status = attempts != NULL ? strstr(attempts, "last_status=%d") : NULL;
	const char *forward
		= source != NULL ? strstr(source, "\ncluster_gcs_handle_block_forward_envelope(") : NULL;
	const char *forward_end = forward != NULL ? strstr(forward, "\n}\n") : NULL;
	const char *refusal_name
		= forward != NULL ? strstr(forward, "cluster_bufmgr_gcs_copy_refusal_name") : NULL;
	const char *refusal_log
		= refusal_name != NULL ? strstr(refusal_name, "holder ship image refused") : NULL;
	const char *request_id = refusal_log != NULL ? strstr(refusal_log, "request_id=") : NULL;

	/* P0-21 observation only: the requester terminal error reports its exact
	 * bounded-attempt result, while the holder process log binds each copy
	 * refusal reason to the forwarded request identity. */
	UT_ASSERT_NOT_NULL(read_image);
	UT_ASSERT_NOT_NULL(read_image_end);
	UT_ASSERT_NOT_NULL(attempts);
	UT_ASSERT_NOT_NULL(last_status);
	if (read_image_end != NULL && attempts != NULL && last_status != NULL)
		UT_ASSERT(attempts < last_status && last_status < read_image_end);
	UT_ASSERT_NOT_NULL(forward);
	UT_ASSERT_NOT_NULL(forward_end);
	UT_ASSERT_NOT_NULL(refusal_name);
	UT_ASSERT_NOT_NULL(refusal_log);
	UT_ASSERT_NOT_NULL(request_id);
	if (forward_end != NULL && refusal_name != NULL && refusal_log != NULL && request_id != NULL)
		UT_ASSERT(refusal_name < refusal_log && refusal_log < request_id
				  && request_id < forward_end);
	UT_ASSERT_NOT_NULL(t400);
	if (t400 != NULL) {
		const char *holder_before = strstr(t400, "holder_evicted_before_by_node");
		const char *holder_after
			= holder_before != NULL ? strstr(holder_before, "holder_evicted_after_by_node") : NULL;
		const char *holder_diag
			= holder_after != NULL ? strstr(holder_after, "holder copy refusal baseline") : NULL;
		const char *holder_final = holder_diag != NULL
									   ? strstr(holder_diag, "'block_forward_holder_evicted_count'")
									   : NULL;

		UT_ASSERT_NOT_NULL(holder_before);
		UT_ASSERT_NOT_NULL(holder_after);
		UT_ASSERT_NOT_NULL(holder_diag);
		UT_ASSERT_NOT_NULL(holder_final);
		if (holder_before != NULL && holder_after != NULL && holder_diag != NULL
			&& holder_final != NULL)
			UT_ASSERT(holder_before < holder_after && holder_after < holder_diag
					  && holder_diag < holder_final);
	}
	free(source);
	free(t400);
}

UT_TEST(test_local_master_x_transfer_revalidates_exact_authority_and_retries_stale)
{
	char *source = read_gcs_block_source();
	const char *transfer
		= source != NULL ? strstr(source, "\ncluster_gcs_local_master_x_transfer_and_wait(") : NULL;
	const char *transfer_end = transfer != NULL ? strstr(transfer, "\n}\n") : NULL;
	const char *precheck;
	const char *reserve;
	const char *forward_id;
	const char *send;
	const char *installed;
	const char *commit;
	const char *stale;
	const char *not_found;
	const char *commit_retry;
	const char *postcheck;
	const char *post_retry;
	const char *legacy_terminal;

	/* P0-26: carry one complete authority token from the local-master
	 * decision through the wire request.  Reject drift before sending; after
	 * an installed reply, atomically commit only that token.  A displaced
	 * token or a denial/timeout after authority drift returns retryable so
	 * bufmgr aborts GRANT_PENDING and mints a fresh token/request identity. */
	UT_ASSERT_NOT_NULL(transfer);
	UT_ASSERT_NOT_NULL(transfer_end);
	if (transfer == NULL || transfer_end == NULL) {
		free(source);
		return;
	}
	precheck = strstr(transfer, "if (!cluster_pcm_lock_authority_matches(tag, expected))");
	reserve = precheck != NULL ? strstr(precheck, "gcs_block_reserve_slot(") : NULL;
	forward_id = reserve != NULL ? strstr(reserve, "fwd.request_id = request_id") : NULL;
	send = forward_id != NULL ? strstr(forward_id, "cluster_grd_outbound_enqueue_backend_msg(")
							  : NULL;
	installed = send != NULL ? strstr(send, "if (installed)") : NULL;
	commit = installed != NULL ? strstr(installed, "cluster_pcm_lock_master_take_x_after_transfer(")
							   : NULL;
	stale = commit != NULL ? strstr(commit, "PCM_X_TRANSFER_COMMIT_STALE") : NULL;
	not_found = stale != NULL ? strstr(stale, "PCM_X_TRANSFER_COMMIT_NOT_FOUND") : NULL;
	commit_retry = not_found != NULL ? strstr(not_found, "*out_retry_denied = true") : NULL;
	postcheck
		= commit_retry != NULL
			  ? strstr(commit_retry, "if (!cluster_pcm_lock_authority_matches(tag, expected))")
			  : NULL;
	post_retry = postcheck != NULL ? strstr(postcheck, "*out_retry_denied = true") : NULL;
	legacy_terminal = post_retry != NULL ? strstr(post_retry, "if (read_image)") : NULL;

	UT_ASSERT_NOT_NULL(precheck);
	UT_ASSERT_NOT_NULL(reserve);
	UT_ASSERT_NOT_NULL(forward_id);
	UT_ASSERT_NOT_NULL(send);
	UT_ASSERT_NOT_NULL(installed);
	UT_ASSERT_NOT_NULL(commit);
	UT_ASSERT_NOT_NULL(stale);
	UT_ASSERT_NOT_NULL(not_found);
	UT_ASSERT_NOT_NULL(commit_retry);
	UT_ASSERT_NOT_NULL(postcheck);
	UT_ASSERT_NOT_NULL(post_retry);
	UT_ASSERT_NOT_NULL(legacy_terminal);
	if (precheck != NULL && reserve != NULL && forward_id != NULL && send != NULL
		&& installed != NULL && commit != NULL && stale != NULL && not_found != NULL
		&& commit_retry != NULL && postcheck != NULL && post_retry != NULL
		&& legacy_terminal != NULL)
		UT_ASSERT(transfer < precheck && precheck < reserve && reserve < forward_id
				  && forward_id < send && send < installed && installed < commit && commit < stale
				  && stale < not_found && not_found < commit_retry && commit_retry < postcheck
				  && postcheck < post_retry && post_retry < legacy_terminal
				  && legacy_terminal < transfer_end);
	free(source);
}

UT_TEST(test_remote_downgrade_prepares_exact_image_before_notify_and_reply)
{
	char *gcs_source = read_gcs_block_source();
	char *bufmgr_source = read_source_path("../../backend/storage/buffer/bufmgr.c");
	const char *forward = gcs_source != NULL
							  ? strstr(gcs_source, "\ncluster_gcs_handle_block_forward_envelope(")
							  : NULL;
	const char *forward_end = forward != NULL ? strstr(forward, "\n}\n") : NULL;
	const char *inject
		= forward != NULL ? strstr(forward, "cluster-gcs-block-evict-holder-before-ship") : NULL;
	const char *downgrade
		= inject != NULL
			  ? strstr(inject, "cluster_bufmgr_downgrade_x_to_s_remote_for_gcs_prepare_image(")
			  : NULL;
	const char *reuse
		= downgrade != NULL ? strstr(downgrade, "holder_ship_ok = remote_downgraded") : NULL;
	const char *post_notify_guard
		= downgrade != NULL
			  ? strstr(downgrade, "remote_downgrade_outcome == "
								  "CLUSTER_BUFMGR_GCS_DOWNGRADE_FAILCLOSED_POST_NOTIFY")
			  : NULL;
	const char *post_notify_return
		= post_notify_guard != NULL ? strstr(post_notify_guard, "return;") : NULL;
	const char *copy = reuse != NULL ? strstr(reuse, "gcs_block_get_ship_image(") : NULL;
	const char *helper
		= bufmgr_source != NULL
			  ? strstr(bufmgr_source,
					   "\ncluster_bufmgr_downgrade_x_to_s_remote_for_gcs_prepare_image(")
			  : NULL;
	const char *helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
	const char *reserve
		= helper != NULL ? strstr(helper, "cluster_bufmgr_pcm_own_begin_x_revoke(") : NULL;
	const char *precopy = reserve != NULL ? strstr(reserve, "memcpy(dst,") : NULL;
	const char *notify
		= precopy != NULL ? strstr(precopy, "cluster_gcs_send_transition_nowait(") : NULL;
	const char *commit
		= notify != NULL ? strstr(notify, "cluster_bufmgr_pcm_own_finish_x_to_s_downgrade(") : NULL;
	const char *abort
		= helper != NULL ? strstr(helper, "cluster_bufmgr_pcm_own_abort_x_revoke(") : NULL;
	const char *local_helper
		= bufmgr_source != NULL
			  ? strstr(bufmgr_source, "\ncluster_bufmgr_downgrade_x_to_s_for_gcs_prepare_image(")
			  : NULL;
	const char *local_helper_end = local_helper != NULL ? strstr(local_helper, "\n}\n") : NULL;
	const char *local_reserve = local_helper != NULL
									? strstr(local_helper, "cluster_bufmgr_pcm_own_begin_x_revoke(")
									: NULL;
	const char *local_copy = local_reserve != NULL ? strstr(local_reserve, "memcpy(dst,") : NULL;
	const char *local_master
		= local_copy != NULL ? strstr(local_copy, "cluster_pcm_lock_apply_gcs_transition(") : NULL;
	const char *local_commit
		= local_master != NULL
			  ? strstr(local_master, "cluster_bufmgr_pcm_own_finish_x_to_s_downgrade(")
			  : NULL;
	const char *local_call
		= gcs_source != NULL
			  ? strstr(gcs_source, "cluster_bufmgr_downgrade_x_to_s_for_gcs_prepare_image(")
			  : NULL;
	const char *prepared
		= local_call != NULL ? strstr(local_call, "scache_image_prepared = true") : NULL;
	const char *produce
		= prepared != NULL
			  ? strstr(prepared, "gcs_block_produce_reply(req, block_buf, scache_image_prepared")
			  : NULL;
	const char *produce_def
		= gcs_source != NULL ? strstr(gcs_source, "\ngcs_block_produce_reply(") : NULL;
	const char *skip_second_copy
		= produce_def != NULL
			  ? strstr(produce_def, "if (!preprepared_image\n\t\t&& !gcs_block_get_ship_image(")
			  : NULL;

	/* P0-32: injection is decided before any irreversible downgrade.  The
	 * successful arm reuses bytes proven under the same content EXCLUSIVE
	 * interval; it must not take a second conditional copy. */
	UT_ASSERT_NOT_NULL(forward);
	UT_ASSERT_NOT_NULL(forward_end);
	UT_ASSERT_NOT_NULL(inject);
	UT_ASSERT_NOT_NULL(downgrade);
	UT_ASSERT_NOT_NULL(reuse);
	UT_ASSERT_NOT_NULL(post_notify_guard);
	UT_ASSERT_NOT_NULL(post_notify_return);
	UT_ASSERT_NOT_NULL(copy);
	if (forward_end != NULL && inject != NULL && downgrade != NULL && reuse != NULL && copy != NULL)
		UT_ASSERT(inject < downgrade && downgrade < reuse && reuse < copy && copy < forward_end);
	if (post_notify_guard != NULL && post_notify_return != NULL && copy != NULL)
		UT_ASSERT(downgrade < post_notify_guard && post_notify_guard < post_notify_return
				  && post_notify_return < copy);

	/* The remote holder's only irreversible order is exact reserve, flush +
	 * pre-copy/LSN proof, nowait notify, then exact X+REVOKING -> S commit.
	 * Pre-notify refusal has an exact abort; post-notify commit failure is
	 * fail-closed and therefore cannot produce S_GRANTED. */
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(reserve);
	UT_ASSERT_NOT_NULL(precopy);
	UT_ASSERT_NOT_NULL(notify);
	UT_ASSERT_NOT_NULL(commit);
	UT_ASSERT_NOT_NULL(abort);
	if (helper_end != NULL && reserve != NULL && precopy != NULL && notify != NULL
		&& commit != NULL)
		UT_ASSERT(reserve < precopy && precopy < notify && notify < commit && commit < helper_end);
	if (helper_end != NULL && abort != NULL)
		UT_ASSERT(abort < notify && abort < helper_end);
	UT_ASSERT_NOT_NULL(helper != NULL ? strstr(helper, "cluster_pcm_x_runtime_fail_closed()")
									  : NULL);
	UT_ASSERT_NOT_NULL(local_helper);
	UT_ASSERT_NOT_NULL(local_helper_end);
	UT_ASSERT_NOT_NULL(local_reserve);
	UT_ASSERT_NOT_NULL(local_copy);
	UT_ASSERT_NOT_NULL(local_master);
	UT_ASSERT_NOT_NULL(local_commit);
	if (local_helper_end != NULL && local_reserve != NULL && local_copy != NULL
		&& local_master != NULL && local_commit != NULL)
		UT_ASSERT(local_reserve < local_copy && local_copy < local_master
				  && local_master < local_commit && local_commit < local_helper_end);
	UT_ASSERT_NOT_NULL(local_call);
	UT_ASSERT_NOT_NULL(prepared);
	UT_ASSERT_NOT_NULL(produce);
	UT_ASSERT_NOT_NULL(skip_second_copy);
	if (local_call != NULL && prepared != NULL && produce != NULL)
		UT_ASSERT(local_call < prepared && prepared < produce);
	free(gcs_source);
	free(bufmgr_source);
}

UT_TEST(test_preprepared_image_accepts_exact_zero_lsn_and_rejects_mismatch)
{
	char *source = read_gcs_block_source();
	const char *produce = source != NULL ? strstr(source, "\ngcs_block_produce_reply(") : NULL;
	const char *produce_end
		= produce != NULL
			  ? strstr(produce, "\n}\n\nstatic bool\ngcs_block_queue_pending_x_authoritative(")
			  : NULL;
	const char *prepared = produce != NULL ? strstr(produce, "if (!preprepared_image)") : NULL;
	const char *nonzero_assert
		= prepared != NULL ? strstr(prepared, "*out_page_lsn != InvalidXLogRecPtr") : NULL;
	const char *lsn_match
		= prepared != NULL
			  ? strstr(prepared, "PageGetLSN((Page)block_buf) != *out_page_lsn")
			  : NULL;
	const char *payload_match
		= prepared != NULL ? strstr(prepared, "*out_block_payload != block_buf") : NULL;
	const char *fail_closed
		= payload_match != NULL ? strstr(payload_match, "GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER")
								: NULL;

	/* A copied PostgreSQL page may legitimately carry pd_lsn == 0.  The
	 * prepared fast path must preserve that exact sample just like the generic
	 * ship path.  Its safety proof is payload identity plus byte/LSN equality;
	 * an inconsistent prepared carrier is an explicit denial, not an LMS
	 * assertion. */
	UT_ASSERT_NOT_NULL(produce);
	UT_ASSERT_NOT_NULL(produce_end);
	UT_ASSERT_NOT_NULL(prepared);
	UT_ASSERT(nonzero_assert == NULL || produce_end == NULL || nonzero_assert > produce_end);
	UT_ASSERT_NOT_NULL(lsn_match);
	UT_ASSERT_NOT_NULL(payload_match);
	UT_ASSERT_NOT_NULL(fail_closed);
	if (produce_end != NULL && prepared != NULL && payload_match != NULL && lsn_match != NULL
		&& fail_closed != NULL)
		UT_ASSERT(produce < prepared && prepared < payload_match && payload_match < lsn_match
				  && lsn_match < fail_closed && fail_closed < produce_end);
	free(source);
}


UT_TEST(test_pending_x_apply_race_maps_to_retryable_block_denial)
{
	char *source = read_gcs_block_source();
	const char *produce = source != NULL ? strstr(source, "\ngcs_block_produce_reply(") : NULL;
	const char *produce_end
		= produce != NULL
			  ? strstr(produce, "\n}\n\nstatic bool\ngcs_block_queue_pending_x_authoritative(")
			  : NULL;
	const char *apply1;
	const char *deny1;
	const char *apply2;
	const char *deny2;
	const char *requester_retry;

	/* P0-25: the entry-lock final admission distinguishes a raced pending-X
	 * from structural incompatibility.  Both direct-storage and image-present
	 * N->S apply sites must return DENIED_PENDING_X, whose requester path exits
	 * through the existing fresh-identity retry boundary instead of a client
	 * terminal ERROR. */
	UT_ASSERT_NOT_NULL(produce);
	UT_ASSERT_NOT_NULL(produce_end);
	if (produce == NULL || produce_end == NULL) {
		free(source);
		return;
	}
	apply1 = strstr(produce, "cluster_pcm_lock_apply_gcs_transition_result(");
	deny1 = apply1 != NULL ? strstr(apply1, "GcsBlockApplyRefusalStatus(") : NULL;
	apply2 = deny1 != NULL ? strstr(deny1, "cluster_pcm_lock_apply_gcs_transition_result(") : NULL;
	deny2 = apply2 != NULL ? strstr(apply2, "GcsBlockApplyRefusalStatus(") : NULL;
	requester_retry = strstr(source, "if (final_status == GCS_BLOCK_REPLY_DENIED_PENDING_X)");
	UT_ASSERT_NOT_NULL(apply1);
	UT_ASSERT_NOT_NULL(deny1);
	UT_ASSERT_NOT_NULL(apply2);
	UT_ASSERT_NOT_NULL(deny2);
	UT_ASSERT_NOT_NULL(requester_retry);
	if (apply1 != NULL && deny1 != NULL && apply2 != NULL && deny2 != NULL)
		UT_ASSERT(produce < apply1 && apply1 < deny1 && deny1 < apply2 && apply2 < deny2
				  && deny2 < produce_end);
	free(source);
}


UT_TEST(test_gcs_apply_state_drift_restarts_with_fresh_request_identity)
{
	char *source = read_gcs_block_source();
	const char *produce = source != NULL ? strstr(source, "\ngcs_block_produce_reply(") : NULL;
	const char *produce_end
		= produce != NULL
			  ? strstr(produce, "\n}\n\nstatic bool\ngcs_block_queue_pending_x_authoritative(")
			  : NULL;
	const char *apply1;
	const char *map1;
	const char *apply2;
	const char *map2;

	/* The request transition was already wire-validated.  If authority moves
	 * after the master's outer S/X decision but before its entry-lock apply,
	 * N->S/N->X must abandon this attempt and re-enter bufmgr with a fresh
	 * request/token identity.  Other transition incompatibilities remain
	 * structural terminal denials. */
	UT_ASSERT_EQ((int)GcsBlockApplyRefusalStatus(PCM_GCS_TRANSITION_PENDING_X, PCM_TRANS_N_TO_S),
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)GcsBlockApplyRefusalStatus(PCM_GCS_TRANSITION_INCOMPATIBLE, PCM_TRANS_N_TO_S),
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)GcsBlockApplyRefusalStatus(PCM_GCS_TRANSITION_INCOMPATIBLE, PCM_TRANS_N_TO_X),
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ(
		(int)GcsBlockApplyRefusalStatus(PCM_GCS_TRANSITION_INCOMPATIBLE, PCM_TRANS_S_TO_X_UPGRADE),
		(int)GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE);

	UT_ASSERT_NOT_NULL(produce);
	UT_ASSERT_NOT_NULL(produce_end);
	if (produce == NULL || produce_end == NULL) {
		free(source);
		return;
	}
	apply1 = strstr(produce, "cluster_pcm_lock_apply_gcs_transition_result(");
	map1 = apply1 != NULL ? strstr(apply1, "GcsBlockApplyRefusalStatus(") : NULL;
	apply2 = map1 != NULL ? strstr(map1, "cluster_pcm_lock_apply_gcs_transition_result(") : NULL;
	map2 = apply2 != NULL ? strstr(apply2, "GcsBlockApplyRefusalStatus(") : NULL;
	UT_ASSERT_NOT_NULL(apply1);
	UT_ASSERT_NOT_NULL(map1);
	UT_ASSERT_NOT_NULL(apply2);
	UT_ASSERT_NOT_NULL(map2);
	if (apply1 != NULL && map1 != NULL && apply2 != NULL && map2 != NULL)
		UT_ASSERT(produce < apply1 && apply1 < map1 && map1 < apply2 && apply2 < map2
				  && map2 < produce_end);
	free(source);
}


UT_TEST(test_master_direct_copy_busy_uses_only_fresh_identity_retry_boundary)
{
	struct CopyRefusalCase {
		ClusterBufmgrGcsCopyRefusal refusal;
		GcsBlockReplyStatus expected;
	};
	static const struct CopyRefusalCase cases[] = {
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INVALID_ARGUMENT,
		  GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID,
		  GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST, GCS_BLOCK_REPLY_DENIED_PENDING_X },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND, GCS_BLOCK_REPLY_DENIED_PENDING_X },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_OWNERSHIP_REVOKE_BUSY,
		  GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_HC89_LSN_DRIFT,
		  GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_SMART_FUSION_UNCLASSIFIED,
		  GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
		{ CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INJECTED_EVICT,
		  GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER },
	};
	char *source = read_gcs_block_source();
	const char *produce = source != NULL ? strstr(source, "\ngcs_block_produce_reply(") : NULL;
	const char *produce_end
		= produce != NULL
			  ? strstr(produce, "\n}\n\nstatic bool\ngcs_block_queue_pending_x_authoritative(")
			  : NULL;
	const char *copy_refusal;
	const char *copy_refusal_end;
	const char *mapping;
	const char *terminal_literal;
	const char *requester_retry;
	const char *retry_flag;
	const char *retry_break;
	const char *same_id_continue;
	size_t i;

	/* P0-21 residual: a DATA worker's conditional BufferContent refusal is
	 * transient by construction (the lock owner may itself be waiting for that
	 * worker), so only the two conditional stages enter the existing
	 * DENIED_PENDING_X fresh-request/token retry boundary.  Structural
	 * residency/current-image failures and HC89's bounded second LSN drift stay
	 * terminal status 6; no authority or safety gate is weakened. */
	for (i = 0; i < lengthof(cases); i++)
		UT_ASSERT_EQ((int)GcsBlockMasterDirectCopyRefusalStatus(cases[i].refusal),
					 (int)cases[i].expected);

	UT_ASSERT_NOT_NULL(produce);
	UT_ASSERT_NOT_NULL(produce_end);
	if (produce == NULL || produce_end == NULL) {
		free(source);
		return;
	}
	copy_refusal = strstr(produce, "produce-copy-refused:%s");
	copy_refusal_end = copy_refusal != NULL ? strstr(copy_refusal, "return true;") : NULL;
	mapping = copy_refusal != NULL
				  ? strstr(copy_refusal, "GcsBlockMasterDirectCopyRefusalStatus(copy_refusal)")
				  : NULL;
	terminal_literal
		= copy_refusal != NULL
			  ? strstr(copy_refusal, "*out_status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER")
			  : NULL;
	UT_ASSERT_NOT_NULL(copy_refusal);
	UT_ASSERT_NOT_NULL(copy_refusal_end);
	UT_ASSERT_NOT_NULL(mapping);
	if (copy_refusal != NULL && copy_refusal_end != NULL && mapping != NULL)
		UT_ASSERT(copy_refusal < mapping && mapping < copy_refusal_end);
	UT_ASSERT(terminal_literal == NULL || copy_refusal_end == NULL
			  || terminal_literal > copy_refusal_end);

	/* Status 10 must leave the per-call retransmit loop rather than retrying
	 * the same cached request id.  Bufmgr then aborts the exact old reservation,
	 * waits, and rearms a fresh token/request identity (covered by
	 * test_pending_x_denied_retry_leaves_master_invalidate_gap). */
	requester_retry = strstr(source, "if (final_status == GCS_BLOCK_REPLY_DENIED_PENDING_X)");
	retry_flag = requester_retry != NULL ? strstr(requester_retry, "retry_denied = true;") : NULL;
	retry_break = retry_flag != NULL ? strstr(retry_flag, "break;") : NULL;
	same_id_continue = retry_flag != NULL ? strstr(retry_flag, "continue;") : NULL;
	UT_ASSERT_NOT_NULL(requester_retry);
	UT_ASSERT_NOT_NULL(retry_flag);
	UT_ASSERT_NOT_NULL(retry_break);
	if (requester_retry != NULL && retry_flag != NULL && retry_break != NULL)
		UT_ASSERT(requester_retry < retry_flag && retry_flag < retry_break
				  && retry_break < produce);
	UT_ASSERT(same_id_continue == NULL || retry_break == NULL || same_id_continue > retry_break);
	free(source);
}


UT_TEST(test_master_not_holder_producers_log_one_coherent_authority_snapshot)
{
	char *source = read_gcs_block_source();
	const char *helper
		= source != NULL ? strstr(source, "\ngcs_block_log_master_not_holder_producer(") : NULL;
	const char *helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
	const char *produce = source != NULL ? strstr(source, "\ngcs_block_produce_reply(") : NULL;
	const char *produce_end
		= produce != NULL
			  ? strstr(produce, "\n}\n\nstatic bool\ngcs_block_queue_pending_x_authoritative(")
			  : NULL;
	static const char *const required_fields[] = {
		"cluster_pcm_lock_authority_snapshot(tag, &authority)",
		"authority.state",
		"authority.x_holder_node",
		"authority.s_holders_bitmap",
		"authority.master_holder.node_id",
		"authority.master_holder.procno",
		"authority.master_holder.cluster_epoch",
		"authority.master_holder.request_id",
		"authority.pending_x_requester_node",
		"authority.pending_x_since_lsn",
		"authority.transition_count",
	};
	static const char *const required_reasons[] = {
		"direct-land-nonsendable", "direct-land-forward-rearm",
		"pcm-x-image-not-ready",   "produce-no-resident-authority",
		"produce-copy-refused",	   "clean-third-party-master",
		"live-x-other-holder",	   "pending-x-reserve-failed",
		"x-forward-send-failed",   "x-state-holder-unroutable",
		"holder-immediate-deny",   "holder-drop-pinned",
		"holder-drop-stale",	   "holder-copy-refused",
	};
	size_t i;

	/* A status=6 requester error is not enough to identify its producer: the
	 * aggregate counter is also incremented at requester consumption.  Every
	 * producer therefore logs a stable reason plus one entry-lock-coherent
	 * authority snapshot, correlated by the unchanged request identity. */
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	if (helper != NULL && helper_end != NULL) {
		const char *snapshot;
		const char *duplicate_snapshot;
		const char *forbidden;

		for (i = 0; i < lengthof(required_fields); i++) {
			const char *field = strstr(helper, required_fields[i]);

			UT_ASSERT_NOT_NULL(field);
			if (field != NULL)
				UT_ASSERT(field < helper_end);
		}
		snapshot = strstr(helper, "cluster_pcm_lock_authority_snapshot(");
		duplicate_snapshot = snapshot != NULL
								 ? strstr(snapshot + 1, "cluster_pcm_lock_authority_snapshot(")
								 : NULL;
		UT_ASSERT_NOT_NULL(snapshot);
		UT_ASSERT(snapshot != NULL && snapshot < helper_end);
		UT_ASSERT(duplicate_snapshot == NULL || duplicate_snapshot >= helper_end);
		forbidden = strstr(helper, "cluster_pcm_lock_query(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		forbidden = strstr(helper, "cluster_pcm_master_holder_node_by_tag(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		forbidden = strstr(helper, "cluster_pcm_lock_query_s_holders_bitmap(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
	}
	for (i = 0; i < lengthof(required_reasons); i++)
		UT_ASSERT_NOT_NULL(source != NULL ? strstr(source, required_reasons[i]) : NULL);
	/* The final master-direct copy refusal must retain the bufmgr's exact
	 * nonblocking stage in the producer reason; no status/retry change here. */
	UT_ASSERT_NOT_NULL(produce);
	UT_ASSERT_NOT_NULL(produce_end);
	if (produce != NULL && produce_end != NULL) {
		const char *capture = strstr(produce, "ClusterBufmgrGcsCopyRefusal copy_refusal");
		const char *pass = strstr(produce, "&copy_refusal");
		const char *name = strstr(produce, "cluster_bufmgr_gcs_copy_refusal_name(copy_refusal)");

		UT_ASSERT_NOT_NULL(capture);
		UT_ASSERT_NOT_NULL(pass);
		UT_ASSERT_NOT_NULL(name);
		if (capture != NULL && pass != NULL && name != NULL)
			UT_ASSERT(produce < capture && capture < pass && pass < name && name < produce_end);
	}
	free(source);
}


UT_TEST(test_pi_durable_note_drain_stages_before_consuming_on_data_plane)
{
	char *source = read_gcs_block_source();
	const char *drain = strstr(source, "\ncluster_gcs_block_pi_discard_drain(");
	const char *drain_end = drain != NULL ? strstr(drain, "\n}\n") : NULL;
	const char *data_plane;
	const char *status3;
	const char *shard;
	const char *enqueue;
	const char *enqueue_refusal;
	const char *advance;
	const char *data_continue;
	const char *control_apply;
	const char *control_send;

	/*
	 * Checkpoint-confirmed PI durable notes are produced in shared memory,
	 * but the DATA-plane drain runs only in LMS worker 0.  It must therefore
	 * stage both local- and remote-master status-3 ACKs onto tag->worker,
	 * and may consume the source note only after that staging succeeds.  The
	 * old CONTROL-plane direct apply/send path remains available.
	 */
	UT_ASSERT_NOT_NULL(drain);
	UT_ASSERT_NOT_NULL(drain_end);
	if (drain == NULL || drain_end == NULL) {
		free(source);
		return;
	}
	data_plane = strstr(drain, "cluster_gcs_block_family_on_data_plane()");
	status3 = strstr(drain, "GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE");
	shard = strstr(drain, "cluster_lms_shard_for_tag(&tag, cluster_lms_workers)");
	enqueue = strstr(drain, "cluster_lms_outbound_enqueue(");
	enqueue_refusal = enqueue != NULL ? strstr(enqueue, "break;") : NULL;
	advance = enqueue != NULL ? strstr(enqueue, "ClusterGcsBlock->pi_note_drain_seq++") : NULL;
	data_continue = advance != NULL ? strstr(advance, "continue;") : NULL;
	control_apply = strstr(drain, "gcs_block_pi_discard_master_apply(tag, page_scn)");
	control_send = strstr(drain, "cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK");
	UT_ASSERT_NOT_NULL(data_plane);
	UT_ASSERT_NOT_NULL(status3);
	UT_ASSERT_NOT_NULL(shard);
	UT_ASSERT_NOT_NULL(enqueue);
	UT_ASSERT_NOT_NULL(enqueue_refusal);
	UT_ASSERT_NOT_NULL(advance);
	UT_ASSERT_NOT_NULL(data_continue);
	UT_ASSERT_NOT_NULL(control_apply);
	UT_ASSERT_NOT_NULL(control_send);
	if (data_plane != NULL && status3 != NULL && shard != NULL && enqueue != NULL
		&& enqueue_refusal != NULL && advance != NULL && data_continue != NULL)
		UT_ASSERT(data_plane < status3 && status3 < shard && shard < enqueue
				  && enqueue < enqueue_refusal && enqueue_refusal < advance
				  && advance < data_continue && data_continue < drain_end);
	if (control_apply != NULL)
		UT_ASSERT(data_continue < control_apply && control_apply < drain_end);
	if (control_send != NULL)
		UT_ASSERT(data_continue < control_send && control_send < drain_end);
	free(source);
}


UT_TEST(test_pi_durable_note_receive_is_observable_before_apply)
{
	char *source = read_gcs_block_source();
	const char *handler
		= source != NULL ? strstr(source, "\ncluster_gcs_handle_block_invalidate_ack_envelope(")
						 : NULL;
	const char *status3 = handler != NULL
							  ? strstr(handler, "GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE")
							  : NULL;
	const char *epoch
		= status3 != NULL ? strstr(status3, "ack->epoch == cluster_epoch_get_current()") : NULL;
	const char *accepted = epoch != NULL ? strstr(epoch, "pi_durable_note_apply_count") : NULL;
	const char *apply
		= accepted != NULL ? strstr(accepted, "gcs_block_pi_discard_master_apply(") : NULL;

	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(status3);
	UT_ASSERT_NOT_NULL(epoch);
	UT_ASSERT_NOT_NULL(accepted);
	UT_ASSERT_NOT_NULL(apply);
	if (handler != NULL && status3 != NULL && epoch != NULL && accepted != NULL && apply != NULL)
		UT_ASSERT(handler < status3 && status3 < epoch && epoch < accepted && accepted < apply);
	free(source);
}

/* P0-20 source-floor V2 must remain bound to the HELLO connection sampled
 * around source authority.  The reliable REVOKE leg is armed first; a later
 * generation drift returns retryable without ACKing it, while the exact-match
 * V2 enters a guarded LMS slot.  A current peer without the additive bit gets
 * the byte-compatible V1 fallback. */
UT_TEST(test_pcm_x_source_floor_v2_is_connection_bound_until_lms_drain)
{
	char *source = read_gcs_block_source();
	const char *stage_bound;
	const char *stage_bound_end;
	const char *shard;
	const char *enqueue;
	const char *transfer;
	const char *transfer_end;
	const char *auth;
	const char *arm;
	const char *sample;
	const char *generation;
	const char *guarded_stage;
	const char *v1_fallback;
	const char *stray_simple_query;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	stage_bound = strstr(source, "\ngcs_block_pcm_x_stage_frame_cap_bound(");
	stage_bound_end = stage_bound != NULL ? strstr(stage_bound, "\n}\n") : NULL;
	shard = stage_bound != NULL ? strstr(stage_bound, "cluster_gcs_block_payload_shard(") : NULL;
	enqueue = shard != NULL ? strstr(shard, "cluster_lms_outbound_enqueue_cap_bound(") : NULL;
	UT_ASSERT_NOT_NULL(stage_bound);
	UT_ASSERT_NOT_NULL(stage_bound_end);
	UT_ASSERT_NOT_NULL(shard);
	UT_ASSERT_NOT_NULL(enqueue);
	if (stage_bound != NULL && stage_bound_end != NULL && shard != NULL && enqueue != NULL)
		UT_ASSERT(stage_bound < shard && shard < enqueue && enqueue < stage_bound_end);

	transfer = strstr(source, "\ngcs_block_pcm_x_master_drive_transfer(");
	transfer_end = transfer != NULL ? strstr(transfer, "\n}\n") : NULL;
	auth = transfer != NULL ? strstr(transfer, "gcs_block_pcm_x_authenticated_session_result(")
							: NULL;
	arm = auth != NULL ? strstr(auth, "cluster_pcm_x_master_revoke_arm_exact(") : NULL;
	sample = arm != NULL ? strstr(arm, "cluster_sf_peer_pcm_x_source_floor_sample(") : NULL;
	generation = sample != NULL ? strstr(sample, "auth_sample.connection_generation_before") : NULL;
	guarded_stage
		= generation != NULL ? strstr(generation, "gcs_block_pcm_x_stage_frame_cap_bound(") : NULL;
	v1_fallback
		= guarded_stage != NULL
			  ? strstr(guarded_stage, "cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_REVOKE")
			  : NULL;
	stray_simple_query
		= transfer != NULL ? strstr(transfer, "cluster_sf_peer_supports_pcm_x_source_floor(source)")
						   : NULL;
	UT_ASSERT_NOT_NULL(transfer);
	UT_ASSERT_NOT_NULL(transfer_end);
	UT_ASSERT_NOT_NULL(auth);
	UT_ASSERT_NOT_NULL(arm);
	UT_ASSERT_NOT_NULL(sample);
	UT_ASSERT_NOT_NULL(generation);
	UT_ASSERT_NOT_NULL(guarded_stage);
	UT_ASSERT_NOT_NULL(v1_fallback);
	if (transfer != NULL && transfer_end != NULL && auth != NULL && arm != NULL && sample != NULL
		&& generation != NULL && guarded_stage != NULL && v1_fallback != NULL) {
		UT_ASSERT(transfer < auth && auth < arm && arm < sample && sample < generation
				  && generation < guarded_stage && guarded_stage < v1_fallback
				  && v1_fallback < transfer_end);
		UT_ASSERT(stray_simple_query == NULL || stray_simple_query > transfer_end);
	}
	free(source);
}


UT_TEST(test_resource_x_target_executor_orders_t1_t2_t3_before_writable_return)
{
	static const char *const executor_contract[]
		= { "cluster_pcm_lock_resource_x_executor_enter(",
			"cluster_pcm_lock_resource_x_t1_grant_exact(",
			"cluster_pcm_lock_resource_x_executor_probe_exact(",
			"cluster_bufmgr_pcm_own_capture_current_x_by_tag(",
			"cluster_bufmgr_pcm_own_activate_x_by_tag(",
			"cluster_pcm_lock_resource_x_requester_apply_exact(",
			"cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(",
			"cluster_pcm_lock_resource_x_requester_activate_exact(",
			"cluster_pcm_lock_resource_x_executor_leave(" };
	static const char *const requester_contract[]
		= { "progress.member_state == PCM_XL_GRANTED",
			"progress.identity.node_id != cluster_node_id",
			"resource_x_episode_deadline_us = gcs_block_pcm_x_saturating_add_us(",
			"cluster_pcm_lock_resource_x_executor_rearm_exact(",
			"gcs_block_pcm_x_resource_x_terminal_try(",
				"gcs_block_pcm_x_resource_x_wait_exact(",
				"continue;",
				"claim_out->semantic_generation = progress.semantic_generation",
				"PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM", "break;" };
	static const char *const wait_contract[]
		= { "now_us >= episode_deadline_us",
			"cluster_pcm_lock_resource_x_executor_wait_exact(ref, 0)",
			"cluster_pcm_lock_resource_x_executor_wait_exact(ref, timeout_ms)",
			"cluster_gcs_pcm_x_requester_wait_index_advance(current)" };
	static const char *const no_progress_contract[]
		= { "cluster_pcm_lock_resource_x_publish_no_progress_exact(",
			"cluster_pcm_lock_resource_x_executor_probe_exact(",
			"probe_result == RESOURCE_X_EXECUTOR_BLOCKED",
			"*rearm_after_wait_out = true" };
	static const char *const formation_contract[]
		= { "runtime = cluster_pcm_x_runtime_snapshot()",
			"cluster_pcm_lock_resource_x_gate_bind_formation_exact(runtime.gate_generation)",
				"gcs_block_pcm_x_resource_retry_tick", "gcs_block_pcm_x_terminal_retry_tick" };
	static const char *const target_tick_contract[]
		= { "cluster_pcm_x_local_target_activation_work_next(",
			"cluster_pcm_lock_resource_x_executor_rearm_exact(",
			"gcs_block_pcm_x_resource_x_terminal_try(",
			"cluster_pcm_x_local_target_activation_publish_exact(",
			"gcs_block_pcm_x_wake_requester(",
			"cluster_pcm_x_retry_work_next(" };
	char *source = read_gcs_block_source();
	const char *requester;
	const char *requester_end;
	const char *adapter_granted;
	const char *grant_projection;
	const char *adapter_success;

	assert_ordered_in_function(source, "\ngcs_block_pcm_x_resource_x_terminal_try(",
							   "\nstatic PcmXQueueResult\n"
							   "gcs_block_pcm_x_acquire_writer_impl(", executor_contract,
							   lengthof(executor_contract));
	assert_ordered_in_function(source, "\ngcs_block_pcm_x_acquire_writer_impl(",
								   "\nPcmXQueueResult\ncluster_gcs_pcm_x_acquire_writer(",
								   requester_contract, lengthof(requester_contract));
	requester = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	requester_end = requester != NULL
		? strstr(requester,
			"\nPcmXQueueResult\ncluster_gcs_pcm_x_acquire_writer(") : NULL;
	adapter_granted = requester != NULL
		? strstr(requester,
			"progress.resource_x_authority_domain") : NULL;
	grant_projection = adapter_granted != NULL
		? strstr(adapter_granted,
			"claim_out->grant_base_own_generation = progress.grant_base_own_generation")
		: NULL;
	adapter_success = grant_projection != NULL
		? strstr(grant_projection, "result = PCM_X_QUEUE_OK") : NULL;
	UT_ASSERT_NOT_NULL(requester_end);
	UT_ASSERT_NOT_NULL(adapter_granted);
	UT_ASSERT_NOT_NULL(grant_projection);
	UT_ASSERT_NOT_NULL(adapter_success);
	if (requester_end != NULL && adapter_granted != NULL
		&& grant_projection != NULL && adapter_success != NULL)
		UT_ASSERT(adapter_granted < grant_projection
			&& grant_projection < adapter_success
			&& adapter_success < requester_end);
	assert_ordered_in_function(source, "\ngcs_block_pcm_x_resource_x_wait_exact(",
							   "\n\n/*\n * Drive one ordinary", wait_contract,
							   lengthof(wait_contract));
	assert_ordered_in_function(source, "\ngcs_block_pcm_x_resource_x_publish_and_arm_wait(",
							   "\n\n/* One nonblocking terminal attempt", no_progress_contract,
							   lengthof(no_progress_contract));
	assert_ordered_in_function(source, "\ncluster_gcs_block_pcm_x_formation_tick(",
							   "\n\nstatic void\ngcs_block_pcm_x_resource_retry_tick(",
							   formation_contract, lengthof(formation_contract));
	assert_ordered_in_function(source, "\ngcs_block_pcm_x_resource_retry_tick(",
							   "\n\n#define GCS_BLOCK_PCM_X_TERMINAL_TICK_BUDGET",
							   target_tick_contract, lengthof(target_tick_contract));
	free(source);
}

UT_TEST(test_resource_x_writer_completion_does_not_own_acquisition_retirement)
{
	static const char *const completion_contract[] = {
		"cluster_pcm_x_local_writer_claim_release_collect_exact(",
		"gcs_block_pcm_x_wake_requester_exact("
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ncluster_gcs_pcm_x_writer_claim_release_and_wake_exact(",
		"\n\n/* Cleanup runs while", completion_contract,
		lengthof(completion_contract));
	UT_ASSERT(strstr(source,
		"cluster_pcm_lock_resource_x_executor_release_capture_exact") == NULL);
	UT_ASSERT(strstr(source,
		"cluster_pcm_lock_resource_x_executor_release_exact") == NULL);
	UT_ASSERT(strstr(source,
		"gcs_block_pcm_x_resource_x_writer_complete_release_exact") == NULL);
	free(source);
}

UT_TEST(test_resource_x_common_x_to_n_finish_does_not_retire_acquisition)
{
	static const char *const finish_contract[] = {
		"PG_TRY();",
		"cluster_bufmgr_pcm_own_finish_revoke_retain(",
		"PG_CATCH();",
		"return (ClusterPcmOwnResult)result"
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_finish_revoke_retain(",
		"\n\n/* Retry only the reversible", finish_contract,
		lengthof(finish_contract));
	free(source);
}

UT_TEST(test_resource_x_self_x_to_x_commit_does_not_retire_acquisition)
{
	static const char *const commit_contract[] = {
		"cluster_bufmgr_pcm_own_finish_x_commit(",
		"committed = true",
		"*out_committed_generation = committed_generation"
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ncluster_gcs_pcm_x_finish_self_image_x(",
		"\n\n/*\n * Fetch the immutable holder READY record", commit_contract,
		lengthof(commit_contract));
	free(source);
}

UT_TEST(test_resource_x_epoch_hook_freezes_sweeps_and_thaws_before_existing_wake)
{
	static const char *const actor_contract[]
		= { "claimed_epoch = pg_atomic_compare_exchange_u64(",
			"if (!claimed_epoch && expected_epoch == new_epoch)",
			"resource_x_reconfig_actor_active",
			"resource_x_reconfig_completed_epoch", "return true;",
			"runtime = cluster_pcm_x_runtime_snapshot()",
			"cluster_resource_x_reconfig_freeze_pending_exact(",
			"cluster_pcm_x_runtime_fail_closed()", "gcs_block_pcm_x_collect_formation(",
			"cluster_pcm_x_runtime_reform(", "cluster_pcm_x_runtime_snapshot()",
			"cluster_resource_x_reconfig_bind_new_formation_exact(",
			"cluster_resource_x_reconfig_sweep(", "CHECK_FOR_INTERRUPTS()",
			"cluster_resource_x_reconfig_zero_proof_exact(",
			"cluster_pcm_lock_resource_x_clean_completion_prove_exact(",
			"cluster_pcm_lock_resource_x_cutover_proofs_exact(",
			"cluster_resource_x_reconfig_thaw_exact(",
			"cluster_resource_x_reconfig_stats_snapshot(",
			"resource_x_reconfig_actor_active, 0" };
	static const char *const hook_contract[]
		= { "gcs_block_resource_x_dead_requester_bitmap(dead_bitmap)",
			"gcs_block_resource_x_reconfig_epoch(new_epoch, dead_requester_bitmap)",
			"cluster_gcs_block_dedup_r4_route_sweep_epoch(new_epoch)",
			"ConditionVariableBroadcast(&slot->reply_cv)" };
	static const char *const observer_contract[]
		= { "cluster_reconfig_get_last_event(&resource_x_event)",
			"resource_x_event.new_epoch != current_epoch)",
			"gcs_block_resource_x_dead_requester_bitmap(",
			"resource_x_event.dead_bitmap)",
			"gcs_block_resource_x_reconfig_epoch(current_epoch,",
			"dead_requester_bitmap))",
			"gcs_block_resource_x_r11_cutover_tick()",
			"runtime = cluster_pcm_x_runtime_snapshot()",
			"cluster_pcm_lock_resource_x_gate_bind_formation_exact(runtime.gate_generation)" };
	static const char *const cutover_contract[]
		= { "cluster_semantic_activation_r11_cutover_snapshot(",
			"cluster_pcm_lock_resource_x_gate_snapshot(",
			"CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED",
			"cluster_resource_x_reconfig_cutover_freeze_exact(",
			"cluster_resource_x_reconfig_sweep(",
			"cluster_resource_x_reconfig_bind_new_formation_exact(",
			"cluster_resource_x_reconfig_sweep(",
			"cluster_resource_x_reconfig_zero_proof_exact(",
			"cluster_pcm_lock_resource_x_clean_completion_prove_exact(",
			"cluster_pcm_lock_resource_x_cutover_proofs_exact(",
			"CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL",
			"cluster_pcm_lock_resource_x_cutover_proofs_exact(",
			"cluster_resource_x_reconfig_thaw_exact(",
			"cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(" };
	char *source = read_gcs_block_source();
	const char *formation;
	const char *formation_end;
	const char *target_open_short_circuit;

	assert_ordered_in_function(
		source, "\ngcs_block_resource_x_reconfig_epoch(",
		"\n\n/* ============================================================\n * PGRAC: spec-2.34 D4",
		actor_contract, lengthof(actor_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_resource_x_r11_cutover_tick(",
		"\ncluster_gcs_block_pcm_x_formation_tick(",
		cutover_contract, lengthof(cutover_contract));
	assert_ordered_in_function(
		source, "\ncluster_gcs_block_on_epoch_advance_exact(",
		"\n\n/* ============================================================\n * PGRAC MODIFICATIONS by SqlRush — spec-5.13 D5",
		hook_contract, lengthof(hook_contract));
	assert_ordered_in_function(
		source, "\ncluster_gcs_block_pcm_x_formation_tick(",
		"\n\n/*\n * PCM-X is an application protocol", observer_contract,
		lengthof(observer_contract));
	formation = strstr(source, "\ncluster_gcs_block_pcm_x_formation_tick(");
	formation_end = formation != NULL
		? strstr(formation, "\n\n/*\n * PCM-X is an application protocol")
		: NULL;
	target_open_short_circuit = formation != NULL
		? strstr(formation, "CLUSTER_SEMANTIC_R11_CUTOVER_TARGET_OPEN")
		: NULL;
	UT_ASSERT_NOT_NULL(formation_end);
	if (formation_end != NULL && target_open_short_circuit != NULL)
		UT_ASSERT(target_open_short_circuit >= formation_end);
	free(source);
}

UT_TEST(test_resource_x_reused_type_ingress_precedes_every_legacy_path)
{
	static const char *const handler_names[] = {
		"\ncluster_gcs_handle_block_request_envelope(",
		"\ncluster_gcs_handle_block_reply_envelope(",
		"\ncluster_gcs_handle_block_done_envelope(",
		"\ncluster_gcs_handle_block_invalidate_envelope(",
		"\ncluster_gcs_handle_block_invalidate_ack_envelope("
	};
	static const char *const legacy_boundaries[] = {
		"gcs_block_try_r4_request80(env, payload)",
		"gcs_block_try_land_current_mx_reply(env, payload)",
		"sizeof(GcsBlockDonePayload)",
		"CLUSTER_INJECTION_POINT(\"cluster-gcs-block-invalidate-stall-ack\")",
		"if (ClusterGcsBlock == NULL)"
	};
	char *source = read_gcs_block_source();
	const char *helper;
	int i;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	helper = strstr(source, "\ngcs_block_try_resource_x_frame(");
	UT_ASSERT_NOT_NULL(helper);
	if (helper != NULL) {
		UT_ASSERT_NOT_NULL(strstr(helper, "gcs_block_resource_x_payload_candidate("));
		UT_ASSERT_NOT_NULL(strstr(helper, "cluster_resource_x_wire_decode("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1"));
		UT_ASSERT_NOT_NULL(strstr(helper, "cluster_sf_peer_capability_word_sample("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"authenticated_capability_generation = connection_generation"));
		UT_ASSERT_NULL(strstr(helper,
			"frame.common.sender_connection_generation != connection_generation"));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_assert_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_local_proof_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_block_to_n_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_blocked_to_n_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_install_settlement_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_release_x_exact("));
	}
	for (i = 0; i < (int)lengthof(handler_names); i++) {
		const char *handler = strstr(source, handler_names[i]);
		const char *intercept;
		const char *legacy;

		UT_ASSERT_NOT_NULL(handler);
		if (handler == NULL)
			continue;
		intercept = strstr(handler, "gcs_block_try_resource_x_frame(env, payload)");
		legacy = strstr(handler, legacy_boundaries[i]);
		UT_ASSERT_NOT_NULL(intercept);
		UT_ASSERT_NOT_NULL(legacy);
		if (intercept != NULL && legacy != NULL)
			UT_ASSERT(intercept < legacy);
	}
	free(source);
}

UT_TEST(test_resource_x_kind9_ingress_is_target_native_and_no_fallback)
{
	char *source = read_gcs_block_source();
	const char *candidate;
	const char *ingress;
	const char *ack_stage;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	candidate = strstr(source, "\ngcs_block_resource_x_payload_candidate(");
	ingress = strstr(source, "\ngcs_block_resource_x_kind9_ingress(");
	ack_stage = strstr(source,
		"\ngcs_block_resource_x_bootstrap_ack_stage_exact(");
	UT_ASSERT_NOT_NULL(candidate);
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(ack_stage);
	if (candidate != NULL) {
		UT_ASSERT_NOT_NULL(strstr(candidate,
			"msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT"));
		UT_ASSERT_NOT_NULL(strstr(candidate,
			"payload_length == RESOURCE_X_CONTROL_V1_BYTES"));
	}
	if (ingress != NULL) {
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1"));
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"cluster_semantic_activation_enter("));
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"cluster_pcm_lock_resource_x_bootstrap_request_exact("));
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact("));
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"cluster_semantic_activation_recheck("));
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"gcs_block_resource_x_bootstrap_ack_stage_exact("));
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"gcs_block_pcm_x_resource_x_assert_stage_exact("));
	}
	UT_ASSERT_NOT_NULL(strstr(source,
		"cluster_pcm_lock_resource_x_assert_bootstrapped_exact("));
	if (ack_stage != NULL) {
		const char *encode = strstr(ack_stage,
			"cluster_resource_x_wire_encode(");
		const char *enqueue = strstr(ack_stage,
			"cluster_grd_outbound_enqueue_backend_msg(");

		UT_ASSERT_NOT_NULL(encode);
		UT_ASSERT_NOT_NULL(enqueue);
		if (encode != NULL && enqueue != NULL)
			UT_ASSERT(encode < enqueue);
	}
	free(source);
}

UT_TEST(test_resource_x_native_target_driver_uses_round_and_no_ticket_family)
{
	char *source = read_gcs_block_source();
	const char *driver;
	const char *driver_end;
	const char *gate_helper;
	const char *gate_helper_end;
	const char *terminal;
	const char *terminal_end;
	static const char *const required[] = {
		"cluster_semantic_activation_enter(",
		"CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1",
		"gcs_block_resource_x_gate_session_snapshot(",
		"gcs_block_resource_x_gate_session_recheck(",
		"cluster_bufmgr_pcm_own_snapshot(",
		"writer_activation_token == 0",
		"resource_x_activation_generation == 0",
		"cluster_pcm_lock_resource_x_bootstrap_round_step_exact(",
		"RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST",
		"gcs_block_resource_x_bootstrap_request_stage_exact(",
		"RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT",
		"gcs_block_pcm_x_resource_x_assert_stage_exact(",
		"RESOURCE_X_BOOTSTRAP_ROUND_WAIT",
		"cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(",
		"RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL"
	};
	static const char *const forbidden[] = {
		"cluster_gcs_pcm_x_acquire_writer(",
		"cluster_pcm_x_local_join_begin_semantic(",
		"cluster_pcm_x_local_resource_x_attempt_exact(",
		"cluster_pcm_x_local_resource_x_grant_publish_exact(",
		"cluster_pcm_x_local_writer_claim_exact(",
		"cluster_pcm_x_master_",
		"cluster_pcm_x_runtime_snapshot(",
		"cluster_pcm_x_holder_retry_delay_ms("
	};
	size_t i;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	driver = strstr(source,
		"\ngcs_block_resource_x_target_acquire_internal(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	if (driver != NULL && driver_end != NULL) {
		const char *ownership_loss_guard = strstr(driver,
			"if (!direct_init && !cached_local_x");
		const char *ownership_loss_invalidate = strstr(driver,
			"cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(");

		for (i = 0; i < lengthof(required); i++) {
			const char *site = strstr(driver, required[i]);

			UT_ASSERT_NOT_NULL(site);
			if (site != NULL)
				UT_ASSERT(site < driver_end);
		}
		UT_ASSERT_NOT_NULL(ownership_loss_guard);
		UT_ASSERT_NOT_NULL(ownership_loss_invalidate);
		if (ownership_loss_guard != NULL
			&& ownership_loss_invalidate != NULL)
			UT_ASSERT(ownership_loss_guard < ownership_loss_invalidate
				&& ownership_loss_invalidate < driver_end);
		for (i = 0; i < lengthof(forbidden); i++) {
			const char *site = strstr(driver, forbidden[i]);

			UT_ASSERT(site == NULL || site >= driver_end);
		}
	}
	gate_helper = strstr(source,
		"\ngcs_block_resource_x_gate_session_snapshot(");
	gate_helper_end = gate_helper != NULL
		? strstr(gate_helper, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(gate_helper);
	UT_ASSERT_NOT_NULL(gate_helper_end);
	if (gate_helper != NULL && gate_helper_end != NULL) {
		const char *gate_snapshot = strstr(gate_helper,
			"cluster_pcm_lock_resource_x_gate_snapshot(");
		const char *session_snapshot = strstr(gate_helper,
			"gcs_block_pcm_x_authenticated_session(");

		UT_ASSERT_NOT_NULL(gate_snapshot);
		UT_ASSERT_NOT_NULL(session_snapshot);
		if (gate_snapshot != NULL)
			UT_ASSERT(gate_snapshot < gate_helper_end);
		if (session_snapshot != NULL)
			UT_ASSERT(session_snapshot < gate_helper_end);
	}

	/* The shared R9 executor may retain the SOURCE adapter temporarily, but
	 * TARGET must derive its local base directly from BufferDesc and skip both
	 * legacy local-attempt and grant-publication projections. */
	terminal = strstr(source,
		"\ngcs_block_pcm_x_resource_x_join_terminal_try(");
	terminal_end = terminal != NULL
		? strstr(terminal, "\nstatic ResourceXApplyResult\n"
			"gcs_block_pcm_x_resource_x_master_durable_try(")
		: NULL;
	UT_ASSERT_NOT_NULL(terminal);
	UT_ASSERT_NOT_NULL(terminal_end);
	if (terminal != NULL && terminal_end != NULL) {
		const char *target_snapshot = strstr(terminal,
			"cluster_bufmgr_pcm_own_snapshot_by_tag(");
		const char *source_attempt = strstr(terminal,
			"if (!target_native)");
		const char *attempt = strstr(terminal,
			"cluster_pcm_x_local_resource_x_attempt_exact(");
		const char *publish = strstr(terminal,
			"cluster_pcm_x_local_resource_x_grant_publish_exact(");

		UT_ASSERT_NOT_NULL(target_snapshot);
		UT_ASSERT_NOT_NULL(source_attempt);
		UT_ASSERT_NOT_NULL(attempt);
		UT_ASSERT_NOT_NULL(publish);
		if (target_snapshot != NULL && source_attempt != NULL
			&& attempt != NULL && publish != NULL)
			UT_ASSERT(target_snapshot < source_attempt
				&& source_attempt < attempt && attempt < publish
				&& publish < terminal_end);
	}
	free(source);
}

UT_TEST(test_resource_x_type15_exact_join_is_the_only_new_r9_entry)
{
	static const char *const ingress_contract[] = {
		"cluster_pcm_lock_resource_x_requester_join_exact(",
		"RESOURCE_X_REQUESTER_JOIN_READY",
		"gcs_block_resource_x_requester_terminal_try("
	};
	static const char *const executor_contract[] = {
		"cluster_pcm_lock_resource_x_requester_join_frames_exact(",
		"join.requester_target_generation != join.assertion_sequence",
		"ref.acquisition_generation = join.requester_target_generation",
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"if (!target_native)",
		"cluster_pcm_x_local_resource_x_attempt_exact(",
		"cluster_pcm_lock_resource_x_executor_enter(",
		"cluster_pcm_lock_resource_x_t1_grant_exact(",
		"cluster_bufmgr_pcm_own_activate_x_by_tag(",
		"cluster_pcm_lock_resource_x_requester_apply_exact(",
		"cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(",
		"cluster_pcm_lock_resource_x_requester_activate_exact(",
		"cluster_pcm_lock_resource_x_executor_leave("
	};
	char *source = read_gcs_block_source();
	const char *executor;

	assert_ordered_in_function(
		source, "\ngcs_block_try_resource_x_frame(",
		"\n/*\n * cluster_gcs_handle_block_request_envelope", ingress_contract,
		lengthof(ingress_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(",
		"\nstatic bool\ngcs_block_try_resource_x_frame(", executor_contract,
		lengthof(executor_contract));
	executor = strstr(source,
		"\ngcs_block_pcm_x_resource_x_join_terminal_try(");
	if (executor != NULL)
		UT_ASSERT_NULL(strstr(executor,
			"&ref, join.base_authority_generation"));
	free(source);
}

UT_TEST(test_resource_x_direct_n_uses_exact_durable_storage_proof)
{
	static const char *const requester_assert_contract[] = {
		"cluster_pcm_x_local_resource_x_grant_rebase_publish_exact(",
		"before.pcm_state == (uint8)PCM_STATE_N",
		"cluster_bufmgr_pcm_own_n_assertion_candidate_exact(",
		"gcs_block_pcm_x_resource_x_assert_stage_exact("
	};
	static const char *const master_proof_contract[] = {
		"captured->incompatible_holders_bitmap != captured->blocked_holders_bitmap",
		"cluster_bufmgr_read_storage_image_for_resource_x(",
		"gcs_block_pcm_x_resource_x_durable_proof_crc(",
		"cluster_pcm_lock_resource_x_durable_proof_exact("
	};
	static const char *const requester_join_contract[] = {
		"cluster_pcm_lock_resource_x_t1_grant_exact(",
		"cluster_bufmgr_read_storage_image_for_resource_x(",
		"gcs_block_pcm_x_resource_x_durable_proof_crc(",
		"gcs_block_pcm_x_resource_x_prepare_target_x(",
		"cluster_bufmgr_pcm_own_activate_x_by_tag("
	};
	static const char *const target_direct_init_contract[] = {
		"cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(",
		"cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(",
		"cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(",
		"current.flags == PCM_OWN_FLAG_GRANT_PENDING",
		"if (target_native)",
		"if (!direct_init_bound || !require_clean_n",
		"cluster_bufmgr_pcm_own_n_direct_init_candidate_exact(",
		"cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(",
		"cluster_bufmgr_pcm_own_finish_x_commit("
	};
	static const char *const target_direct_init_terminal_contract[] = {
		"gcs_block_pcm_x_resource_x_prepare_target_x(",
		"cluster_bufmgr_pcm_own_direct_init_bind_x_by_tag_exact(",
		"cluster_pcm_lock_resource_x_requester_apply_exact(",
		"cluster_bufmgr_pcm_own_direct_init_clear_x_by_tag_exact("
	};
	char *source = read_gcs_block_source();
	const char *prepare;
	const char *n_branch;
	const char *copy;
	const char *rebase;
	const char *rebase_publish;
	const char *zero_token_gate;

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_prepare_and_assert(",
		"\n\n/* A READY probe", requester_assert_contract,
		lengthof(requester_assert_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_master_durable_try(",
		"\n\n/* Consume the exact Resource-X subdomain", master_proof_contract,
		lengthof(master_proof_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(",
		"\nstatic bool\ngcs_block_try_resource_x_frame(", requester_join_contract,
		lengthof(requester_join_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_prepare_target_x(",
		"\n/* Consume one complete retained type-15 join",
		target_direct_init_contract,
		lengthof(target_direct_init_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(",
		"\nstatic bool\ngcs_block_try_resource_x_frame(",
		target_direct_init_terminal_contract,
		lengthof(target_direct_init_terminal_contract));
	prepare = strstr(source, "\ngcs_block_pcm_x_resource_x_prepare_and_assert(");
	rebase = prepare != NULL
		? strstr(prepare, "if (before.generation != effective_own_generation)")
		: NULL;
	rebase_publish = rebase != NULL
		? strstr(rebase,
			"cluster_pcm_x_local_resource_x_grant_rebase_publish_exact(")
		: NULL;
	zero_token_gate = rebase != NULL
		? strstr(rebase, "before.reservation_token != 0") : NULL;
	UT_ASSERT_NOT_NULL(rebase);
	UT_ASSERT_NOT_NULL(rebase_publish);
	/* The token is a monotonic ABA witness and stays nonzero after an idle
	 * revoke commit.  Eligibility keys on flags==0; the second snapshot below
	 * still has to preserve the exact historical token. */
	UT_ASSERT(zero_token_gate == NULL
			  || (rebase_publish != NULL && zero_token_gate > rebase_publish));
	n_branch = prepare != NULL
		? strstr(prepare, "before.pcm_state == (uint8)PCM_STATE_N") : NULL;
	copy = n_branch != NULL
		? strstr(n_branch, "cluster_bufmgr_copy_block_for_gcs(") : NULL;
	UT_ASSERT_NOT_NULL(n_branch);
	/* The N branch must return through assertion-only staging before the
	 * S/X current-image copy path can be reached. */
	if (n_branch != NULL && copy != NULL)
		UT_ASSERT_NOT_NULL(strstr(n_branch, "return gcs_block_pcm_x_resource_x_assert_stage_exact("));
	free(source);
}

UT_TEST(test_resource_x_native_target_accepts_only_exact_clean_n_before_bootstrap)
{
	char *source = read_gcs_block_source();
	const char *target;
	const char *target_end;
	const char *snapshot;
	const char *n_branch;
	const char *target_install_helper;
	const char *target_install_wait;
	const char *retained_pair_helper;
	const char *retained_buffer_helper;
	const char *retained_wait;
	const char *retained_admission_recheck;
	const char *retained_gate_recheck;
	const char *retained_deadline;
	const char *retained_sleep;
	const char *retained_continue;
	const char *n_candidate;
	const char *step;
	const char *ownership_loss_skip;
	const char *generation_zero_reject;
	const char *terminal;
	const char *terminal_end;
	const char *terminal_snapshot;
	const char *terminal_n_branch;
	const char *terminal_n_candidate;
	const char *terminal_generation_zero_reject;

	/* A cold remote writer legitimately starts from a generation-zero N
	 * descriptor after ordinary buffer input.  The descriptor contributes no
	 * authority or bytes: the existing exact BM_VALID/no-IO N predicate must
	 * pass before kind-9 bootstrap, and DURABLE_STORAGE remains master-selected. */
	target = strstr(source, "\ngcs_block_resource_x_target_acquire_internal(");
	target_end = target != NULL
		? strstr(target, "\nResourceXApplyResult\n"
			"cluster_gcs_resource_x_target_acquire_exact(")
		: NULL;
	snapshot = target != NULL
		? strstr(target, "cluster_bufmgr_pcm_own_snapshot(buf, &own)") : NULL;
	target_install_helper = snapshot != NULL
		? strstr(snapshot,
			"cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(")
		: NULL;
	/* Anchor the ordinary TARGET N branch after its mode-independent inflight
	 * classifier; the direct-init branch has an earlier, separate N shape. */
	n_branch = target_install_helper != NULL
		? strstr(target_install_helper,
			"own.pcm_state == (uint8)PCM_STATE_N") : NULL;
	target_install_wait = target_install_helper != NULL
		? strstr(target_install_helper,
			"action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT") : NULL;
	retained_pair_helper = n_branch != NULL
		? strstr(n_branch,
			"cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(")
		: NULL;
	retained_buffer_helper = retained_pair_helper != NULL
		? strstr(retained_pair_helper,
			"cluster_bufmgr_pcm_own_n_retained_release_inflight_exact(")
		: NULL;
	retained_wait = retained_buffer_helper != NULL
		? strstr(retained_buffer_helper,
			"if (target_retained_release_inflight)") : NULL;
	retained_admission_recheck = retained_wait != NULL
		? strstr(retained_wait,
			"cluster_semantic_activation_recheck(&admission)") : NULL;
	retained_gate_recheck = retained_admission_recheck != NULL
		? strstr(retained_admission_recheck,
			"gcs_block_resource_x_gate_session_recheck(") : NULL;
	retained_deadline = retained_gate_recheck != NULL
		? strstr(retained_gate_recheck,
			"now_us >= absolute_deadline_us") : NULL;
	retained_sleep = retained_wait != NULL
		? strstr(retained_wait, "pg_usleep(timeout_ms * 1000L)") : NULL;
	retained_continue = retained_sleep != NULL
		? strstr(retained_sleep, "continue;") : NULL;
	n_candidate = n_branch != NULL
		? strstr(n_branch,
			"cluster_bufmgr_pcm_own_n_assertion_candidate_exact(")
		: NULL;
	step = n_candidate != NULL
		? strstr(n_candidate,
			"cluster_pcm_lock_resource_x_bootstrap_round_step_exact(")
		: NULL;
	generation_zero_reject = snapshot != NULL
		? strstr(snapshot, "if (own.generation == 0)") : NULL;
	ownership_loss_skip = step != NULL
		? strstr(step, "&& !target_install_inflight") : NULL;

	UT_ASSERT_NOT_NULL(target);
	UT_ASSERT_NOT_NULL(target_end);
	UT_ASSERT_NOT_NULL(snapshot);
	UT_ASSERT_NOT_NULL(n_branch);
	UT_ASSERT_NOT_NULL(target_install_helper);
	UT_ASSERT_NOT_NULL(target_install_wait);
	UT_ASSERT_NOT_NULL(retained_pair_helper);
	UT_ASSERT_NOT_NULL(retained_buffer_helper);
	UT_ASSERT_NOT_NULL(retained_wait);
	UT_ASSERT_NOT_NULL(retained_admission_recheck);
	UT_ASSERT_NOT_NULL(retained_gate_recheck);
	UT_ASSERT_NOT_NULL(retained_deadline);
	UT_ASSERT_NOT_NULL(retained_sleep);
	UT_ASSERT_NOT_NULL(retained_continue);
	UT_ASSERT_NOT_NULL(n_candidate);
	UT_ASSERT_NOT_NULL(step);
	UT_ASSERT_NOT_NULL(ownership_loss_skip);
	if (target_end != NULL && snapshot != NULL && n_branch != NULL
		&& target_install_helper != NULL && target_install_wait != NULL
		&& retained_pair_helper != NULL && retained_buffer_helper != NULL
		&& retained_wait != NULL && retained_admission_recheck != NULL
		&& retained_gate_recheck != NULL && retained_deadline != NULL
		&& retained_sleep != NULL
		&& retained_continue != NULL
		&& n_candidate != NULL && step != NULL
		&& ownership_loss_skip != NULL) {
		UT_ASSERT(snapshot < n_branch);
		UT_ASSERT(target_install_helper < n_branch);
		UT_ASSERT(n_branch < retained_pair_helper);
		UT_ASSERT(retained_pair_helper < retained_buffer_helper);
		UT_ASSERT(retained_buffer_helper < n_candidate);
		UT_ASSERT(target_install_helper < n_candidate);
		UT_ASSERT(n_candidate < target_install_wait);
		UT_ASSERT(target_install_wait < step);
		UT_ASSERT(retained_wait < retained_admission_recheck);
		UT_ASSERT(retained_admission_recheck < retained_gate_recheck);
		UT_ASSERT(retained_gate_recheck < retained_deadline);
		UT_ASSERT(retained_deadline < retained_sleep);
		UT_ASSERT(retained_wait < retained_sleep);
		UT_ASSERT(retained_sleep < retained_continue);
		UT_ASSERT(retained_continue < step);
		UT_ASSERT(target_install_wait < ownership_loss_skip);
		UT_ASSERT(step < target_end);
		UT_ASSERT(ownership_loss_skip < target_end);
	}
	/* S/X generation zero remains impossible; its rejection must be the
	 * alternate branch after the exact N predicate, never a pre-N gate. */
	UT_ASSERT_NOT_NULL(generation_zero_reject);
	if (generation_zero_reject != NULL && n_candidate != NULL && step != NULL) {
		UT_ASSERT(n_candidate < generation_zero_reject);
		UT_ASSERT(generation_zero_reject < step);
	}

	/* The same cold-N shape must be revalidated at type-15 terminal before
	 * T1/T2/T3.  Rejecting generation zero before the exact N predicate would
	 * invalidate the round that this function admitted above. */
	terminal = strstr(source,
		"\ngcs_block_pcm_x_resource_x_join_terminal_try(");
	terminal_end = terminal != NULL
		? strstr(terminal, "\nstatic ResourceXApplyResult\n"
			"gcs_block_pcm_x_resource_x_master_durable_try(")
		: NULL;
	terminal_snapshot = terminal != NULL
		? strstr(terminal, "cluster_bufmgr_pcm_own_snapshot_by_tag(") : NULL;
	terminal_n_branch = terminal_snapshot != NULL
		? strstr(terminal_snapshot,
			"target_base.pcm_state == (uint8)PCM_STATE_N") : NULL;
	terminal_n_candidate = terminal_n_branch != NULL
		? strstr(terminal_n_branch,
			"cluster_bufmgr_pcm_own_n_assertion_candidate_exact(") : NULL;
	terminal_generation_zero_reject = terminal_snapshot != NULL
		? strstr(terminal_snapshot, "target_base.generation == 0") : NULL;
	UT_ASSERT_NOT_NULL(terminal);
	UT_ASSERT_NOT_NULL(terminal_end);
	UT_ASSERT_NOT_NULL(terminal_snapshot);
	UT_ASSERT_NOT_NULL(terminal_n_branch);
	UT_ASSERT_NOT_NULL(terminal_n_candidate);
	UT_ASSERT_NOT_NULL(terminal_generation_zero_reject);
	if (terminal_end != NULL && terminal_snapshot != NULL
		&& terminal_n_branch != NULL && terminal_n_candidate != NULL
		&& terminal_generation_zero_reject != NULL) {
		UT_ASSERT(terminal_snapshot < terminal_n_branch);
		UT_ASSERT(terminal_n_branch < terminal_n_candidate);
		UT_ASSERT(terminal_n_candidate < terminal_generation_zero_reject);
		UT_ASSERT(terminal_generation_zero_reject < terminal_end);
	}
	free(source);
}

UT_TEST(test_resource_x_passive_pi_waits_for_exact_remote_image_before_x)
{
	static const char *const assertion_contract[] = {
		"before.pcm_state == (uint8)PCM_STATE_N",
		"cluster_bufmgr_pcm_own_n_assertion_candidate_exact(",
		"gcs_block_pcm_x_resource_x_assert_stage_exact("
	};
	static const char *const target_contract[] = {
		"cluster_bufmgr_pcm_own_begin_x_reservation(",
		"gcs_block_pcm_x_resource_x_install_target_image_exact(",
		"cluster_bufmgr_pcm_own_finish_x_commit("
	};
	char *source = read_gcs_block_source();
	const char *join;
	const char *remote_image;
	const char *prepare_target;

	/* The passive PI is not a proof source.  It only allows the no-local-proof
	 * assertion to reach the master.  Once an exact retained remote image is
	 * joined, install those bytes under GRANT_PENDING before X commit; never
	 * relabel or read the PI as current authority. */
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_prepare_and_assert(",
		"\n\n/* A READY probe", assertion_contract,
		lengthof(assertion_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_prepare_target_x(",
		"\n/* Consume one complete retained type-15 join", target_contract,
		lengthof(target_contract));
	join = strstr(source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(");
	remote_image = join != NULL
		? strstr(join, "image_frame.body.image_envelope.page_bytes") : NULL;
	prepare_target = join != NULL
		? strstr(join, "gcs_block_pcm_x_resource_x_prepare_target_x(") : NULL;
	UT_ASSERT_NOT_NULL(remote_image);
	UT_ASSERT_NOT_NULL(prepare_target);
	UT_ASSERT(remote_image < prepare_target);
	free(source);
}

UT_TEST(test_resource_x_settlement_removes_exact_wfg_before_locator_completion)
{
	static const char *const completion_contract[] = {
		"cluster_pcm_x_master_resource_x_owner_exact(",
		"cluster_gcs_pcm_x_vertex_from_identity(",
		"cluster_lmd_graph_remove_edge_by_waiter_exact_result(",
		"CLUSTER_LMD_GRAPH_REMOVE_STALE",
		"cluster_pcm_x_master_resource_x_complete_exact("
	};
	char *source = read_gcs_block_source();
	const char *helper;
	const char *ingress;
	const char *master_wait;

	/* A SETTLED Resource-X request no longer waits for its predecessor.  Drop
	 * only its exact wait_seq graph before retiring the compatibility locator;
	 * ABSENT is an idempotent settlement replay, while STALE protects a newer
	 * backend wait instance from the old terminal frame. */
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact(",
		"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_resource_x_master_wait_exact(",
		completion_contract, lengthof(completion_contract));
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact(");
	ingress = strstr(source, "case RESOURCE_X_WIRE_INSTALL_SETTLEMENT:");
	master_wait = strstr(source,
		"\nstatic PcmXQueueResult\ngcs_block_pcm_x_resource_x_master_wait_exact(");
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(master_wait);
	if (ingress != NULL)
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"gcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact("));
	if (master_wait != NULL)
		UT_ASSERT_NOT_NULL(strstr(master_wait,
			"gcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact("));
	free(source);
}

UT_TEST(test_resource_x_native_settlement_has_no_legacy_locator_cleanup)
{
	char *source = read_gcs_block_source();
	const char *ingress;
	const char *ingress_end;
	const char *native_branch;
	const char *legacy_cleanup;
	const char *resource_x_retire;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	ingress = strstr(source, "case RESOURCE_X_WIRE_INSTALL_SETTLEMENT:");
	ingress_end = ingress != NULL
		? strstr(ingress, "case RESOURCE_X_WIRE_RELEASE_X:") : NULL;
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(ingress_end);
	if (ingress != NULL && ingress_end != NULL) {
		native_branch = strstr(ingress,
			"if (frame.common.ordered_lane == 0)");
		legacy_cleanup = strstr(ingress,
			"gcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact(");
		resource_x_retire = strstr(ingress,
			"cluster_pcm_lock_resource_x_settled_retire_exact(");
		UT_ASSERT_NOT_NULL(native_branch);
		UT_ASSERT_NOT_NULL(legacy_cleanup);
		UT_ASSERT_NOT_NULL(resource_x_retire);
		if (native_branch != NULL && legacy_cleanup != NULL
			&& resource_x_retire != NULL) {
			UT_ASSERT(native_branch < legacy_cleanup);
			UT_ASSERT(native_branch < resource_x_retire);
			UT_ASSERT(legacy_cleanup < ingress_end);
			UT_ASSERT(resource_x_retire < ingress_end);
		}
	}
	free(source);
}

UT_TEST(test_resource_x_source_settlement_uses_exact_typed_debt_and_ack)
{
	static const char *const source_apply_contract[] = {
		"cluster_pcm_lock_resource_x_source_settlement_prepare_exact(",
		"cluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(",
		"cluster_pcm_lock_resource_x_source_settlement_commit_exact(",
		"cluster_pcm_lock_resource_x_source_settlement_ack_build_exact(",
		"cluster_resource_x_wire_encode(",
		"cluster_grd_outbound_enqueue_backend_msg("
	};
	char *source = read_gcs_block_source();
	const char *candidate;
	const char *candidate_end;
	const char *ingress;
	const char *ingress_end;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	candidate = strstr(source,
		"if (msg_type == RESOURCE_X_MSG_BLOCK_TO_N)");
	candidate_end = candidate != NULL
		? strstr(candidate,
			"if (msg_type == RESOURCE_X_MSG_BLOCKED_TO_N)") : NULL;
	UT_ASSERT_NOT_NULL(candidate);
	UT_ASSERT_NOT_NULL(candidate_end);
	if (candidate != NULL && candidate_end != NULL) {
		const char *proof = strstr(candidate, "RESOURCE_X_PROOF_V1_BYTES");

		UT_ASSERT_NOT_NULL(proof);
		if (proof != NULL)
			UT_ASSERT(proof < candidate_end);
	}
	assert_ordered_in_function(
		source, "\ngcs_block_resource_x_source_settlement_ingress(",
		"\n\nstatic", source_apply_contract,
		lengthof(source_apply_contract));
	ingress = strstr(source, "case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:");
	ingress_end = ingress != NULL
		? strstr(ingress, "case RESOURCE_X_WIRE_RELEASE_X:") : NULL;
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(ingress_end);
	if (ingress != NULL && ingress_end != NULL) {
		const char *apply = strstr(ingress,
			"gcs_block_resource_x_source_settlement_ingress(");
		const char *ack_case = strstr(ingress,
			"case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2:");
		const char *ack_apply = ack_case != NULL
			? strstr(ack_case,
				"cluster_pcm_lock_resource_x_source_settlement_ack_exact(")
			: NULL;

		UT_ASSERT_NOT_NULL(apply);
		UT_ASSERT_NOT_NULL(ack_case);
		UT_ASSERT_NOT_NULL(ack_apply);
		if (apply != NULL)
			UT_ASSERT(apply < ingress_end);
		if (ack_apply != NULL)
			UT_ASSERT(ack_apply < ingress_end);
	}
	free(source);
}

UT_TEST(test_resource_x_type17_x_source_fences_before_retained_release)
{
	static const char *const source_contract[] = {
		"cluster_pcm_lock_resource_x_holder_image_exact(",
		"cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(",
		"cluster_pcm_x_local_writer_revoke_fence_acquire_exact(",
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"current.generation > image.body.image_envelope.source_carrier_generation",
		"carrier_superseded = true;",
		"cluster_bufmgr_pcm_own_begin_x_revoke(",
		"cluster_pcm_x_local_writer_revoke_fence_revalidate_held_exact(",
		"cluster_bufmgr_copy_block_for_gcs(",
		"gcs_block_pcm_x_resource_x_build_source_frames(",
		"cluster_pcm_lock_resource_x_block_to_n_source_exact(",
		"semantic_retained && carrier_superseded",
		"result == RESOURCE_X_APPLY_STALE",
		"cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(",
		"pair_result == RESOURCE_X_APPLY_APPLIED",
		"cluster_bufmgr_pcm_own_finish_revoke_retain(",
		"cluster_pcm_lock_resource_x_holder_pair_publish_exact(",
		"cluster_pcm_x_local_writer_revoke_fence_release_exact("
	};
	char *source = read_gcs_block_source();
	const char *ingress;
	const char *source_call;
	const char *s_consumer;
	const char *builder;
	const char *helper;
	const char *abort;
	const char *arm;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_source_block_to_n(");
	UT_ASSERT_NOT_NULL(helper);
	if (helper != NULL)
		assert_ordered_in_function(
			source, "\ngcs_block_pcm_x_resource_x_source_block_to_n(",
			"\n\n/* Consume the exact Resource-X subdomain", source_contract,
			lengthof(source_contract));
	if (helper != NULL)
		UT_ASSERT_NOT_NULL(strstr(helper,
			"status_result != RESOURCE_X_APPLY_NOT_FOUND\n"
			"\t\t&& status_result != RESOURCE_X_APPLY_STALE"));
	if (helper != NULL) {
		UT_ASSERT_NOT_NULL(strstr(helper,
			"PCM-X Resource-X finish-error evidence exact"));
		UT_ASSERT_NOT_NULL(strstr(helper, "CopyErrorData();"));
		UT_ASSERT_NOT_NULL(strstr(helper, "pair_retained"));
	}
	ingress = strstr(source, "\ngcs_block_try_resource_x_frame(");
	source_call = ingress != NULL
		? strstr(ingress, "gcs_block_pcm_x_resource_x_source_block_to_n(")
		: NULL;
	s_consumer = source_call != NULL
		? strstr(source_call, "cluster_pcm_lock_resource_x_block_to_n_exact(")
		: NULL;
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(source_call);
	UT_ASSERT_NOT_NULL(s_consumer);
	if (source_call != NULL && s_consumer != NULL)
		UT_ASSERT(source_call < s_consumer);
	builder = strstr(source, "\ngcs_block_pcm_x_resource_x_build_source_frames(");
	UT_ASSERT_NOT_NULL(builder);
	if (builder != NULL) {
		UT_ASSERT_NOT_NULL(strstr(builder,
			"requester_target_generation = block->common.assertion_sequence"));
		UT_ASSERT_NOT_NULL(strstr(builder,
			"source_proof_crc32c = image->common.semantic_crc32c"));
	}
	abort = strstr(source, "\ngcs_block_pcm_x_resource_x_abort_pre_arm(");
	arm = strstr(source, "cluster_pcm_lock_resource_x_block_to_n_source_exact(");
	UT_ASSERT_NOT_NULL(abort);
	UT_ASSERT_NOT_NULL(arm);
	if (abort != NULL)
		UT_ASSERT_NOT_NULL(strstr(abort,
			"cluster_bufmgr_pcm_own_abort_x_revoke("));
	free(source);
}

UT_TEST(test_resource_x_type17_x_source_separates_wire_and_local_fence_formations)
{
	static const char *const dual_domain_contract[] = {
		"gcs_block_resource_x_gate_session_snapshot(",
		"resource_gate.formation != block->common.resource_formation",
		"resource_master_node != authenticated_master_node",
		"resource_master_session\n\t\t\t!= block->common.master_session_incarnation",
		"local_runtime = cluster_pcm_x_runtime_snapshot();",
		"cluster_pcm_x_local_writer_revoke_fence_acquire_exact(",
		"local_runtime.gate_generation,",
		"gcs_block_resource_x_gate_session_recheck("
	};
	char *source = read_gcs_block_source();
	const char *helper;
	const char *helper_end;
	const char *fence;
	const char *fence_end;
	const char *wire_formation_argument;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_source_block_to_n(",
		"\n\n/* Install the exact retained remote carrier", dual_domain_contract,
		lengthof(dual_domain_contract));
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_source_block_to_n(");
	helper_end = helper != NULL
		? strstr(helper, "\n\n/* Install the exact retained remote carrier")
		: NULL;
	fence = helper != NULL
		? strstr(helper,
			"cluster_pcm_x_local_writer_revoke_fence_acquire_exact(")
		: NULL;
	fence_end = fence != NULL ? strstr(fence, "&writer_fence);") : NULL;
	wire_formation_argument = fence != NULL
		? strstr(fence, "block->common.resource_formation,") : NULL;
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(fence);
	UT_ASSERT_NOT_NULL(fence_end);
	if (wire_formation_argument != NULL && fence_end != NULL)
		UT_ASSERT(wire_formation_argument >= fence_end);
	free(source);
}

UT_TEST(test_resource_x_type17_target_x_after_l3_uses_no_legacy_shell)
{
	static const char *const tagless_contract[] = {
		"cluster_pcm_x_local_writer_revoke_fence_acquire_exact(",
		"writer_result == PCM_X_QUEUE_NOT_FOUND",
		"cluster_resource_x_writer_path_snapshot(",
		"writer_path != RESOURCE_X_WRITER_TARGET",
		"tagless_target_x = true;",
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(",
		"cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(",
		"cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(",
		"cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(",
		"cluster_bufmgr_copy_block_for_gcs(",
		"cluster_pcm_lock_resource_x_block_to_n_source_exact(",
		"cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		"gcs_block_resource_x_terminal_owner_release("
	};
	char *source = read_gcs_block_source();
	const char *helper;
	const char *helper_end;
	const char *lineage_first;
	const char *lineage_second;
	const char *lineage_third;
	const char *lineage_fourth;
	const char *legacy_shell;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_source_block_to_n(",
		"\n\n/* Install the exact retained remote carrier", tagless_contract,
		lengthof(tagless_contract));
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_source_block_to_n(");
	helper_end = helper != NULL
		? strstr(helper, "\n\n/* Install the exact retained remote carrier")
		: NULL;
	legacy_shell = helper != NULL
		? strstr(helper, "pcm_x_allocator_reserve_locked(") : NULL;
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	lineage_first = helper != NULL ? strstr(helper,
		"cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(")
		: NULL;
	lineage_second = lineage_first != NULL ? strstr(lineage_first + 1,
		"cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(")
		: NULL;
	lineage_third = lineage_second != NULL ? strstr(lineage_second + 1,
		"cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(")
		: NULL;
	lineage_fourth = lineage_third != NULL ? strstr(lineage_third + 1,
		"cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(")
		: NULL;
	UT_ASSERT_NOT_NULL(lineage_first);
	UT_ASSERT_NOT_NULL(lineage_second);
	if (lineage_second != NULL && helper_end != NULL)
		UT_ASSERT(lineage_second < helper_end);
	UT_ASSERT(lineage_third == NULL || lineage_third >= helper_end);
	UT_ASSERT(lineage_fourth == NULL || lineage_fourth >= helper_end);
	UT_ASSERT_NOT_NULL(strstr(helper,
		"cluster_bufmgr_pcm_own_abort_held_x_revoke("));
	UT_ASSERT_NOT_NULL(strstr(helper,
		"cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed("));
	if (legacy_shell != NULL && helper_end != NULL)
		UT_ASSERT(legacy_shell >= helper_end);
	free(source);
}

UT_TEST(test_resource_x_type17_target_drains_before_semantic_retention)
{
	char *source = read_gcs_block_source();
	const char *helper;
	const char *helper_end;
	const char *preflight;
	const char *busy;
	const char *abort;
	const char *yield;
	const char *retry_return;
	const char *copy;
	const char *retain;
	const char *finish;
	const char *second_yield;
	const char *forbidden;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_source_block_to_n(");
	helper_end = helper != NULL
		? strstr(helper, "\n\n/* Install the exact retained remote carrier")
		: NULL;
	preflight = helper != NULL
		? strstr(helper, "cluster_bufmgr_pcm_own_try_drain_held_x_revoke(")
		: NULL;
	busy = preflight != NULL
		? strstr(preflight, "own_result == CLUSTER_PCM_OWN_BUSY") : NULL;
	abort = busy != NULL
		? strstr(busy, "cluster_bufmgr_pcm_own_abort_held_x_revoke(") : NULL;
	yield = abort != NULL
		? strstr(abort,
			"cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(")
		: NULL;
	retry_return = yield != NULL
		? strstr(yield, "return RESOURCE_X_APPLY_BAD_STATE;") : NULL;
	copy = retry_return != NULL
		? strstr(retry_return, "cluster_bufmgr_copy_block_for_gcs(") : NULL;
	retain = copy != NULL
		? strstr(copy, "cluster_pcm_lock_resource_x_block_to_n_source_exact(")
		: NULL;
	finish = retain != NULL
		? strstr(retain,
			"cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(")
		: NULL;

	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(preflight);
	UT_ASSERT_NOT_NULL(busy);
	UT_ASSERT_NOT_NULL(abort);
	UT_ASSERT_NOT_NULL(yield);
	UT_ASSERT_NOT_NULL(retry_return);
	UT_ASSERT_NOT_NULL(copy);
	UT_ASSERT_NOT_NULL(retain);
	UT_ASSERT_NOT_NULL(finish);
	if (helper_end != NULL && preflight != NULL && busy != NULL
		&& abort != NULL && yield != NULL && retry_return != NULL
		&& copy != NULL && retain != NULL && finish != NULL) {
		UT_ASSERT(helper < preflight && preflight < busy && busy < abort
			&& abort < yield && yield < retry_return && retry_return < copy
			&& copy < retain && retain < finish && finish < helper_end);
		UT_ASSERT(strstr(preflight, "pg_usleep(") == NULL
			|| strstr(preflight, "pg_usleep(") >= helper_end);
		UT_ASSERT(strstr(preflight,
			"GCS_RESOURCE_X_TERMINAL_FINISH_ATTEMPTS") == NULL
			|| strstr(preflight,
				"GCS_RESOURCE_X_TERMINAL_FINISH_ATTEMPTS") >= helper_end);
		forbidden = strstr(preflight, "LWLockAcquire(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		forbidden = strstr(preflight, "LWLockAcquireOrWait(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		forbidden = strstr(busy,
			"cluster_pcm_lock_resource_x_block_to_n_source_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= retry_return);
		forbidden = strstr(busy,
			"cluster_pcm_lock_resource_x_holder_pair_publish_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= retry_return);
		second_yield = strstr(yield + 1,
			"cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(");
		UT_ASSERT(second_yield == NULL || second_yield >= helper_end);
	}
	free(source);
}

UT_TEST(test_resource_x_type17_selected_s_source_reuses_fenced_pair_core)
{
	static const char *const source_contract[] = {
		"source_mode = block->common.observed_mode",
		"cluster_pcm_x_local_writer_revoke_fence_acquire_exact(",
		"cluster_bufmgr_pcm_own_prepare_s_source_image(",
		"cluster_pcm_x_local_writer_revoke_fence_revalidate_held_exact(",
		"gcs_block_pcm_x_resource_x_build_source_frames(",
		"cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(",
		"cluster_bufmgr_pcm_own_finish_revoke_retain(",
		"cluster_pcm_lock_resource_x_holder_pair_publish_exact(",
		"cluster_pcm_x_local_writer_revoke_fence_release_exact("
	};
	char *source = read_gcs_block_source();
	const char *abort;
	const char *builder;
	const char *ingress;
	const char *source_dispatch;
	const char *status_dispatch;

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_source_block_to_n(",
		"\n\n/* Install the exact retained remote carrier", source_contract,
		lengthof(source_contract));
	builder = strstr(source,
		"\ngcs_block_pcm_x_resource_x_build_source_frames(");
	UT_ASSERT_NOT_NULL(builder);
	if (builder != NULL) {
		UT_ASSERT_NOT_NULL(strstr(builder,
			"revoking->pcm_state != source_mode"));
		UT_ASSERT_NOT_NULL(strstr(builder,
			"image->common.observed_mode = source_mode"));
		UT_ASSERT_NOT_NULL(strstr(builder,
			"image_body->source_fence[28] = source_mode"));
	}
	abort = strstr(source, "\ngcs_block_pcm_x_resource_x_abort_pre_arm(");
	UT_ASSERT_NOT_NULL(abort);
	if (abort != NULL) {
		UT_ASSERT_NOT_NULL(strstr(abort,
			"cluster_bufmgr_pcm_own_abort_s_revoke("));
		UT_ASSERT_NOT_NULL(strstr(abort,
			"cluster_bufmgr_pcm_own_abort_x_revoke("));
	}
	ingress = strstr(source, "\ngcs_block_try_resource_x_frame(");
	source_dispatch = ingress != NULL
		? strstr(ingress,
			"gcs_block_pcm_x_resource_x_source_block_to_n(") : NULL;
	status_dispatch = ingress != NULL
		? strstr(ingress,
			"gcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(") : NULL;
	UT_ASSERT_NOT_NULL(source_dispatch);
	UT_ASSERT_NOT_NULL(status_dispatch);
	if (source_dispatch != NULL && status_dispatch != NULL)
		UT_ASSERT(source_dispatch < status_dispatch);
	free(source);
}

UT_TEST(test_resource_x_type17_remote_s_holder_uses_exact_buffer_evidence)
{
	static const char *const holder_contract[] = {
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"cluster_bufmgr_pcm_own_s_holder_candidate_exact(",
		"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
		"cluster_bufmgr_pcm_own_begin_s_revoke(",
		"LWLockRelease(content_lock)",
		"cluster_lms_outbound_stage_resource_x_remote_s_status_exact(",
		"cluster_sf_peer_capability_generation_matches(",
		"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
		"cluster_bufmgr_pcm_own_finish_remote_s_block_to_n(",
		"LWLockRelease(content_lock)",
		"cluster_lms_outbound_publish_resource_x_remote_s_status_exact("
	};
	char *source = read_gcs_block_source();
	const char *helper;
	const char *helper_end;
	const char *forbidden;
	const char *ingress;
	const char *remote_call;
	const char *master_entry_call;

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(",
		"\n\n/* Consume the exact Resource-X subdomain", holder_contract,
		lengthof(holder_contract));
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(");
	helper_end = helper != NULL
		? strstr(helper, "\n\n/* Consume the exact Resource-X subdomain")
		: NULL;
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	if (helper != NULL && helper_end != NULL) {
		forbidden = strstr(helper, "pcm_get_or_create_entry(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		forbidden = strstr(helper,
			"cluster_pcm_lock_resource_x_block_to_n_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		forbidden = strstr(helper,
			"cluster_lms_outbound_enqueue_resource_x_remote_s_status_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= helper_end);
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_lms_outbound_cancel_resource_x_remote_s_status_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_bufmgr_pcm_own_abort_s_revoke("));
	}
	ingress = strstr(source, "\ngcs_block_try_resource_x_frame(");
	remote_call = ingress != NULL
		? strstr(ingress,
			"gcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(")
		: NULL;
	master_entry_call = remote_call != NULL
		? strstr(remote_call,
			"cluster_pcm_lock_resource_x_block_to_n_exact(")
		: NULL;
	UT_ASSERT_NOT_NULL(remote_call);
	UT_ASSERT_NOT_NULL(master_entry_call);
	if (remote_call != NULL && master_entry_call != NULL)
		UT_ASSERT(remote_call < master_entry_call);
	free(source);
}

UT_TEST(test_resource_x_preassert_late_bind_race_refreshes_exactly)
{
	PcmXLocalProgress before;
	PcmXLocalProgress after;
	PcmXLocalProgress changed;
	char *source = read_gcs_block_source();
	const char *caller;
	const char *refresh;
	const char *classify;
	const char *retry;

	memset(&before, 0, sizeof(before));
	before.identity.node_id = 3;
	before.identity.procno = 7;
	before.identity.xid = 101;
	before.identity.cluster_epoch = 11;
	before.identity.request_id = 19;
	before.identity.wait_seq = 23;
	before.identity.base_own_generation = 5;
	before.ref.identity = before.identity;
	before.ref.handle.ticket_id = 29;
	before.ref.handle.queue_generation = 31;
	before.member_state = PCM_XL_REMOTE_WAIT;
	before.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	before.last_response_opcode = PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK;
	before.master_session_incarnation = 37;
	before.master_node = 0;
	before.resource_x_admission_sequence = 7;
	before.resource_x_base_authority_generation = 13;
	before.resource_x_authority_domain = PCM_X_AUTHORITY_DOMAIN_RESOURCE_X;

	after = before;
	after.resource_x_base_authority_generation = 15;
	after.resource_x_reserved = PCM_X_LOCAL_RESOURCE_X_F_HEAD_READY
		| PCM_X_LOCAL_RESOURCE_X_F_LATE_BOUND;
	UT_ASSERT(cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(
		&before, &after));

	changed = after;
	changed.resource_x_base_authority_generation
		= before.resource_x_base_authority_generation;
	UT_ASSERT(!cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(
		&before, &changed));
	changed = after;
	changed.resource_x_reserved |= PCM_X_LOCAL_RESOURCE_X_F_ASSERT_STARTED;
	UT_ASSERT(!cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(
		&before, &changed));
	changed = after;
	changed.resource_x_admission_sequence++;
	UT_ASSERT(!cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(
		&before, &changed));
	changed = after;
	changed.identity.request_id++;
	UT_ASSERT(!cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(
		&before, &changed));
	changed = after;
	changed.last_response_opcode = PGRAC_IC_MSG_PCM_X_ADMIT_ACK;
	UT_ASSERT(!cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(
		&before, &changed));

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	caller = strstr(source,
		"result = gcs_block_pcm_x_resource_x_prepare_and_assert(");
	refresh = caller != NULL
		? strstr(caller, "cluster_pcm_x_local_progress_exact(&handle, &refreshed_progress)")
		: NULL;
	classify = refresh != NULL
		? strstr(refresh,
			"cluster_gcs_pcm_x_resource_x_preassert_late_bind_refresh_exact(")
		: NULL;
	retry = classify != NULL ? strstr(classify, "continue;") : NULL;
	UT_ASSERT_NOT_NULL(caller);
	UT_ASSERT_NOT_NULL(refresh);
	UT_ASSERT_NOT_NULL(classify);
	UT_ASSERT_NOT_NULL(retry);
	free(source);
}

UT_TEST(test_pcm_x_local_s_barrier_covers_active_and_exact_late_bind_head)
{
	static const char *const barrier_contract[] = {
		"cluster_pcm_lock_resource_x_s_barrier_active_exact(&tag)",
		"goto barrier_active",
		"cluster_pcm_x_master_drive_snapshot_exact(",
		"goto barrier_active",
		"cluster_pcm_x_master_resource_x_head_admission_work_exact(",
		"goto barrier_active",
		"barrier_active:",
		"starvation_denied_pending_x_count"
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ncluster_gcs_block_pcm_x_local_s_barrier_active(",
		"\n\nstatic bool\ngcs_block_resource_x_payload_candidate(",
		barrier_contract, lengthof(barrier_contract));
	free(source);
}

UT_TEST(test_resource_x_s_barrier_closes_remote_registration_race)
{
	static const char *const ingress_contract[] = {
		"resource_x_s_barrier_before =",
		"cluster_gcs_block_pcm_x_local_s_barrier_active(req->tag)",
		"s_barrier_authority_before_valid = cluster_pcm_lock_authority_snapshot(",
		"cluster_gcs_block_dedup_lookup_or_register(",
		"resource_x_s_barrier_after =",
		"cluster_gcs_block_pcm_x_local_s_barrier_active(req->tag)",
		"s_barrier_authority_after_valid = cluster_pcm_lock_authority_snapshot(",
		"gcs_block_s_barrier_read_action_exact(",
		"s_barrier_read_image_only =",
		"cluster_gcs_block_dedup_pending_x_deny_exact("
	};
	static const char *const selected_driver_contract[] = {
		"cluster_pcm_x_master_resource_x_owner_exact(",
		"gcs_block_pcm_x_resource_x_deny_legacy_readers(",
		"gcs_block_pcm_x_resource_x_master_wait_exact("
	};
	static const char *const resource_x_scan_contract[] = {
		"cluster_pcm_x_master_resource_x_owner_exact(",
		"gcs_block_pcm_x_deny_s_dedup_rights("
	};
	char *source = read_gcs_block_source();
	const char *resource_x_scan;
	const char *resource_x_scan_end;

	assert_ordered_in_function(
		source, "\ncluster_gcs_handle_block_request_envelope(",
		"\ncluster_gcs_handle_block_reply_envelope(",
		ingress_contract, lengthof(ingress_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_master_drive_tag(",
		"\n\n/*\n * First convert-queue DATA slice",
		selected_driver_contract, lengthof(selected_driver_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_deny_legacy_readers(",
		"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_stage_queue_invalidations(",
		resource_x_scan_contract, lengthof(resource_x_scan_contract));
	resource_x_scan = strstr(source,
		"\ngcs_block_pcm_x_resource_x_deny_legacy_readers(");
	resource_x_scan_end = resource_x_scan != NULL
		? strstr(resource_x_scan,
			"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_stage_queue_invalidations(")
		: NULL;
	UT_ASSERT_NOT_NULL(resource_x_scan);
	UT_ASSERT_NOT_NULL(resource_x_scan_end);
	if (resource_x_scan != NULL && resource_x_scan_end != NULL) {
		const char *legacy_cookie = strstr(resource_x_scan,
			"cluster_pcm_lock_queue_pending_x_exact(");

		UT_ASSERT(legacy_cookie == NULL || legacy_cookie >= resource_x_scan_end);
	}
	free(source);
}

UT_TEST(test_resource_x_ticket_adapter_exposes_sequence_and_selects_one_revoke_owner)
{
	static const char *const assertion_ingress_contract[] = {
		"cluster_pcm_x_master_resource_x_assertion_ready_exact(",
		"cluster_pcm_lock_resource_x_assert_exact("
	};
	static const char *const admit_contract[] = {
		"gcs_block_pcm_x_resource_x_peer_ready_exact(",
		"cluster_pcm_x_master_resource_x_owner_bind_exact(",
		"gcs_block_pcm_x_resource_x_admission_base_exact(",
		"PCM_X_ADMIT_F_RESOURCE_X_ADAPTER",
		"ack.ref.grant_generation = admission.admission_sequence"
	};
	static const char *const admission_base_contract[] = {
		"cluster_pcm_lock_authority_snapshot(",
		"cluster_pcm_lock_resource_x_adapter_base_bind_exact(",
		"PCM_X_ADMIT_F_QUEUE_HEAD",
		"cluster_pcm_lock_resource_x_adapter_head_rebind_exact(",
		"cluster_pcm_lock_resource_x_adapter_successor_base_exact(",
		"cluster_pcm_x_master_resource_x_owner_exact(",
		"cluster_pcm_lock_authority_matches("
	};
	static const char *const requester_contract[] = {
		"cluster_pcm_x_local_apply_admit_ack_resource_x_exact("
	};
	static const char *const assertion_contract[] = {
		"cluster_pcm_x_local_resource_x_admission_exact(",
		"gcs_block_pcm_x_resource_x_assert_stage_exact("
	};
	static const char *const assertion_stage_contract[] = {
		"cluster_grd_outbound_enqueue_backend_msg(",
		"cluster_grd_outbound_enqueue_backend_msg("
	};
	static const char *const master_contract[] = {
		"cluster_pcm_x_master_resource_x_owner_exact(",
		"gcs_block_pcm_x_resource_x_master_wait_exact("
	};
	static const char *const resource_x_drive_contract[] = {
		"cluster_pcm_x_master_resource_x_owner_exact(",
		"gcs_block_pcm_x_resource_x_master_wait_exact("
	};
	static const char *const head_refresh_contract[] = {
		"cluster_pcm_x_master_resource_x_head_admission_exact(",
		"gcs_block_pcm_x_resource_x_peer_ready_exact(",
		"cluster_pcm_x_runtime_snapshot(",
		"cluster_pcm_lock_authority_snapshot(",
		"cluster_pcm_lock_resource_x_adapter_head_rebind_exact(",
		"bind_result != RESOURCE_X_APPLY_APPLIED",
		"bind_result != RESOURCE_X_APPLY_DUPLICATE",
		"cluster_pcm_x_master_resource_x_head_admission_exact(",
		"cluster_pcm_x_runtime_snapshot(",
		"cluster_pcm_lock_authority_snapshot(",
		"gcs_block_pcm_x_resource_x_peer_ready_exact(",
		"gcs_block_pcm_x_stage_frame_cap_bound("
	};
	static const char *const historical_terminal_refresh_contract[] = {
		"resource_x_assertion_equal(&snapshot.assertion, &assertion)",
		"snapshot.assertion_sequence != admission_sequence",
		"snapshot.phase == RESOURCE_X_MASTER_SETTLED",
		"snapshot.phase == RESOURCE_X_MASTER_RELEASED",
		"gcs_block_pcm_x_resource_x_head_ack_refresh_exact("
	};
	static const char *const queued_successor_post_assert_contract[] = {
		"case RESOURCE_X_MASTER_QUEUED:",
		"return PCM_X_QUEUE_OK;"
	};
	static const char *const requester_terminal_contract[] = {
		"cluster_pcm_lock_resource_x_t1_grant_exact(",
		"gcs_block_pcm_x_resource_x_prepare_target_x(",
		"cluster_bufmgr_pcm_own_activate_x_by_tag(",
		"cluster_pcm_lock_resource_x_requester_activate_exact(",
		"cluster_pcm_x_local_resource_x_grant_publish_exact("
	};
	static const char *const master_terminal_contract[] = {
		"cluster_pcm_lock_resource_x_install_settlement_exact(",
		"gcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact(",
		"cluster_pcm_lock_resource_x_settled_retire_exact("
	};
	static const char *const ingress_freshness_contract[] = {
		"cluster_resource_x_wire_decode(",
		"env->epoch != cluster_epoch_get_current()",
		"cluster_sf_peer_capability_word_sample(",
		"authenticated_capability_generation = connection_generation"
	};
	char *source = read_gcs_block_source();
	const char *prepare;
	const char *assert_begin;
	const char *assert_stage;
	const char *head_refresh;
	const char *resource_x_drive;
	const char *resource_x_selected;
	const char *resource_x_fallback;

	assert_ordered_in_function(
		source, "\ncluster_gcs_handle_pcm_x_enqueue_envelope(",
		"\n\n/*\n * Apply the exact ADMIT_ACK", admit_contract,
		lengthof(admit_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_admission_base_exact(",
		"\n\n/*\n * First convert-queue DATA slice", admission_base_contract,
		lengthof(admission_base_contract));
	assert_ordered_in_function(
		source, "\ncluster_gcs_handle_pcm_x_admit_ack_envelope(",
		"\n\n/* Publish the exact queued-leader", requester_contract,
		lengthof(requester_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_prepare_and_assert(",
		"\n\n/* A READY probe", assertion_contract,
		lengthof(assertion_contract));
	prepare = strstr(source,
		"\ngcs_block_pcm_x_resource_x_prepare_and_assert(");
	assert_begin = prepare != NULL ? strstr(prepare,
		"cluster_pcm_x_local_resource_x_assert_begin_exact(") : NULL;
	assert_stage = prepare != NULL ? strstr(prepare,
		"gcs_block_pcm_x_resource_x_assert_stage_exact(") : NULL;
	UT_ASSERT_NOT_NULL(assert_begin);
	UT_ASSERT_NOT_NULL(assert_stage);
	if (assert_begin != NULL && assert_stage != NULL)
		UT_ASSERT(assert_begin < assert_stage);
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_assert_stage_exact(",
		"\n\n/* Capture the requester-local", assertion_stage_contract,
		lengthof(assertion_stage_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_master_begin_transfer(",
		"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_master_drive_probe(",
		master_contract, lengthof(master_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_master_drive_tag(",
		"\n\n/*\n * First convert-queue DATA slice", resource_x_drive_contract,
		lengthof(resource_x_drive_contract));
	resource_x_drive = strstr(source,
		"\ngcs_block_pcm_x_master_drive_tag(");
	resource_x_selected = resource_x_drive != NULL
		? strstr(resource_x_drive,
			"result = cluster_pcm_x_master_resource_x_owner_exact(")
		: NULL;
	resource_x_fallback = resource_x_selected != NULL
		? strstr(resource_x_selected,
			"} else if (result == PCM_X_QUEUE_NOT_FOUND) {")
		: NULL;
	UT_ASSERT_NOT_NULL(resource_x_selected);
	UT_ASSERT_NOT_NULL(resource_x_fallback);
	if (resource_x_selected != NULL && resource_x_fallback != NULL) {
		const char *legacy_claim = strstr(resource_x_selected,
			"gcs_block_pcm_x_ensure_pending_x_claim(");
		const char *legacy_deny = strstr(resource_x_selected,
			"gcs_block_pcm_x_deny_legacy_readers(");
		const char *legacy_transfer = strstr(resource_x_selected,
			"gcs_block_pcm_x_master_begin_transfer(");
		const char *wait = strstr(resource_x_selected,
			"gcs_block_pcm_x_resource_x_master_wait_exact(");

		UT_ASSERT_NOT_NULL(wait);
		UT_ASSERT(wait == NULL || wait < resource_x_fallback);
		UT_ASSERT(legacy_claim == NULL || legacy_claim >= resource_x_fallback);
		UT_ASSERT(legacy_deny == NULL || legacy_deny >= resource_x_fallback);
		UT_ASSERT(legacy_transfer == NULL || legacy_transfer >= resource_x_fallback);
	}
	head_refresh = strstr(source,
		"\ngcs_block_pcm_x_resource_x_head_ack_refresh_exact(");
	UT_ASSERT_NOT_NULL(head_refresh);
	if (head_refresh != NULL)
		assert_ordered_in_function(
			source, "\ngcs_block_pcm_x_resource_x_head_ack_refresh_exact(",
			"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_resource_x_master_wait_exact(",
			head_refresh_contract, lengthof(head_refresh_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_master_wait_exact(",
		"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_master_begin_transfer(",
		historical_terminal_refresh_contract,
		lengthof(historical_terminal_refresh_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_master_wait_exact(",
		"\n\nstatic PcmXQueueResult\ngcs_block_pcm_x_master_begin_transfer(",
		queued_successor_post_assert_contract,
		lengthof(queued_successor_post_assert_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(",
		"\nstatic bool\ngcs_block_try_resource_x_frame(",
		requester_terminal_contract, lengthof(requester_terminal_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_try_resource_x_frame(",
		"\n/*\n * cluster_gcs_handle_block_request_envelope",
		master_terminal_contract, lengthof(master_terminal_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_try_resource_x_frame(",
		"\n/*\n * cluster_gcs_handle_block_request_envelope",
		ingress_freshness_contract, lengthof(ingress_freshness_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_try_resource_x_frame(",
		"\n/*\n * cluster_gcs_handle_block_request_envelope",
		assertion_ingress_contract, lengthof(assertion_ingress_contract));
	UT_ASSERT_NULL(strstr(source,
		"frame.common.sender_connection_generation != connection_generation"));
	UT_ASSERT_NULL(strstr(source,
		"env->epoch != frame.common.resource_formation"));
	free(source);
}

UT_TEST(test_r4_tx_origin_epoch_zero_is_four_node_and_session_generation_exact)
{
	static const char *const accept_contract[] = {
		"forward->base.epoch != env->epoch",
		"forward->base.epoch != cluster_epoch_get_current()",
		"forward->base.epoch == 0 && cluster_conf_node_count() != 4",
		"cluster_sf_peer_capability_family_sample(",
		"capability_generation == 0",
		"cluster_semantic_activation_enter_r4_terminal_census(",
		"cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(",
		"cluster_semantic_activation_recheck_r4_terminal_census("
	};
	static const char *const send_contract[] = {
		"cluster_epoch_get_current()",
		"context->forward.base.epoch",
		"cluster_sf_peer_capability_family_sample(",
		"capability_generation",
		"context->requester_capability_generation",
		"cluster_semantic_activation_recheck_r4_terminal_census("
	};
	static const char *const drain_contract[] = {
		"for (step_budget = 0;",
		"phase_before = context->phase",
		"gcs_block_r4_tx_origin_step(context)",
		"!context->in_use || context->phase == phase_before"
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ngcs_block_r4_tx_origin_try_accept(",
		"\nstatic bool\ngcs_block_r4_tx_origin_terminal_resolution_valid(",
		accept_contract, lengthof(accept_contract));
	UT_ASSERT_NULL(strstr(source,
		"forward->base.epoch == 0 && admission.record_generation != 0"));
	assert_ordered_in_function(
		source, "\ngcs_block_r4_tx_origin_step(",
		"\nvoid\ncluster_gcs_block_r4_tx_resolve_drain(",
		send_contract, lengthof(send_contract));
	assert_ordered_in_function(
		source, "\ncluster_gcs_block_r4_tx_resolve_drain(",
		"\nstatic void\ngcs_block_send_reply(",
		drain_contract, lengthof(drain_contract));
	free(source);
}

UT_TEST(test_r4_tx_origin_pending_work_uses_bounded_lms_poll_slice)
{
	static const char *const lms_contract[] = {
		"cluster_lms_cr_drain()",
		"cluster_gcs_block_r4_tx_resolve_wait_timeout(LMS_IDLE_TIMEOUT_MS)",
		"cluster_lms_data_plane_tick(lms_wait_timeout_ms)"
	};
	char *source = read_source_path(LMS_SOURCE_PATH);

	/* An idle worker retains its normal sleep.  A genuinely pending exact
	 * origin context gets a bounded poll slice so the 100ms idle tick cannot
	 * multiply an eight-slot census beyond the unchanged R7 deadline. */
	UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_wait_timeout_for_count(100, 0), 100);
	UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_wait_timeout_for_count(100, 1), 1);
	UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_wait_timeout_for_count(100, 4), 1);
	assert_ordered_in_function(
		source, "\nLmsMain(", "\n\n/* ============================================================",
		lms_contract, lengthof(lms_contract));
	free(source);
}

UT_TEST(test_r11_legacy_source_requester_is_generation_bound)
{
	char *source = read_gcs_block_source();
	const char *wrapper;
	const char *wrapper_end;
	const char *first_sample;
	const char *drive;
	const char *second_sample;
	const char *handoff;

	UT_ASSERT_EQ(cluster_gcs_pcm_x_source_admission_exact(
		RESOURCE_X_WRITER_SOURCE, 7, 7), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_source_admission_exact(
		RESOURCE_X_WRITER_SOURCE, 8, 7), PCM_X_QUEUE_STALE);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_source_admission_exact(
		RESOURCE_X_WRITER_CLOSED, 7, 7), PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_source_admission_exact(
		RESOURCE_X_WRITER_TARGET, 7, 7), PCM_X_QUEUE_BARRIER_CLOSED);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_source_admission_exact(
		RESOURCE_X_WRITER_SOURCE, 0, 0), PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_source_admission_exact(
		RESOURCE_X_WRITER_SOURCE, UINT64_MAX, UINT64_MAX),
		PCM_X_QUEUE_STALE);

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	wrapper = strstr(source, "\ncluster_gcs_pcm_x_acquire_writer(");
	wrapper_end = wrapper != NULL ? strstr(wrapper, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper_end);
	if (wrapper != NULL && wrapper_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(wrapper, "uint64 r4_record_generation"));
		first_sample = strstr(wrapper,
			"cluster_resource_x_writer_path_snapshot(");
		drive = strstr(wrapper, "gcs_block_pcm_x_acquire_writer_impl(");
		second_sample = first_sample != NULL
			? strstr(first_sample + 1,
				"cluster_resource_x_writer_path_snapshot(")
			: NULL;
		handoff = strstr(wrapper, "*claim_handed_off = true");
		UT_ASSERT_NOT_NULL(first_sample);
		UT_ASSERT_NOT_NULL(drive);
		UT_ASSERT_NOT_NULL(second_sample);
		UT_ASSERT_NOT_NULL(handoff);
		if (first_sample != NULL && drive != NULL)
			UT_ASSERT(first_sample < drive);
		if (drive != NULL && second_sample != NULL)
			UT_ASSERT(drive < second_sample);
		if (second_sample != NULL && handoff != NULL)
			UT_ASSERT(second_sample < handoff);
		UT_ASSERT(handoff == NULL || handoff < wrapper_end);
	}
	free(source);
}

UT_TEST(test_r11_legacy_transport_drains_closed_but_rejects_target)
{
	static const struct
	{
		const char *function;
		const char *sink;
	} producers[] = {
		{"\ncluster_gcs_pcm_x_stage_frame(",
		 "cluster_grd_outbound_enqueue_backend_msg("},
		{"\ngcs_block_pcm_x_stage_frame_cap_bound(",
		 "cluster_lms_outbound_enqueue_cap_bound("},
		{"\ngcs_block_pcm_x_stage_retire_up_to(",
		 "cluster_lms_outbound_enqueue("},
		{"\ngcs_block_pcm_x_send_retire_ack(",
		 "cluster_ic_send_envelope("}
	};
	char *source = read_gcs_block_source();
	int i;

	UT_ASSERT(cluster_gcs_pcm_x_legacy_transport_allowed(
		RESOURCE_X_WRITER_SOURCE));
	UT_ASSERT(cluster_gcs_pcm_x_legacy_transport_allowed(
		RESOURCE_X_WRITER_CLOSED));
	UT_ASSERT(!cluster_gcs_pcm_x_legacy_transport_allowed(
		RESOURCE_X_WRITER_TARGET));

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	for (i = 0; i < lengthof(producers); i++) {
		const char *begin = strstr(source, producers[i].function);
		const char *end = begin != NULL ? strstr(begin, "\n}\n") : NULL;
		const char *guard = begin != NULL
			? strstr(begin, "gcs_block_pcm_x_legacy_transport_current()")
			: NULL;
		const char *sink = begin != NULL ? strstr(begin, producers[i].sink) : NULL;

		UT_ASSERT_NOT_NULL(begin);
		UT_ASSERT_NOT_NULL(end);
		UT_ASSERT_NOT_NULL(guard);
		UT_ASSERT_NOT_NULL(sink);
		if (begin != NULL && end != NULL && guard != NULL && sink != NULL) {
			UT_ASSERT(guard < sink);
			UT_ASSERT(sink < end);
		}
	}
	free(source);
}

UT_TEST(test_r11_legacy_retry_and_pump_roots_are_target_dormant)
{
	static const struct
	{
		const char *function;
		const char *first_legacy_action;
	} roots[] = {
		{"\ncluster_gcs_block_pcm_x_image_pump_tick(",
		 "cluster_gcs_block_dedup_pcm_x_next_work("},
		{"\ncluster_gcs_pcm_x_terminal_kick(",
		 "current_epoch = cluster_epoch_get_current()"},
		{"\ncluster_gcs_pcm_x_blocker_probe_kick(",
		 "current_epoch = cluster_epoch_get_current()"}
	};
	char *source = read_gcs_block_source();
	const char *formation;
	const char *formation_end;
	const char *gate;
	const char *guard;
	const char *resource_retry;
	const char *terminal_retry;
	int i;

	UT_ASSERT(cluster_gcs_pcm_x_legacy_transport_allowed(
		RESOURCE_X_WRITER_CLOSED));
	UT_ASSERT(!cluster_gcs_pcm_x_legacy_transport_allowed(
		RESOURCE_X_WRITER_TARGET));
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;

	formation = strstr(source, "\ncluster_gcs_block_pcm_x_formation_tick(");
	formation_end = formation != NULL ? strstr(formation, "\nfail_closed:") : NULL;
	gate = formation != NULL
		? strstr(formation,
			"cluster_pcm_lock_resource_x_gate_bind_formation_exact(")
		: NULL;
	guard = gate != NULL
		? strstr(gate, "gcs_block_pcm_x_legacy_transport_current()")
		: NULL;
	resource_retry = formation != NULL
		? strstr(formation, "gcs_block_pcm_x_resource_retry_tick(bindings_before);")
		: NULL;
	terminal_retry = formation != NULL
		? strstr(formation, "gcs_block_pcm_x_terminal_retry_tick();")
		: NULL;
	UT_ASSERT_NOT_NULL(formation);
	UT_ASSERT_NOT_NULL(formation_end);
	UT_ASSERT_NOT_NULL(gate);
	UT_ASSERT_NOT_NULL(guard);
	UT_ASSERT_NOT_NULL(resource_retry);
	UT_ASSERT_NOT_NULL(terminal_retry);
	if (formation_end != NULL && gate != NULL && guard != NULL
		&& resource_retry != NULL && terminal_retry != NULL) {
		UT_ASSERT(gate < guard);
		UT_ASSERT(guard < resource_retry);
		UT_ASSERT(resource_retry < terminal_retry);
		UT_ASSERT(terminal_retry < formation_end);
	}

	for (i = 0; i < lengthof(roots); i++) {
		const char *begin = strstr(source, roots[i].function);
		const char *end = begin != NULL ? strstr(begin, "\n}\n") : NULL;
		const char *root_guard = begin != NULL
			? strstr(begin, "gcs_block_pcm_x_legacy_transport_current()")
			: NULL;
		const char *action = begin != NULL
			? strstr(begin, roots[i].first_legacy_action)
			: NULL;

		UT_ASSERT_NOT_NULL(begin);
		UT_ASSERT_NOT_NULL(end);
		UT_ASSERT_NOT_NULL(root_guard);
		UT_ASSERT_NOT_NULL(action);
		if (end != NULL && root_guard != NULL && action != NULL) {
			UT_ASSERT(root_guard < action);
			UT_ASSERT(action < end);
		}
	}
	free(source);
}


int
main(void)
{
	UT_PLAN(142);
	UT_RUN(test_gcs_block_msg_type_enum_values_no_collision);
	UT_RUN(test_gcs_block_payload_sizes_locked);
	UT_RUN(test_gcs_block_request_field_offsets);
	UT_RUN(test_gcs_block_reply_header_field_offsets);
	UT_RUN(test_gcs_block_reply_status_enum_values_through_spec_2_35);
	UT_RUN(test_gcs_block_sparse_hash_mod_n_distribution);
	UT_RUN(test_gcs_block_hash_deterministic_same_tag_same_master);
	UT_RUN(test_gcs_block_lwlock_tranche_distinct);
	UT_RUN(test_gcs_block_wait_events_distinct);
	UT_RUN(test_gcs_block_reply_total_size_is_8240);
	UT_RUN(test_pcm_x_session_auth_sample_classifies_epoch_zero_and_torn_reads);
	UT_RUN(test_pcm_x_acquire_observation_real_api_counts_exactly_once);
	UT_RUN(test_gcs_block_reply_key_is_compound);
	UT_RUN(test_gcs_block_reserved_padding_present);
	UT_RUN(test_gcs_block_data_size_equals_blcksz);
	UT_RUN(test_gcs_block_msg_type_enum_extends_without_gap);
	UT_RUN(test_gcs_block_tag_is_standard_buffer_tag_20b);
	UT_RUN(test_xheld_read_ship_decision_truth_table);
	UT_RUN(test_xheld_read_image_bypasses_only_exact_stable_s_barrier);
	UT_RUN(test_forward_replay_requires_current_exact_authority);
	UT_RUN(test_forward_payload_read_image_flag_roundtrip);
	UT_RUN(test_clean_page_xfer_eligible_flag_roundtrip_and_orthogonal);
	UT_RUN(test_request_payload_direct_land_flag_roundtrip_and_orthogonal);
	UT_RUN(test_clean_xfer_master_decision_5_branches);
	UT_RUN(test_clean_xfer_stale_break_predicate);
	UT_RUN(test_forwarded_holder_refusal_allows_one_master_cleanup_retry);
	UT_RUN(test_forward_payload_undo_authority_verdict_kind4);
	UT_RUN(test_forward_payload_undo_verdict_kinds_no_collision);
	UT_RUN(test_undo_authority_fetch_tag_owner_roundtrip);
	UT_RUN(test_undo_verdict_version_authority_distinct);
	UT_RUN(test_pcm_x_enqueue_ingress_binds_transport_epoch_and_master);
	UT_RUN(test_pcm_x_admit_confirm_ingress_binds_requester_and_master);
	UT_RUN(test_pcm_x_admit_confirm_ack_binds_exact_master_source);
	UT_RUN(test_pcm_x_cancel_requests_bind_exact_source_epoch_master_and_phase);
	UT_RUN(test_pcm_x_cancel_acks_bind_exact_master_and_canonical_payload);
	UT_RUN(test_pcm_x_wait_identity_maps_to_real_wfg_vertex);
	UT_RUN(test_pcm_x_initial_epoch_zero_is_exact_across_wire_classes);
	UT_RUN(test_pcm_x_blocker_header_ingress_binds_master_not_requester_source);
	UT_RUN(test_pcm_x_blocker_edge_ingress_binds_blocker_to_holder_source);
	UT_RUN(test_pcm_x_blocker_ack_carries_full_generation_and_binds_master_source);
	UT_RUN(test_pcm_x_formation_identical_complete_samples_may_revalidate);
	UT_RUN(test_pcm_x_formation_transient_or_inconsistent_sample_is_tick_noop);
	UT_RUN(test_pcm_x_periodic_retry_reports_pre_mutation_exit_stage);
	UT_RUN(test_resource_x_retry_domain_remains_capability_gated);
	UT_RUN(test_pcm_x_resource_terminal_result_replays_through_requester_cleanup);
	UT_RUN(test_pcm_x_install_ready_v2_self_loopback_is_admissible);
	UT_RUN(test_pcm_x_formation_samples_capability_family_atomically);
	UT_RUN(test_pcm_x_confirm_publish_then_stale_requires_exact_graph_close);
	UT_RUN(test_pcm_x_drain_poll_binds_exact_master_and_generation);
	UT_RUN(test_pcm_x_drain_ack_binds_participant_and_canonical_payload);
	UT_RUN(test_pcm_x_retire_request_binds_master_session_and_target);
	UT_RUN(test_pcm_x_retire_ack_binds_responder_and_master_authority);
	UT_RUN(test_pcm_x_revoke_ingress_binds_master_and_exact_transfer_key);
	UT_RUN(test_pcm_x_image_ready_ingress_binds_holder_image_to_master);
	UT_RUN(test_pcm_x_prepare_grant_ingress_binds_master_to_requester);
	UT_RUN(test_pcm_x_install_ready_ingress_is_canonical_requester_ack);
	UT_RUN(test_pcm_x_commit_x_ingress_is_canonical_master_phase);
	UT_RUN(test_pcm_x_final_ack_ingress_binds_monotonic_committed_floor);
	UT_RUN(test_pcm_x_final_commit_ack_ingress_is_canonical_master_phase);
	UT_RUN(test_pcm_x_final_confirm_ingress_is_canonical_requester_phase);
	UT_RUN(test_pcm_x_master_drive_selects_exact_authority_and_next_holder);
	UT_RUN(test_pcm_x_master_drive_wiring_binds_grd_barrier_to_exact_ticket);
	UT_RUN(test_pcm_x_cancel_cleanup_classifies_exact_wfg_and_post_clear_failure);
	UT_RUN(test_pcm_x_terminal_retry_reclaims_cancel_cleanup_after_owner_death);
	UT_RUN(test_pcm_x_terminal_driver_selects_one_frozen_role_not_membership);
	UT_RUN(test_pcm_x_invalidate_ack_matches_only_exact_unacked_holder);
	UT_RUN(test_pcm_x_invalidate_busy_routes_to_exact_ticket_backoff);
	UT_RUN(test_pcm_x_direct_invalidate_refusal_diagnosis_is_deterministic);
	UT_RUN(test_pcm_x_local_pending_s_denial_match_is_attempt_exact);
	UT_RUN(test_pcm_x_grant_pending_invalidate_wakes_local_s_before_busy);
	UT_RUN(test_pcm_x_grant_pending_orphan_observation_is_identity_exact);
	UT_RUN(test_pcm_x_final_ack_builds_exact_grd_handoff_token);
	UT_RUN(test_pcm_x_final_ack_fail_closed_names_exact_handoff_stage);
	UT_RUN(test_pcm_x_holder_image_evidence_never_uses_generation_as_presence);
	UT_RUN(test_pcm_x_pending_x_marker_is_only_a_pre_handoff_gate);
	UT_RUN(test_pcm_x_ready_publication_follows_exact_retained_commit);
	UT_RUN(test_pcm_x_ready_materializes_exact_n_s_or_x_source_without_wire_change);
	UT_RUN(test_pcm_x_s_source_hard_failure_observation_is_reason_exact);
	UT_RUN(test_pcm_x_self_and_remote_drain_share_full_image_release_wrapper);
	UT_RUN(test_pcm_x_ready_admission_marks_before_send_and_rolls_back_refusal);
	UT_RUN(test_pcm_x_lms_owner_death_and_restart_audit_fail_closed);
	UT_RUN(test_resource_x_scan_more_wakes_the_current_lms_latch);
	UT_RUN(test_pcm_x_lms_reload_acknowledges_finish_flush_injection);
	UT_RUN(test_pcm_x_destructive_finish_fault_times_out_in_sql_before_harness);
	UT_RUN(test_pcm_x_image_fetch_intercepts_canonical_id_before_generic_dedup);
	UT_RUN(test_pcm_x_requester_fetch_revalidates_queue_and_reservation_before_install);
	UT_RUN(test_pcm_x_self_source_handoff_is_no_copy_and_drain_preserves_x);
	UT_RUN(test_pcm_x_retire_wake_identity_is_wait_generation_exact);
	UT_RUN(test_pcm_x_requester_driver_owns_fifo_and_transfer_lifecycles);
	UT_RUN(test_pcm_x_requester_retry_policy_is_operation_exact);
	UT_RUN(test_pcm_x_drive_anomaly_streak_gates_fail_closed);
	UT_RUN(test_pcm_x_transient_churn_recovers_without_runtime_fuse);
	UT_RUN(test_pcm_x_remote_reservation_preflight_binds_identity_base);
	UT_RUN(test_pcm_x_requester_wait_backoff_saturates);
	UT_RUN(test_pcm_x_requester_cleanup_never_guesses_after_irreversible_boundary);
	UT_RUN(test_pcm_x_retire_commit_wakes_exact_waiters_before_ack_or_resolve);
	UT_RUN(test_pcm_x_tagless_retire_uses_explicit_data_plane_handoff);
	UT_RUN(test_pcm_x_role_refresh_accepts_only_same_member_promotion);
	UT_RUN(test_legacy_byte_proof_sites_republish_kept_pi_mirror);
	UT_RUN(test_revoke_handler_silent_refusal_arms_all_note);
	UT_RUN(test_local_master_read_image_retries_holder_busy_with_fresh_identity);
	UT_RUN(test_local_master_read_image_stops_retrying_displaced_holder_exactly);
	UT_RUN(test_local_master_read_image_refusal_evidence_is_attempt_exact);
	UT_RUN(test_remote_downgrade_prepares_exact_image_before_notify_and_reply);
	UT_RUN(test_preprepared_image_accepts_exact_zero_lsn_and_rejects_mismatch);
	UT_RUN(test_local_master_x_transfer_revalidates_exact_authority_and_retries_stale);
	UT_RUN(test_pending_x_apply_race_maps_to_retryable_block_denial);
	UT_RUN(test_gcs_apply_state_drift_restarts_with_fresh_request_identity);
	UT_RUN(test_master_direct_copy_busy_uses_only_fresh_identity_retry_boundary);
	UT_RUN(test_master_not_holder_producers_log_one_coherent_authority_snapshot);
	UT_RUN(test_pi_durable_note_drain_stages_before_consuming_on_data_plane);
	UT_RUN(test_pi_durable_note_receive_is_observable_before_apply);
	UT_RUN(test_pcm_x_source_floor_v2_is_connection_bound_until_lms_drain);
	UT_RUN(test_resource_x_target_executor_orders_t1_t2_t3_before_writable_return);
	UT_RUN(test_resource_x_writer_completion_does_not_own_acquisition_retirement);
	UT_RUN(test_resource_x_common_x_to_n_finish_does_not_retire_acquisition);
	UT_RUN(test_resource_x_self_x_to_x_commit_does_not_retire_acquisition);
	UT_RUN(test_resource_x_epoch_hook_freezes_sweeps_and_thaws_before_existing_wake);
	UT_RUN(test_resource_x_reused_type_ingress_precedes_every_legacy_path);
	UT_RUN(test_resource_x_kind9_ingress_is_target_native_and_no_fallback);
	UT_RUN(test_resource_x_native_target_driver_uses_round_and_no_ticket_family);
	UT_RUN(test_resource_x_type15_exact_join_is_the_only_new_r9_entry);
	UT_RUN(test_resource_x_direct_n_uses_exact_durable_storage_proof);
	UT_RUN(test_resource_x_native_target_accepts_only_exact_clean_n_before_bootstrap);
	UT_RUN(test_resource_x_passive_pi_waits_for_exact_remote_image_before_x);
	UT_RUN(test_resource_x_settlement_removes_exact_wfg_before_locator_completion);
	UT_RUN(test_resource_x_native_settlement_has_no_legacy_locator_cleanup);
	UT_RUN(test_resource_x_source_settlement_uses_exact_typed_debt_and_ack);
	UT_RUN(test_resource_x_type17_x_source_fences_before_retained_release);
	UT_RUN(test_resource_x_type17_x_source_separates_wire_and_local_fence_formations);
	UT_RUN(test_resource_x_type17_target_x_after_l3_uses_no_legacy_shell);
	UT_RUN(test_resource_x_type17_target_drains_before_semantic_retention);
	UT_RUN(test_resource_x_type17_selected_s_source_reuses_fenced_pair_core);
	UT_RUN(test_resource_x_type17_remote_s_holder_uses_exact_buffer_evidence);
	UT_RUN(test_resource_x_preassert_late_bind_race_refreshes_exactly);
	UT_RUN(test_pcm_x_local_s_barrier_covers_active_and_exact_late_bind_head);
	UT_RUN(test_resource_x_s_barrier_closes_remote_registration_race);
	UT_RUN(test_resource_x_ticket_adapter_exposes_sequence_and_selects_one_revoke_owner);
	UT_RUN(test_r4_tx_origin_epoch_zero_is_four_node_and_session_generation_exact);
	UT_RUN(test_r4_tx_origin_pending_work_uses_bounded_lms_poll_slice);
	UT_RUN(test_r11_legacy_source_requester_is_generation_bound);
	UT_RUN(test_r11_legacy_transport_drains_closed_but_rejects_target);
	UT_RUN(test_r11_legacy_retry_and_pump_roots_are_target_dormant);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
