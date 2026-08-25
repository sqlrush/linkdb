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


/* Static source checks in this binary do not enter backend runtime paths. */
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
		RESOURCE_X_PROTOCOL_NODE_LIMIT));

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
		"produce-no-resident-authority",
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
UT_TEST(test_resource_x_target_executor_orders_t1_t2_t3_before_writable_return)
{
	static const char *const executor_contract[]
		= { "cluster_pcm_lock_resource_x_requester_join_frames_exact(",
			"join.requester_target_generation != join.assertion_sequence",
			"ref.acquisition_generation = join.requester_target_generation",
			"cluster_bufmgr_pcm_own_snapshot_by_tag(",
			"cluster_pcm_lock_resource_x_executor_probe_exact(",
			"cluster_pcm_lock_resource_x_executor_enter(",
			"cluster_pcm_lock_resource_x_t1_grant_exact(",
			"gcs_block_pcm_x_resource_x_prepare_target_x(",
			"cluster_bufmgr_pcm_own_activate_x_by_tag(",
			"cluster_pcm_lock_resource_x_requester_apply_exact(",
			"cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(",
			"cluster_pcm_lock_resource_x_requester_activate_exact(",
			"cluster_pcm_lock_resource_x_executor_leave(" };
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(",
		"\nstatic ResourceXApplyResult\n"
		"gcs_block_pcm_x_resource_x_master_durable_try(",
		executor_contract, lengthof(executor_contract));
	free(source);
}

UT_TEST(test_resource_x_writer_completion_does_not_own_acquisition_retirement)
{
	char *source = read_gcs_block_source();

	UT_ASSERT_NOT_NULL(source);
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
	char *source = read_gcs_block_source();

	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT(strstr(source, "gcs_block_pcm_x_finish_revoke_retain(") == NULL);
	free(source);
}

UT_TEST(test_resource_x_self_x_to_x_commit_does_not_retire_acquisition)
{
	char *source = read_gcs_block_source();

	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT(strstr(source, "cluster_gcs_pcm_x_finish_self_image_x(") == NULL);
	free(source);
}

UT_TEST(test_resource_x_epoch_hook_freezes_sweeps_and_thaws_before_existing_wake)
{
	static const char *const actor_contract[]
		= { "claimed_epoch = pg_atomic_compare_exchange_u64(",
			"resource_x_reconfig_actor_active",
			"cluster_pcm_lock_resource_x_gate_snapshot(",
			"cluster_resource_x_reconfig_freeze_pending_exact(",
			"token.new_formation == 0",
			"gcs_block_resource_x_fail_closed_current()",
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
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ngcs_block_resource_x_reconfig_epoch(",
		"\n\n/* ============================================================\n * PGRAC: spec-2.34 D4",
		actor_contract, lengthof(actor_contract));
	assert_ordered_in_function(
		source, "\ncluster_gcs_block_on_epoch_advance_exact(",
		"\n\n/* ============================================================\n * PGRAC MODIFICATIONS by SqlRush — spec-5.13 D5",
		hook_contract, lengthof(hook_contract));
	free(source);
}

UT_TEST(test_resource_x_r11_cutover_tick_is_native_bounded_and_lmon_owned)
{
	static const char *const cutover_contract[] = {
		"cluster_semantic_activation_r11_cutover_snapshot(",
		"cluster_pcm_lock_resource_x_gate_snapshot(",
		"cluster_resource_x_reconfig_cutover_begin_native_exact(",
		"cluster_resource_x_reconfig_sweep(",
		"cluster_resource_x_reconfig_cutover_bind_native_successor_exact(",
		"cluster_resource_x_reconfig_sweep(",
		"cluster_resource_x_reconfig_zero_proof_exact(",
		"cluster_pcm_lock_resource_x_clean_completion_prove_exact(",
		"cluster_pcm_lock_resource_x_cutover_proofs_exact(",
		"cluster_resource_x_reconfig_thaw_exact(",
		"cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact("
	};
	char *source = read_gcs_block_source();
	char *lmon = read_source_path("../../../src/backend/cluster/cluster_lmon.c");
	const char *first_semantic;
	const char *cutover_start;
	const char *cutover_end;
	const char *forbidden_bind;
	const char *forbidden_r4_arithmetic;
	const char *forbidden_external_successor;
	const char *first_recovery;
	const char *first_cutover;
	const char *first_reschedule;
	const char *second_semantic;
	const char *second_recovery;
	const char *second_cutover;
	const char *second_reschedule;

	assert_ordered_in_function(
		source, "\ncluster_gcs_block_resource_x_cutover_tick(",
		"\n\n/* ============================================================",
		cutover_contract, lengthof(cutover_contract));
	cutover_start = strstr(source,
		"\ncluster_gcs_block_resource_x_cutover_tick(");
	cutover_end = cutover_start != NULL
		? strstr(cutover_start,
			"\n\n/* ============================================================") : NULL;
	forbidden_bind = cutover_start != NULL
		? strstr(cutover_start,
			"cluster_pcm_lock_resource_x_gate_bind_formation_exact(") : NULL;
	forbidden_external_successor = cutover_start != NULL
		? strstr(cutover_start,
			"cluster_resource_x_reconfig_bind_new_formation_exact(") : NULL;
	forbidden_r4_arithmetic = cutover_start != NULL
		? strstr(cutover_start,
			"cutover.resource_x_old_formation + 1") : NULL;
	UT_ASSERT_NOT_NULL(cutover_start);
	UT_ASSERT_NOT_NULL(cutover_end);
	UT_ASSERT(forbidden_bind == NULL || forbidden_bind >= cutover_end);
	UT_ASSERT(forbidden_external_successor == NULL
			  || forbidden_external_successor >= cutover_end);
	UT_ASSERT(forbidden_r4_arithmetic == NULL
			  || forbidden_r4_arithmetic >= cutover_end);
	UT_ASSERT_NULL(strstr(source, "cluster_gcs_block_pcm_x_formation_tick"));
	UT_ASSERT_NULL(strstr(source, "cluster_pcm_x_runtime_snapshot"));
	UT_ASSERT_NOT_NULL(strstr(source,
		"return result == RESOURCE_X_RECONFIG_MORE\n"
		"\t\t\t\t&& batch.examined_count != 0;"));

	first_semantic = strstr(lmon, "cluster_semantic_activation_lmon_tick();");
	first_recovery = first_semantic != NULL
		? strstr(first_semantic, "cluster_grd_recovery_lmon_tick();") : NULL;
	first_cutover = first_recovery != NULL
		? strstr(first_recovery,
			"if (cluster_gcs_block_resource_x_cutover_tick())") : NULL;
	first_reschedule = first_cutover != NULL
		? strstr(first_cutover, "SetLatch(MyLatch);") : NULL;
	second_semantic = first_reschedule != NULL
		? strstr(first_reschedule + 1,
			"cluster_semantic_activation_lmon_tick();") : NULL;
	second_recovery = second_semantic != NULL
		? strstr(second_semantic, "cluster_grd_recovery_lmon_tick();") : NULL;
	second_cutover = second_recovery != NULL
		? strstr(second_recovery,
			"if (cluster_gcs_block_resource_x_cutover_tick())") : NULL;
	second_reschedule = second_cutover != NULL
		? strstr(second_cutover, "SetLatch(MyLatch);") : NULL;
	UT_ASSERT_NOT_NULL(first_semantic);
	UT_ASSERT_NOT_NULL(first_recovery);
	UT_ASSERT_NOT_NULL(first_cutover);
	UT_ASSERT_NOT_NULL(first_reschedule);
	UT_ASSERT_NOT_NULL(second_semantic);
	UT_ASSERT_NOT_NULL(second_recovery);
	UT_ASSERT_NOT_NULL(second_cutover);
	UT_ASSERT_NOT_NULL(second_reschedule);
	free(lmon);
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
			"gcs_block_resource_x_bootstrapped_assert_ingress("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_local_proof_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"gcs_block_resource_x_type17_ingress("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_blocked_to_n_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_install_settlement_exact("));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"cluster_pcm_lock_resource_x_release_x_exact("));
	}
	UT_ASSERT_NOT_NULL(strstr(source,
		"cluster_pcm_lock_resource_x_assert_bootstrapped_exact("));
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

UT_TEST(test_retired_pcm_x_ingress_drops_only_authenticated_current_target_peer)
{
	static const char *const stale_contract[] = {
		"cluster_semantic_activation_enter(",
		"admission_result != CLUSTER_SEMANTIC_ADMISSION_OK",
		"gcs_block_legacy_pcm_x_fail_closed(source_node)",
		"cluster_sf_peer_capability_word_sample(",
		"gcs_block_resource_x_target_peer_matches_exact(",
		"cluster_semantic_activation_recheck(&admission)",
		"cluster_semantic_activation_leave(&admission)",
		"gcs_block_legacy_pcm_x_fail_closed(source_node)"
	};
	char *source = read_gcs_block_source();
	const char *helper;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	helper = strstr(source, "\ngcs_block_legacy_pcm_x_stale_ingress(");
	UT_ASSERT_NOT_NULL(helper);
	if (helper != NULL) {
		assert_ordered_in_function(
			source, "\ngcs_block_legacy_pcm_x_stale_ingress(",
			"\n\n#define LEGACY_PCM_X_STALE_INFO", stale_contract,
			lengthof(stale_contract));
		UT_ASSERT_NOT_NULL(strstr(helper,
			"if (!peer_sampled || !peer_exact || !admission_current)"));
	}
	free(source);
}

UT_TEST(test_resource_x_kind9_ingress_is_target_native_and_no_fallback)
{
	static const char *const native_local_proof_contract[] = {
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"before.pcm_state != (uint8)PCM_STATE_S",
		"before.pcm_state != (uint8)PCM_STATE_X",
		"cluster_bufmgr_copy_block_for_r4_cr(",
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"memcmp(&before, &after, sizeof(before)) != 0",
		"local_proof.kind = RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION",
		"local_proof.body.local_proof.local_holder_authority_generation",
		"local_proof.body.local_proof.requester_target_generation",
		"gcs_block_pcm_x_resource_x_dependency_crc(zero_dependencies)",
		"gcs_block_resource_x_assert_stage_exact("
	};
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
			"gcs_block_resource_x_native_assert_stage_exact("));
	}
	assert_ordered_in_function(
		source, "\ngcs_block_resource_x_native_assert_stage_exact(",
		"\n/* PGRAC adaptation: requester round extraction",
		native_local_proof_contract,
		lengthof(native_local_proof_contract));
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
		"gcs_block_resource_x_native_assert_stage_exact(",
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

	/* TARGET derives its local base directly from BufferDesc.  The deleted
	 * source adapter and its local attempt/grant projections must not reappear
	 * anywhere in the retained terminal executor. */
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
		UT_ASSERT_NOT_NULL(target_snapshot);
		UT_ASSERT(strstr(terminal,
			"cluster_pcm_x_local_resource_x_attempt_exact(") == NULL);
		UT_ASSERT(strstr(terminal,
			"cluster_pcm_x_local_resource_x_grant_publish_exact(") == NULL);
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
		"cluster_pcm_lock_resource_x_executor_probe_exact(",
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
		"\nstatic ResourceXApplyResult\n"
		"gcs_block_pcm_x_resource_x_master_durable_try(", executor_contract,
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

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_master_durable_try(",
		"\nstatic ResourceXApplyResult\n"
		"gcs_block_pcm_x_resource_x_remote_s_own_result(", master_proof_contract,
		lengthof(master_proof_contract));
	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_join_terminal_try(",
		"\nstatic ResourceXApplyResult\n"
		"gcs_block_pcm_x_resource_x_master_durable_try(", requester_join_contract,
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
	free(source);
}

UT_TEST(test_resource_x_target_x_freezes_exact_committed_generation_before_replay)
{
	static const char *const generation_contract[] = {
		"expected_local_generation >= UINT64_MAX - 1",
		"expected_committed_generation = expected_local_generation + 1;",
		"cluster_pcm_x_resource_x_t2_snapshot_exact(ref, &current)",
		"current.generation != expected_committed_generation",
		"committed_generation != expected_committed_generation"
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_prepare_target_x(",
		"\n/* Consume one complete retained type-15 join",
		generation_contract, lengthof(generation_contract));
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
		"cluster_pcm_lock_resource_x_install_settlement_exact(",
		"snapshot.phase == RESOURCE_X_MASTER_SETTLED",
		"cluster_pcm_lock_resource_x_settled_retire_exact("
	};
	char *source = read_gcs_block_source();

	/* Source-removed TARGET has no legacy WFG/locator completion.  An exact
	 * settlement reaches the canonical master entry and then retires that same
	 * assertion generation; failures remain fail closed. */
	assert_ordered_in_function(
		source, "\ngcs_block_try_resource_x_frame(",
		"\n/*\n * cluster_gcs_handle_block_request_envelope",
		completion_contract, lengthof(completion_contract));
	UT_ASSERT(strstr(source,
		"gcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact(") == NULL);
	UT_ASSERT(strstr(source, "cluster_gcs_pcm_x_vertex_from_identity(") == NULL);
	free(source);
}

UT_TEST(test_resource_x_native_settlement_has_no_legacy_locator_cleanup)
{
	char *source = read_gcs_block_source();
	const char *ingress;
	const char *ingress_end;
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
		legacy_cleanup = strstr(ingress,
			"gcs_block_pcm_x_resource_x_complete_and_remove_wfg_exact(");
		resource_x_retire = strstr(ingress,
			"cluster_pcm_lock_resource_x_settled_retire_exact(");
		UT_ASSERT_NULL(legacy_cleanup);
		UT_ASSERT_NOT_NULL(resource_x_retire);
		if (resource_x_retire != NULL)
			UT_ASSERT(resource_x_retire < ingress_end);
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
		"cluster_resource_x_writer_path_snapshot(",
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(",
		"current.generation > image.body.image_envelope.source_carrier_generation",
		"carrier_superseded = true;",
		"cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(",
		"cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(",
		"cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(",
		"cluster_bufmgr_pcm_own_try_drain_held_x_revoke(",
		"cluster_bufmgr_copy_block_for_gcs(",
		"gcs_block_pcm_x_resource_x_build_source_frames(",
		"cluster_pcm_lock_resource_x_block_to_n_source_exact(",
		"semantic_retained && carrier_superseded",
		"result == RESOURCE_X_APPLY_STALE",
		"cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(",
		"pair_result == RESOURCE_X_APPLY_APPLIED",
		"cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		"cluster_pcm_lock_resource_x_holder_pair_publish_exact(",
		"gcs_block_resource_x_terminal_owner_release("
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
		const char *helper_end = strstr(
			helper, "\n\n/* Consume the exact Resource-X subdomain");
		const char *legacy_fence = strstr(
			helper, "cluster_pcm_x_local_writer_revoke_fence_");

		UT_ASSERT_NOT_NULL(helper_end);
		UT_ASSERT(legacy_fence == NULL || legacy_fence >= helper_end);
		UT_ASSERT_NOT_NULL(strstr(helper,
			"PCM-X Resource-X finish-error evidence exact"));
		UT_ASSERT_NOT_NULL(strstr(helper, "CopyErrorData();"));
		UT_ASSERT_NOT_NULL(strstr(helper, "pair_retained"));
	}
	ingress = strstr(source, "\ngcs_block_resource_x_type17_ingress(");
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
		"cluster_resource_x_writer_path_snapshot(",
		"cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(",
		"cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(",
		"cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(",
		"cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(",
		"gcs_block_resource_x_gate_session_recheck("
	};
	char *source = read_gcs_block_source();
	const char *helper;
	const char *helper_end;

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
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	if (helper != NULL && helper_end != NULL) {
		const char *legacy_fence = strstr(helper,
			"cluster_pcm_x_local_writer_revoke_fence_");

		UT_ASSERT(legacy_fence == NULL || legacy_fence >= helper_end);
	}
	free(source);
}

UT_TEST(test_resource_x_type17_target_x_after_l3_uses_no_legacy_shell)
{
	static const char *const tagless_contract[] = {
		"cluster_resource_x_writer_path_snapshot(",
		"writer_path != RESOURCE_X_WRITER_TARGET",
		"tagless_target_x = source_mode == (uint8)PCM_STATE_X;",
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
		"cluster_bufmgr_pcm_own_snapshot_by_tag(",
		"cluster_sf_peer_capability_word_sample(",
		"gcs_block_resource_x_gate_session_recheck(",
		"cluster_bufmgr_pcm_own_prepare_s_source_image(",
		"gcs_block_resource_x_gate_session_recheck(",
		"gcs_block_pcm_x_resource_x_build_source_frames(",
		"cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(",
		"cluster_bufmgr_pcm_own_finish_revoke_retain(",
		"cluster_pcm_lock_resource_x_holder_pair_publish_exact("
	};
	char *source = read_gcs_block_source();
	const char *abort;
	const char *builder;
	const char *ingress;
	const char *type17_ingress;
	const char *source_dispatch;
	const char *status_dispatch;

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_source_block_to_n(",
		"\n\n/* Install the exact retained remote carrier", source_contract,
		lengthof(source_contract));
	if (source != NULL) {
		const char *helper = strstr(
			source, "\ngcs_block_pcm_x_resource_x_source_block_to_n(");
		const char *helper_end = helper != NULL
			? strstr(helper, "\n\n/* Install the exact retained remote carrier")
			: NULL;
		const char *legacy_fence = helper != NULL
			? strstr(helper, "cluster_pcm_x_local_writer_revoke_fence_")
			: NULL;

		UT_ASSERT_NOT_NULL(helper);
		UT_ASSERT_NOT_NULL(helper_end);
		UT_ASSERT(legacy_fence == NULL || legacy_fence >= helper_end);
	}
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
	type17_ingress = strstr(source,
		"\ngcs_block_resource_x_type17_ingress(");
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(type17_ingress);
	if (ingress != NULL)
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"gcs_block_resource_x_type17_ingress("));
	source_dispatch = type17_ingress != NULL
		? strstr(type17_ingress,
			"gcs_block_pcm_x_resource_x_source_block_to_n(") : NULL;
	status_dispatch = type17_ingress != NULL
		? strstr(type17_ingress,
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
	const char *type17_ingress;
	const char *remote_call;
	const char *master_entry_call;

	assert_ordered_in_function(
		source, "\ngcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(",
		"\n\nstatic bool\ngcs_block_resource_x_target_peer_matches_exact(",
		holder_contract,
		lengthof(holder_contract));
	helper = strstr(source,
		"\ngcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(");
	helper_end = helper != NULL
		? strstr(helper,
			"\n\nstatic bool\ngcs_block_resource_x_target_peer_matches_exact(")
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
	type17_ingress = strstr(source,
		"\ngcs_block_resource_x_type17_ingress(");
	UT_ASSERT_NOT_NULL(ingress);
	UT_ASSERT_NOT_NULL(type17_ingress);
	if (ingress != NULL)
		UT_ASSERT_NOT_NULL(strstr(ingress,
			"gcs_block_resource_x_type17_ingress("));
	if (type17_ingress != NULL) {
		UT_ASSERT_NOT_NULL(strstr(type17_ingress,
			"cluster_semantic_activation_enter("));
		UT_ASSERT_NOT_NULL(strstr(type17_ingress,
			"gcs_block_resource_x_target_peer_matches_exact("));
		UT_ASSERT_NOT_NULL(strstr(type17_ingress,
			"gcs_block_resource_x_gate_session_snapshot("));
		UT_ASSERT_NOT_NULL(strstr(type17_ingress,
			"admission.record_generation"));
		UT_ASSERT_NOT_NULL(strstr(type17_ingress,
			"cluster_semantic_activation_leave("));
	}
	remote_call = type17_ingress != NULL
		? strstr(type17_ingress,
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

UT_TEST(test_pcm_x_local_s_barrier_covers_active_and_exact_late_bind_head)
{
	static const char *const barrier_contract[] = {
		"cluster_pcm_lock_resource_x_s_barrier_active(&tag)",
		"return false",
		"starvation_denied_pending_x_count"
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ncluster_gcs_block_resource_x_local_s_barrier_active(",
		"\n\nstatic bool\ngcs_block_resource_x_payload_candidate(",
		barrier_contract, lengthof(barrier_contract));
	free(source);
}

UT_TEST(test_resource_x_s_barrier_closes_remote_registration_race)
{
	static const char *const ingress_contract[] = {
		"resource_x_s_barrier_before =",
		"cluster_gcs_block_resource_x_local_s_barrier_active(req->tag)",
		"cluster_gcs_block_dedup_lookup_or_register(",
		"resource_x_s_barrier_after =",
		"cluster_gcs_block_resource_x_local_s_barrier_active(req->tag)",
		"gcs_block_s_barrier_read_action_exact(",
		"resource_x_s_barrier_before, resource_x_s_barrier_after",
		"cluster_gcs_block_dedup_pending_x_deny_exact("
	};
	char *source = read_gcs_block_source();

	assert_ordered_in_function(
		source, "\ncluster_gcs_handle_block_request_envelope(",
		"\ncluster_gcs_handle_block_reply_envelope(",
		ingress_contract, lengthof(ingress_contract));
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

int
main(void)
{
	UT_PLAN(69);
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
	UT_RUN(test_resource_x_target_executor_orders_t1_t2_t3_before_writable_return);
	UT_RUN(test_resource_x_writer_completion_does_not_own_acquisition_retirement);
	UT_RUN(test_resource_x_common_x_to_n_finish_does_not_retire_acquisition);
	UT_RUN(test_resource_x_self_x_to_x_commit_does_not_retire_acquisition);
	UT_RUN(test_resource_x_epoch_hook_freezes_sweeps_and_thaws_before_existing_wake);
	UT_RUN(test_resource_x_r11_cutover_tick_is_native_bounded_and_lmon_owned);
	UT_RUN(test_resource_x_reused_type_ingress_precedes_every_legacy_path);
	UT_RUN(test_retired_pcm_x_ingress_drops_only_authenticated_current_target_peer);
	UT_RUN(test_resource_x_kind9_ingress_is_target_native_and_no_fallback);
	UT_RUN(test_resource_x_native_target_driver_uses_round_and_no_ticket_family);
	UT_RUN(test_resource_x_type15_exact_join_is_the_only_new_r9_entry);
	UT_RUN(test_resource_x_direct_n_uses_exact_durable_storage_proof);
	UT_RUN(test_resource_x_target_x_freezes_exact_committed_generation_before_replay);
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
	UT_RUN(test_pcm_x_local_s_barrier_covers_active_and_exact_late_bind_head);
	UT_RUN(test_resource_x_s_barrier_closes_remote_registration_race);
	UT_RUN(test_r4_tx_origin_epoch_zero_is_four_node_and_session_generation_exact);
	UT_RUN(test_r4_tx_origin_pending_work_uses_bounded_lms_poll_slice);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
