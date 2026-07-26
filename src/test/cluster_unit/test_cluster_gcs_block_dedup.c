/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_dedup.c
 *	  Behavioral invariants for the spec-7.3 D5 per-worker sharded GCS
 *	  block dedup HTAB.  spec-2.34 shipped a single global dedup table +
 *	  lock;  spec-7.3 D5 shards it into per-worker private instances
 *	  (dedup_shards[worker_id]) so the LMS worker pool (worker[shard(tag)])
 *	  never contends on one lock and never shares dedup state across
 *	  workers.
 *
 *	  This binary links cluster_gcs_block_dedup.o + cluster_lms_shard.o and
 *	  provides minimal fake-shmem stubs (an N-shard fake HTAB, one array
 *	  per worker) so the real sharded lookup/install/remove/GC/counter
 *	  paths run standalone.  It is the direct proof of "dedup 跨 worker 零
 *	  共享" (spec-7.3 §7 DoD): registering on shard i must be invisible to
 *	  shard j.
 *
 *	  The cross-node behavioral ground truth (2-node retransmit + dedup
 *	  CACHED_REPLY replay at the default cluster.lms_workers=2) lives in
 *	  cluster_tap t/112_gcs_block_retransmit_2node.pl;  the multi-tag ->
 *	  multi-worker dispatch e2e + injection-forced misroute land in D9.
 *
 *	  Tests (U1-U15):
 *	    U1  per-worker isolation: install on shard 0 is invisible to shard 1
 *	    U2  dedup per-shard: MISS -> IN_FLIGHT_DUPLICATE -> CACHED_REPLY
 *	    U3  counter accessors sum across shards
 *	    U4  cross-shard GC: cleanup_on_node_dead reaches every shard
 *	    U5  bounds fail-closed: worker_id out of range -> FULL + misroute++
 *	    U6  N=1 degenerate: only shard 0 valid; worker 1 out of range
 *	    U7  per-shard cap: fill shard 0 to cap -> FULL + full_count++
 *	    U8  cross-shard TTL sweep reaches every shard
 *	    U9  backend-exit cleanup reaches every shard
 *	    U10 remove releases an IN_FLIGHT entry for re-evaluation
 *	    U11 READ_IMAGE forward marker -> FORWARDED; direct serve -> CACHED
 *	    U12 TTL threshold covers the (retries+1) x reply-timeout lifetime
 *	    U13 mark_done truth table: identity gates + idempotent stamp (RC-F)
 *	    U14 TTL posture pinned at registration: hint beats GUCs, no re-read
 *	    U15 DONE linger ages a proven entry out before the full lifetime
 *	    U16 capability-routed registration: violations denied, legacy pinned
 *	        at the protocol maximum (review F5 / calibration 2)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_dedup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_shmem.h"
#include "../../backend/cluster/cluster_gcs_block_internal.h"
#include "port/pg_crc32c.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

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

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

int
s_lock(volatile slock_t *lock pg_attribute_unused(), const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	return 0;
}


/* ============================================================
 * GUC / global stubs the module reads.
 * ============================================================ */

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_lms_workers = 2;
int NBuffers = 8;
int MaxConnections = 1; /* spec-7.2a D4 floor input (x declared nodes = 1) */
int cluster_gcs_block_dedup_max_entries = 8;
int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_reply_timeout_ms = 5000;
int MyBackendId = 1;
bool IsUnderPostmaster = true;


/* ============================================================
 * N-shard fake HTAB.  ShmemInitHash hands out one fake shard array per
 * call, in init order (shard 0, 1, ...);  hash_search / hash_seq operate
 * on the shard identified by the returned handle.
 * ============================================================ */

#define FAKE_DEDUP_CAP 8

typedef struct FakeDedupShardHtab {
	char entries[FAKE_DEDUP_CAP][sizeof(GcsBlockDedupEntry)];
	long count;
	long max_entries;
	Size keysize;
	Size entrysize;
} FakeDedupShardHtab;

static FakeDedupShardHtab fake_htab[CLUSTER_LMS_MAX_WORKERS * 4];
static int fake_htab_init_seq;
static int fake_hash_seq_init_count;
static int fake_hash_seq_term_count;
#define FAKE_LOCK_EVENT_CAP 256
static LWLock *fake_lwlock_acquire_order[FAKE_LOCK_EVENT_CAP];
static LWLockMode fake_lwlock_acquire_mode[FAKE_LOCK_EVENT_CAP];
static LWLock *fake_lwlock_release_order[FAKE_LOCK_EVENT_CAP];
static int fake_lwlock_acquire_count;
static int fake_lwlock_shared_count;
static int fake_lwlock_exclusive_count;
static int fake_lwlock_release_count;
static int fake_lwlock_held_count;
static int fake_lwlock_max_held_count;
static int fake_lwlock_release_underflow_count;
static bool fake_allow_multi_lwlock;
static int fake_hash_seq_held_count[FAKE_LOCK_EVENT_CAP];

/* ShmemInitStruct blob for the dedup ctl header + per-shard structs. */
static union {
	uint64 force_align;
	char data[16384];
} fake_dedup_struct;
static bool fake_dedup_struct_found;

static int fake_before_shmem_exit_registered;


static void
reset_fake_dedup(int n_workers, int max_entries)
{
	memset(fake_htab, 0, sizeof(fake_htab));
	fake_htab_init_seq = 0;
	fake_hash_seq_init_count = 0;
	fake_hash_seq_term_count = 0;
	memset(fake_lwlock_acquire_order, 0, sizeof(fake_lwlock_acquire_order));
	memset(fake_lwlock_acquire_mode, 0, sizeof(fake_lwlock_acquire_mode));
	memset(fake_lwlock_release_order, 0, sizeof(fake_lwlock_release_order));
	fake_lwlock_acquire_count = 0;
	fake_lwlock_shared_count = 0;
	fake_lwlock_exclusive_count = 0;
	fake_lwlock_release_count = 0;
	fake_lwlock_held_count = 0;
	fake_lwlock_max_held_count = 0;
	fake_lwlock_release_underflow_count = 0;
	fake_allow_multi_lwlock = false;
	memset(fake_hash_seq_held_count, 0, sizeof(fake_hash_seq_held_count));
	memset(&fake_dedup_struct, 0, sizeof(fake_dedup_struct));
	fake_dedup_struct_found = false;
	fake_before_shmem_exit_registered = 0;

	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_lms_workers = n_workers;
	cluster_gcs_block_dedup_max_entries = max_entries;

	/* size() then init() — same order the shmem bootstrap uses. */
	(void)cluster_gcs_block_dedup_shmem_size();
	cluster_gcs_block_dedup_shmem_init();
}


/* ============================================================
 * PG-runtime stubs.
 * ============================================================ */

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	Assert(size <= sizeof(fake_dedup_struct.data));
	*foundPtr = fake_dedup_struct_found;
	fake_dedup_struct_found = true;
	return fake_dedup_struct.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size, HASHCTL *infoP, int hash_flags)
{
	FakeDedupShardHtab *h;

	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->entrysize <= sizeof(GcsBlockDedupEntry));
	Assert(fake_htab_init_seq < CLUSTER_LMS_MAX_WORKERS * 4);
	h = &fake_htab[fake_htab_init_seq++];
	h->count = 0;
	h->max_entries = max_size;
	h->keysize = infoP->keysize;
	h->entrysize = infoP->entrysize;
	return (HTAB *)h;
}

void *
hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr)
{
	FakeDedupShardHtab *h = (FakeDedupShardHtab *)hashp;
	long i;

	Assert(h != NULL);
	Assert(h->keysize > 0);

	for (i = 0; i < h->count; i++) {
		char *entry = h->entries[i];

		if (memcmp(entry, keyPtr, h->keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE) {
				if (i + 1 < h->count)
					memmove(h->entries[i], h->entries[i + 1],
							(size_t)(h->count - i - 1) * h->entrysize);
				h->count--;
				return entry;
			}
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if (action == HASH_ENTER_NULL
		&& (h->count >= h->max_entries || h->count >= FAKE_DEDUP_CAP))
		return NULL;
	if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
		char *entry = h->entries[h->count++];

		memset(entry, 0, h->entrysize);
		memcpy(entry, keyPtr, h->keysize);
		return entry;
	}
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	if (fake_hash_seq_init_count < FAKE_LOCK_EVENT_CAP)
		fake_hash_seq_held_count[fake_hash_seq_init_count] = fake_lwlock_held_count;
	fake_hash_seq_init_count++;
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeDedupShardHtab *h = (FakeDedupShardHtab *)status->hashp;

	if (status->curBucket >= (uint32)h->count) {
		hash_seq_term(status);
		return NULL;
	}
	return h->entries[status->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	fake_hash_seq_term_count++;
}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	Assert(fake_allow_multi_lwlock || fake_lwlock_held_count == 0);
	if (fake_lwlock_acquire_count < FAKE_LOCK_EVENT_CAP) {
		fake_lwlock_acquire_order[fake_lwlock_acquire_count] = lock;
		fake_lwlock_acquire_mode[fake_lwlock_acquire_count] = mode;
	}
	fake_lwlock_acquire_count++;
	if (mode == LW_SHARED)
		fake_lwlock_shared_count++;
	else
		fake_lwlock_exclusive_count++;
	fake_lwlock_held_count++;
	if (fake_lwlock_held_count > fake_lwlock_max_held_count)
		fake_lwlock_max_held_count = fake_lwlock_held_count;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (fake_lwlock_release_count < FAKE_LOCK_EVENT_CAP)
		fake_lwlock_release_order[fake_lwlock_release_count] = lock;
	fake_lwlock_release_count++;
	if (fake_lwlock_held_count > 0)
		fake_lwlock_held_count--;
	else
		fake_lwlock_release_underflow_count++;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return (TimestampTz)1000;
}

/* elog(LOG) plumbing for the spec-7.2a D5 saturation LOG-once in the TTL
 * sweep (never fires in these tests: full_count stays below threshold). */
bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false; /* suppress: no message assembly in unit context */
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* spec-7.2a D4: pre-shmem conf sniff — a single declared node keeps the
 * auto-size floor at MaxConnections (=1), so the tests' tiny configured
 * capacities stay in force. */
int
cluster_conf_declared_node_count_early(void)
{
	return 1;
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{
	fake_before_shmem_exit_registered++;
}


/* ============================================================
 * Test helpers.
 * ============================================================ */

static GcsBlockDedupKey
make_key(uint32 origin, int32 backend, uint64 reqid, uint64 epoch)
{
	GcsBlockDedupKey k;

	memset(&k, 0, sizeof(k));
	k.origin_node_id = origin;
	k.requester_backend_id = backend;
	k.request_id = reqid;
	k.cluster_epoch = epoch;
	return k;
}

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

static void
install_granted(int worker_id, const GcsBlockDedupKey *key)
{
	GcsBlockReplyHeader hdr;
	static char block[GCS_BLOCK_DATA_SIZE];

	memset(&hdr, 0, sizeof(hdr));
	memset(block, 0x5a, sizeof(block));
	cluster_gcs_block_dedup_install_reply(worker_id, key, GCS_BLOCK_REPLY_GRANTED, &hdr, block);
}

static GcsBlockPcmXImageBinding
make_pcm_x_binding(BufferTag tag, uint32 requester_node, uint32 requester_procno,
				   uint64 requester_request_id, uint64 epoch, uint64 image_id,
				   uint64 master_session)
{
	GcsBlockPcmXImageBinding binding;

	memset(&binding, 0, sizeof(binding));
	binding.identity.ref.identity.tag = tag;
	binding.identity.ref.identity.node_id = (int32)requester_node;
	binding.identity.ref.identity.procno = requester_procno;
	binding.identity.ref.identity.xid = (TransactionId)17;
	binding.identity.ref.identity.cluster_epoch = epoch;
	binding.identity.ref.identity.request_id = requester_request_id;
	binding.identity.ref.identity.wait_seq = 19;
	binding.identity.ref.identity.base_own_generation = 23;
	binding.identity.ref.handle.ticket_id = 29;
	binding.identity.ref.handle.queue_generation = 31;
	binding.identity.ref.grant_generation = 37;
	binding.identity.image.image_id = image_id;
	binding.identity.image.source_own_generation = 41;
	binding.identity.image.page_scn = 43;
	binding.identity.image.page_lsn = 47;
	binding.identity.image.source_node = 0;
	binding.identity.image.page_checksum = 53;
	binding.master_session = master_session;
	return binding;
}

static GcsBlockReplyHeader
make_pcm_x_reply_header(const GcsBlockDedupKey *key, const GcsBlockPcmXImageBinding *binding)
{
	GcsBlockReplyHeader hdr;

	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = key->request_id;
	hdr.page_lsn = binding->identity.image.page_lsn;
	hdr.epoch = key->cluster_epoch;
	hdr.checksum = binding->identity.image.page_checksum;
	hdr.sender_node = (int32)binding->identity.image.source_node;
	hdr.requester_backend_id = key->requester_backend_id;
	hdr.transition_id = (uint8)PCM_TRANS_N_TO_S;
	hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	return hdr;
}

static uint32
pcm_x_test_block_checksum(const char *page)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static void
prepare_pcm_x_page(char *page, GcsBlockPcmXImageBinding *binding, GcsBlockReplyHeader *hdr)
{
	PageHeaderData page_header;

	memcpy(&page_header, page, sizeof(page_header));
	PageXLogRecPtrSet(page_header.pd_lsn, (XLogRecPtr)binding->identity.image.page_lsn);
	page_header.pd_block_scn = (SCN)binding->identity.image.page_scn;
	memcpy(page, &page_header, sizeof(page_header));
	binding->identity.image.page_checksum = pcm_x_test_block_checksum(page);
	hdr->page_lsn = binding->identity.image.page_lsn;
	hdr->checksum = binding->identity.image.page_checksum;
}

static GcsBlockPcmXImageResult
stage_pcm_x_ready(int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
				  const GcsBlockPcmXImageBinding *binding, const GcsBlockReplyHeader *hdr,
				  const char *page)
{
	GcsBlockPcmXImageBinding reserved = *binding;
	GcsBlockPcmXImageResult result;

	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	result = cluster_gcs_block_dedup_pcm_x_reserve(worker_id, key, tag, &reserved);
	if (result != GCS_BLOCK_PCM_X_IMAGE_RESERVED && result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE)
		return result;
	result = cluster_gcs_block_dedup_pcm_x_materialize(worker_id, key, tag, binding, UINT64_C(41),
													   (uint8)PCM_STATE_X, hdr, page);
	if (result != GCS_BLOCK_PCM_X_IMAGE_STORED && result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE)
		return result;
	return cluster_gcs_block_dedup_pcm_x_publish_ready_exact(worker_id, key, tag, binding);
}


/* ============================================================
 * U1 — per-worker isolation: an install on shard 0 is invisible to
 * shard 1.  Same key registered on both shards stays independent;
 * completing shard 0's entry does NOT complete shard 1's.
 * ============================================================ */
UT_TEST(u1_per_worker_isolation)
{
	GcsBlockDedupKey key = make_key(0, 1, 42, 7);
	BufferTag tag = make_tag(10);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* Register the same key on shard 0 and shard 1 — separate tables, so
	 * both see a fresh MISS. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	/* Complete shard 0's entry only. */
	install_granted(0, &key);

	/* Shard 0 now serves a cached reply; shard 1 is untouched (still
	 * in-flight) — proves zero cross-worker sharing. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
}


/* ============================================================
 * U2 — dedup lifecycle within one shard.
 * ============================================================ */
UT_TEST(u2_dedup_lifecycle_per_shard)
{
	GcsBlockDedupKey key = make_key(0, 1, 43, 7);
	BufferTag tag = make_tag(11);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* retransmit before reply installed */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	install_granted(0, &key);
	/* retransmit after reply installed → cached replay */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U3 — counter accessors sum across shards.
 * ============================================================ */
UT_TEST(u3_counters_sum_across_shards)
{
	GcsBlockDedupKey ka = make_key(0, 1, 50, 7);
	GcsBlockDedupKey kb = make_key(0, 2, 51, 7);
	BufferTag ta = make_tag(20);
	BufferTag tb = make_tag(21);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	/* miss on shard 0 + miss on shard 1 = 2 (aggregate view). */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_miss_count(), 2);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_full_count(), 0);
}


/* ============================================================
 * U4 — cross-shard GC: node-dead cleanup reaches every shard.
 * ============================================================ */
UT_TEST(u4_cleanup_on_node_dead_all_shards)
{
	GcsBlockDedupKey ka = make_key(3, 1, 60, 7); /* origin node 3 */
	GcsBlockDedupKey kb = make_key(3, 2, 61, 7); /* origin node 3 */
	BufferTag ta = make_tag(30);
	BufferTag tb = make_tag(31);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	cluster_gcs_block_dedup_cleanup_on_node_dead(3);

	/* both shards drained */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U5 — bounds fail-closed: worker_id out of range → FULL + misroute++.
 * ============================================================ */
UT_TEST(u5_out_of_range_worker_fail_closed)
{
	GcsBlockDedupKey key = make_key(0, 1, 70, 7);
	BufferTag tag = make_tag(40);
	GcsBlockDedupEntry cached;
	uint64 before;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	before = cluster_gcs_block_dedup_get_misroute_failclosed_count();

	/* worker_id >= live shard count → fail-closed, no crash, no store. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(99, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_FULL);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(-1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_FULL);

	UT_ASSERT_EQ((int)(cluster_gcs_block_dedup_get_misroute_failclosed_count() - before), 2);
	/* nothing was stored */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U6 — N=1 degenerate: only shard 0 valid; worker 1 out of range.
 * ============================================================ */
UT_TEST(u6_n1_only_shard0)
{
	GcsBlockDedupKey key = make_key(0, 1, 80, 7);
	BufferTag tag = make_tag(50);
	GcsBlockDedupEntry cached;
	uint64 before;

	reset_fake_dedup(1, FAKE_DEDUP_CAP);
	before = cluster_gcs_block_dedup_get_misroute_failclosed_count();

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* worker 1 does not exist when lms_workers=1 → fail-closed. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_FULL);
	UT_ASSERT_EQ((int)(cluster_gcs_block_dedup_get_misroute_failclosed_count() - before), 1);
}


/* ============================================================
 * U7 — per-shard cap: fill shard 0 to cap → FULL + full_count++.
 * ============================================================ */
UT_TEST(u7_per_shard_cap_full)
{
	BufferTag tag = make_tag(60);
	GcsBlockDedupEntry cached;
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	for (i = 0; i < FAKE_DEDUP_CAP; i++) {
		GcsBlockDedupKey k = make_key(0, 1, (uint64)(100 + i), 7);

		UT_ASSERT_EQ(
			(int)cluster_gcs_block_dedup_lookup_or_register(0, &k, tag, 1, 0, false, &cached),
			(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
	{
		GcsBlockDedupKey overflow = make_key(0, 1, 999, 7);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &overflow, tag, 1, 0, false,
																	 &cached),
					 (int)GCS_BLOCK_DEDUP_FULL);
	}
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_full_count(), 1);
	/* shard 1 is unaffected by shard 0 being full. */
	{
		GcsBlockDedupKey k = make_key(0, 2, 500, 7);
		BufferTag t1 = make_tag(61);

		UT_ASSERT_EQ(
			(int)cluster_gcs_block_dedup_lookup_or_register(1, &k, t1, 1, 0, false, &cached),
			(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
}


/* ============================================================
 * U8 — cross-shard TTL sweep reaches every shard.
 * ============================================================ */
UT_TEST(u8_ttl_sweep_all_shards)
{
	GcsBlockDedupKey ka = make_key(0, 1, 200, 7);
	GcsBlockDedupKey kb = make_key(0, 2, 201, 7);
	BufferTag ta = make_tag(70);
	BufferTag tb = make_tag(71);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	/* now far past the expiry threshold → both shards swept. */
	cluster_gcs_block_dedup_sweep_expired((TimestampTz)100000000000LL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U9 — backend-exit cleanup reaches every shard.
 * ============================================================ */
UT_TEST(u9_backend_exit_cleanup_all_shards)
{
	GcsBlockDedupKey ka = make_key(0, 9, 300, 7); /* local origin, backend 9 */
	GcsBlockDedupKey kb = make_key(0, 9, 301, 7); /* local origin, backend 9 */
	BufferTag ta = make_tag(80);
	BufferTag tb = make_tag(81);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	cluster_gcs_block_dedup_cleanup_on_backend_exit(0, 9);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U10 — remove releases an IN_FLIGHT entry for re-evaluation.
 *
 *	The retryable-deny paths (DENIED_PENDING_X / direct-land deny) call
 *	cluster_gcs_block_dedup_remove before replying, because the
 *	requester's convergence retry reuses the same key: a leftover
 *	in-flight entry would swallow it as IN_FLIGHT_DUPLICATE until the
 *	TTL sweep (the S3 RC-B reply-timeout burn).  remove must turn the
 *	next same-key lookup back into MISS_REGISTERED.
 * ============================================================ */
UT_TEST(u10_remove_reopens_in_flight_entry)
{
	GcsBlockDedupKey k = make_key(0, 3, 400, 7);
	BufferTag t = make_tag(90);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* leftover in-flight entry swallows the same-key retry ... */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	/* ... and remove re-opens it for a fresh master evaluation. */
	cluster_gcs_block_dedup_remove(0, &k);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}

/* S3-P0-21: the HC101 REARM path may publish DENIED_PENDING_X only after an
 * identity-exact removal.  Wrong tag/transition and completed entries remain
 * untouched; only the original uncompleted registration can be removed. */
UT_TEST(u10b_exact_remove_refuses_identity_or_phase_drift)
{
	GcsBlockDedupKey k = make_key(0, 3, 401, 7);
	BufferTag t = make_tag(91);
	BufferTag wrong_t = make_tag(92);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT(!cluster_gcs_block_dedup_remove_inflight_exact(
		0, &k, &wrong_t, 1));
	UT_ASSERT(!cluster_gcs_block_dedup_remove_inflight_exact(
		0, &k, &t, 2));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT(cluster_gcs_block_dedup_remove_inflight_exact(
		0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* A completed same-identity reply is immutable, never reopened as a
	 * fresh route attempt. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(0, &k);
	UT_ASSERT(!cluster_gcs_block_dedup_remove_inflight_exact(
		0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U11 — a READ_IMAGE forward MARKER classifies FORWARDED, a master-direct
 * READ_IMAGE cached serve stays CACHED.
 *
 *	The xheld-read FORWARD install stamps forwarding_master_node and
 *	carries no page; treating it as CACHED_REPLY resends a payload-less
 *	header whose never-computed checksum (0) matches the 31-hash of the
 *	all-zero page — a verifying zero-page install at the requester
 *	(PageIsNew false-empty read, 8.A).  The master-DIRECT xheld serve
 *	installs READ_IMAGE with NO_FORWARDING_MASTER + the real page and
 *	must keep resending as a genuine cached reply.
 * ============================================================ */
UT_TEST(u11_read_image_marker_classifies_forwarded)
{
	GcsBlockDedupKey km = make_key(0, 4, 500, 7);
	GcsBlockDedupKey kd = make_key(0, 4, 501, 7);
	BufferTag t = make_tag(95);
	GcsBlockDedupEntry cached;
	GcsBlockReplyHeader hdr;
	static char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* forward MARKER: forwarding_master_node stamped, no payload. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &km, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = 500;
	hdr.sender_node = 2; /* holder id rides here (HC113) */
	hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 0);
	cluster_gcs_block_dedup_install_reply(0, &km, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &hdr,
										  NULL);
	memset(&cached, 0, sizeof(cached));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &km, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ((int)cached.reply_header.sender_node, 2);

	/* master-DIRECT cached serve: NO_FORWARDING_MASTER + real page. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kd, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = 501;
	hdr.sender_node = 0;
	hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	memset(page, 0x3c, sizeof(page));
	cluster_gcs_block_dedup_install_reply(0, &kd, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &hdr,
										  page);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kd, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U12 — the TTL threshold covers the LEGAL request lifetime.
 *
 *	Every attempt may wait a full cluster.gcs_reply_timeout_ms before its
 *	retry fires, so an in-flight entry is live for up to
 *	(max_retries + 1) x reply_timeout + total backoff.  The pre-fix
 *	threshold (2 x backoff only) swept a still-live request's entry
 *	mid-flight (S3 rig: 25.5s TTL vs 57.75s lifetime) — the late
 *	retransmit then re-registered as MISS and re-executed a request whose
 *	earlier attempt may already have granted.
 * ============================================================ */
UT_TEST(u12_ttl_covers_reply_timeout_lifetime)
{
	GcsBlockDedupKey k = make_key(0, 5, 600, 7);
	BufferTag t = make_tag(96);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	/* S3 rig shape: 8 retries x 50ms backoff base, 5s reply timeout ->
	 * lifetime = 12.75s backoff + 45s reply windows = 57.75s. */
	cluster_gcs_block_retransmit_initial_backoff_ms = 50;
	cluster_gcs_block_retransmit_max_retries = 8;
	cluster_gcs_reply_timeout_ms = 5000;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 57750, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	t0 = GetCurrentTimestamp();

	/* 40s in: inside the legal lifetime — must SURVIVE the sweep. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)40 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	/* far past 2 x lifetime — must be swept. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)300 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	cluster_gcs_block_retransmit_initial_backoff_ms = 100;
	cluster_gcs_block_retransmit_max_retries = 4;
	cluster_gcs_reply_timeout_ms = 5000;
}


/* ============================================================
 * U13 — mark_done truth table (GCS-race round-2 RC-F).
 *
 *	DONE is advisory: every identity or state doubt drops the proof
 *	(done_mismatch_count++) and leaves the TTL backstop in charge.  Only
 *	a full boot-bound key + tag + transition_id match on a COMPLETED entry
 *	stamps done_at_ts; a duplicate DONE is idempotent-true.
 * ============================================================ */
UT_TEST(u13_mark_done_truth_table)
{
	GcsBlockDedupKey k = make_key(0, 6, 700, 7);
	BufferTag t = make_tag(97);
	BufferTag wrong_tag = make_tag(98);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* miss: DONE for a key that was never registered. */
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 1);

	/* in-flight: entry exists but no reply installed (not COMPLETED). */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 2);

	/* completed, but identity mismatches refuse the stamp. */
	install_granted(0, &k);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &wrong_tag, 1));
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &t, 2));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 4);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 0);

	/* exact identity on a COMPLETED entry stamps the proof. */
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 1);

	/* duplicate DONE (retransmit reorder) is idempotent-true. */
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 2);

	/* the entry still serves its cached reply inside the done-linger
	 * quarantine — DONE never removes it outright. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U14 — the TTL posture is PINNED at registration (RC-F).
 *
 *	A nonzero wire hint (the requester's own legal lifetime) beats the
 *	master's GUC-derived threshold; and once registered, a master-local
 *	GUC change never re-shortens a live entry's window (GC paths do not
 *	re-read GUCs).
 * ============================================================ */
UT_TEST(u14_pinned_ttl_wire_hint_and_no_guc_reread)
{
	GcsBlockDedupKey kh = make_key(0, 7, 800, 7);
	GcsBlockDedupKey kg = make_key(0, 7, 801, 7);
	BufferTag t = make_tag(99);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* master GUCs describe an ENORMOUS lifetime (~1590s threshold). */
	cluster_gcs_block_retransmit_initial_backoff_ms = 1000;
	cluster_gcs_block_retransmit_max_retries = 8;
	cluster_gcs_reply_timeout_ms = 60000;

	/* hint 1000ms pins 2s: the sweep obeys the requester's budget, not
	 * the master's huge threshold. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kh, t, 1, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	t0 = GetCurrentTimestamp();
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)1 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)3 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* legacy peer (no capability): the PROTOCOL-MAXIMUM lifetime is pinned
	 * at registration (review F5 / calibration 2); shrinking the GUCs
	 * afterwards must NOT shorten the live window. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kg, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	t0 = GetCurrentTimestamp();
	cluster_gcs_block_retransmit_initial_backoff_ms = 50;
	cluster_gcs_block_retransmit_max_retries = 0;
	cluster_gcs_reply_timeout_ms = 100; /* new threshold would be 200ms */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)10 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	cluster_gcs_block_retransmit_initial_backoff_ms = 100;
	cluster_gcs_block_retransmit_max_retries = 4;
	cluster_gcs_reply_timeout_ms = 5000;
}


/* ============================================================
 * U15 — the DONE proof shortens a completed entry to its pinned
 * done-linger quarantine (RC-F).
 *
 *	The wire hint pins lifetime 53s; default GUCs pin linger 10s.  A completed-but-not-done
 *	sibling survives the same sweeps that age out the DONE-proven entry —
 *	the proof, not the timestamps, is what releases the slot early.
 * ============================================================ */
UT_TEST(u15_done_linger_beats_full_lifetime)
{
	GcsBlockDedupKey kd = make_key(0, 8, 900, 7);
	GcsBlockDedupKey ks = make_key(0, 8, 901, 7);
	BufferTag t = make_tag(100);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &kd, t, 1, 26500, true, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &ks, t, 1, 26500, true, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(0, &kd);
	install_granted(0, &ks);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &kd, &t, 1));
	t0 = GetCurrentTimestamp();

	/* inside the 10s linger both survive. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)5 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	/* past the linger, inside the 53s lifetime: only the DONE-proven
	 * entry ages out. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)11 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &ks, t, 1, 26500, true, &cached),
		(int)GCS_BLOCK_DEDUP_CACHED_REPLY);

	/* past the pinned lifetime the sibling goes too. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)60 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U16 — capability-routed registration (review F5 / calibration 2).
 *
 *	A GCS_DONE_V1-capable peer MUST carry a sane lifetime hint: zero or
 *	over-protocol-maximum is counted and DENIED without claiming a slot.
 *	A legacy peer's window is unknowable, so it pins the protocol-maximum
 *	lifetime (counted) -- capacity pressure surfaces as FULL, never as an
 *	early reclaim.
 * ============================================================ */
UT_TEST(u16_capability_routing_truth_table)
{
	GcsBlockDedupKey kv = make_key(0, 9, 950, 7);
	GcsBlockDedupKey ko = make_key(0, 9, 951, 7);
	GcsBlockDedupKey kl = make_key(0, 9, 952, 7);
	BufferTag t = make_tag(101);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* capable + hint 0: protocol violation -> denied, counted, no slot. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kv, t, 1, 0, true, &cached),
				 (int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_hint_violation_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* capable + over-maximum hint (would pin the slot for days): same. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(
			0, &ko, t, 1, (uint32)(GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS + 1), true, &cached),
		(int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_hint_violation_count(), 2);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* legacy peer: registered, counted, pinned at the protocol maximum. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kl, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_legacy_pin_count(), 1);

	/* far past any GUC posture (600s) but inside the 2x protocol maximum
	 * (3630s): the legacy entry must SURVIVE the sweep... */
	t0 = GetCurrentTimestamp();
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)600 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	/* ...and past it, age out. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)3700 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* PCM-X still overlays the shared metadata cell.  The later FORWARD
 * boot/capability/relation identity expansion grows that cell by 40B. */
UT_TEST(u17_pcm_x_binding_layout_tracks_forward_identity_growth)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockPcmXImageBinding), 144);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, entry_kind), 54);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, pcm_x_master_session), 56);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, reply_header), 64);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, payload_meta), 120);
	UT_ASSERT_EQ((int)sizeof(((GcsBlockDedupEntry *)0)->payload_meta), 168);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, block_data), 288);
	UT_ASSERT_EQ((int)sizeof(GcsBlockDedupEntry), 8520);
}


/* An exact duplicate reuses immutable bytes.  A generic install with the
 * same key must not overwrite a staged PCM-X image. */
UT_TEST(u18_pcm_x_stage_duplicate_and_generic_overwrite_refused)
{
	BufferTag tag = make_tag(110);
	uint64 requester_id = gcs_reqid_requester(1, 2, 77);
	uint64 image_id;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding conflicting_binding;
	GcsBlockReplyHeader hdr;
	GcsBlockReplyHeader conflicting_hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	char overwrite[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 7, &image_id));
	key = make_key(1, 3, image_id, 0);
	binding = make_pcm_x_binding(tag, 1, 5, requester_id, 0, image_id, 101);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x6a, sizeof(page));
	memset(overwrite, 0x7b, sizeof(overwrite));
	prepare_pcm_x_page(page, &binding, &hdr);
	conflicting_binding = binding;
	conflicting_binding.identity.image.page_scn++;
	conflicting_binding.identity.image.page_lsn++;
	conflicting_hdr = make_pcm_x_reply_header(&key, &conflicting_binding);
	prepare_pcm_x_page(overwrite, &conflicting_binding, &conflicting_hdr);

	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(0, &key, &tag, &conflicting_binding,
																UINT64_C(41), (uint8)PCM_STATE_X,
																&conflicting_hdr, overwrite),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);

	/* The generic completion path cannot mutate a dedicated entry. */
	cluster_gcs_block_dedup_install_reply(0, &key, GCS_BLOCK_REPLY_GRANTED, &hdr, overwrite);
	memset(&cached, 0, sizeof(cached));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT(memcmp(cached.block_data, page, sizeof(page)) == 0);
	UT_ASSERT(
		memcmp(&cached.payload_meta.pcm_x_identity, &binding.identity, sizeof(binding.identity))
		== 0);
	UT_ASSERT_EQ((uint64)cached.pcm_x_master_session, (uint64)binding.master_session);
	UT_ASSERT_EQ((int)cached.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_replay_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 2);
}


/* Generic lookup, remove and DONE must all reject a dedicated image. */
UT_TEST(u19_pcm_x_entry_isolated_from_generic_lifecycle)
{
	BufferTag tag = make_tag(111);
	uint64 image_id;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 8, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 78), 13, image_id, 102);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x5c, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S, 1,
																 true, &cached),
				 (int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	cluster_gcs_block_dedup_remove(0, &key);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &key, &tag, PCM_TRANS_N_TO_S));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 3);
}


/* Wall-clock, backend exit and node death are not application ACKs.  Only
 * the exact terminal binding may retire the image. */
UT_TEST(u20_pcm_x_entry_survives_generic_gc_and_retires_exactly)
{
	BufferTag tag = make_tag(112);
	uint64 image_id;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding wrong;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 9, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 79), 13, image_id, 103);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x4d, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	cluster_gcs_block_dedup_sweep_expired((TimestampTz)INT64_MAX);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 3);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);

	wrong = binding;
	wrong.identity.ref.grant_generation++;
	memset(&cached, 0x5a, sizeof(cached));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &wrong, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cached.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_GENERIC);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &wrong, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_release_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 2);
}


/* A full shared shard refuses staging without reclaiming live generic or
 * dedicated entries. */
UT_TEST(u21_pcm_x_stage_full_is_fail_closed)
{
	BufferTag tag = make_tag(113);
	GcsBlockDedupEntry cached;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockReplyHeader hdr;
	uint64 image_id;
	char page[GCS_BLOCK_DATA_SIZE];
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	for (i = 0; i < FAKE_DEDUP_CAP; i++) {
		GcsBlockDedupKey ordinary = make_key(0, 1, (uint64)(2000 + i), 13);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
						 0, &ordinary, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
					 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 10, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 80), 13, image_id, 104);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x3e, sizeof(page));
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_FULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_full_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 1);
}


/* Capacity reservation is durable protocol evidence: generic time and
 * process-lifecycle cleanup cannot retire it, and only its exact binding can. */
UT_TEST(u22_pcm_x_reserved_entry_waits_for_exact_release)
{
	BufferTag tag = make_tag(114);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockDedupEntry cached;
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 11, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 81), 13, image_id, 105);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &reserved, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);

	cluster_gcs_block_dedup_sweep_expired((TimestampTz)INT64_MAX);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 3);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &reserved, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &reserved, -1),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_replay_count(), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_release_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 2);
}


/* READY publication validates every byte carrier before changing RESERVED:
 * local source, reply binding, page CRC, page LSN, page SCN and master session. */
UT_TEST(u23_pcm_x_materialize_validation_is_fail_closed_and_byte_stable)
{
	BufferTag tag = make_tag(115);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageBinding bad_binding;
	GcsBlockReplyHeader hdr;
	GcsBlockReplyHeader bad_hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	char bad_page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 12, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 82), 13, image_id, 106);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x2d, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);

	bad_binding = binding;
	bad_binding.identity.image.source_node = 1;
	bad_hdr = hdr;
	bad_hdr.sender_node = 1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_hdr = hdr;
	bad_hdr.sender_node = 1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	memcpy(bad_page, page, sizeof(bad_page));
	bad_page[sizeof(bad_page) - 1] ^= 0x1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, bad_page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.identity.image.page_lsn++;
	bad_hdr = hdr;
	bad_hdr.page_lsn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.identity.image.page_scn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.identity.image.page_checksum++;
	bad_hdr = hdr;
	bad_hdr.checksum++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.master_session++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 7);
}


/* A canonical image id is intercepted before generic registration, including
 * on a cold miss; there is no legacy fallback entry to complete later. */
UT_TEST(u24_pcm_x_namespace_cannot_register_as_generic)
{
	BufferTag tag = make_tag(116);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockDedupEntry cached;
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 13, &image_id));
	key = make_key(1, 3, image_id, 13);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S, 1,
																 true, &cached),
				 (int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 83), 13, image_id, 107);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 1);
}


/* The LMS image pump must never let a READY resend monopolize a shard while
 * an unmaterialized reservation is waiting.  Once a READY leg is admitted to
 * the outbound ring it disappears from work scans until an exact type-49
 * retransmit positively re-arms it. */
UT_TEST(u25_pcm_x_work_prefers_reserved_and_marks_ready_staged)
{
	BufferTag ready_tag = make_tag(117);
	BufferTag reserved_tag = make_tag(118);
	GcsBlockDedupKey ready_key;
	GcsBlockDedupKey reserved_key;
	GcsBlockPcmXImageBinding ready_binding;
	GcsBlockPcmXImageBinding reserved_binding;
	GcsBlockPcmXImageBinding wrong_floor;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	uint64 ready_image_id;
	uint64 reserved_image_id;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 14, &ready_image_id));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 15, &reserved_image_id));
	ready_key = make_key(1, 3, ready_image_id, 13);
	reserved_key = make_key(1, 4, reserved_image_id, 13);
	ready_binding = make_pcm_x_binding(ready_tag, 1, 5, gcs_reqid_requester(1, 2, 84), 13,
									   ready_image_id, 108);
	hdr = make_pcm_x_reply_header(&ready_key, &ready_binding);
	memset(page, 0x1d, sizeof(page));
	prepare_pcm_x_page(page, &ready_binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &ready_key, &ready_tag, &ready_binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	reserved_binding = make_pcm_x_binding(reserved_tag, 1, 6, gcs_reqid_requester(1, 3, 85), 13,
										  reserved_image_id, 109);
	reserved_binding.identity.image.page_scn = 0;
	reserved_binding.identity.image.page_lsn = 0;
	reserved_binding.identity.image.page_checksum = 0;
	reserved_binding.required_page_scn = UINT64_C(72057594037950810);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &reserved_key, &reserved_tag,
															&reserved_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	wrong_floor = reserved_binding;
	wrong_floor.required_page_scn++;
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_pcm_x_reserve(0, &reserved_key, &reserved_tag, &wrong_floor),
		(int)GCS_BLOCK_PCM_X_IMAGE_STALE);

	memset(&work, 0, sizeof(work));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(memcmp(&work.key, &reserved_key, sizeof(reserved_key)), 0);
	UT_ASSERT_EQ(memcmp(&work.binding, &reserved_binding, sizeof(reserved_binding)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &reserved_key, &reserved_tag,
																  &reserved_binding, -1),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);

	memset(&work, 0, sizeof(work));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.key, &ready_key, sizeof(ready_key)), 0);
	UT_ASSERT_EQ(memcmp(&work.binding, &ready_binding, sizeof(ready_binding)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &ready_key, &ready_tag,
																	  &ready_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STAGED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &ready_key, &ready_tag,
																	  &ready_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
}


/* A retransmitted type 49 is the only positive evidence that an admitted
 * type 50 needs replay.  Rearm accepts the reservation identity (page fields
 * still zero) but validates the complete ticket/generation/session tuple;
 * an almost-equal ticket cannot make a READY image sendable again. */
UT_TEST(u26_pcm_x_ready_rearm_is_exact)
{
	BufferTag tag = make_tag(119);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageBinding wrong;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	uint64 image_id;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 16, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 86), 13, image_id, 110);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x0d, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STAGED);

	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	wrong = reserved;
	wrong.identity.ref.grant_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_rearm_exact(0, &key, &tag, &wrong),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_rearm_exact(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REARMED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.binding, &binding, sizeof(binding)), 0);
}


/* One pinned holder must not make the hash table's first RESERVED entry
 * monopolize every LMS tick.  Work selection is process-local round robin;
 * the second scan must advance to the other exact reservation. */
UT_TEST(u27_pcm_x_reserved_work_scan_rotates)
{
	BufferTag tag_a = make_tag(120);
	BufferTag tag_b = make_tag(121);
	GcsBlockDedupKey key_a;
	GcsBlockDedupKey key_b;
	GcsBlockPcmXImageBinding binding_a;
	GcsBlockPcmXImageBinding binding_b;
	GcsBlockPcmXImageWork first;
	GcsBlockPcmXImageWork second;
	uint64 image_a;
	uint64 image_b;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 17, &image_a));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 18, &image_b));
	key_a = make_key(1, 3, image_a, 13);
	key_b = make_key(1, 4, image_b, 13);
	binding_a = make_pcm_x_binding(tag_a, 1, 5, gcs_reqid_requester(1, 2, 87), 13, image_a, 111);
	binding_b = make_pcm_x_binding(tag_b, 1, 6, gcs_reqid_requester(1, 3, 88), 13, image_b, 112);
	binding_a.identity.image.page_scn = binding_a.identity.image.page_lsn = 0;
	binding_a.identity.image.page_checksum = 0;
	binding_b.identity.image.page_scn = binding_b.identity.image.page_lsn = 0;
	binding_b.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_a, &tag_a, &binding_a),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_b, &tag_b, &binding_b),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &first),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &second),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT(memcmp(&first.key, &second.key, sizeof(first.key)) != 0);
}


/* The LMS tick must not rescan a potentially multi-thousand-entry generic
 * dedup shard forever when no PCM-X byte work exists.  One initial scan may
 * establish the empty hint; an exact reservation must wake it again.  The
 * fake scan also mirrors dynahash's natural auto-term, so one full scan has
 * exactly one registration and one termination. */
UT_TEST(u28_pcm_x_idle_hint_avoids_empty_rescan_and_reserve_rearms)
{
	BufferTag tag = make_tag(122);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageWork work;
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 1);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 1);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 1);

	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 19, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 89), 13, image_id, 113);
	binding.identity.image.page_scn = binding.identity.image.page_lsn = 0;
	binding.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 2);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 2);
}


/* Materialized bytes are retained evidence, not a sendable READY image.  The
 * ownership X->N commit must happen between materialize and the explicit
 * publication call.  A live owner must receive a distinct commit-only work
 * token so conditional BufferContent contention can retry without recopying,
 * aborting the A-record, or synthesizing type 50. */
UT_TEST(u29_pcm_x_materialized_bytes_require_explicit_ready_publication)
{
	BufferTag tag = make_tag(123);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 20, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 90), 13, image_id, 114);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x4e, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ((int)work.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED);
	UT_ASSERT_EQ(memcmp(&work.binding, &binding, sizeof(binding)), 0);
	UT_ASSERT_EQ(work.reservation_token, UINT64_C(41));
	UT_ASSERT_EQ((int)work.source_pcm_state, (int)PCM_STATE_X);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_publish_ready_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);
}


/* A replacement LMS must never infer progress from retained holder evidence.
 * Startup audit is read-only: it detects any dedicated entry and leaves the
 * exact bytes/reservation available for the recovery layer. */
UT_TEST(u30_pcm_x_owner_restart_audit_detects_and_retains_evidence)
{
	BufferTag tag = make_tag(124);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(!cluster_gcs_block_dedup_pcm_x_restart_audit(0));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 21, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 91), 13, image_id, 115);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x5f, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_restart_audit(0));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
}


/* A pinned reservation must not starve an already READY image forever.  The
 * two work classes share one LMS tick budget, so when both remain runnable a
 * READY leg must be selected no later than the tick after RESERVED. */
UT_TEST(u31_pcm_x_work_classes_alternate_when_both_remain_runnable)
{
	BufferTag ready_tag = make_tag(125);
	BufferTag reserved_tag = make_tag(126);
	GcsBlockDedupKey ready_key;
	GcsBlockDedupKey reserved_key;
	GcsBlockPcmXImageBinding ready_binding;
	GcsBlockPcmXImageBinding reserved_binding;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 ready_image_id;
	uint64 reserved_image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 22, &ready_image_id));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 23, &reserved_image_id));
	ready_key = make_key(1, 3, ready_image_id, 13);
	reserved_key = make_key(1, 4, reserved_image_id, 13);
	ready_binding = make_pcm_x_binding(ready_tag, 1, 5, gcs_reqid_requester(1, 2, 92), 13,
									   ready_image_id, 116);
	hdr = make_pcm_x_reply_header(&ready_key, &ready_binding);
	memset(page, 0x60, sizeof(page));
	prepare_pcm_x_page(page, &ready_binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &ready_key, &ready_tag, &ready_binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	reserved_binding = make_pcm_x_binding(reserved_tag, 1, 6, gcs_reqid_requester(1, 3, 93), 13,
										  reserved_image_id, 117);
	reserved_binding.identity.image.page_scn = 0;
	reserved_binding.identity.image.page_lsn = 0;
	reserved_binding.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &reserved_key, &reserved_tag,
															&reserved_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(memcmp(&work.key, &reserved_key, sizeof(reserved_key)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.key, &ready_key, sizeof(ready_key)), 0);
}


/* Outbound admission is a small transaction: the exact READY entry is marked
 * first, and a ring refusal must roll that marker back so the image remains
 * runnable.  Repeating the rollback is an idempotent no-op, never corruption. */
UT_TEST(u32_pcm_x_staged_marker_rolls_back_exactly)
{
	BufferTag tag = make_tag(127);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding wrong;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 24, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 94), 13, image_id, 118);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x61, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STAGED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);

	wrong = binding;
	wrong.identity.ref.grant_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(0, &key, &tag, &wrong),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REARMED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
}


/* Shape-B: a legacy S request may already be registered or forwarded before
 * the PCM-X queue publishes its pending-X claim.  The queue arm must revoke
 * every still-live grant/forward right under the dedup lock, cache an exact
 * retry denial for loss/replay, and fence a late producer from restoring a
 * GRANTED reply.  DONE stops periodic replay; a new request identity remains
 * a normal MISS after the X round. */
UT_TEST(u33_pending_x_arm_terminates_inflight_legacy_s_exactly)
{
	BufferTag tag = make_tag(130);
	BufferTag other_tag = make_tag(131);
	GcsBlockDedupKey inflight = make_key(1, 3, 3001, 17);
	GcsBlockDedupKey forwarded = make_key(2, 4, 3002, 17);
	GcsBlockDedupKey unrelated = make_key(3, 5, 3003, 17);
	GcsBlockDedupKey writer = make_key(1, 6, 3004, 17);
	GcsBlockDedupKey fresh = make_key(1, 3, 4001, 17);
	GcsBlockReplyHeader forward_hdr;
	GcsBlockReplyHeader late_granted;
	GcsBlockDedupEntry cached;
	GcsBlockDedupEntry denied;
	GcsBlockDedupKey denied_keys[2];
	char page[GCS_BLOCK_DATA_SIZE];
	int result;
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&forward_hdr, 0, sizeof(forward_hdr));
	memset(&late_granted, 0, sizeof(late_granted));
	memset(page, 0x6d, sizeof(page));

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &inflight, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &forwarded, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	forward_hdr.request_id = forwarded.request_id;
	forward_hdr.epoch = forwarded.cluster_epoch;
	forward_hdr.sender_node = 3;
	forward_hdr.requester_backend_id = forwarded.requester_backend_id;
	forward_hdr.transition_id = (uint8)PCM_TRANS_N_TO_S;
	forward_hdr.status = (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&forward_hdr, cluster_node_id);
	cluster_gcs_block_dedup_install_reply(0, &forwarded, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
										  &forward_hdr, NULL);

	/* Different tag and same-tag writer identities are not legacy-S victims. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &unrelated, other_tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &writer, tag, PCM_TRANS_N_TO_X,
																 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	for (i = 0; i < 2; i++) {
		memset(&denied, 0, sizeof(denied));
		result = cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied);
		UT_ASSERT_EQ(result, GCS_BLOCK_PENDING_X_DENY_NEW);
		UT_ASSERT_EQ((int)denied.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_GENERIC);
		UT_ASSERT_EQ((int)denied.transition_id, (int)PCM_TRANS_N_TO_S);
		UT_ASSERT_EQ((int)denied.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
		UT_ASSERT_EQ((int)denied.reply_header.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
		UT_ASSERT_EQ(denied.reply_header.request_id, denied.key.request_id);
		UT_ASSERT_EQ(denied.reply_header.epoch, denied.key.cluster_epoch);
		UT_ASSERT_EQ(denied.reply_header.requester_backend_id, denied.key.requester_backend_id);
		UT_ASSERT_EQ((int)denied.reply_header.transition_id, (int)PCM_TRANS_N_TO_S);
		UT_ASSERT_EQ(denied.reply_header.sender_node, cluster_node_id);
		UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&denied.reply_header),
					 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
		denied_keys[i] = denied.key;
	}
	UT_ASSERT(memcmp(&denied_keys[0], &denied_keys[1], sizeof(denied_keys[0])) != 0);

	/* Initial denial loss is recovered by periodic replay of the same exact
	 * identity, without minting another request or reviving FORWARD. */
	memset(&denied, 0, sizeof(denied));
	UT_ASSERT_EQ(cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied),
				 GCS_BLOCK_PENDING_X_DENY_REPLAY);
	UT_ASSERT(memcmp(&denied.key, &denied_keys[0], sizeof(denied.key)) == 0
			  || memcmp(&denied.key, &denied_keys[1], sizeof(denied.key)) == 0);

	/* An asynchronous old GRANTED producer has lost its right once the deny
	 * is cached; installing it must be a zero-mutation no-op. */
	late_granted.request_id = denied.key.request_id;
	late_granted.epoch = denied.key.cluster_epoch;
	late_granted.sender_node = cluster_node_id;
	late_granted.requester_backend_id = denied.key.requester_backend_id;
	late_granted.transition_id = (uint8)PCM_TRANS_N_TO_S;
	late_granted.status = (uint8)GCS_BLOCK_REPLY_GRANTED;
	cluster_gcs_block_dedup_install_reply(0, &denied.key, GCS_BLOCK_REPLY_GRANTED, &late_granted,
										  page);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &denied.key, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cached.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)cached.reply_header.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);

	/* Duplicate DONE is idempotent and removes both old identities from the
	 * denial replay set; unrelated entries remain in their original states. */
	for (i = 0; i < 2; i++) {
		UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &denied_keys[i], &tag, PCM_TRANS_N_TO_S));
		UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &denied_keys[i], &tag, PCM_TRANS_N_TO_S));
	}
	UT_ASSERT_EQ(cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied),
				 GCS_BLOCK_PENDING_X_DENY_NOT_FOUND);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &unrelated, other_tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &writer, tag, PCM_TRANS_N_TO_X,
																 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);

	/* Once the queue round is over, a reader with a fresh identity is admitted
	 * normally; the two denied identities cannot poison the new request. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &fresh, tag, PCM_TRANS_N_TO_S,
																 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}

/* A reader arriving after the queue claim is registered only to make its
 * denial reliable: exact arbitration replaces even a pre-existing cached
 * grant before the handler can take a normal dedup shortcut.  The original
 * direct-land property remains attached to the cached denial for replay. */
UT_TEST(u34_pending_x_new_reader_exact_deny_precedes_cached_shortcut)
{
	BufferTag tag = make_tag(132);
	GcsBlockDedupKey key = make_key(2, 7, 5001, 19);
	GcsBlockDedupKey absent = make_key(2, 7, 5002, 19);
	GcsBlockReplyHeader granted;
	GcsBlockDedupEntry cached;
	GcsBlockDedupEntry denied;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&granted, 0, sizeof(granted));
	memset(page, 0x71, sizeof(page));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S,
																 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT(cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_S, GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));

	granted.request_id = key.request_id;
	granted.epoch = key.cluster_epoch;
	granted.sender_node = cluster_node_id;
	granted.requester_backend_id = key.requester_backend_id;
	granted.transition_id = (uint8)PCM_TRANS_N_TO_S;
	granted.status = (uint8)GCS_BLOCK_REPLY_GRANTED;
	GcsBlockReplyHeaderSetForwardingMasterNode(&granted, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	cluster_gcs_block_dedup_install_reply(0, &key, GCS_BLOCK_REPLY_GRANTED, &granted, page);

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_pending_x_deny_exact(0, &key, &tag, PCM_TRANS_N_TO_S, &denied),
		(int)GCS_BLOCK_PENDING_X_DENY_NEW);
	UT_ASSERT_EQ((int)denied.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)denied.request_flags,
				 (int)(GCS_BLOCK_DEDUP_REQUEST_F_PINNED | GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_pending_x_deny_exact(0, &key, &tag, PCM_TRANS_N_TO_S, &denied),
		(int)GCS_BLOCK_PENDING_X_DENY_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S,
																 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cached.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);

	/* Missing and mismatched identities are zero-mutation invalid results. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(0, &absent, &tag,
																   PCM_TRANS_N_TO_S, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_INVALID);
	UT_ASSERT(!cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_X, GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S,
																 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cached.request_flags,
				 (int)(GCS_BLOCK_DEDUP_REQUEST_F_PINNED | GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
}


/* A contended tag A may remain at the commit-only boundary, but its exact
 * retry must not prevent the same DATA worker from advancing independent tag
 * B.  This exercises the production HTAB scan/cursor, not a scheduler model. */
UT_TEST(u35_pcm_x_commit_pending_rotates_to_independent_reserved_tag)
{
	BufferTag tag_a = make_tag(130);
	BufferTag tag_b = make_tag(131);
	GcsBlockDedupKey key_a;
	GcsBlockDedupKey key_b;
	GcsBlockPcmXImageBinding binding_a;
	GcsBlockPcmXImageBinding reserved_a;
	GcsBlockPcmXImageBinding reserved_b;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr_a;
	char page_a[GCS_BLOCK_DATA_SIZE];
	uint64 image_a;
	uint64 image_b;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 30, &image_a));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 31, &image_b));
	key_a = make_key(1, 3, image_a, 13);
	key_b = make_key(1, 4, image_b, 13);
	binding_a = make_pcm_x_binding(tag_a, 1, 5, gcs_reqid_requester(1, 2, 100), 13, image_a, 120);
	hdr_a = make_pcm_x_reply_header(&key_a, &binding_a);
	memset(page_a, 0x68, sizeof(page_a));
	prepare_pcm_x_page(page_a, &binding_a, &hdr_a);
	reserved_a = binding_a;
	reserved_a.identity.image.page_scn = 0;
	reserved_a.identity.image.page_lsn = 0;
	reserved_a.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_a, &tag_a, &reserved_a),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(0, &key_a, &tag_a, &binding_a,
																UINT64_C(51), (uint8)PCM_STATE_X,
																&hdr_a, page_a),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	reserved_b = make_pcm_x_binding(tag_b, 1, 6, gcs_reqid_requester(1, 3, 101), 13, image_b, 121);
	reserved_b.identity.image.page_scn = 0;
	reserved_b.identity.image.page_lsn = 0;
	reserved_b.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_b, &tag_b, &reserved_b),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ(memcmp(&work.key, &key_a, sizeof(key_a)), 0);
	UT_ASSERT_EQ(work.reservation_token, UINT64_C(51));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(memcmp(&work.key, &key_b, sizeof(key_b)), 0);
}


/* A local terminal DRAIN is not an ACK boundary by itself.  Exact byte and
 * descriptor cleanup publishes a replayable tombstone; duplicate DRAIN stays
 * provably complete until the matching RETIRE watermark removes that proof. */
UT_TEST(u36_pcm_x_drain_cleanup_is_replayable_until_exact_retire)
{
	BufferTag tag = make_tag(132);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;
	uint64 removed;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 32, &image_id));
	/* The initial live cluster episode is epoch zero; RETIRE must preserve the
	 * same exact-match semantics there as after the first reconfiguration. */
	key = make_key(1, 3, image_id, 0);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 102), 0, image_id, 122);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x79, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_drain_status_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_drain_status_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_rearm_exact(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);

	cluster_gcs_block_dedup_sweep_expired((TimestampTz)INT64_MAX);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 3);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	removed = UINT64_MAX;
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(12, 2, 122, 29, &removed));
	UT_ASSERT_EQ(removed, 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	removed = UINT64_MAX;
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(0, 1, 122, 29, &removed));
	UT_ASSERT_EQ(removed, 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	removed = UINT64_MAX;
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(0, 2, 121, 29, &removed));
	UT_ASSERT_EQ(removed, 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	removed = UINT64_MAX;
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(0, 2, 122, 28, &removed));
	UT_ASSERT_EQ(removed, 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	removed = UINT64_MAX;
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(0, 2, 122, 29, &removed));
	UT_ASSERT_EQ(removed, 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_release_count(), 1);
}


/* A FlushBuffer ERROR occurs after materialization.  Its catch handler must
 * validate, but never delete or rewrite, the immutable A-record and revoke
 * token needed by recovery. */
UT_TEST(u37_pcm_x_finish_error_preserves_exact_materialized_evidence)
{
	BufferTag tag = make_tag(133);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 33, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 103), 13, image_id, 123);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x6a, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(61), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(
					 0, &key, &tag, &binding, UINT64_C(62), (uint8)PCM_STATE_X),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(
					 0, &key, &tag, &binding, UINT64_C(61), (uint8)PCM_STATE_X),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ(memcmp(&work.binding, &binding, sizeof(binding)), 0);
	UT_ASSERT_EQ(work.reservation_token, UINT64_C(61));
	UT_ASSERT_EQ((int)work.source_pcm_state, (int)PCM_STATE_X);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, -1),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}

UT_TEST(u38_forward_marker_prepare_claim_finish_is_exact_and_serial)
{
	BufferTag tag = make_tag(134);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x123456789abcdef0), 21);
	GcsBlockDedupEntry cached;
	GcsBlockDedupEntry claimed;
	GcsBlockDedupEntry denied;
	PcmAuthoritySnapshot authority;
	PcmAuthoritySnapshot drifted;
	GcsBlockForwardPayload forward;
	GcsBlockForwardMarker marker_copy;
	GcsBlockReplyHeader late;
	const GcsBlockForwardMarker *marker;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = 2;
	authority.transition_count = 41;
	authority.state = PCM_STATE_S;
	authority.x_holder_node = -1;
	authority.s_holders_bitmap = UINT32_C(1) << 2;
	authority.pending_x_requester_node = -1;
	authority.authority_generation = 43;
	memset(&forward, 0, sizeof(forward));
	forward.request_id = key.request_id;
	forward.epoch = key.cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = (int32)key.origin_node_id;
	forward.requester_backend_id = key.requester_backend_id;
	forward.master_node = 0;
	forward.transition_id = (uint8)PCM_TRANS_N_TO_S;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward, (SCN)47);

	drifted = authority;
	drifted.authority_generation = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, 2, 0,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &drifted, &forward, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INVALID);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, 2, 0,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &authority, &forward, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	marker = &cached.payload_meta.forward_marker;
	UT_ASSERT_EQ(memcmp(&marker->authority, &authority, sizeof(authority)), 0);
	UT_ASSERT_EQ(memcmp(&marker->forward, &forward, sizeof(forward)), 0);
	UT_ASSERT_EQ((uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&marker->forward),
				 UINT64_C(47));
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&cached),
				 (int)GCS_BLOCK_FORWARD_MARK_PREPARED);
	UT_ASSERT_EQ((int)cached.completed_at_ts, 0);
	marker_copy = *marker;
	marker = &marker_copy;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &claimed),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);

	/* Generic remove and late reply production cannot erase/overlay PREPARED. */
	cluster_gcs_block_dedup_remove(0, &key);
	memset(&late, 0, sizeof(late));
	late.request_id = key.request_id;
	late.epoch = key.cluster_epoch;
	late.sender_node = 0;
	late.requester_backend_id = key.requester_backend_id;
	late.transition_id = (uint8)PCM_TRANS_N_TO_S;
	late.status = (uint8)GCS_BLOCK_REPLY_GRANTED;
	GcsBlockReplyHeaderSetForwardingMasterNode(&late, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	cluster_gcs_block_dedup_install_reply(
		0, &key, GCS_BLOCK_REPLY_GRANTED, &late, NULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, marker,
					 GCS_BLOCK_FORWARD_MARK_PREPARED, &claimed),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&claimed),
				 (int)GCS_BLOCK_FORWARD_MARK_SEND_ARMED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &claimed),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, marker,
					 GCS_BLOCK_FORWARD_MARK_PREPARED, &claimed),
				 (int)GCS_BLOCK_FORWARD_MARK_BUSY);
	UT_ASSERT(!cluster_gcs_block_dedup_forward_abort_prepared_exact(
		0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, marker));

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, marker, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&cached),
				 (int)GCS_BLOCK_FORWARD_MARK_FORWARDED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ(memcmp(&cached.payload_meta.forward_marker.forward, &forward, sizeof(forward)), 0);

	/* A queue-kind X claim may race this already-admitted legacy-S forward.
	 * The marker is valid authority, not corruption and not a denial target:
	 * keep it byte-exact and ask the queue driver to retry after DONE (or the
	 * stronger stale-X certificate lifecycle) resolves the identity. */
	memset(&denied, 0, sizeof(denied));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &claimed),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ(memcmp(&claimed.payload_meta.forward_marker, marker, sizeof(*marker)), 0);

	/* Replay has one exact send claim.  The marker is back in FORWARDED, and
	 * S3-P0-09 makes that phase outrank a generic completion proof: a blind or
	 * duplicate requester DONE carries a perfectly valid identity, but the
	 * forward leg is still in flight, so mark_done() must refuse it rather than
	 * stamp done_at_ts.  Refusing keeps the cell in its live posture -- still a
	 * FORWARDED duplicate to a racing retry, still an exact blocker to queue-kind
	 * X arbitration -- instead of collapsing it to a retired DONE that
	 * pending_x_deny_next() would skip into a false NOT_FOUND (which
	 * pending_x_deny_exact() would simultaneously answer FORWARD_BLOCKED for). */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, marker,
					 GCS_BLOCK_FORWARD_MARK_FORWARDED, &claimed),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, marker, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &key, &tag, (uint8)PCM_TRANS_N_TO_S));
	/* The refusal is attributable, not silent: it lands on the FORWARDED
	 * counter alone.  It is deliberately NOT folded into done_mismatch_count
	 * (the identity matched perfectly -- this is a phase refusal), and it must
	 * not be mistaken for an accepted proof.  This is the whole reason the
	 * counter pair exists: without it a field run cannot tell a refused blind
	 * DONE from an ordinary accepted one. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_forwarded_refused_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_cancelling_refused_count(), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED);
}

UT_TEST(u39_admitted_forward_survives_local_cleanup_without_holder_fence_ack)
{
	BufferTag tag = make_tag(135);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xabc001), 22);
	GcsBlockDedupEntry cached;
	PcmAuthoritySnapshot authority;
	GcsBlockForwardPayload forward;
	GcsBlockForwardMarker marker;
	TimestampTz far_future;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_X, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = 2;
	authority.transition_count = 51;
	authority.state = PCM_STATE_X;
	authority.x_holder_node = 2;
	authority.pending_x_requester_node = 1;
	authority.pending_x_since_lsn = UINT64_C(0x34567);
	authority.authority_generation = 53;
	memset(&forward, 0, sizeof(forward));
	forward.request_id = key.request_id;
	forward.epoch = key.cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = 1;
	forward.requester_backend_id = key.requester_backend_id;
	forward.master_node = 0;
	forward.transition_id = (uint8)PCM_TRANS_N_TO_X;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward, (SCN)57);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_X, 2, 0,
					 GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, &authority, &forward, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	marker = cached.payload_meta.forward_marker;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, &marker,
					 GCS_BLOCK_FORWARD_MARK_PREPARED, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, &marker, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);

	/* Backend/node death and wall-clock TTL are not wire-quiescence proofs.
	 * A paused requester may still consume a late holder grant, so generic
	 * cleanup must retain the exact marker until a holder-side fence ACK (or
	 * a stronger membership/connection-incarnation proof) exists. */
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 7);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_X, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&cached),
				 (int)GCS_BLOCK_FORWARD_MARK_FORWARDED);
	UT_ASSERT_EQ(memcmp(&cached.payload_meta.forward_marker, &marker, sizeof(marker)), 0);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	far_future = cached.completed_at_ts + cached.pinned_lifetime_us * 4 + 1;
	cluster_gcs_block_dedup_sweep_expired(far_future);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_X, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	/* PREPARED has no admitted frame and may still be aborted, but only by
	 * the exact producer path; generic death/TTL cleanup does not guess. */
	key = make_key(3, 9, UINT64_C(0xabc002), 23);
	tag = make_tag(136);
	authority.master_holder.node_id = 2;
	authority.pending_x_requester_node = 3;
	authority.pending_x_since_lsn = UINT64_C(0x45678);
	authority.authority_generation++;
	forward.request_id = key.request_id;
	forward.epoch = key.cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = 3;
	forward.requester_backend_id = key.requester_backend_id;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_X, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_X, 2, 0,
					 GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, &authority, &forward, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	marker = cached.payload_meta.forward_marker;
	cluster_gcs_block_dedup_cleanup_on_node_dead(3);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_X, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT(cluster_gcs_block_dedup_forward_abort_prepared_exact(
		0, &key, &tag, 2, GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, &marker));
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
}

UT_TEST(u40_stale_x_holder_fence_is_exact_replayable_and_not_gc_owned)
{
	BufferTag tag = make_tag(137);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xabc003), 24);
	GcsStaleXCertPayload report;
	GcsStaleXCertPayload install;
	GcsStaleXCertPayload ack;
	GcsStaleXCertPayload commit;
	GcsStaleXCertPayload stored;
	GcsStaleXCertPayload drifted;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&report, 0, sizeof(report));
	report.request_id = key.request_id;
	report.request_epoch = key.cluster_epoch;
	report.release_cert_nonce = UINT64_C(0x123456789);
	report.source_own_generation = 31;
	report.final_page_scn = (SCN)41;
	report.durable_page_scn = (SCN)43;
	report.tag = tag;
	report.requester_node = (int32)key.origin_node_id;
	report.requester_backend_id = key.requester_backend_id;
	report.master_node = 0;
	report.holder_node = 2;
	report.requester_incarnation = 51;
	report.master_incarnation = 31;
	report.holder_incarnation = 41;
	report.relation_generation = 9;
	report.phase = (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT;
	report.reason = (uint8)GCS_STALE_X_CERT_REASON_NOT_RESIDENT;
	report.proof = GCS_STALE_X_PROOF_REPORT_MASK;
	report.transition_id = (uint8)PCM_TRANS_N_TO_S;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 0, &key, &report, &stored),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ(memcmp(&stored, &report, sizeof(report)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 0, &key, &report, &stored),
				 (int)GCS_STALE_X_FENCE_REPLAY);
	drifted = report;
	drifted.release_cert_nonce++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 0, &key, &drifted, NULL),
				 (int)GCS_STALE_X_FENCE_STALE);

	/* A provisional negative fence is authority state, not a generic cache
	 * row: requester/backend death and TTL cannot remove it. */
	cluster_gcs_block_dedup_cleanup_on_backend_exit(
		key.origin_node_id, key.requester_backend_id);
	cluster_gcs_block_dedup_cleanup_on_node_dead(key.origin_node_id);
	cluster_gcs_block_dedup_sweep_expired(GetCurrentTimestamp() + INT64_C(1000000000));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
					 0, &key, &tag, &stored),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ(memcmp(&stored, &report, sizeof(report)), 0);

	install = report;
	install.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_INSTALL;
	install.pre_authority_generation = 47;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_fence_install(
					 0, &key, &install, &ack),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ((int)ack.phase, (int)GCS_STALE_X_CERT_PHASE_FENCE_ACK);
	UT_ASSERT_EQ((int)ack.proof, (int)GCS_STALE_X_PROOF_FENCED_MASK);
	UT_ASSERT_EQ((uint64)ack.pre_authority_generation, UINT64_C(47));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_fence_install(
					 0, &key, &install, &stored),
				 (int)GCS_STALE_X_FENCE_REPLAY);
	UT_ASSERT_EQ(memcmp(&stored, &ack, sizeof(ack)), 0);

	drifted = install;
	drifted.final_page_scn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_fence_install(
					 0, &key, &drifted, NULL),
				 (int)GCS_STALE_X_FENCE_STALE);

	commit = ack;
	commit.phase = (uint8)GCS_STALE_X_CERT_PHASE_COMMIT;
	commit.post_authority_generation = commit.pre_authority_generation + 1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_commit(
					 0, &key, &commit, &stored),
				 (int)GCS_STALE_X_FENCE_COMMITTED);
	UT_ASSERT_EQ(memcmp(&stored, &commit, sizeof(commit)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_commit(
					 0, &key, &commit, &stored),
				 (int)GCS_STALE_X_FENCE_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
					 0, &key, &tag, &stored),
				 (int)GCS_STALE_X_FENCE_COMMITTED);

	drifted = commit;
	drifted.post_authority_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_commit(
					 0, &key, &drifted, NULL),
				 (int)GCS_STALE_X_FENCE_INVALID);
}

UT_TEST(u41_stale_x_master_report_serializes_with_send_finish_and_ack)
{
	BufferTag tag = make_tag(138);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xabc004), 25);
	GcsBlockDedupEntry cached;
	PcmAuthoritySnapshot authority;
	GcsBlockForwardPayload forward;
	GcsBlockForwardBootIdentity boot_identity;
	GcsBlockForwardMarker marker;
	GcsBlockForwardMarker marker_out;
	GcsStaleXCertPayload report;
	GcsStaleXCertPayload install;
	GcsStaleXCertPayload ack;
	GcsStaleXCertPayload commit;
	GcsStaleXCertPayload commit_ack;
	GcsStaleXCertPayload retire;
	GcsStaleXCertPayload retire_ack;
	GcsStaleXCertPayload drifted;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = 2;
	authority.transition_count = 61;
	authority.state = PCM_STATE_X;
	authority.x_holder_node = 2;
	authority.pending_x_requester_node = -1;
	authority.authority_generation = 67;
	memset(&forward, 0, sizeof(forward));
	forward.request_id = key.request_id;
	forward.epoch = key.cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = (int32)key.origin_node_id;
	forward.requester_backend_id = key.requester_backend_id;
	forward.master_node = 0;
	forward.transition_id = (uint8)PCM_TRANS_N_TO_S;
	forward.reserved_0[0] = 1;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward, (SCN)71);
	memset(&boot_identity, 0, sizeof(boot_identity));
	boot_identity.requester_incarnation = 51;
	boot_identity.master_incarnation = 31;
	boot_identity.holder_incarnation = 41;
	boot_identity.relation_generation = 9;
	boot_identity.capability_generation = 77;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_identity_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, 2, 0,
					 GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &authority, &forward,
					 &boot_identity, &cached),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	marker = cached.payload_meta.forward_marker;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &marker,
					 GCS_BLOCK_FORWARD_MARK_PREPARED, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);

	memset(&report, 0, sizeof(report));
	report.request_id = key.request_id;
	report.request_epoch = key.cluster_epoch;
	report.release_cert_nonce = UINT64_C(0x777777);
	report.source_own_generation = 73;
	report.final_page_scn = (SCN)71;
	report.durable_page_scn = (SCN)79;
	report.tag = tag;
	report.requester_node = (int32)key.origin_node_id;
	report.requester_backend_id = key.requester_backend_id;
	report.master_node = 0;
	report.holder_node = 2;
	report.requester_incarnation = 51;
	report.master_incarnation = 31;
	report.holder_incarnation = 41;
	report.relation_generation = boot_identity.relation_generation;
	report.phase = (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT;
	report.reason = (uint8)GCS_STALE_X_CERT_REASON_NOT_RESIDENT;
	report.proof = GCS_STALE_X_PROOF_REPORT_MASK;
	report.transition_id = (uint8)PCM_TRANS_N_TO_S;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_report_exact(
					 0, &key, 77, &report, &install, &marker_out),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ((uint64)install.pre_authority_generation, UINT64_C(67));
	UT_ASSERT_EQ((int)install.phase, (int)GCS_STALE_X_CERT_PHASE_FENCE_INSTALL);
	UT_ASSERT_EQ(memcmp(&marker_out, &marker, sizeof(marker)), 0);

	/* REPORT may arrive before the sender publishes SEND_ARMED->FORWARDED.
	 * The report owns the stronger state, so finish must replay rather than
	 * overwrite HOLDER_MISS_PENDING. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &marker, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_report_exact(
					 0, &key, 77, &report, &install, NULL),
				 (int)GCS_STALE_X_FENCE_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_report_exact(
					 0, &key, 78, &report, NULL, NULL),
				 (int)GCS_STALE_X_FENCE_STALE);

	drifted = report;
	drifted.durable_page_scn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_report_exact(
					 0, &key, 77, &drifted, NULL, NULL),
				 (int)GCS_STALE_X_FENCE_STALE);

	ack = install;
	ack.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_ACK;
	ack.proof = GCS_STALE_X_PROOF_FENCED_MASK;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_ack_exact(
					 0, &key, 77, &ack, &marker_out),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ(memcmp(&marker_out, &marker, sizeof(marker)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_ack_exact(
					 0, &key, 77, &ack, &marker_out),
				 (int)GCS_STALE_X_FENCE_REPLAY);
	drifted = ack;
	drifted.pre_authority_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_ack_exact(
					 0, &key, 77, &drifted, NULL),
				 (int)GCS_STALE_X_FENCE_STALE);

	/* PCM authority commit happens between ACK and this exact metadata
	 * publication.  Once COMMIT is recorded, late old FORWARDs can no
	 * longer re-enter; terminal phases are replayable until exact retire. */
	commit = ack;
	commit.phase = (uint8)GCS_STALE_X_CERT_PHASE_COMMIT;
	commit.post_authority_generation = commit.pre_authority_generation + 1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_commit_exact(
					 0, &key, 77, &commit, &marker_out),
				 (int)GCS_STALE_X_FENCE_COMMITTED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_commit_exact(
					 0, &key, 77, &commit, &marker_out),
				 (int)GCS_STALE_X_FENCE_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_ack_exact(
					 0, &key, 77, &ack, &marker_out),
				 (int)GCS_STALE_X_FENCE_REPLAY);

	commit_ack = commit;
	commit_ack.phase = (uint8)GCS_STALE_X_CERT_PHASE_COMMIT_ACK;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_commit_ack_exact(
					 0, &key, 77, &commit_ack),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_commit_ack_exact(
					 0, &key, 77, &commit_ack),
				 (int)GCS_STALE_X_FENCE_REPLAY);

	retire = commit_ack;
	retire.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE;
	retire.proof = GCS_STALE_X_PROOF_RETIRE_MASK;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_retire_arm_exact(
					 0, &key, 77, &retire),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_retire_arm_exact(
					 0, &key, 77, &retire),
				 (int)GCS_STALE_X_FENCE_REPLAY);

	retire_ack = retire;
	retire_ack.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE_ACK;
	drifted = retire_ack;
	drifted.release_cert_nonce++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_retire_ack_exact(
					 0, &key, 77, &drifted),
				 (int)GCS_STALE_X_FENCE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_master_retire_ack_exact(
					 0, &key, 77, &retire_ack),
				 (int)GCS_STALE_X_FENCE_RETIRED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}

UT_TEST(u42_stale_x_holder_retire_replays_lost_ack_and_reopens_small_cap)
{
	GcsStaleXCertPayload last_retire;
	int i;

	reset_fake_dedup(2, 2);
	cluster_node_id = 2;
	memset(&last_retire, 0, sizeof(last_retire));

	/* More attempts than the physical shard cap must complete without a
	 * stale-X tombstone leak.  RETIRE_ACK loss is represented by replaying
	 * the exact RETIRE after the holder entry has already been removed. */
	for (i = 0; i < 6; i++) {
		BufferTag tag = make_tag((uint32)(140 + i));
		GcsBlockDedupKey key
			= make_key(1, 7, UINT64_C(0xabc100) + (uint64)i, (uint32)(30 + i));
		GcsStaleXCertPayload report;
		GcsStaleXCertPayload install;
		GcsStaleXCertPayload ack;
		GcsStaleXCertPayload commit;
		GcsStaleXCertPayload retire;
		GcsStaleXCertPayload retire_ack;
		GcsStaleXCertPayload expected_retire_ack;
		GcsStaleXCertPayload replay_ack;

		memset(&report, 0, sizeof(report));
		report.request_id = key.request_id;
		report.request_epoch = key.cluster_epoch;
		report.release_cert_nonce = UINT64_C(0x12346000) + (uint64)i;
		report.source_own_generation = UINT64_C(100) + (uint64)i;
		report.final_page_scn = (SCN)(200 + i);
		report.durable_page_scn = (SCN)(300 + i);
		report.tag = tag;
		report.requester_node = (int32)key.origin_node_id;
		report.requester_backend_id = key.requester_backend_id;
		report.master_node = 0;
		report.holder_node = 2;
		report.requester_incarnation = 51;
		report.master_incarnation = 31;
		report.holder_incarnation = 41;
		report.relation_generation = 9;
		report.phase = (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT;
		report.reason = (uint8)GCS_STALE_X_CERT_REASON_NOT_RESIDENT;
		report.proof = GCS_STALE_X_PROOF_REPORT_MASK;
		report.transition_id = (uint8)PCM_TRANS_N_TO_S;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
						 0, &key, &report, NULL),
					 (int)GCS_STALE_X_FENCE_INSTALLED);

		install = report;
		install.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_INSTALL;
		install.pre_authority_generation = UINT64_C(400) + (uint64)i * 2;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_fence_install(
						 0, &key, &install, &ack),
					 (int)GCS_STALE_X_FENCE_INSTALLED);

		commit = ack;
		commit.phase = (uint8)GCS_STALE_X_CERT_PHASE_COMMIT;
		commit.post_authority_generation = commit.pre_authority_generation + 1;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_commit(
						 0, &key, &commit, NULL),
					 (int)GCS_STALE_X_FENCE_COMMITTED);

		retire = commit;
		retire.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE;
		retire.proof = GCS_STALE_X_PROOF_RETIRE_MASK;
		expected_retire_ack = retire;
		expected_retire_ack.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE_ACK;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_retire(
						 0, &key, &retire, &retire_ack),
					 (int)GCS_STALE_X_FENCE_RETIRED);
		UT_ASSERT_EQ((int)retire_ack.phase,
					 (int)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE_ACK);
		UT_ASSERT_EQ(memcmp(&retire_ack, &expected_retire_ack,
							sizeof(expected_retire_ack)),
					 0);
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
						 0, &key, &tag, NULL),
					 (int)GCS_STALE_X_FENCE_NOT_FOUND);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_retire(
						 0, &key, &retire, &replay_ack),
					 (int)GCS_STALE_X_FENCE_REPLAY);
		UT_ASSERT_EQ(memcmp(&replay_ack, &retire_ack, sizeof(retire_ack)), 0);
		last_retire = retire;
	}
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* Even the current master cannot make an existing different attempt
	 * disappear by presenting a well-shaped but non-exact RETIRE. */
	{
		BufferTag tag = make_tag(150);
		GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xabc200), 40);
		GcsStaleXCertPayload report = last_retire;
		GcsStaleXCertPayload install;
		GcsStaleXCertPayload ack;
		GcsStaleXCertPayload commit;
		GcsStaleXCertPayload retire;
		GcsStaleXCertPayload drifted;

		report.request_id = key.request_id;
		report.request_epoch = key.cluster_epoch;
		report.release_cert_nonce++;
		report.tag = tag;
		report.requester_backend_id = key.requester_backend_id;
		report.phase = (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT;
		report.proof = GCS_STALE_X_PROOF_REPORT_MASK;
		report.pre_authority_generation = 0;
		report.post_authority_generation = 0;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
						 0, &key, &report, NULL),
					 (int)GCS_STALE_X_FENCE_INSTALLED);
		install = report;
		install.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_INSTALL;
		install.pre_authority_generation = 600;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_fence_install(
						 0, &key, &install, &ack),
					 (int)GCS_STALE_X_FENCE_INSTALLED);
		commit = ack;
		commit.phase = (uint8)GCS_STALE_X_CERT_PHASE_COMMIT;
		commit.post_authority_generation = 601;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_commit(
						 0, &key, &commit, NULL),
					 (int)GCS_STALE_X_FENCE_COMMITTED);
		retire = commit;
		retire.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE;
		retire.proof = GCS_STALE_X_PROOF_RETIRE_MASK;
		drifted = retire;
		drifted.release_cert_nonce++;
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_retire(
						 0, &key, &drifted, NULL),
					 (int)GCS_STALE_X_FENCE_STALE);
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_retire(
						 0, &key, &retire, NULL),
					 (int)GCS_STALE_X_FENCE_RETIRED);
	}
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}

UT_TEST(u43_stale_x_release_journal_is_state_and_residency_exact)
{
	GcsStaleXReleaseRecord first;
	GcsStaleXReleaseRecord newer;
	GcsStaleXReleaseRecord sealed;
	GcsStaleXReleaseRecord committing;
	GcsStaleXReleaseRecord released;
	GcsStaleXReleaseRecord found;
	int worker_id;

	reset_fake_dedup(2, 2);
	memset(&first, 0, sizeof(first));
	first.key.tag = make_tag(160);
	first.key.source_buf_id = 5;
	first.key.durability_generation = 7;
	first.key.source_own_generation = 0; /* legal first ownership generation */
	first.key.holder_incarnation = 41;
	first.key.release_cert_nonce = 12;
	first.release_epoch = 7;
	first.relation_generation = 9;
	first.master_incarnation = 31;
	first.final_page_scn = InvalidScn;
	first.durable_page_scn = InvalidScn;
	first.master_node = 0;
	first.holder_node = 2;
	first.state = (uint8)GCS_STALE_X_RELEASE_RESERVED;
	worker_id = cluster_lms_shard_for_tag(&first.key.tag, cluster_lms_workers);

	UT_ASSERT_EQ((int)cluster_gcs_block_stale_x_release_reserve(&first),
				 (int)GCS_STALE_X_RELEASE_INSTALLED);
	/* Nothing before RELEASED is reportable. */
	UT_ASSERT(!cluster_gcs_block_stale_x_release_lookup_exact(
		worker_id, &first.key.tag, first.release_epoch, first.master_node,
		first.holder_node, first.master_incarnation, first.key.holder_incarnation,
		(SCN)501, NULL));

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_stale_x_release_seal_exact(
			&first.key, (SCN)501, (SCN)501, UINT64_C(91), &sealed),
		(int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT_EQ((int)sealed.state, (int)GCS_STALE_X_RELEASE_SEALED);
	UT_ASSERT(!cluster_gcs_block_stale_x_release_lookup_exact(
		worker_id, &first.key.tag, first.release_epoch, first.master_node,
		first.holder_node, first.master_incarnation, first.key.holder_incarnation,
		(SCN)501, NULL));
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_stale_x_release_advance_exact(
			&sealed, GCS_STALE_X_RELEASE_COMMITTING, 0, &committing),
		(int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT(!cluster_gcs_block_stale_x_release_lookup_exact(
		worker_id, &first.key.tag, first.release_epoch, first.master_node,
		first.holder_node, first.master_incarnation, first.key.holder_incarnation,
		(SCN)501, NULL));
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_stale_x_release_advance_exact(
			&committing, GCS_STALE_X_RELEASE_RELEASED, UINT64_C(1), &released),
		(int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT(cluster_gcs_block_stale_x_release_lookup_exact(
		worker_id, &first.key.tag, first.release_epoch, first.master_node,
		first.holder_node, first.master_incarnation, first.key.holder_incarnation,
		(SCN)501, &found));
	UT_ASSERT_EQ(memcmp(&found, &released, sizeof(released)), 0);

	/* A later unresolved residency of the same tag is a distinct key; it
	 * cannot overwrite or become consumable as the older release. */
	newer = first;
	newer.key.source_buf_id = 6;
	newer.key.source_own_generation = 21;
	newer.key.durability_generation = 3;
	newer.key.release_cert_nonce = 22;
	UT_ASSERT_EQ((int)cluster_gcs_block_stale_x_release_reserve(&newer),
				 (int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT(cluster_gcs_block_stale_x_release_lookup_exact(
		worker_id, &released.key.tag, released.release_epoch, released.master_node,
		released.holder_node, released.master_incarnation,
		released.key.holder_incarnation, released.final_page_scn, &found));
	UT_ASSERT_EQ(memcmp(&found, &released, sizeof(released)), 0);
	UT_ASSERT(cluster_gcs_block_stale_x_release_forget_exact(&released));
	UT_ASSERT(!cluster_gcs_block_stale_x_release_forget_exact(&released));
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_stale_x_release_seal_exact(
			&newer.key, (SCN)601, (SCN)602, UINT64_C(92), &sealed),
		(int)GCS_STALE_X_RELEASE_INSTALLED);
}

UT_TEST(u44_checkpoint_durable_seal_is_residency_and_redirty_exact)
{
	GcsPiWriteNote note;
	GcsStaleXReleaseKey release_key;
	GcsStaleXDurableSeal seal;
	int worker_id;

	reset_fake_dedup(2, 2);
	memset(&note, 0, sizeof(note));
	note.tag = make_tag(161);
	note.source_buf_id = 5;
	note.durability_generation = 7;
	note.source_own_generation = 0;
	note.source_node_incarnation = 41;
	note.page_scn = (SCN)501;
	worker_id = cluster_lms_shard_for_tag(&note.tag, cluster_lms_workers);

	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_publish(&note, UINT64_C(91)));
	memset(&release_key, 0, sizeof(release_key));
	release_key.tag = note.tag;
	release_key.source_buf_id = note.source_buf_id;
	release_key.durability_generation = note.durability_generation;
	release_key.source_own_generation = note.source_own_generation;
	release_key.holder_incarnation = note.source_node_incarnation;
	release_key.release_cert_nonce = 1;
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)501, &seal));
	UT_ASSERT_EQ(seal.checkpoint_seal_id, UINT64_C(91));
	UT_ASSERT_EQ((uint64)seal.durable_page_scn, UINT64_C(501));

	/* Any residency component drift, especially redirty g->g+1, refuses
	 * the old checkpoint seal even when the page SCN is unchanged. */
	release_key.durability_generation++;
	UT_ASSERT(!cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)501, NULL));
	release_key.durability_generation--;
	release_key.source_buf_id++;
	UT_ASSERT(!cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)501, NULL));
	release_key.source_buf_id--;
	release_key.holder_incarnation++;
	UT_ASSERT(!cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)501, NULL));
	release_key.holder_incarnation--;

	/* A later confirmed write of the same residency advances, never regresses,
	 * the independently retained durable floor. */
	note.page_scn = (SCN)601;
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_publish(&note, UINT64_C(92)));
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)601, &seal));
	UT_ASSERT_EQ(seal.checkpoint_seal_id, UINT64_C(92));
	UT_ASSERT_EQ((uint64)seal.durable_page_scn, UINT64_C(601));
	note.page_scn = (SCN)501;
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_publish(&note, UINT64_C(93)));
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)601, &seal));
	UT_ASSERT_EQ((uint64)seal.durable_page_scn, UINT64_C(601));

	/* The per-buf fixed slot replaces an older dirty generation instead of
	 * consuming unbounded HTAB capacity across writeback cycles. */
	note.durability_generation = 8;
	note.page_scn = (SCN)701;
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_publish(&note, UINT64_C(94)));
	UT_ASSERT(!cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)501, NULL));
	release_key.durability_generation = 8;
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)701, &seal));

	note.durability_generation = UINT32_MAX;
	UT_ASSERT(!cluster_gcs_block_stale_x_durable_seal_publish(&note, UINT64_C(95)));
	UT_ASSERT(cluster_gcs_block_stale_x_durable_seal_forget_exact(&seal));
	UT_ASSERT(!cluster_gcs_block_stale_x_durable_seal_lookup_exact(
		worker_id, &release_key, (SCN)501, NULL));
}

UT_TEST(u45_release_journal_capacity_fails_before_evidence_loss)
{
	GcsStaleXReleaseRecord first;
	GcsStaleXReleaseRecord second;

	reset_fake_dedup(1, 1);
	memset(&first, 0, sizeof(first));
	first.key.tag = make_tag(170);
	first.key.source_buf_id = 1;
	first.key.durability_generation = 2;
	first.key.holder_incarnation = 41;
	first.key.release_cert_nonce = 1;
	first.release_epoch = 7;
	first.relation_generation = 9;
	first.master_incarnation = 31;
	first.final_page_scn = InvalidScn;
	first.durable_page_scn = InvalidScn;
	first.master_node = 0;
	first.holder_node = 2;
	first.state = (uint8)GCS_STALE_X_RELEASE_RESERVED;
	second = first;
	second.key.tag = make_tag(171);
	second.key.source_buf_id = 2;
	second.key.release_cert_nonce = 2;

	UT_ASSERT_EQ((int)cluster_gcs_block_stale_x_release_reserve(&first),
				 (int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_stale_x_release_reserve(&second),
				 (int)GCS_STALE_X_RELEASE_FULL);
	UT_ASSERT(cluster_gcs_block_stale_x_release_forget_exact(&first));
	UT_ASSERT_EQ((int)cluster_gcs_block_stale_x_release_reserve(&second),
				 (int)GCS_STALE_X_RELEASE_INSTALLED);
}

UT_TEST(u46_eviction_gate_blocks_tag_and_source_until_exact_ack)
{
	GcsBlockEvictionGateRecord gate;
	GcsBlockEvictionGateRecord same_tag;
	GcsBlockEvictionGateRecord released;
	GcsBlockEvictionGateRecord found;
	BufferTag other_tag;
	int worker_id;

	reset_fake_dedup(2, 2);
	memset(&gate, 0, sizeof(gate));
	gate.key.tag = make_tag(180);
	gate.key.source_buf_id = 3;
	gate.key.durability_generation = 9;
	gate.key.source_own_generation = 17;
	gate.key.holder_incarnation = 41;
	gate.key.release_cert_nonce = 77;
	gate.release_epoch = 8;
	gate.relation_generation = 9;
	gate.master_incarnation = 31;
	gate.master_node = 0;
	gate.holder_node = 2;
	gate.capability_generation = 11;
	gate.old_pcm_mode = (uint8)PCM_LOCK_MODE_X;
	gate.state = (uint8)GCS_BLOCK_EVICTION_GATE_COMMITTING;
	gate.has_stale_x_journal = 1;
	worker_id = cluster_lms_shard_for_tag(&gate.key.tag, cluster_lms_workers);

	UT_ASSERT_EQ((int)cluster_gcs_block_eviction_gate_reserve(&gate),
				 (int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT(cluster_gcs_block_eviction_gate_conflict(
		worker_id, &gate.key.tag, -1, &found));
	UT_ASSERT_EQ(memcmp(&found, &gate, sizeof(gate)), 0);

	/* A second residency cannot reserve the same logical tag while the old
	 * release is unacked, even with a different descriptor/generation. */
	same_tag = gate;
	same_tag.key.source_buf_id = 4;
	same_tag.key.source_own_generation = 1;
	same_tag.key.release_cert_nonce++;
	UT_ASSERT_EQ((int)cluster_gcs_block_eviction_gate_reserve(&same_tag),
				 (int)GCS_STALE_X_RELEASE_STALE);

	/* Descriptor reuse for another tag is blocked too. */
	other_tag = make_tag(181);
	UT_ASSERT(cluster_gcs_block_eviction_gate_conflict(
		cluster_lms_shard_for_tag(&other_tag, cluster_lms_workers),
		&other_tag, gate.key.source_buf_id, &found));
	UT_ASSERT_EQ(memcmp(&found, &gate, sizeof(gate)), 0);

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_eviction_gate_advance_exact(
			&gate, GCS_BLOCK_EVICTION_GATE_RELEASED, UINT64_C(18), &released),
		(int)GCS_STALE_X_RELEASE_INSTALLED);
	UT_ASSERT(cluster_gcs_block_eviction_gate_conflict(
		worker_id, &gate.key.tag, -1, &found));
	UT_ASSERT_EQ(memcmp(&found, &released, sizeof(released)), 0);

	/* Only exact release ACK removal reopens both tag and descriptor. */
	UT_ASSERT(cluster_gcs_block_eviction_gate_forget_exact(&released));
	UT_ASSERT(!cluster_gcs_block_eviction_gate_conflict(
		worker_id, &gate.key.tag, -1, NULL));
	UT_ASSERT(!cluster_gcs_block_eviction_gate_conflict(
		cluster_lms_shard_for_tag(&other_tag, cluster_lms_workers),
		&other_tag, gate.key.source_buf_id, NULL));
}

UT_TEST(u47_stale_x_relation_generation_is_always_on_exact_and_bounded)
{
	RelFileLocator first = { .spcOid = 1663, .dbOid = 5, .relNumber = 901 };
	RelFileLocator second = { .spcOid = 1663, .dbOid = 5, .relNumber = 902 };
	RelFileLocator overflow = { .spcOid = 1663, .dbOid = 5, .relNumber = 903 };
	RelFileLocator untracked = { .spcOid = 1663, .dbOid = 5, .relNumber = 904 };
	RelFileLocator invalid = { 0 };
	GcsStaleXRelationGenerationGuard guard;
	uint64 generation = 0;

	reset_fake_dedup(1, 2);
	UT_ASSERT_EQ((int)sizeof(GcsStaleXRelationGenerationEntry), 24);

	UT_ASSERT(cluster_gcs_block_stale_x_relation_register(first, &generation));
	UT_ASSERT_EQ(generation, UINT64_C(1));
	UT_ASSERT(cluster_gcs_block_stale_x_relation_current(first, &generation));
	UT_ASSERT_EQ(generation, UINT64_C(1));
	UT_ASSERT(cluster_gcs_block_stale_x_relation_guard_acquire(
		first, generation, &guard));
	UT_ASSERT_EQ(guard.generation, UINT64_C(1));
	cluster_gcs_block_stale_x_relation_guard_release(&guard);
	UT_ASSERT_EQ(guard.shard_index, -1);
	/* Exact duplicate registration never advances the incarnation. */
	UT_ASSERT(cluster_gcs_block_stale_x_relation_register(first, &generation));
	UT_ASSERT_EQ(generation, UINT64_C(1));

	/* smgrdounlinkall's exact-locator bump invalidates the captured value. */
	UT_ASSERT(cluster_gcs_block_stale_x_relation_bump(first));
	UT_ASSERT(cluster_gcs_block_stale_x_relation_current(first, &generation));
	UT_ASSERT_EQ(generation, UINT64_C(2));
	UT_ASSERT(!cluster_gcs_block_stale_x_relation_guard_acquire(
		first, UINT64_C(1), &guard));
	UT_ASSERT(!GcsStaleXStorageProofExact(
		(SCN)501, (SCN)501, (SCN)501, UINT64_C(1), generation));

	UT_ASSERT(cluster_gcs_block_stale_x_relation_register(second, &generation));
	UT_ASSERT_EQ(generation, UINT64_C(1));
	/* Capacity is bounded; overflow fails before a release journal exists. */
	UT_ASSERT(!cluster_gcs_block_stale_x_relation_register(
		overflow, &generation));
	UT_ASSERT_EQ(generation, UINT64_C(0));
	/* An unlink of an untracked locator is a safe no-op and consumes no slot. */
	UT_ASSERT(cluster_gcs_block_stale_x_relation_bump(untracked));
	UT_ASSERT(!cluster_gcs_block_stale_x_relation_current(
		untracked, &generation));
	UT_ASSERT(!cluster_gcs_block_stale_x_relation_register(
		invalid, &generation));
}

UT_TEST(u48_relation_supersede_keeps_old_holder_fence_fail_closed)
{
	BufferTag tag = make_tag(190);
	RelFileLocator locator = BufTagGetRelFileLocator(&tag);
	GcsBlockDedupKey key = make_key(1, 8, UINT64_C(0xabc048), 48);
	GcsStaleXCertPayload report;
	GcsStaleXCertPayload stored;
	GcsStaleXCertPayload new_relation_report;
	uint64 relation_generation = 0;
	int worker_id;

	reset_fake_dedup(1, FAKE_DEDUP_CAP);
	worker_id = cluster_lms_shard_for_tag(&tag, cluster_lms_workers);
	UT_ASSERT(cluster_gcs_block_stale_x_relation_register(
		locator, &relation_generation));
	UT_ASSERT_EQ(relation_generation, UINT64_C(1));

	memset(&report, 0, sizeof(report));
	report.request_id = key.request_id;
	report.request_epoch = key.cluster_epoch;
	report.release_cert_nonce = 77;
	report.source_own_generation = 17;
	report.relation_generation = relation_generation;
	report.final_page_scn = (SCN)501;
	report.durable_page_scn = (SCN)501;
	report.tag = tag;
	report.requester_node = (int32)key.origin_node_id;
	report.requester_backend_id = key.requester_backend_id;
	report.master_node = 0;
	report.holder_node = 2;
	report.requester_incarnation = 51;
	report.master_incarnation = 31;
	report.holder_incarnation = 41;
	report.phase = (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT;
	report.reason = (uint8)GCS_STALE_X_CERT_REASON_NOT_RESIDENT;
	report.proof = GCS_STALE_X_PROOF_REPORT_MASK;
	report.transition_id = (uint8)PCM_TRANS_N_TO_S;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 worker_id, &key, &report, &stored),
				 (int)GCS_STALE_X_FENCE_INSTALLED);

	/* DROP/reuse invalidates every old-generation phase, but does not delete
	 * the old negative fence: a legacy FORWARD may still be queued on DATA. */
	UT_ASSERT(cluster_gcs_block_stale_x_relation_bump(locator));
	UT_ASSERT(cluster_gcs_block_stale_x_relation_current(
		locator, &relation_generation));
	UT_ASSERT_EQ(relation_generation, UINT64_C(2));
	UT_ASSERT(!GcsStaleXRelationGenerationExact(
		report.relation_generation, relation_generation));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
					 worker_id, &key, &tag, &stored),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ(memcmp(&stored, &report, sizeof(report)), 0);

	/* The reused physical tag cannot overwrite that same request identity
	 * with its new lifecycle generation.  Only RETIRE/quiescence may remove
	 * the old tombstone. */
	new_relation_report = report;
	new_relation_report.relation_generation = relation_generation;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 worker_id, &key, &new_relation_report, NULL),
				 (int)GCS_STALE_X_FENCE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
					 worker_id, &key, &tag, &stored),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ(memcmp(&stored, &report, sizeof(report)), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
}

UT_TEST(u49_forward_cancel_master_transition_is_exact_and_replayable)
{
	BufferTag tag = make_tag(183);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xcace001), 29);
	GcsBlockDedupEntry entry;
	GcsBlockDedupEntry replay;
	GcsBlockDedupEntry denied;
	PcmAuthoritySnapshot authority;
	GcsBlockForwardPayload forward;
	GcsBlockForwardBootIdentity boot;
	GcsBlockForwardMarker marker;
	GcsBlockForwardCancelPayload cancel;
	GcsBlockForwardCancelPayload ack;
	GcsBlockForwardCancelPayload drifted;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &entry),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = 2;
	authority.transition_count = 71;
	authority.state = PCM_STATE_S;
	authority.x_holder_node = -1;
	authority.s_holders_bitmap = UINT32_C(1) << 2;
	authority.pending_x_requester_node = -1;
	authority.authority_generation = 73;
	memset(&forward, 0, sizeof(forward));
	forward.request_id = key.request_id;
	forward.epoch = key.cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = (int32)key.origin_node_id;
	forward.requester_backend_id = key.requester_backend_id;
	forward.master_node = 0;
	forward.transition_id = (uint8)PCM_TRANS_N_TO_S;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward, (SCN)79);
	memset(&boot, 0, sizeof(boot));
	boot.requester_incarnation = 83;
	boot.master_incarnation = 89;
	boot.holder_incarnation = 97;
	boot.relation_generation = 101;
	boot.capability_generation = 103;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_identity_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, 2, 0,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &authority, &forward,
					 &boot, &entry),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	marker = entry.payload_meta.forward_marker;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
					 &marker, GCS_BLOCK_FORWARD_MARK_PREPARED, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
					 &marker, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
	UT_ASSERT_EQ(memcmp(&entry.payload_meta.forward_marker, &marker,
						sizeof(marker)),
				 0);
	memcpy(&cancel, entry.block_data, sizeof(cancel));
	UT_ASSERT_EQ(cancel.request_id, key.request_id);
	UT_ASSERT_EQ(cancel.request_epoch, key.cluster_epoch);
	UT_ASSERT_EQ(cancel.pre_authority_generation,
				 authority.authority_generation);
	UT_ASSERT_EQ(cancel.relation_generation, boot.relation_generation);
	UT_ASSERT_EQ(cancel.expected_pi_watermark_scn, UINT64_C(79));
	UT_ASSERT_EQ(cancel.requester_incarnation, boot.requester_incarnation);
	UT_ASSERT_EQ(cancel.master_incarnation, boot.master_incarnation);
	UT_ASSERT_EQ(cancel.holder_incarnation, boot.holder_incarnation);
	UT_ASSERT_EQ(cancel.requester_node, (int32)key.origin_node_id);
	UT_ASSERT_EQ(cancel.requester_backend_id, key.requester_backend_id);
	UT_ASSERT_EQ(cancel.master_node, 0);
	UT_ASSERT_EQ(cancel.holder_node, 2);
	UT_ASSERT_EQ((int)cancel.phase,
				 (int)GCS_FORWARD_CANCEL_PHASE_MASTER_TO_HOLDER);
	UT_ASSERT_EQ((int)cancel.proof,
				 (int)GCS_FORWARD_CANCEL_PROOF_MASTER_MASK);
	UT_ASSERT_EQ(cancel.master_holder_capability_generation,
				 boot.capability_generation);
	UT_ASSERT_EQ(cancel.holder_requester_capability_generation, 0);

	memset(&replay, 0, sizeof(replay));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ(memcmp(&replay, &entry, sizeof(entry)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &replay),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);

	ack = cancel;
	ack.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_REQUESTER_FENCE_ACK;
	ack.proof = GCS_FORWARD_CANCEL_PROOF_ACK_MASK;
	ack.holder_requester_capability_generation = 107;
	ack.requester_master_capability_generation = 109;
	drifted = ack;
	drifted.pre_authority_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_ack_exact(
					 0, &key, 109, &drifted, &denied),
				 (int)GCS_BLOCK_FORWARD_MARK_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	memset(&denied, 0, sizeof(denied));
	/* CONTROL capability generations are endpoint-local reconnect counters:
	 * the requester can legitimately stamp 109 while the master currently
	 * observes that requester at 111.  Each sender's outbound ring already
	 * binds its frame to its own exact generation, so ingress must require
	 * both generations to be live, not falsely require cross-endpoint
	 * numeric equality. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_ack_exact(
					 0, &key, 111, &ack, &denied),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)denied.status,
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)denied.reply_header.status,
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&denied),
				 (int)GCS_BLOCK_FORWARD_MARK_NONE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_ack_exact(
					 0, &key, 109, &ack, &denied),
				 (int)GCS_BLOCK_FORWARD_MARK_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_ack_exact(
					 0, &key, 109, &ack, &replay),
				 (int)GCS_BLOCK_FORWARD_MARK_REPLAY);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(
		0, &key, &tag, (uint8)PCM_TRANS_N_TO_S));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_NOT_FOUND);
}

/*
 * S3-P0-09 R3: S->N release and type67 use different transport planes, so
 * ACK may reach the master before release.  Exercise the real master dedup
 * lifecycle around the production policy seam: the early ACK gate keeps the
 * exact CANCELLING marker; the master's type66 replay causes a second exact
 * requester ACK; only that post-release ACK terminalizes the marker.
 */
UT_TEST(u59_forward_cancel_cross_plane_early_ack_replays_to_terminal)
{
#ifndef GCS_BLOCK_FORWARD_CANCEL_REPLAY_POLICY_V1
	UT_ASSERT(false);
#else
	BufferTag tag = make_tag(189);
	GcsBlockDedupKey key = make_key(1, 9, UINT64_C(0xcace059), 37);
	GcsBlockDedupEntry entry;
	GcsBlockDedupEntry replay;
	GcsBlockDedupEntry denied;
	PcmAuthoritySnapshot authority;
	GcsBlockForwardPayload forward;
	GcsBlockForwardBootIdentity boot;
	GcsBlockForwardMarker marker;
	GcsBlockForwardCancelPayload cancel;
	GcsBlockForwardCancelPayload barrier;
	GcsBlockForwardCancelPayload ack;
	GcsBlockForwardCancelReplayEntry ledger[1];
	size_t ledger_slot = 0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &entry),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = 2;
	authority.transition_count = 131;
	authority.state = PCM_STATE_S;
	authority.x_holder_node = -1;
	authority.s_holders_bitmap = UINT32_C(1) << 2;
	authority.pending_x_requester_node = -1;
	authority.authority_generation = 137;
	memset(&forward, 0, sizeof(forward));
	forward.request_id = key.request_id;
	forward.epoch = key.cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = (int32)key.origin_node_id;
	forward.requester_backend_id = key.requester_backend_id;
	forward.master_node = 0;
	forward.transition_id = (uint8)PCM_TRANS_N_TO_S;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward, (SCN)139);
	memset(&boot, 0, sizeof(boot));
	boot.requester_incarnation = 149;
	boot.master_incarnation = 151;
	boot.holder_incarnation = 157;
	boot.relation_generation = 163;
	boot.capability_generation = 167;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_prepare_identity_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, 2, 0,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &authority,
					 &forward, &boot, &entry),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	marker = entry.payload_meta.forward_marker;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 0, &key, &tag, 2,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &marker,
					 GCS_BLOCK_FORWARD_MARK_PREPARED, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 0, &key, &tag, 2,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &marker, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
	memcpy(&cancel, entry.block_data, sizeof(cancel));
	barrier = cancel;
	barrier.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_HOLDER_BARRIER;
	barrier.proof = GCS_FORWARD_CANCEL_PROOF_BARRIER_MASK;
	barrier.holder_requester_capability_generation = 173;
	ack = barrier;
	ack.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_REQUESTER_FENCE_ACK;
	ack.proof = GCS_FORWARD_CANCEL_PROOF_ACK_MASK;
	ack.requester_master_capability_generation = 179;

	/* Requester admits release then ACK, but DATA wins the cross-plane race.
	 * The requester ledger may finish only after ACK admission; the durable
	 * master marker remains the replay source if that admitted ACK is early. */
	memset(ledger, 0, sizeof(ledger));
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_ledger_park(
					 ledger, lengthof(ledger), &barrier, &ledger_slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_release_exact(
		ledger, lengthof(ledger), ledger_slot, &barrier));
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_ack_exact(
		ledger, lengthof(ledger), ledger_slot, &barrier));
	UT_ASSERT(gcs_block_forward_cancel_replay_finish_exact(
		ledger, lengthof(ledger), ledger_slot, &barrier));

	authority.s_holders_bitmap = UINT32_C(1) << key.origin_node_id;
	UT_ASSERT(!gcs_block_forward_cancel_master_ack_ready(
		&authority, (int32)key.origin_node_id));
	memset(&replay, 0, sizeof(replay));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&replay),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);

	/* CONTROL release now lands.  The master's replay recreates the exact
	 * requester entry, which again advances monotonically to one admitted
	 * type67; exact duplicate park never rolls a live phase backward. */
	authority.s_holders_bitmap = 0;
	UT_ASSERT(gcs_block_forward_cancel_master_ack_ready(
		&authority, (int32)key.origin_node_id));
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_ledger_park(
					 ledger, lengthof(ledger), &barrier, &ledger_slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_release_exact(
		ledger, lengthof(ledger), ledger_slot, &barrier));
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_ledger_park(
					 ledger, lengthof(ledger), &barrier, &ledger_slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE);
	UT_ASSERT_EQ((int)ledger[ledger_slot].phase,
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED);
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_ack_exact(
		ledger, lengthof(ledger), ledger_slot, &barrier));

	memset(&denied, 0, sizeof(denied));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_ack_exact(
					 0, &key, 181, &ack, &denied),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)denied.reply_header.status,
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&denied),
				 (int)GCS_BLOCK_FORWARD_MARK_NONE);
	UT_ASSERT(gcs_block_forward_cancel_replay_finish_exact(
		ledger, lengthof(ledger), ledger_slot, &barrier));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(
					 0, &tag, &replay),
				 (int)GCS_BLOCK_PENDING_X_DENY_REPLAY);
#endif
}

/*
 * A direct-land request retries with the SAME wire identity after an
 * authoritative no-forward denial, but deliberately clears the direct-land
 * transport preference.  That one-way DIRECT -> generic downgrade is not an
 * identity collision: the cached reply must switch to the generic lane.
 * Re-arming DIRECT after the generic retry is pinned remains forbidden.
 */
UT_TEST(u52_direct_land_same_identity_retry_can_downgrade_to_generic)
{
	BufferTag tag = make_tag(186);
	GcsBlockDedupKey key = make_key(3, 7, UINT64_C(0xd1ec7001), 33);
	GcsBlockDedupEntry cached;
	GcsBlockReplyHeader denied;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT(cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_S,
		GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));

	memset(&denied, 0, sizeof(denied));
	denied.request_id = key.request_id;
	denied.epoch = key.cluster_epoch;
	denied.sender_node = 0;
	denied.requester_backend_id = key.requester_backend_id;
	denied.transition_id = (uint8)PCM_TRANS_N_TO_S;
	denied.status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&denied, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	cluster_gcs_block_dedup_install_reply(
		0, &key, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, &denied, NULL);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT(cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_S, 0));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(
		(int)cached.request_flags, (int)GCS_BLOCK_DEDUP_REQUEST_F_PINNED);

	UT_ASSERT(!cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_S,
		GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(
		(int)cached.request_flags, (int)GCS_BLOCK_DEDUP_REQUEST_F_PINNED);
}

UT_TEST(u50_forward_cancel_holder_barrier_tombstone_is_exact)
{
	BufferTag tag = make_tag(184);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xcace002), 31);
	GcsBlockForwardCancelPayload cancel;
	GcsBlockForwardCancelPayload barrier;
	GcsBlockForwardCancelPayload replay;
	GcsBlockForwardCancelPayload drifted;
	TimestampTz far_future;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&cancel, 0, sizeof(cancel));
	cancel.request_id = key.request_id;
	cancel.request_epoch = key.cluster_epoch;
	cancel.pre_authority_generation = 113;
	cancel.relation_generation = 127;
	cancel.expected_pi_watermark_scn = 131;
	cancel.requester_incarnation = 137;
	cancel.master_incarnation = 139;
	cancel.holder_incarnation = 149;
	cancel.tag = tag;
	cancel.requester_node = (int32)key.origin_node_id;
	cancel.requester_backend_id = key.requester_backend_id;
	cancel.master_node = 0;
	cancel.holder_node = 2;
	cancel.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_MASTER_TO_HOLDER;
	cancel.reason = (uint8)GCS_FORWARD_CANCEL_REASON_PENDING_X;
	cancel.proof = GCS_FORWARD_CANCEL_PROOF_MASTER_MASK;
	cancel.transition_id = (uint8)PCM_TRANS_N_TO_S;
	cancel.master_holder_capability_generation = 151;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &cancel, 157, &barrier),
				 (int)GCS_FORWARD_CANCEL_INSTALLED);
	UT_ASSERT_EQ((int)barrier.phase,
				 (int)GCS_FORWARD_CANCEL_PHASE_HOLDER_BARRIER);
	UT_ASSERT_EQ((int)barrier.proof,
				 (int)GCS_FORWARD_CANCEL_PROOF_BARRIER_MASK);
	UT_ASSERT_EQ(barrier.holder_requester_capability_generation,
				 UINT32_C(157));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_lookup(
					 0, &key, &tag, &replay),
				 (int)GCS_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ(memcmp(&replay, &barrier, sizeof(barrier)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &cancel, 157, &replay),
				 (int)GCS_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ(memcmp(&replay, &barrier, sizeof(barrier)), 0);

	drifted = cancel;
	drifted.pre_authority_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &drifted, 157, NULL),
				 (int)GCS_FORWARD_CANCEL_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &cancel, 0, NULL),
				 (int)GCS_FORWARD_CANCEL_INVALID);

	/* Generic cleanup and TTL cannot erase a not-yet-admitted ordered
	 * barrier. */
	cluster_gcs_block_dedup_cleanup_on_backend_exit(
		key.origin_node_id, key.requester_backend_id);
	cluster_gcs_block_dedup_cleanup_on_node_dead(
		(int32)key.origin_node_id);
	far_future = GetCurrentTimestamp()
				 + ((TimestampTz)GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS
					* (TimestampTz)1000 * 4);
	cluster_gcs_block_dedup_sweep_expired(far_future);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_lookup(
					 0, &key, &tag, &replay),
				 (int)GCS_FORWARD_CANCEL_REPLAY);

	/* Only the byte-exact barrier that the ordered DATA queue admitted may
	 * release the transient holder tombstone. */
	drifted = barrier;
	drifted.holder_requester_capability_generation++;
	UT_ASSERT(!cluster_gcs_block_dedup_forward_cancel_holder_admitted_exact(
		0, &key, &drifted));
	UT_ASSERT(cluster_gcs_block_dedup_forward_cancel_holder_admitted_exact(
		0, &key, &barrier));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_lookup(
					 0, &key, &tag, &replay),
				 (int)GCS_FORWARD_CANCEL_NOT_FOUND);

	/* Lost barrier: replayed master CANCEL reinstalls the same canonical
	 * barrier without borrowing a new request identity. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &cancel, 157, &replay),
				 (int)GCS_FORWARD_CANCEL_INSTALLED);
	UT_ASSERT_EQ(memcmp(&replay, &barrier, sizeof(barrier)), 0);
}

UT_TEST(u51_forward_cancel_supersedes_only_exact_provisional_stale_x_report)
{
	BufferTag tag = make_tag(185);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xcace003), 33);
	GcsStaleXCertPayload report;
	GcsStaleXCertPayload stored_report;
	GcsStaleXCertPayload install;
	GcsStaleXCertPayload fence_ack;
	GcsBlockForwardCancelPayload cancel;
	GcsBlockForwardCancelPayload barrier;
	GcsBlockForwardCancelPayload drifted;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&report, 0, sizeof(report));
	report.request_id = key.request_id;
	report.request_epoch = key.cluster_epoch;
	report.release_cert_nonce = 173;
	report.source_own_generation = 179;
	report.relation_generation = 181;
	report.final_page_scn = (SCN)191;
	report.durable_page_scn = (SCN)193;
	report.tag = tag;
	report.requester_node = (int32)key.origin_node_id;
	report.requester_backend_id = key.requester_backend_id;
	report.master_node = 0;
	report.holder_node = 2;
	report.requester_incarnation = 197;
	report.master_incarnation = 199;
	report.holder_incarnation = 211;
	report.phase = (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT;
	report.reason = (uint8)GCS_STALE_X_CERT_REASON_NOT_RESIDENT;
	report.proof = GCS_STALE_X_PROOF_REPORT_MASK;
	report.transition_id = (uint8)PCM_TRANS_N_TO_S;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 0, &key, &report, &stored_report),
				 (int)GCS_STALE_X_FENCE_INSTALLED);

	memset(&cancel, 0, sizeof(cancel));
	cancel.request_id = key.request_id;
	cancel.request_epoch = key.cluster_epoch;
	cancel.pre_authority_generation = 223;
	cancel.relation_generation = report.relation_generation;
	cancel.expected_pi_watermark_scn = (uint64)report.final_page_scn;
	cancel.requester_incarnation = report.requester_incarnation;
	cancel.master_incarnation = report.master_incarnation;
	cancel.holder_incarnation = report.holder_incarnation;
	cancel.tag = tag;
	cancel.requester_node = report.requester_node;
	cancel.requester_backend_id = report.requester_backend_id;
	cancel.master_node = report.master_node;
	cancel.holder_node = report.holder_node;
	cancel.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_MASTER_TO_HOLDER;
	cancel.reason = (uint8)GCS_FORWARD_CANCEL_REASON_PENDING_X;
	cancel.proof = GCS_FORWARD_CANCEL_PROOF_MASTER_MASK;
	cancel.transition_id = (uint8)PCM_TRANS_N_TO_S;
	cancel.master_holder_capability_generation = 227;

	drifted = cancel;
	drifted.requester_incarnation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &drifted, 229, NULL),
				 (int)GCS_FORWARD_CANCEL_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
					 0, &key, &tag, &stored_report),
				 (int)GCS_STALE_X_FENCE_INSTALLED);

	/* The cancel-wins race atomically replaces only the same attempt's
	 * provisional MISS_REPORT with its ordered requester barrier. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &cancel, 229, &barrier),
				 (int)GCS_FORWARD_CANCEL_INSTALLED);
	UT_ASSERT_EQ((int)barrier.phase,
				 (int)GCS_FORWARD_CANCEL_PHASE_HOLDER_BARRIER);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_lookup(
					 0, &key, &tag, &drifted),
				 (int)GCS_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ(memcmp(&drifted, &barrier, sizeof(barrier)), 0);

	/* Once stale-X advanced past the provisional report, cancellation may
	 * not steal its stronger fence. */
	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_report_prepare(
					 0, &key, &report, &stored_report),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	install = report;
	install.phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_INSTALL;
	install.pre_authority_generation = cancel.pre_authority_generation;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_fence_install(
					 0, &key, &install, &fence_ack),
				 (int)GCS_STALE_X_FENCE_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_holder_install(
					 0, &key, &cancel, 229, NULL),
				 (int)GCS_FORWARD_CANCEL_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_stale_x_holder_lookup(
					 0, &key, &tag, &stored_report),
				 (int)GCS_STALE_X_FENCE_FULL);
}

/* ============================================================
 * U53 — S3-P0-18 restart request-id incarnation ABA.
 *
 *	A requester backend's sequence restarts at 1 with the postmaster, so
 *	the legacy 4-tuple can alias an old DONE-linger entry even though the
 *	requester boot/session identity changed.  A different tag is then
 *	rejected as a key collision; the fresh incarnation must instead own a
 *	new MISS.  The two incarnation constants document the authenticated
 *	identity which the production key/wire path must bind.
 * ============================================================ */
UT_TEST(u53_restart_incarnation_different_tag_is_fresh_miss)
{
	GcsBlockDedupKey old_key = make_key(1, 2, UINT64_C(0x0100010000000058), 0);
	GcsBlockDedupKey fresh_key = make_key(1, 2, UINT64_C(0x0100010000000058), 0);
	BufferTag old_tag = make_tag(1249);
	BufferTag fresh_tag = make_tag(1262);
	GcsBlockDedupEntry cached;
	const uint64 old_requester_incarnation = UINT64_C(838266221071564);
	const uint64 fresh_requester_incarnation = UINT64_C(838266228102000);

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(old_requester_incarnation != fresh_requester_incarnation);
	old_key.origin_boot_incarnation = old_requester_incarnation;
	fresh_key.origin_boot_incarnation = fresh_requester_incarnation;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &old_key, old_tag, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(0, &old_key);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &old_key, &old_tag, 1));

	/* Old code returns VALIDATION_FAIL because requester incarnation is
	 * absent from the dedup key.  A restart-safe identity must register a
	 * new entry without weakening tag/transition collision validation. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &fresh_key, fresh_tag, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}

/* Same ABA with an identical tag is more dangerous: old code silently
 * returns CACHED_REPLY containing the previous postmaster's page/result. */
UT_TEST(u54_restart_incarnation_same_tag_never_replays_old_cache)
{
	GcsBlockDedupKey old_key = make_key(1, 2, UINT64_C(0x0100010000000059), 0);
	GcsBlockDedupKey fresh_key = make_key(1, 2, UINT64_C(0x0100010000000059), 0);
	BufferTag tag = make_tag(2663);
	GcsBlockDedupEntry cached;
	const uint64 old_requester_incarnation = UINT64_C(838266221121996);
	const uint64 fresh_requester_incarnation = UINT64_C(838266230675000);

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(old_requester_incarnation != fresh_requester_incarnation);
	old_key.origin_boot_incarnation = old_requester_incarnation;
	fresh_key.origin_boot_incarnation = fresh_requester_incarnation;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &old_key, tag, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(0, &old_key);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &old_key, &tag, 1));

	/* Old code returns CACHED_REPLY.  A fresh requester incarnation must
	 * never inherit the previous process lifetime's cached result. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &fresh_key, tag, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}

UT_TEST(u55_legacy_restart_boot_change_quarantines_unbound_frames)
{
	const uint64 boot_a = UINT64_C(838266221071564);
	const uint64 boot_b = UINT64_C(838266228102000);
	const TimestampTz t0 = (TimestampTz)100;
	const int64 quarantine_us = 1000;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(!cluster_gcs_block_dedup_origin_boot_admit(
		1, 0, false, t0, quarantine_us));
	UT_ASSERT(cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_a, false, t0, quarantine_us));
	UT_ASSERT(cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_a, false, t0 + 1, quarantine_us));
	UT_ASSERT(!cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_b, false, t0 + 100, quarantine_us));
	UT_ASSERT(!cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_b, false, t0 + 1099, quarantine_us));
	UT_ASSERT(cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_b, false, t0 + 1100, quarantine_us));

	/* A wire-bound new-boot request may proceed immediately, but it still
	 * arms the fence which rejects any unbound late old DATA frame. */
	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_a, false, t0, quarantine_us));
	UT_ASSERT(cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_b, true, t0 + 100, quarantine_us));
	UT_ASSERT(!cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_b, false, t0 + 101, quarantine_us));
	UT_ASSERT(cluster_gcs_block_dedup_origin_boot_admit(
		1, boot_b, false, t0 + 1100, quarantine_us));
}

UT_TEST(u56_debug_exact_probe_distinguishes_requester_boot_incarnations)
{
	GcsBlockDedupKey key_a = make_key(1, 2, UINT64_C(0x0100010000000060), 17);
	GcsBlockDedupKey key_b = key_a;
	GcsBlockDedupKey absent = key_a;
	BufferTag tag_a = make_tag(2671);
	BufferTag tag_b = make_tag(2672);
	GcsBlockDedupEntry cached;
	GcsBlockDedupDebugExactSnapshot snapshot;

	key_a.origin_boot_incarnation = UINT64_C(838266221071564);
	key_b.origin_boot_incarnation = UINT64_C(838266228102000);
	absent.origin_boot_incarnation = UINT64_C(838266299999999);
	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key_a, tag_a, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(0, &key_a);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &key_a, &tag_a, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 1, &key_b, tag_b, 2, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(1, &key_b);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(1, &key_b, &tag_b, 2));

	memset(&snapshot, 0, sizeof(snapshot));
	UT_ASSERT(cluster_gcs_block_dedup_debug_exact(&key_a, &snapshot));
	UT_ASSERT_EQ(snapshot.match_count, 1);
	UT_ASSERT_EQ(snapshot.worker_id, 0);
	UT_ASSERT(memcmp(&snapshot.key, &key_a, sizeof(key_a)) == 0);
	UT_ASSERT(memcmp(&snapshot.tag, &tag_a, sizeof(tag_a)) == 0);
	UT_ASSERT_EQ(snapshot.entry_kind, GCS_BLOCK_DEDUP_ENTRY_GENERIC);
	UT_ASSERT_EQ(snapshot.transition_id, 1);
	UT_ASSERT_EQ(snapshot.status, GCS_BLOCK_REPLY_GRANTED);
	UT_ASSERT(snapshot.completed);
	UT_ASSERT(snapshot.done);
	UT_ASSERT_EQ(snapshot.miss_count, 2);
	UT_ASSERT_EQ(snapshot.hit_count, 0);
	UT_ASSERT_EQ(snapshot.collision_count, 0);
	UT_ASSERT_EQ(snapshot.done_marked_count, 2);
	UT_ASSERT_EQ(snapshot.done_mismatch_count, 0);

	memset(&snapshot, 0, sizeof(snapshot));
	UT_ASSERT(cluster_gcs_block_dedup_debug_exact(&key_b, &snapshot));
	UT_ASSERT_EQ(snapshot.match_count, 1);
	UT_ASSERT_EQ(snapshot.worker_id, 1);
	UT_ASSERT(memcmp(&snapshot.key, &key_b, sizeof(key_b)) == 0);
	UT_ASSERT(memcmp(&snapshot.tag, &tag_b, sizeof(tag_b)) == 0);
	UT_ASSERT_EQ(snapshot.transition_id, 2);
	UT_ASSERT(snapshot.completed);
	UT_ASSERT(snapshot.done);

	memset(&snapshot, 0x7f, sizeof(snapshot));
	UT_ASSERT(!cluster_gcs_block_dedup_debug_exact(&absent, &snapshot));
	UT_ASSERT_EQ(snapshot.match_count, 0);
	UT_ASSERT_EQ(snapshot.worker_id, -1);
	UT_ASSERT(memcmp(&snapshot.key, &absent, sizeof(absent)) == 0);
}

UT_TEST(u57_forward_phase_census_is_atomic_across_shards)
{
	GcsBlockDedupEntry *cancelling;
	GcsBlockDedupEntry *forwarded;
	GcsBlockForwardPhaseCensus census;

	memset(&census, 0x7f, sizeof(census));
	UT_ASSERT(!cluster_gcs_block_dedup_forward_phase_census(&census));
	UT_ASSERT(!census.valid);
	UT_ASSERT_EQ(census.marker_count, 0);
	UT_ASSERT_EQ(census.forwarded_count, 0);
	UT_ASSERT_EQ(census.cancelling_count, 0);
	UT_ASSERT_EQ(census.invalid_count, 0);

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	forwarded = (GcsBlockDedupEntry *)fake_htab[0].entries[0];
	cancelling = (GcsBlockDedupEntry *)fake_htab[1].entries[0];
	fake_htab[0].count = 1;
	fake_htab[1].count = 1;

	memset(forwarded, 0, sizeof(*forwarded));
	forwarded->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	forwarded->status = GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	forwarded->sf_flags = GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
						  | GCS_BLOCK_FORWARD_MARK_FORWARDED;

	memset(cancelling, 0, sizeof(*cancelling));
	cancelling->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	cancelling->status = GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER;
	cancelling->sf_flags = GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
						   | GCS_BLOCK_FORWARD_MARK_CANCELLING;

	fake_allow_multi_lwlock = true;
	memset(&census, 0x7f, sizeof(census));
	UT_ASSERT(cluster_gcs_block_dedup_forward_phase_census(&census));
	UT_ASSERT(census.valid);
	UT_ASSERT_EQ(census.marker_count, 2);
	UT_ASSERT_EQ(census.forwarded_count, 1);
	UT_ASSERT_EQ(census.cancelling_count, 1);
	UT_ASSERT_EQ(census.invalid_count, 0);
	UT_ASSERT_EQ(fake_lwlock_acquire_count, 2);
	UT_ASSERT_EQ(fake_lwlock_shared_count, 2);
	UT_ASSERT_EQ(fake_lwlock_exclusive_count, 0);
	UT_ASSERT_EQ(fake_lwlock_release_count, 2);
	UT_ASSERT_EQ(fake_lwlock_release_underflow_count, 0);
	UT_ASSERT_EQ(fake_lwlock_held_count, 0);
	UT_ASSERT_EQ(fake_lwlock_max_held_count, 2);
	UT_ASSERT(fake_lwlock_acquire_order[0] != fake_lwlock_acquire_order[1]);
	UT_ASSERT(fake_lwlock_release_order[0] == fake_lwlock_acquire_order[1]);
	UT_ASSERT(fake_lwlock_release_order[1] == fake_lwlock_acquire_order[0]);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 2);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 2);
	UT_ASSERT_EQ(fake_hash_seq_held_count[0], 2);
	UT_ASSERT_EQ(fake_hash_seq_held_count[1], 2);
	UT_ASSERT(!cluster_gcs_block_dedup_forward_phase_census(NULL));
	UT_ASSERT_EQ(fake_lwlock_acquire_count, 2);
	UT_ASSERT_EQ(fake_lwlock_release_count, 2);
}

UT_TEST(u58_forward_phase_census_ignores_cache_and_counts_malformed_markers)
{
	GcsBlockDedupEntry *generic_cache;
	GcsBlockDedupEntry *pcm_x_cache;
	GcsBlockDedupEntry *bad_phase;
	GcsBlockDedupEntry *bad_kind;
	GcsBlockDedupEntry *bad_status;
	GcsBlockForwardPhaseCensus census;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	fake_htab[0].count = 2;
	fake_htab[1].count = 3;
	generic_cache = (GcsBlockDedupEntry *)fake_htab[0].entries[0];
	pcm_x_cache = (GcsBlockDedupEntry *)fake_htab[0].entries[1];
	bad_phase = (GcsBlockDedupEntry *)fake_htab[1].entries[0];
	bad_kind = (GcsBlockDedupEntry *)fake_htab[1].entries[1];
	bad_status = (GcsBlockDedupEntry *)fake_htab[1].entries[2];

	memset(generic_cache, 0, sizeof(*generic_cache));
	generic_cache->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	memset(pcm_x_cache, 0, sizeof(*pcm_x_cache));
	pcm_x_cache->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE;

	memset(bad_phase, 0, sizeof(*bad_phase));
	bad_phase->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	bad_phase->status = GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	bad_phase->sf_flags = GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
						  | GCS_BLOCK_FORWARD_MARK_PREPARED;

	memset(bad_kind, 0, sizeof(*bad_kind));
	bad_kind->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE;
	bad_kind->status = GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	bad_kind->sf_flags = GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
						 | GCS_BLOCK_FORWARD_MARK_FORWARDED;

	memset(bad_status, 0, sizeof(*bad_status));
	bad_status->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	bad_status->status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
	bad_status->sf_flags = GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
						   | GCS_BLOCK_FORWARD_MARK_FORWARDED;

	fake_allow_multi_lwlock = true;
	UT_ASSERT(cluster_gcs_block_dedup_forward_phase_census(&census));
	UT_ASSERT(census.valid);
	UT_ASSERT_EQ(census.marker_count, 3);
	UT_ASSERT_EQ(census.forwarded_count, 0);
	UT_ASSERT_EQ(census.cancelling_count, 0);
	UT_ASSERT_EQ(census.invalid_count, 3);
}

int
main(void)
{
	UT_PLAN(60);
	UT_RUN(u57_forward_phase_census_is_atomic_across_shards);
	UT_RUN(u58_forward_phase_census_ignores_cache_and_counts_malformed_markers);
	UT_RUN(u1_per_worker_isolation);
	UT_RUN(u2_dedup_lifecycle_per_shard);
	UT_RUN(u3_counters_sum_across_shards);
	UT_RUN(u4_cleanup_on_node_dead_all_shards);
	UT_RUN(u5_out_of_range_worker_fail_closed);
	UT_RUN(u6_n1_only_shard0);
	UT_RUN(u7_per_shard_cap_full);
	UT_RUN(u8_ttl_sweep_all_shards);
	UT_RUN(u9_backend_exit_cleanup_all_shards);
	UT_RUN(u10_remove_reopens_in_flight_entry);
	UT_RUN(u10b_exact_remove_refuses_identity_or_phase_drift);
	UT_RUN(u11_read_image_marker_classifies_forwarded);
	UT_RUN(u12_ttl_covers_reply_timeout_lifetime);
	UT_RUN(u13_mark_done_truth_table);
	UT_RUN(u14_pinned_ttl_wire_hint_and_no_guc_reread);
	UT_RUN(u15_done_linger_beats_full_lifetime);
	UT_RUN(u16_capability_routing_truth_table);
	UT_RUN(u17_pcm_x_binding_layout_tracks_forward_identity_growth);
	UT_RUN(u18_pcm_x_stage_duplicate_and_generic_overwrite_refused);
	UT_RUN(u19_pcm_x_entry_isolated_from_generic_lifecycle);
	UT_RUN(u20_pcm_x_entry_survives_generic_gc_and_retires_exactly);
	UT_RUN(u21_pcm_x_stage_full_is_fail_closed);
	UT_RUN(u22_pcm_x_reserved_entry_waits_for_exact_release);
	UT_RUN(u23_pcm_x_materialize_validation_is_fail_closed_and_byte_stable);
	UT_RUN(u24_pcm_x_namespace_cannot_register_as_generic);
	UT_RUN(u25_pcm_x_work_prefers_reserved_and_marks_ready_staged);
	UT_RUN(u26_pcm_x_ready_rearm_is_exact);
	UT_RUN(u27_pcm_x_reserved_work_scan_rotates);
	UT_RUN(u28_pcm_x_idle_hint_avoids_empty_rescan_and_reserve_rearms);
	UT_RUN(u29_pcm_x_materialized_bytes_require_explicit_ready_publication);
	UT_RUN(u30_pcm_x_owner_restart_audit_detects_and_retains_evidence);
	UT_RUN(u31_pcm_x_work_classes_alternate_when_both_remain_runnable);
	UT_RUN(u32_pcm_x_staged_marker_rolls_back_exactly);
	UT_RUN(u33_pending_x_arm_terminates_inflight_legacy_s_exactly);
	UT_RUN(u34_pending_x_new_reader_exact_deny_precedes_cached_shortcut);
	UT_RUN(u35_pcm_x_commit_pending_rotates_to_independent_reserved_tag);
	UT_RUN(u36_pcm_x_drain_cleanup_is_replayable_until_exact_retire);
	UT_RUN(u37_pcm_x_finish_error_preserves_exact_materialized_evidence);
	UT_RUN(u38_forward_marker_prepare_claim_finish_is_exact_and_serial);
	UT_RUN(u39_admitted_forward_survives_local_cleanup_without_holder_fence_ack);
	UT_RUN(u40_stale_x_holder_fence_is_exact_replayable_and_not_gc_owned);
	UT_RUN(u41_stale_x_master_report_serializes_with_send_finish_and_ack);
	UT_RUN(u42_stale_x_holder_retire_replays_lost_ack_and_reopens_small_cap);
	UT_RUN(u43_stale_x_release_journal_is_state_and_residency_exact);
	UT_RUN(u44_checkpoint_durable_seal_is_residency_and_redirty_exact);
	UT_RUN(u45_release_journal_capacity_fails_before_evidence_loss);
	UT_RUN(u46_eviction_gate_blocks_tag_and_source_until_exact_ack);
	UT_RUN(u47_stale_x_relation_generation_is_always_on_exact_and_bounded);
	UT_RUN(u48_relation_supersede_keeps_old_holder_fence_fail_closed);
	UT_RUN(u49_forward_cancel_master_transition_is_exact_and_replayable);
	UT_RUN(u59_forward_cancel_cross_plane_early_ack_replays_to_terminal);
	UT_RUN(u50_forward_cancel_holder_barrier_tombstone_is_exact);
	UT_RUN(u51_forward_cancel_supersedes_only_exact_provisional_stale_x_report);
	UT_RUN(u52_direct_land_same_identity_retry_can_downgrade_to_generic);
	UT_RUN(u53_restart_incarnation_different_tag_is_fresh_miss);
	UT_RUN(u54_restart_incarnation_same_tag_never_replays_old_cache);
	UT_RUN(u55_legacy_restart_boot_change_quarantines_unbound_frames);
	UT_RUN(u56_debug_exact_probe_distinguishes_requester_boot_incarnations);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
