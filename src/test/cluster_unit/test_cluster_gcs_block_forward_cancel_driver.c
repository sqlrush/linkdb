/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_forward_cancel_driver.c
 *	  Production-linked RED contract for the S3-P0-09 forward-cancel drivers.
 *
 * Runtime ground truth this contract encodes (r58 R2 pristine, 2026-07-26):
 * a 4-node smoke ended with port6034 FORWARDED=2, port6035/6036 CANCELLING=1
 * each, and 9 non-terminal raw ltags.  n2 reached the exact ticket through a
 * real master drive within ~3 s and got PCM_X_QUEUE_NOT_READY;  90 s later
 * the same ticket was still state4/node0/ticket1.  The exact ticket WAS
 * selected -- it simply never converged and never explained itself.
 *
 * ==========================================================================
 * TIER MAP -- what each tier can and cannot reach
 * ==========================================================================
 *
 * The periodic forward-cancel driver is split across two linkage domains and
 * this file is split the same way.  Mixing them is what produced the previous
 * round's unsatisfiable assertions, so the boundary is stated up front.
 *
 * Tier-1 (default target, runs today).  Links the REAL
 * cluster_gcs_block_dedup.o + cluster_lms_shard.o, and compiles the REAL
 * requester replay policy out of src/backend/cluster/cluster_gcs_block_internal.h.
 * That header contains ZERO extern declarations -- every helper in it is
 * `static inline`, so Tier-1 exercises the policy *source text* compiled into
 * this binary, plus the genuine master-side dedup symbols.  Reachable:
 *	  - the 5-valued park/mark/finish/next_action policy seam
 *	  - the master marker lifecycle FORWARDED -> CANCELLING -> type-67 ACK
 *	  - TTL / backend-exit / node-dead interaction with a live CANCELLING cell
 *
 * Tier-1 CANNOT reach the periodic orchestration.  Every function that turns
 * a policy verdict into an action is `static` in cluster_gcs_block.c:
 *	  gcs_block_forward_cancel_replay_park        decl :477   defn :17133
 *	  gcs_block_forward_cancel_replay_drive       decl :479   defn :17240
 *	  gcs_block_forward_cancel_replay_tick        decl :481   defn :17366
 *	  gcs_block_forward_cancel_no_slot_cleanup    (no decl)   defn :17233
 * Linking cluster_gcs_block.o exposes none of them and drags in 308 residual
 * undefined symbols, so it is deliberately not linked.  Any assertion about
 * arm selection, admission gating, lock order, slot reclamation or counter
 * exposure is therefore a TIER-2 assertion by construction, not a choice.
 *
 * Tier-2 (target test_cluster_gcs_block_forward_cancel_driver_tier2).
 * Requires the not-yet-existing cluster_gcs_block_forward_cancel_driver.[ch]
 * and does not build today by design.  It is the real acceptance gate for
 * S3-P0-09:  the eight typed outcomes of spec §6.3, the drive() orchestration,
 * and the three HIGH#1 capabilities (enumeration / real orphan path /
 * autonomous safe termination).  The required API is specified verbatim in the
 * contract block at the end of this file.
 *
 * ==========================================================================
 * GROUP MAP (stable letters -- previous rounds drifted)
 * ==========================================================================
 *	  A  a1-a6   requester replay policy seam (header-inline, Tier-1)
 *	  B  b1-b5   master exact ticket lifecycle (real dedup.o, Tier-1)
 *	  D  d0-d6   live forward-leg survivability vs the retirement paths (Tier-1)
 *	  T2 t2_01-t2_17  typed driver outcomes + orchestration (Tier-2)
 *
 * Tier-1 reds today: b3, d0, d1, d2.  All four are behavioural failures of
 * currently-shipping code, not link errors and not missing objects.
 *
 * d0 and d1 are the two live phases of the SAME hole -- mark_done() has no
 * phase leg -- and they assert in the same direction (the DONE must be
 * refused).  d2 is the consequence.  Nothing in this file asserts that
 * mark_done() succeeds on a live marker;  the previous round did, in a test
 * that used the very P0 d1 forbids as its own fixture, and the two were
 * mutually unsatisfiable.  That test is now Tier-2 t2_16, where the contract
 * it was reaching for (distinct typed outcomes) is actually expressible.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_forward_cancel_driver.c
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


/* ============================================================
 * PG runtime + GUC stubs (mirrors test_cluster_gcs_block_dedup.c, which
 * already solved the standalone symbol surface for this object).
 * ============================================================ */

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

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_lms_workers = 2;
int NBuffers = 8;
int MaxConnections = 1;
int cluster_gcs_block_dedup_max_entries = 8;
int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_reply_timeout_ms = 5000;
int MyBackendId = 1;
bool IsUnderPostmaster = true;


/* ------------------------------------------------------------
 * N-shard fake HTAB.
 *
 * Slots are STABLE:  a removal clears an in-use bit and never shifts a
 * neighbour.  That is not cosmetic.  Both
 * cluster_gcs_block_dedup_cleanup_on_backend_exit() (dedup.c:5194-5202) and
 * cluster_gcs_block_dedup_cleanup_on_node_dead() (dedup.c:5229-5236) call
 * hash_search(HASH_REMOVE) on the entry hash_seq_search() just handed them.
 * dynahash supports that exact pattern.  An array-compacting fake would slide
 * the successor into the slot the cursor has already passed, silently skipping
 * it -- so a multi-entry cleanup fixture would under-remove and the retention
 * tests below would pass for the wrong reason.
 * ------------------------------------------------------------ */

#define FAKE_DEDUP_CAP 8

typedef struct FakeDedupShardHtab {
	char entries[FAKE_DEDUP_CAP][sizeof(GcsBlockDedupEntry)];
	bool slot_used[FAKE_DEDUP_CAP];
	long count;
	long max_entries;
	Size keysize;
	Size entrysize;
} FakeDedupShardHtab;

static FakeDedupShardHtab fake_htab[CLUSTER_LMS_MAX_WORKERS * 4];
static int fake_htab_init_seq;

static union {
	uint64 force_align;
	char data[16384];
} fake_dedup_struct;
static bool fake_dedup_struct_found;

static int fake_lwlock_held_count;
static bool fake_allow_multi_lwlock;

/* Monotonic clock so TTL-style aging is expressible without wall time. */
static TimestampTz fake_now = 1000;


static void
reset_fake_dedup(int n_workers, int max_entries)
{
	memset(fake_htab, 0, sizeof(fake_htab));
	fake_htab_init_seq = 0;
	memset(&fake_dedup_struct, 0, sizeof(fake_dedup_struct));
	fake_dedup_struct_found = false;
	fake_lwlock_held_count = 0;
	fake_allow_multi_lwlock = false;
	fake_now = 1000;

	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_lms_workers = n_workers;
	cluster_gcs_block_dedup_max_entries = max_entries;

	(void)cluster_gcs_block_dedup_shmem_size();
	cluster_gcs_block_dedup_shmem_init();
}

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
	long free_slot = -1;
	long i;
	char *entry;

	Assert(h != NULL);
	Assert(h->keysize > 0);

	for (i = 0; i < FAKE_DEDUP_CAP; i++) {
		if (!h->slot_used[i]) {
			if (free_slot < 0)
				free_slot = i;
			continue;
		}
		if (memcmp(h->entries[i], keyPtr, h->keysize) != 0)
			continue;

		if (foundPtr != NULL)
			*foundPtr = true;
		if (action == HASH_REMOVE) {
			/* dynahash leaves the removed element readable and moves no
			 * neighbour;  model exactly that. */
			h->slot_used[i] = false;
			h->count--;
		}
		return h->entries[i];
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;

	if (free_slot < 0 || h->count >= h->max_entries) {
		if (action == HASH_ENTER_NULL)
			return NULL;
		/* Real dynahash raises ERROR here.  Fail closed rather than
		 * overrunning the slot array. */
		fprintf(stderr, "# fake hash_search: HASH_ENTER on a full table\n");
		abort();
	}

	entry = h->entries[free_slot];
	memset(entry, 0, h->entrysize);
	memcpy(entry, keyPtr, h->keysize);
	h->slot_used[free_slot] = true;
	h->count++;
	return entry;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeDedupShardHtab *h = (FakeDedupShardHtab *)status->hashp;

	while (status->curBucket < (uint32)FAKE_DEDUP_CAP) {
		uint32 slot = status->curBucket++;

		if (h->slot_used[slot])
			return h->entries[slot];
	}
	return NULL;
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	Assert(fake_allow_multi_lwlock || fake_lwlock_held_count == 0);
	fake_lwlock_held_count++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	if (fake_lwlock_held_count > 0)
		fake_lwlock_held_count--;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}

/*
 * ereport() stubs.
 *
 * Returning false for every elevel used to leave elevel >= ERROR falling
 * through the caller's `if (errstart(...)) {...}` into pg_unreachable(), i.e.
 * undefined behaviour rather than a diagnosable failure.  No test currently
 * drives an ERROR path, but a future one must fail closed, not unpredictably.
 */
static bool
fake_errstart(int elevel)
{
	if (elevel >= ERROR) {
		fprintf(stderr, "# unexpected ereport(elevel=%d) in a pure unit test\n", elevel);
		abort();
	}
	return false;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return fake_errstart(elevel);
}

bool
errstart_cold(int elevel, const char *domain pg_attribute_unused())
{
	return fake_errstart(elevel);
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
{}


/* ============================================================
 * Test helpers.
 * ============================================================ */

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

/*
 * Build a requester-side HOLDER_BARRIER payload that the production shape
 * validator (gcs_block_forward_cancel_replay_barrier_shape_valid) accepts.
 */
static GcsBlockForwardCancelPayload
make_barrier(uint64 request_id, uint64 epoch, int32 backend_id, uint32 blockno)
{
	GcsBlockForwardCancelPayload b;

	memset(&b, 0, sizeof(b));
	b.request_id = request_id;
	b.request_epoch = epoch;
	b.pre_authority_generation = 73;
	b.relation_generation = 101;
	b.expected_pi_watermark_scn = 79;
	b.requester_incarnation = 83;
	b.master_incarnation = 89;
	b.holder_incarnation = 97;
	b.tag = make_tag(blockno);
	b.requester_node = 0;
	b.requester_backend_id = backend_id;
	b.master_node = 1;
	b.holder_node = 2;
	b.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_HOLDER_BARRIER;
	b.reason = (uint8)GCS_FORWARD_CANCEL_REASON_PENDING_X;
	b.proof = GCS_FORWARD_CANCEL_PROOF_BARRIER_MASK;
	b.transition_id = (uint8)PCM_TRANS_N_TO_S;
	b.master_holder_capability_generation = 103;
	b.holder_requester_capability_generation = 107;
	b.requester_master_capability_generation = 0;
	return b;
}

/*
 * Register a plain GENERIC in-flight entry -- no forward marker, so every
 * legacy TTL / backend-exit / node-dead sweep is allowed to reclaim it.
 * Used as scan-neighbour ballast in the cleanup fixtures.
 */
static void
register_plain(int worker_id, const GcsBlockDedupKey *key, BufferTag tag)
{
	GcsBlockDedupEntry entry;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 worker_id, key, tag, PCM_TRANS_N_TO_S, 0, false, &entry),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}

/* Probe whether a key is still resident, without disturbing a live marker. */
static bool
entry_resident(int worker_id, const GcsBlockDedupKey *key, BufferTag tag)
{
	GcsBlockDedupEntry entry;

	memset(&entry, 0, sizeof(entry));
	return cluster_gcs_block_dedup_lookup_or_register(worker_id, key, tag, PCM_TRANS_N_TO_S, 0,
													  false, &entry)
		   != GCS_BLOCK_DEDUP_MISS_REGISTERED;
}

/*
 * Drive the REAL master dedup APIs from empty state to a live FORWARDED
 * marker.  Mirrors the production master path exactly:  register, prepare
 * with boot identity, claim the send, finish the send.
 */
static void
install_forwarded(int worker_id, const GcsBlockDedupKey *key, BufferTag tag,
				  PcmAuthoritySnapshot *authority_out, GcsBlockForwardBootIdentity *boot_out,
				  GcsBlockForwardMarker *marker_out)
{
	GcsBlockDedupEntry entry;
	PcmAuthoritySnapshot authority;
	GcsBlockForwardPayload forward;
	GcsBlockForwardBootIdentity boot;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 worker_id, key, tag, PCM_TRANS_N_TO_S, 0, false, &entry),
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
	forward.request_id = key->request_id;
	forward.epoch = key->cluster_epoch;
	forward.tag = tag;
	forward.original_requester_node = (int32)key->origin_node_id;
	forward.requester_backend_id = key->requester_backend_id;
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
					 worker_id, key, &tag, PCM_TRANS_N_TO_S, 2, 0,
					 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &authority, &forward, &boot, &entry),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);

	if (marker_out != NULL)
		*marker_out = entry.payload_meta.forward_marker;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_claim_exact(
					 worker_id, key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
					 &entry.payload_meta.forward_marker, GCS_BLOCK_FORWARD_MARK_PREPARED, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_send_finish_exact(
					 worker_id, key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
					 &entry.payload_meta.forward_marker, NULL),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);

	if (authority_out != NULL)
		*authority_out = authority;
	if (boot_out != NULL)
		*boot_out = boot;
}

/* Install a FORWARDED marker and take it to a live CANCELLING marker via the
 * real queue-kind denial path. */
static void
install_cancelling(int worker_id, const GcsBlockDedupKey *key, BufferTag tag,
				   GcsBlockForwardMarker *marker_out)
{
	GcsBlockDedupEntry entry;

	install_forwarded(worker_id, key, tag, NULL, NULL, marker_out);
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(worker_id, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
}

/* Take a phase census with the multi-shard lock order the production
 * accessor uses. */
static bool
census(GcsBlockForwardPhaseCensus *out)
{
	bool ok;

	fake_allow_multi_lwlock = true;
	ok = cluster_gcs_block_dedup_forward_phase_census(out);
	fake_allow_multi_lwlock = false;
	return ok;
}


/* ============================================================
 * A -- Requester replay policy seam.
 *
 * Every function here is `static inline` in cluster_gcs_block_internal.h, so
 * these tests exercise the real policy source text.  They deliberately assert
 * ONLY things the asserted function can decide from its own arguments:
 * next_action() receives a const GcsBlockForwardCancelReplayObservation * and
 * nothing else, so it can never be asked to mutate, evict or bound a ledger.
 * ============================================================ */

#define LEDGER_MAX 64

UT_TEST(a1_requester_ledger_park_release_ack_finish_is_the_only_legal_order)
{
	GcsBlockForwardCancelReplayEntry ledger[LEDGER_MAX];
	GcsBlockForwardCancelPayload barrier = make_barrier(0xA1, 29, 7, 101);
	size_t slot = LEDGER_MAX;

	memset(ledger, 0, sizeof(ledger));

	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, LEDGER_MAX, &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	UT_ASSERT(slot < LEDGER_MAX);
	UT_ASSERT(ledger[slot].in_use);
	UT_ASSERT_EQ((int)ledger[slot].phase, (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE);

	/* finish before ACK_STAGED must be refused. */
	UT_ASSERT(!gcs_block_forward_cancel_replay_finish_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT(ledger[slot].in_use);

	UT_ASSERT(
		gcs_block_forward_cancel_replay_mark_release_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT_EQ((int)ledger[slot].phase, (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED);

	UT_ASSERT(gcs_block_forward_cancel_replay_mark_ack_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT_EQ((int)ledger[slot].phase, (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED);

	UT_ASSERT(gcs_block_forward_cancel_replay_finish_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT(!ledger[slot].in_use);
}

UT_TEST(a2_requester_ledger_release_and_ack_rejection_stay_reentrant)
{
	GcsBlockForwardCancelReplayEntry ledger[LEDGER_MAX];
	GcsBlockForwardCancelPayload barrier = make_barrier(0xA2, 29, 7, 102);
	GcsBlockForwardCancelPayload wrong;
	GcsBlockForwardCancelReplayEntry before;
	size_t slot = LEDGER_MAX;

	memset(ledger, 0, sizeof(ledger));
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, LEDGER_MAX, &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);

	/*
	 * mark_release_exact() DOES receive the ledger, so a byte-exact
	 * comparison across a rejected call is a real statement about it.
	 */
	wrong = barrier;
	wrong.pre_authority_generation++;
	before = ledger[slot];
	UT_ASSERT(
		!gcs_block_forward_cancel_replay_mark_release_exact(ledger, LEDGER_MAX, slot, &wrong));
	UT_ASSERT_EQ(memcmp(&before, &ledger[slot], sizeof(before)), 0);

	/* The exact release then still succeeds, and is idempotent. */
	UT_ASSERT(
		gcs_block_forward_cancel_replay_mark_release_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT(
		gcs_block_forward_cancel_replay_mark_release_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT_EQ((int)ledger[slot].phase, (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED);

	/* One rejected ACK leaves the entry untouched, then the exact ACK works. */
	before = ledger[slot];
	UT_ASSERT(!gcs_block_forward_cancel_replay_mark_ack_exact(ledger, LEDGER_MAX, slot, &wrong));
	UT_ASSERT_EQ(memcmp(&before, &ledger[slot], sizeof(before)), 0);
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_ack_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_ack_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT_EQ((int)ledger[slot].phase, (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED);
}

UT_TEST(a3_requester_ledger_ack_before_release_is_refused_not_reordered)
{
	GcsBlockForwardCancelReplayEntry ledger[LEDGER_MAX];
	GcsBlockForwardCancelPayload barrier = make_barrier(0xA3, 29, 7, 103);
	GcsBlockForwardCancelReplayEntry before;
	size_t slot = LEDGER_MAX;

	memset(ledger, 0, sizeof(ledger));
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, LEDGER_MAX, &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);

	/*
	 * S->N release (CONTROL) and the type-67 ACK (DATA) travel on different
	 * planes, so the ACK can arrive first.  From PARKED it must be refused
	 * with zero mutation -- accepting it would strand the S right.
	 */
	before = ledger[slot];
	UT_ASSERT(!gcs_block_forward_cancel_replay_mark_ack_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT_EQ(memcmp(&before, &ledger[slot], sizeof(before)), 0);

	/* Re-parking the same barrier is a DUPLICATE, never a second slot. */
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, LEDGER_MAX, &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE);
	UT_ASSERT_EQ((int)ledger[slot].phase, (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE);

	/* Then the normal order still completes. */
	UT_ASSERT(
		gcs_block_forward_cancel_replay_mark_release_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT(gcs_block_forward_cancel_replay_mark_ack_exact(ledger, LEDGER_MAX, slot, &barrier));
	UT_ASSERT(gcs_block_forward_cancel_replay_finish_exact(ledger, LEDGER_MAX, slot, &barrier));
}

/*
 * next_action() is a pure function of one observation.  These four inputs are
 * the complete RETAIN set reachable from a PARKED entry, and pinning them is
 * what makes the Tier-2 arm-selection assertions unambiguous.
 *
 * NOTE (was a4's over-claim in the previous round):  this test does NOT assert
 * that the calls are "byte-exact noops" on the ledger.  next_action() has no
 * ledger parameter, so such a memcmp is true by type signature, not by
 * product behaviour, and asserting it would manufacture a passing test that
 * proves nothing.
 */
UT_TEST(a4_next_action_retains_on_the_four_blocking_observations)
{
	GcsBlockForwardCancelReplayEntry ledger[LEDGER_MAX];
	GcsBlockForwardCancelPayload barrier = make_barrier(0xA4, 29, 7, 104);
	GcsBlockForwardCancelReplayObservation obs;
	size_t slot = LEDGER_MAX;

	memset(ledger, 0, sizeof(ledger));
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, LEDGER_MAX, &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);

	/* (1) identity drift -> RETRY verdict. */
	memset(&obs, 0, sizeof(obs));
	obs.identity_verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETRY;
	/* Mirror the REAL parked ledger state; never assert a phase of our own. */
	obs.phase = (GcsBlockForwardCancelReplayPhase)ledger[slot].phase;
	obs.local_mode = PCM_LOCK_MODE_N;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN);

	/* (2) newer request active on the same tag. */
	memset(&obs, 0, sizeof(obs));
	obs.identity_verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT;
	obs.phase = (GcsBlockForwardCancelReplayPhase)ledger[slot].phase;
	obs.local_mode = PCM_LOCK_MODE_N;
	obs.newer_request_active = true;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN);

	/* (3) GRANT_PENDING reservation still staged. */
	memset(&obs, 0, sizeof(obs));
	obs.identity_verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT;
	obs.phase = (GcsBlockForwardCancelReplayPhase)ledger[slot].phase;
	obs.local_mode = PCM_LOCK_MODE_N;
	obs.grant_pending = true;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN);

	/* (4) local X -- the requester already won a stronger mode. */
	memset(&obs, 0, sizeof(obs));
	obs.identity_verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT;
	obs.phase = (GcsBlockForwardCancelReplayPhase)ledger[slot].phase;
	obs.local_mode = PCM_LOCK_MODE_X;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN);
}

/*
 * The terminal arm EXISTS in policy.  A RETIRED identity yields DISCARD
 * (cluster_gcs_block_internal.h:211-213), and the two productive arms are
 * reachable from PARKED / RELEASE_STAGED.
 *
 * This is the setup that makes Tier-2 t2_14 unambiguous:  the previous round
 * claimed "next_action has no bounded-attempt arm", which is false.  The real
 * defect is one layer up -- gcs_block_forward_cancel_replay_drive()
 * (cluster_gcs_block.c:17357-17361) folds DISCARD into RETAIN/default and
 * `return`s, so the verdict below is computed and then dropped on the floor
 * and the ledger slot is never reclaimed.  That is not expressible here
 * because drive() is static;  it is asserted in Tier-2.
 */
UT_TEST(a5_next_action_emits_discard_for_a_retired_identity)
{
	GcsBlockForwardCancelReplayObservation obs;

	memset(&obs, 0, sizeof(obs));
	obs.identity_verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETIRED;
	obs.phase = GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE;
	obs.local_mode = PCM_LOCK_MODE_N;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DISCARD);

	/* RETIRED wins over every other input, so DISCARD is never masked. */
	obs.newer_request_active = true;
	obs.grant_pending = true;
	obs.local_mode = PCM_LOCK_MODE_X;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DISCARD);

	/* The two productive arms, for contrast. */
	memset(&obs, 0, sizeof(obs));
	obs.identity_verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT;
	obs.phase = GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE;
	obs.local_mode = PCM_LOCK_MODE_N;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_STAGE_RELEASE);
	obs.local_mode = PCM_LOCK_MODE_S;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DROP_S);
	obs.phase = GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED;
	obs.local_mode = PCM_LOCK_MODE_N;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_next_action(&obs),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_STAGE_ACK);
}

/*
 * The admission seam is 5-valued and every value is reachable.  Recording
 * that here is what proves the Tier-2 H1 assertion is about a real loss of
 * information rather than a missing feature:
 * gcs_block_forward_cancel_replay_park() (cluster_gcs_block.c:17148-17149)
 * collapses {INVALID, COLLISION, FULL} AND the ClusterGcsBlock == NULL
 * early-out into a single `false`, and BOTH of its callers -- the requester
 * PG_CATCH at cluster_gcs_block.c:2947 and gcs_block_forward_cancel_no_slot_cleanup
 * at :17236 -- discard even that one bit with `(void)`.
 *
 * So five structurally distinct admission outcomes reach production as
 * nothing at all.  Rule 17 requires a capacity/degradation event to be
 * counted;  Tier-2 t2_12 asserts the counters.
 */
UT_TEST(a6_ledger_admission_is_five_valued_before_the_bool_wrapper_erases_it)
{
	GcsBlockForwardCancelReplayEntry ledger[4];
	GcsBlockForwardCancelPayload barrier;
	GcsBlockForwardCancelPayload collide;
	GcsBlockForwardCancelPayload malformed;
	size_t slot = lengthof(ledger);
	int i;

	memset(ledger, 0, sizeof(ledger));

	/* INVALID: shape rejected before any slot is touched. */
	malformed = make_barrier(0xA6, 29, 7, 105);
	malformed.request_id = 0;
	UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_ledger_park(ledger, lengthof(ledger),
																  &malformed, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_INVALID);

	/* PARKED, then DUPLICATE on the byte-identical replay. */
	barrier = make_barrier(0xA6, 29, 7, 105);
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, lengthof(ledger), &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, lengthof(ledger), &barrier, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE);

	/* COLLISION: same replay key, drifted payload -- must not overwrite. */
	collide = barrier;
	collide.pre_authority_generation++;
	UT_ASSERT_EQ(
		(int)gcs_block_forward_cancel_replay_ledger_park(ledger, lengthof(ledger), &collide, &slot),
		(int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_COLLISION);
	UT_ASSERT_EQ(memcmp(&ledger[slot].barrier, &barrier, sizeof(barrier)), 0);

	/* FULL: bounded lot, correctly refusing rather than evicting. */
	for (i = 1; i < (int)lengthof(ledger); i++) {
		GcsBlockForwardCancelPayload filler
			= make_barrier((uint64)(0xA600 + i), 29, 7, (uint32)(200 + i));

		UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_ledger_park(ledger, lengthof(ledger),
																	  &filler, &slot),
					 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	}
	{
		GcsBlockForwardCancelPayload fresh = make_barrier(0xA6FF, 29, 7, 999);

		UT_ASSERT_EQ((int)gcs_block_forward_cancel_replay_ledger_park(ledger, lengthof(ledger),
																	  &fresh, &slot),
					 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_FULL);
	}
}


/* ============================================================
 * B -- Master exact ticket, driven through the REAL dedup APIs.
 * ============================================================ */

UT_TEST(b1_real_dedup_apis_move_forwarded_to_cancelling)
{
	BufferTag tag = make_tag(301);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xB1), 29);
	GcsBlockDedupEntry entry;
	GcsBlockForwardMarker marker;
	GcsBlockForwardCancelPayload cancel;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_forwarded(0, &key, tag, NULL, NULL, &marker);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);

	/* The canonical forward bytes survive the cancel transition intact. */
	UT_ASSERT_EQ(memcmp(&entry.payload_meta.forward_marker, &marker, sizeof(marker)), 0);

	/* The derived MASTER_TO_HOLDER certificate is exact. */
	memcpy(&cancel, entry.block_data, sizeof(cancel));
	UT_ASSERT_EQ(cancel.request_id, key.request_id);
	UT_ASSERT_EQ(cancel.request_epoch, key.cluster_epoch);
	UT_ASSERT_EQ((int)cancel.phase, (int)GCS_FORWARD_CANCEL_PHASE_MASTER_TO_HOLDER);
	UT_ASSERT_EQ((int)cancel.proof, (int)GCS_FORWARD_CANCEL_PROOF_MASTER_MASK);
	UT_ASSERT_EQ((int)cancel.reason, (int)GCS_FORWARD_CANCEL_REASON_PENDING_X);
}

UT_TEST(b2_first_tick_stages_new_later_ticks_report_replay)
{
	BufferTag tag = make_tag(302);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xB2), 29);
	GcsBlockDedupEntry first;
	GcsBlockDedupEntry later;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_forwarded(0, &key, tag, NULL, NULL, NULL);

	memset(&first, 0, sizeof(first));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &first),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);

	memset(&later, 0, sizeof(later));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &later),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	/* The periodic pass must not have terminalized anything by itself. */
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&later),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
}

/*
 * RED (S3-P0-09 HIGH #2, master half).
 *
 * cluster_gcs_block_dedup_pending_x_deny_next() stamps
 * entry->completed_at_ts = GetCurrentTimestamp() exactly once, inside the
 * FORWARDED/SEND_ARMED -> CANCELLING arm (dedup.c:3557).  Every later replay
 * pass re-reads the cell and returns it unchanged.  After 200 passes spanning
 * 90 s of simulated clock the master still cannot answer "how long has this
 * cancel been outstanding" -- which is exactly the r58 observation that
 * port6035's ticket was still state4/node0/ticket1 ninety seconds later.
 *
 * CONTRACT:  a replayed exact cancel must record when the master last acted on
 * it.  completed_at_ts is the cell's only "master last acted" stamp and the
 * re-stamp belongs in the same already-held shard-lock critical section, so
 * this is a named-field, in-place, minimal fix -- and it is TTL-correct:
 * letting the TTL sweep age out a cell the master is still actively driving is
 * the same silent-orphan class d1/d2 below are about.
 *
 * The three companion assertions are the fields that must NOT move, so the
 * red cannot be satisfied by perturbing an unrelated byte.
 */
UT_TEST(b3_cancel_replay_records_no_progress_stamp)
{
	BufferTag tag = make_tag(303);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xB3), 29);
	GcsBlockDedupEntry replay_first;
	GcsBlockDedupEntry replay_last;
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_cancelling(0, &key, tag, NULL);

	memset(&replay_first, 0, sizeof(replay_first));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &replay_first),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	memset(&replay_last, 0, sizeof(replay_last));
	for (i = 0; i < 200; i++) {
		fake_now += 450000; /* 450 ms per pass -> 90 s total */
		memset(&replay_last, 0, sizeof(replay_last));
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &replay_last),
					 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	}

	/* Must NOT move: registration identity, DONE proof, liveness. */
	UT_ASSERT_EQ(replay_last.registered_at_ts, replay_first.registered_at_ts);
	UT_ASSERT_EQ((int)(replay_last.done_at_ts != 0), 0);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&replay_last),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);

	/* RED: 90 s of replay must be distinguishable from one tick. */
	UT_ASSERT_NE(replay_last.completed_at_ts, replay_first.completed_at_ts);
}

/*
 * Evidence, not a demand.
 *
 * The previous round asserted here that pending_x_deny_next() must NOT answer
 * NOT_FOUND for a tag it holds nothing under.  That was wrong on two counts:
 * the function is a tag filter by construction (dedup.c:3504) and answering
 * otherwise would require the API to lie about a key it does not have.
 *
 * What is actually true is asserted below:  a live CANCELLING marker is
 * PROVABLE through the census yet ADDRESSABLE only by a caller that already
 * knows both the exact BufferTag and, for _deny_exact, the exact 4-tuple key.
 * No exported accessor hands either back.  A driver that has lost them --
 * requester slot closed, cursor reset, master restart -- can prove the stuck
 * ticket exists and still cannot act on it.
 *
 * The missing capability is enumeration, which is a NEW API and therefore a
 * Tier-2 assertion (t2_10), not something the existing signature can express.
 */
UT_TEST(b4_live_cancelling_is_census_visible_but_only_tag_addressable)
{
	BufferTag tag = make_tag(304);
	BufferTag foreign = make_tag(999);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xB4), 29);
	GcsBlockDedupEntry entry;
	GcsBlockForwardPhaseCensus c;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_cancelling(0, &key, tag, NULL);

	/* The census proves it exists, and exposes counts only. */
	memset(&c, 0, sizeof(c));
	UT_ASSERT(census(&c));
	UT_ASSERT(c.valid);
	UT_ASSERT_EQ(c.cancelling_count, UINT64_C(1));
	UT_ASSERT_EQ(c.marker_count, UINT64_C(1));

	/* With the exact tag, the driver can act on it. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	/* With the exact key AND tag, likewise. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(0, &key, &tag,
																   (uint8)PCM_TRANS_N_TO_S, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	/* Without the tag the driver is correctly told "not here" -- the census
	 * count above is the only remaining evidence that work exists. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &foreign, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_NOT_FOUND);
}

/*
 * Only the exact type-67 ACK terminalizes.
 *
 * Scope note:  the ACK certificate here is derived from the MASTER's cancel
 * bytes.  Production builds it on the REQUESTER side, as a struct copy of the
 * parked barrier with exactly three fields overwritten
 * (cluster_gcs_block.c:17328-17333).  That construction lives inside the
 * static drive(), so requester-side certificate construction is covered by
 * Tier-2 t2_15, not here.
 */
UT_TEST(b5_only_exact_type67_ack_terminalizes_the_marker)
{
	BufferTag tag = make_tag(305);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xB5), 29);
	GcsBlockDedupEntry entry;
	GcsBlockDedupEntry denied;
	GcsBlockForwardCancelPayload cancel;
	GcsBlockForwardCancelPayload ack;
	GcsBlockForwardCancelPayload drifted;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_forwarded(0, &key, tag, NULL, NULL, NULL);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);
	memcpy(&cancel, entry.block_data, sizeof(cancel));

	ack = cancel;
	ack.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_REQUESTER_FENCE_ACK;
	ack.proof = GCS_FORWARD_CANCEL_PROOF_ACK_MASK;
	ack.holder_requester_capability_generation = 107;
	ack.requester_master_capability_generation = 109;

	/* A drifted ACK is STALE and leaves the marker CANCELLING. */
	drifted = ack;
	drifted.pre_authority_generation++;
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_forward_cancel_ack_exact(0, &key, 109, &drifted, &denied),
		(int)GCS_BLOCK_FORWARD_MARK_STALE);
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	/* Only the exact ACK terminalizes. */
	memset(&denied, 0, sizeof(denied));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_forward_cancel_ack_exact(0, &key, 111, &ack, &denied),
				 (int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)denied.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&denied),
				 (int)GCS_BLOCK_FORWARD_MARK_NONE);
}


/* ============================================================
 * D -- A live forward leg vs every retirement path.
 * ============================================================ */

/*
 * RED (S3-P0-09, P0 flavour).  The FORWARDED half of the blind-DONE hole.
 *
 * The consistency invariant at dedup.c:3359-3361 --
 *     (phase == FORWARDED || phase == CANCELLING) != (completed_at_ts != 0)
 *         -> reject
 * -- states that BOTH live phases carry a nonzero completed_at_ts, and the
 * FORWARDED producer sets it explicitly (dedup.c:1329-1330, right after
 * dedup_forward_phase_set(entry, FORWARDED)).  cluster_gcs_block_dedup_mark_done()
 * has no phase leg at all (dedup.c:4769-4771), so a blind DONE is accepted on
 * FORWARDED exactly as it is on CANCELLING.
 *
 * FORWARDED is the WIDER window of the two:  CANCELLING only exists after a
 * pending_x_deny_next() pass has flipped the marker, whereas FORWARDED is the
 * marker's natural resting state from the moment the send finishes.  d1/d2
 * below both flip to CANCELLING first, so without this test the larger window
 * has zero coverage.
 *
 * COST OF THE FIX -- register this, it is not free.  The current permissive
 * behaviour is PINNED BY AN EXISTING GREEN TEST, so it is a deliberate
 * encoded semantic, not an oversight.  Measured by rebuilding
 * test_cluster_gcs_block_dedup against a phase-gated mark_done():  exactly one
 * of its 60 tests fails, at exactly three assertions --
 *     src/test/cluster_unit/test_cluster_gcs_block_dedup.c
 *       :2347  UT_ASSERT(cluster_gcs_block_dedup_mark_done(...))  on FORWARDED
 *       :2350  lookup_or_register(...) == GCS_BLOCK_DEDUP_DONE_DUPLICATE
 *              (becomes FORWARDED_DUPLICATE, 7 -> 5)
 *       :2352  pending_x_deny_next(...) == PENDING_X_DENY_NOT_FOUND
 *              (becomes FORWARD_BLOCKED, 0 -> 3)
 *     all inside u38_forward_marker_prepare_claim_finish_is_exact_and_serial.
 * Whoever gates mark_done() on marker phase MUST flip those three assertions
 * in the same change, or the tree carries two contradictory tests.  That is a
 * product-fix-round task;  this round is test-only, so u38 is named here and
 * deliberately left untouched.
 * (Audited: the other 15 mark_done call sites in that file are unaffected --
 *  :967/:973/:978/:979/:984/:988/:1075/:1243/:1963/:1964/:3797/:3826/:3888/:3893
 *  act on entries with no forward marker, and :3372 runs after
 *  forward_cancel_ack_exact() has already driven the phase to MARK_NONE.)
 *
 * CONTRACT:  a generic DONE must not retire a marker whose forward leg is
 * still in flight, in either live phase.
 */
UT_TEST(d0_blind_done_must_not_retire_a_live_forwarded_marker)
{
	BufferTag tag = make_tag(400);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xD0), 29);
	GcsBlockDedupEntry entry;
	GcsBlockForwardPhaseCensus c;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	/* Stop at FORWARDED.  No pending_x_deny_next() pass, so the marker never
	 * reaches CANCELLING -- this is the untested window. */
	install_forwarded(0, &key, tag, NULL, NULL, NULL);

	memset(&c, 0, sizeof(c));
	UT_ASSERT(census(&c));
	UT_ASSERT_EQ(c.marker_count, UINT64_C(1));
	UT_ASSERT_EQ(c.forwarded_count, UINT64_C(1));
	UT_ASSERT_EQ(c.cancelling_count, UINT64_C(0));

	/* (1) the forward leg is in flight -- a generic DONE may not retire it. */
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &key, &tag, (uint8)PCM_TRANS_N_TO_S));

	/* (2) the marker must still be reachable by the cancel driver. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);

	/* (3) and the phase it moved to is real, not an empty pass. */
	memset(&c, 0, sizeof(c));
	UT_ASSERT(census(&c));
	UT_ASSERT_EQ(c.cancelling_count, UINT64_C(1));
}

/*
 * RED (S3-P0-09, P0 flavour -- silent terminalization of a live protocol cell).
 *
 * cluster_gcs_block_dedup_mark_done() (dedup.c:4750-4783) returns true iff
 * {found, kind == GENERIC, tag match, transition_id match, completed_at_ts != 0}.
 * The FORWARDED -> CANCELLING transition sets completed_at_ts itself
 * (dedup.c:3557) and leaves entry_kind GENERIC, so a blind or duplicate
 * requester DONE satisfies all five gates and stamps done_at_ts on a LIVE
 * cancel that has not received its type-67 ACK.
 *
 * CONTRACT:  a generic DONE must not consume a marker in CANCELLING.
 */
UT_TEST(d1_blind_done_must_not_retire_a_live_cancelling_marker)
{
	BufferTag tag = make_tag(401);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xD1), 29);
	GcsBlockDedupEntry entry;
	GcsBlockForwardPhaseCensus c;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_cancelling(0, &key, tag, NULL);

	/* The DONE must be refused while the cancel is live. */
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &key, &tag, (uint8)PCM_TRANS_N_TO_S));

	/* And the cancel must remain drivable. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);

	memset(&c, 0, sizeof(c));
	UT_ASSERT(census(&c));
	UT_ASSERT_EQ(c.cancelling_count, UINT64_C(1));
}

/*
 * RED (the consequence of d1, asserted independently).
 *
 * Both cleanup sweeps retain a live marker via
 * `!GcsBlockDedupEntryHasForwardMarker(entry) || entry->done_at_ts != 0`
 * (dedup.c:5196, :5231).  A blind DONE flips the second disjunct, so the very
 * next backend-exit or node-dead sweep HASH_REMOVEs a live CANCELLING cell --
 * no type-67 ACK, no census entry, no trace.
 *
 * Stated as a conjunction on purpose:  this passes if the DONE is refused
 * (fix at d1) OR if the sweeps also gate on marker phase (fix in the sweeps).
 * It does not presuppose which fix is chosen, and it does not depend on
 * mark_done's return value.
 */
UT_TEST(d2_live_cancelling_must_survive_cleanup_after_a_blind_done)
{
	BufferTag tag = make_tag(402);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xD2), 29);
	GcsBlockDedupEntry entry;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_cancelling(0, &key, tag, NULL);

	/* A late/duplicate requester DONE arrives.  Whether it is accepted is
	 * d1's question;  here we only care that it cannot unlock reclamation. */
	(void)cluster_gcs_block_dedup_mark_done(0, &key, &tag, (uint8)PCM_TRANS_N_TO_S);

	cluster_gcs_block_dedup_cleanup_on_backend_exit(key.origin_node_id, key.requester_backend_id);
	cluster_gcs_block_dedup_cleanup_on_node_dead(key.origin_node_id);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
}

/*
 * Backend-exit sweep: reclaims plain in-flight entries, retains the live
 * marker.  The ballast entries are not decoration -- the sweep HASH_REMOVEs
 * the entry hash_seq_search() just returned, so a fixture with a single
 * resident cannot detect a cursor that skips the removed entry's successor.
 * A/B are removable and bracket the live marker in scan order.
 */
UT_TEST(d3_backend_exit_cleanup_reclaims_plain_entries_and_retains_the_marker)
{
	BufferTag tag_a = make_tag(410);
	BufferTag tag_b = make_tag(411);
	BufferTag tag_live = make_tag(412);
	BufferTag tag_c = make_tag(413);
	GcsBlockDedupKey key_a = make_key(1, 7, UINT64_C(0xD3A), 29);
	GcsBlockDedupKey key_b = make_key(1, 7, UINT64_C(0xD3B), 29);
	GcsBlockDedupKey key_live = make_key(1, 7, UINT64_C(0xD3D), 29);
	GcsBlockDedupKey key_c = make_key(1, 7, UINT64_C(0xD3C), 29);
	GcsBlockDedupEntry entry;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	register_plain(0, &key_a, tag_a);
	register_plain(0, &key_b, tag_b);
	install_cancelling(0, &key_live, tag_live, NULL);
	register_plain(0, &key_c, tag_c);

	cluster_gcs_block_dedup_cleanup_on_backend_exit(key_live.origin_node_id,
													key_live.requester_backend_id);

	/* Every plain entry is gone, including the ones that follow a removal. */
	UT_ASSERT(!entry_resident(0, &key_a, tag_a));
	UT_ASSERT(!entry_resident(0, &key_b, tag_b));
	UT_ASSERT(!entry_resident(0, &key_c, tag_c));

	/* The live marker survives and is still drivable. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag_live, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
}

/* Node-dead sweep: same shape, keyed on origin_node_id alone. */
UT_TEST(d4_node_dead_cleanup_reclaims_plain_entries_and_retains_the_marker)
{
	BufferTag tag_a = make_tag(420);
	BufferTag tag_b = make_tag(421);
	BufferTag tag_live = make_tag(422);
	BufferTag tag_c = make_tag(423);
	GcsBlockDedupKey key_a = make_key(1, 5, UINT64_C(0xD4A), 29);
	GcsBlockDedupKey key_b = make_key(1, 6, UINT64_C(0xD4B), 29);
	GcsBlockDedupKey key_live = make_key(1, 7, UINT64_C(0xD4D), 29);
	GcsBlockDedupKey key_c = make_key(1, 8, UINT64_C(0xD4C), 29);
	GcsBlockDedupEntry entry;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	register_plain(0, &key_a, tag_a);
	register_plain(0, &key_b, tag_b);
	install_cancelling(0, &key_live, tag_live, NULL);
	register_plain(0, &key_c, tag_c);

	cluster_gcs_block_dedup_cleanup_on_node_dead(key_live.origin_node_id);

	UT_ASSERT(!entry_resident(0, &key_a, tag_a));
	UT_ASSERT(!entry_resident(0, &key_b, tag_b));
	UT_ASSERT(!entry_resident(0, &key_c, tag_c));

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag_live, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
}

UT_TEST(d5_late_duplicate_request_must_not_install_over_cancelling)
{
	BufferTag tag = make_tag(404);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xD5), 29);
	GcsBlockDedupEntry entry;
	GcsBlockDedupEntry dup;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_cancelling(0, &key, tag, NULL);

	/* A late holder reply / duplicate REQUEST may not overwrite the cell. */
	memset(&dup, 0, sizeof(dup));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S, 0,
																 false, &dup),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
}

/*
 * GREEN FENCE for the Tier-2 orphan-abort work (t2_11).  Do not relax.
 *
 * cluster_gcs_block_dedup_forward_abort_prepared_exact() (dedup.c:1337-1370)
 * removes the cell under
 *	   found
 *	   && dedup_forward_entry_exact(..., expected_phase = PREPARED)
 *	   && entry->completed_at_ts == 0
 *	   && entry->done_at_ts == 0
 * A live CANCELLING cell fails two of those legs at once:  its phase is
 * CANCELLING, and the CANCELLING transition stamped completed_at_ts.  Both
 * legs exist precisely to keep this abort away from a live cancel.
 *
 * The cheapest way to make an "orphaned CANCELLING can be aborted" test pass
 * is to relax one of them -- which would let HASH_REMOVE take a live cancel
 * and reopen d1/d2 under a different name.  So the orphan path must be a NEW,
 * evidence-gated entry point (Tier-2 t2_11) and THIS function must keep
 * saying no, forever.
 */
UT_TEST(d6_prepared_abort_must_stay_refused_on_a_live_cancelling_marker)
{
	BufferTag tag = make_tag(405);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0xD6), 29);
	GcsBlockDedupEntry entry;
	GcsBlockForwardMarker marker;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	install_cancelling(0, &key, tag, &marker);

	/* Refused while the requester is still nominally present. */
	UT_ASSERT(!cluster_gcs_block_dedup_forward_abort_prepared_exact(
		0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &marker));

	/* Still refused once the requester is provably gone -- "the requester
	 * died" is not evidence this entry point is allowed to consume. */
	cluster_gcs_block_dedup_cleanup_on_backend_exit(key.origin_node_id, key.requester_backend_id);
	cluster_gcs_block_dedup_cleanup_on_node_dead(key.origin_node_id);
	UT_ASSERT(!cluster_gcs_block_dedup_forward_abort_prepared_exact(
		0, &key, &tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &marker));

	/* The cell is untouched by the refused aborts. */
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
}


/* ==========================================================================
 * TIER-2 -- the real S3-P0-09 acceptance gate.
 * ==========================================================================
 *
 * Built only by target test_cluster_gcs_block_forward_cancel_driver_tier2,
 * which defines CLUSTER_FORWARD_CANCEL_DRIVER_LINKED and links
 * cluster_gcs_block_forward_cancel_driver.o.  That object does not exist yet,
 * so this target does not build today.  That is not the red -- the red is
 * b3/d1/d2 above.  This is the CONTRACT the implementer builds against.
 *
 * --------------------------------------------------------------------------
 * REQUIRED API -- src/backend/cluster/cluster_gcs_block_forward_cancel_driver.h
 * --------------------------------------------------------------------------
 * (This test file may not create product files;  the header below is a
 *  specification, transcribed so the tests underneath are unambiguous.)
 *
 *   1. Typed outcome -- one value per structurally distinct blocked reason.
 *      Folding any two is what produced the r58 "PCM_X_QUEUE_NOT_READY means
 *      six different things" symptom, so the enum is the deliverable.
 *
 *      typedef enum GcsBlockForwardCancelDriveOutcome {
 *          FORWARD_CANCEL_DRIVE_NO_WORK                = 0,
 *          FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK    = 1,
 *          FORWARD_CANCEL_DRIVE_REPLAY_STAGED_WAIT_ACK = 2,
 *          FORWARD_CANCEL_DRIVE_STAGE_NOT_ADMITTED     = 3,
 *          FORWARD_CANCEL_DRIVE_IDENTITY_RETRY         = 4,
 *          FORWARD_CANCEL_DRIVE_ACK_AUTHORITY_WAIT     = 5,
 *          FORWARD_CANCEL_DRIVE_LEGACY_DENIAL_STAGED   = 6,
 *          FORWARD_CANCEL_DRIVE_CORRUPT                = 7,
 *          FORWARD_CANCEL_DRIVE_OUTCOME_COUNT          = 8
 *      } GcsBlockForwardCancelDriveOutcome;
 *
 *   2. Per-pass report -- the H2 progress trace the master lacks today.
 *
 *      typedef struct GcsBlockForwardCancelDriveReport {
 *          GcsBlockForwardCancelDriveOutcome outcome;
 *          BufferTag        tag;                 // subject of this pass
 *          GcsBlockDedupKey key;                 // subject of this pass
 *          uint64           retain_attempts;     // strictly increasing while blocked
 *          TimestampTz      first_staged_ts;     // set once, never rewritten
 *          TimestampTz      last_attempt_ts;     // advances every pass
 *          uint32           blocked_detail;      // reason-specific, never the outcome
 *      } GcsBlockForwardCancelDriveReport;
 *
 *   3. Cumulative stats -- rule 17 counter exposure, also the pg_stat surface.
 *
 *      typedef struct GcsBlockForwardCancelDriveStats {
 *          uint64 outcome_count[FORWARD_CANCEL_DRIVE_OUTCOME_COUNT];
 *          uint64 park_result_count[5];   // indexed by ParkResult + 1, so
 *                                         // INVALID/PARKED/DUPLICATE/COLLISION/FULL
 *                                         // are each counted separately (H1)
 *          uint64 ledger_discard_reclaimed;   // DISCARD verdicts that freed a slot
 *          uint64 orphan_retired;             // evidence-gated orphan retirements
 *          uint64 enumerate_calls;
 *      } GcsBlockForwardCancelDriveStats;
 *      extern void cluster_gcs_block_forward_cancel_drive_stats(
 *              GcsBlockForwardCancelDriveStats *out);
 *
 *   4. One periodic pass over one shard.  Must acquire at most one shard lock
 *      at a time and must hold none on return.
 *
 *      extern GcsBlockForwardCancelDriveOutcome
 *      cluster_gcs_block_forward_cancel_drive_once(
 *              int worker_id, GcsBlockForwardCancelDriveReport *report);
 *
 *   5. Admission, made observable rather than collapsed to bool (H1).  This
 *      replaces the internal static gcs_block_forward_cancel_replay_park().
 *
 *      extern GcsBlockForwardCancelReplayParkResult
 *      cluster_gcs_block_forward_cancel_admit(
 *              const GcsBlockForwardCancelPayload *barrier, size_t *slot_out);
 *
 *   6. Enumeration without out-of-band tag knowledge (HIGH#1 a).
 *
 *      typedef struct GcsBlockForwardCancelLiveRef {
 *          GcsBlockDedupKey key;
 *          BufferTag        tag;
 *          GcsBlockForwardMarkerPhase phase;
 *          TimestampTz      staged_at_ts;
 *      } GcsBlockForwardCancelLiveRef;
 *      extern bool cluster_gcs_block_forward_cancel_enumerate_next(
 *              int worker_id, uint32 *cursor,
 *              GcsBlockForwardCancelLiveRef *out);
 *
 *   7. Autonomous, evidence-gated orphan termination (HIGH#1 c).  A SEPARATE
 *      entry point from forward_abort_prepared_exact(), which must keep
 *      refusing CANCELLING -- see d6.
 *
 *      typedef struct GcsBlockForwardCancelOrphanEvidence {
 *          uint64 requester_incarnation;   // the incarnation being retired
 *          uint64 fence_epoch;             // membership epoch that fenced it
 *          bool   requester_backend_gone;
 *          bool   requester_node_fenced;
 *      } GcsBlockForwardCancelOrphanEvidence;
 *      extern bool cluster_gcs_block_forward_cancel_abort_orphaned_exact(
 *              int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
 *              const GcsBlockForwardCancelOrphanEvidence *evidence,
 *              GcsBlockDedupEntry *out);
 *
 *   8. Environment sampling seam (USE_CLUSTER_UNIT only).  drive_once() must
 *      sample identity / grant_pending / local_mode through this hook so the
 *      POLICY and the ORCHESTRATION stay real while only the environment is
 *      mocked -- the same discipline Tier-1 uses.
 *
 *      typedef void (*GcsBlockForwardCancelDriveObserverHook)(
 *              const GcsBlockForwardCancelReplayEntry *entry,
 *              GcsBlockForwardCancelReplayObservation *observation,
 *              uint32 *master_generation_out, void *arg);
 *      extern void cluster_gcs_block_forward_cancel_drive_set_test_observer(
 *              GcsBlockForwardCancelDriveObserverHook hook, void *arg);
 *      // and an admission hook for the STAGE_RELEASE / STAGE_ACK transports:
 *      typedef bool (*GcsBlockForwardCancelDriveTransportHook)(
 *              const GcsBlockForwardCancelPayload *payload, void *arg);
 *      extern void cluster_gcs_block_forward_cancel_drive_set_test_transport(
 *              GcsBlockForwardCancelDriveTransportHook hook, void *arg);
 *      // last payload the driver actually handed to the transport (t2_15)
 *      extern bool cluster_gcs_block_forward_cancel_drive_test_last_staged(
 *              GcsBlockForwardCancelPayload *out);
 *
 *   9. Two symbols the driver must re-export or the header must include, so
 *      Tier-2 can size the bounded lot and corrupt a cell deterministically:
 *        - GCS_BLOCK_FORWARD_CANCEL_REPLAY_MAX, today a bare #define at
 *          cluster_gcs_block.c:118 and therefore invisible outside that
 *          translation unit;
 *        - a USE_CLUSTER_UNIT-only
 *            extern bool cluster_gcs_block_dedup_forward_cancel_test_overwrite_payload(
 *                    int worker_id, const GcsBlockDedupKey *key,
 *                    const GcsBlockForwardCancelPayload *payload);
 *          in cluster_gcs_block_dedup.h, which writes entry->block_data under
 *          the shard lock WITHOUT re-validating it -- the only way to
 *          reproduce a torn cell for the CORRUPT arm without hand-editing
 *          shared memory from the test.
 * ========================================================================== */

#ifdef CLUSTER_FORWARD_CANCEL_DRIVER_LINKED
#include "cluster_gcs_block_forward_cancel_driver.h"

/*
 * Every Tier-2 outcome test asserts the SAME four things, so that a driver
 * which folds two reasons into one outcome fails here instead of passing on
 * an aggregate:
 *   (1) drive_once() returns the expected outcome;
 *   (2) report->outcome agrees with the return value;
 *   (3) stats.outcome_count[expected] advanced by exactly 1;
 *   (4) every OTHER outcome counter is unchanged.
 * (4) is the anti-folding gate.  A previous round summed per-outcome probe
 * calls and asserted each total == 1, which any implementation returning a
 * constant would satisfy;  that is why the check is a full 8-way delta now.
 */
static GcsBlockForwardCancelDriveStats t2_stats_before;

static void
t2_snapshot(void)
{
	memset(&t2_stats_before, 0, sizeof(t2_stats_before));
	cluster_gcs_block_forward_cancel_drive_stats(&t2_stats_before);
}

static void
t2_assert_only_outcome_advanced(GcsBlockForwardCancelDriveOutcome expected)
{
	GcsBlockForwardCancelDriveStats after;
	int i;

	memset(&after, 0, sizeof(after));
	cluster_gcs_block_forward_cancel_drive_stats(&after);

	for (i = 0; i < FORWARD_CANCEL_DRIVE_OUTCOME_COUNT; i++) {
		if (i == (int)expected)
			UT_ASSERT_EQ(after.outcome_count[i], t2_stats_before.outcome_count[i] + 1);
		else
			UT_ASSERT_EQ(after.outcome_count[i], t2_stats_before.outcome_count[i]);
	}
}

/* Run one pass and assert the full typed-outcome contract. */
static void
t2_drive_and_assert(int worker_id, GcsBlockForwardCancelDriveOutcome expected,
					GcsBlockForwardCancelDriveReport *report_out)
{
	GcsBlockForwardCancelDriveReport report;
	GcsBlockForwardCancelDriveOutcome got;

	memset(&report, 0, sizeof(report));
	t2_snapshot();
	got = cluster_gcs_block_forward_cancel_drive_once(worker_id, &report);

	UT_ASSERT_EQ((int)got, (int)expected);
	UT_ASSERT_EQ((int)report.outcome, (int)expected);
	t2_assert_only_outcome_advanced(expected);

	/* Lock discipline: at most one shard lock at a time (enforced by the
	 * LWLockAcquire stub's Assert while fake_allow_multi_lwlock is false),
	 * and none held on return. */
	UT_ASSERT_EQ(fake_lwlock_held_count, 0);

	if (report_out != NULL)
		*report_out = report;
}

/* ---- observation / transport injection ---- */

typedef struct T2Observer {
	GcsBlockForwardCancelReplayIdentityVerdict verdict;
	bool newer_request_active;
	bool grant_pending;
	PcmLockMode local_mode;
	uint32 master_generation;
	int calls;
} T2Observer;

static T2Observer t2_observer;

static void
t2_observe(const GcsBlockForwardCancelReplayEntry *entry pg_attribute_unused(),
		   GcsBlockForwardCancelReplayObservation *observation, uint32 *master_generation_out,
		   void *arg)
{
	T2Observer *o = (T2Observer *)arg;

	o->calls++;
	observation->identity_verdict = o->verdict;
	observation->phase = (GcsBlockForwardCancelReplayPhase)entry->phase;
	observation->newer_request_active = o->newer_request_active;
	observation->grant_pending = o->grant_pending;
	observation->local_mode = o->local_mode;
	*master_generation_out = o->master_generation;
}

static bool t2_transport_admits;
static int t2_transport_calls;

static bool
t2_transport(const GcsBlockForwardCancelPayload *payload pg_attribute_unused(),
			 void *arg pg_attribute_unused())
{
	t2_transport_calls++;
	return t2_transport_admits;
}

static void
t2_reset(void)
{
	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&t2_observer, 0, sizeof(t2_observer));
	t2_observer.verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT;
	t2_observer.local_mode = PCM_LOCK_MODE_N;
	t2_observer.master_generation = 73;
	t2_transport_admits = true;
	t2_transport_calls = 0;
	cluster_gcs_block_forward_cancel_drive_set_test_observer(t2_observe, &t2_observer);
	cluster_gcs_block_forward_cancel_drive_set_test_transport(t2_transport, NULL);
}

/* ---- the eight typed outcomes ---- */

/* T2-01  NO_WORK: nothing staged anywhere.  Must be reachable and must be the
 * ONLY outcome that means "idle", so an orphan can never masquerade as it. */
UT_TEST(t2_01_no_work_is_reported_only_when_the_shard_is_truly_idle)
{
	GcsBlockForwardCancelDriveReport report;

	t2_reset();
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NO_WORK, &report);
	UT_ASSERT_EQ(report.retain_attempts, UINT64_C(0));
	UT_ASSERT_EQ((int)(report.first_staged_ts == 0), 1);
}

/* T2-02  NEW_STAGED_WAIT_ACK: the first pass over a FORWARDED marker performs
 * the FORWARDED -> CANCELLING transition and stages the MASTER_TO_HOLDER
 * certificate.  Distinct from REPLAY so "we just started" is never confused
 * with "we have been retrying for 90 s" (the b3 defect, one layer up). */
UT_TEST(t2_02_first_pass_over_a_forwarded_marker_stages_new)
{
	BufferTag tag = make_tag(501);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x201), 29);
	GcsBlockDedupEntry entry;
	GcsBlockForwardCancelDriveReport report;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);

	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, &report);
	UT_ASSERT_EQ(memcmp(&report.tag, &tag, sizeof(tag)), 0);
	UT_ASSERT_EQ(memcmp(&report.key, &key, sizeof(key)), 0);
	UT_ASSERT_NE(report.first_staged_ts, (TimestampTz)0);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(0, &key, &tag,
																   (uint8)PCM_TRANS_N_TO_S, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&entry),
				 (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
}

/* T2-03  REPLAY_STAGED_WAIT_ACK carries the progress trace b3 demands, at the
 * driver layer:  attempts strictly increase, first_staged_ts is written once
 * and never rewritten, last_attempt_ts advances every pass. */
UT_TEST(t2_03_replay_pass_exposes_monotonic_retry_progress)
{
	BufferTag tag = make_tag(502);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x202), 29);
	GcsBlockForwardCancelDriveReport first;
	GcsBlockForwardCancelDriveReport prev;
	GcsBlockForwardCancelDriveReport cur;
	int i;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, &first);

	prev = first;
	for (i = 0; i < 200; i++) {
		fake_now += 450000; /* 90 s total */
		t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_REPLAY_STAGED_WAIT_ACK, &cur);

		UT_ASSERT_EQ(cur.first_staged_ts, first.first_staged_ts);
		UT_ASSERT(cur.retain_attempts > prev.retain_attempts);
		UT_ASSERT(cur.last_attempt_ts > prev.last_attempt_ts);
		prev = cur;
	}
	UT_ASSERT(cur.last_attempt_ts - cur.first_staged_ts >= 90000000);
}

/* T2-04  STAGE_NOT_ADMITTED: the policy said STAGE_RELEASE/STAGE_ACK but the
 * transport refused admission.  Today gcs_block_forward_cancel_replay_drive()
 * bare-`return`s on `if (!admitted) return;` (cluster_gcs_block.c:17313-17314)
 * and on `if (master_generation == 0) return;` (:17326-17327), so a wedged
 * GRD/CONTROL path is indistinguishable from an idle master. */
UT_TEST(t2_04_refused_admission_is_its_own_outcome_not_silence)
{
	BufferTag tag = make_tag(503);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x203), 29);
	GcsBlockForwardCancelDriveReport report;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	t2_transport_admits = false;

	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_STAGE_NOT_ADMITTED, &report);
	UT_ASSERT(t2_transport_calls > 0);

	/* Refusal must not consume the work item. */
	t2_transport_admits = true;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, NULL);
}

/* T2-05  IDENTITY_RETRY: the RETAIN family, but reported and counted rather
 * than swallowed by the `default: return;` at cluster_gcs_block.c:17359-17361.
 * All four Tier-1 a4 blocking observations must land here. */
UT_TEST(t2_05_retain_observations_report_identity_retry_with_a_trace)
{
	BufferTag tag = make_tag(504);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x204), 29);
	GcsBlockForwardCancelDriveReport report;
	GcsBlockForwardCancelDriveReport later;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, NULL);

	t2_observer.verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETRY;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_IDENTITY_RETRY, &report);

	t2_observer.verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT;
	t2_observer.grant_pending = true;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_IDENTITY_RETRY, &later);
	UT_ASSERT(later.retain_attempts > report.retain_attempts);

	t2_observer.grant_pending = false;
	t2_observer.newer_request_active = true;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_IDENTITY_RETRY, NULL);

	t2_observer.newer_request_active = false;
	t2_observer.local_mode = PCM_LOCK_MODE_X;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_IDENTITY_RETRY, NULL);
}

/* T2-06  ACK_AUTHORITY_WAIT: the requester is ready to send the type-67 ACK
 * but the master generation is not yet observable (master_generation == 0).
 * Distinct from IDENTITY_RETRY because the remedy is different -- one waits on
 * authority propagation, the other on the requester's own state. */
UT_TEST(t2_06_missing_master_generation_is_an_ack_authority_wait)
{
	BufferTag tag = make_tag(505);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x205), 29);
	GcsBlockForwardCancelDriveReport report;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, NULL);

	t2_observer.master_generation = 0;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_ACK_AUTHORITY_WAIT, &report);
	UT_ASSERT_NE(report.first_staged_ts, (TimestampTz)0);

	/* It clears on its own once authority is visible. */
	t2_observer.master_generation = 73;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_REPLAY_STAGED_WAIT_ACK, NULL);
}

/* T2-07  LEGACY_DENIAL_STAGED: a same-tag legacy N->S grant is terminated by
 * the queue-kind claim with no forward marker involved.  It shares
 * pending_x_deny_next()'s return channel with the forward-cancel arms today
 * (GCS_BLOCK_PENDING_X_DENY_NEW / _REPLAY vs _FORWARD_CANCEL_NEW / _REPLAY)
 * and must remain its own driver outcome. */
UT_TEST(t2_07_legacy_denial_is_not_folded_into_the_forward_cancel_arms)
{
	BufferTag tag = make_tag(506);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x206), 29);

	t2_reset();
	register_plain(0, &key, tag);

	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_LEGACY_DENIAL_STAGED, NULL);
}

/* T2-08  CORRUPT: the cell fails its own exactness re-check
 * (dedup_forward_cancel_entry_exact / dedup_forward_cancel_from_entry_exact),
 * i.e. GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED / _INVALID.  Must be a loud,
 * separately counted outcome -- rule 8.A: an unprovable cell is never treated
 * as clean success or as NO_WORK. */
UT_TEST(t2_08_unprovable_cell_reports_corrupt_and_is_never_idle)
{
	BufferTag tag = make_tag(507);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x207), 29);
	GcsBlockForwardCancelDriveReport report;

	t2_reset();
	install_cancelling(0, &key, tag, NULL);

	/* Corrupt the staged certificate in place, exactly as a torn/aliased
	 * shared-memory write would. */
	{
		GcsBlockDedupEntry entry;
		GcsBlockForwardCancelPayload cancel;

		memset(&entry, 0, sizeof(entry));
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
						 0, &key, &tag, (uint8)PCM_TRANS_N_TO_S, &entry),
					 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
		memcpy(&cancel, entry.block_data, sizeof(cancel));
		cancel.request_id ^= UINT64_C(0xFFFF);
		UT_ASSERT(cluster_gcs_block_dedup_forward_cancel_test_overwrite_payload(0, &key, &cancel));
	}

	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_CORRUPT, &report);
	UT_ASSERT_NE((int)report.blocked_detail, 0);
}

/* ---- orchestration + the three HIGH#1 capabilities ---- */

/*
 * T2-09  Lock discipline of the periodic pass.
 *
 * drive_once() must never hold two locks at once and must return with none.
 * The shared replay ledger lock and a dedup shard lock are separate tranches
 * and today drive() takes the ledger lock around mark_release/mark_ack/finish
 * (cluster_gcs_block.c:17315-17323, :17342-17355) while the dedup APIs take a
 * shard lock;  a driver that nests them introduces a new lock order.
 */
UT_TEST(t2_09_drive_once_never_nests_locks_and_releases_all_of_them)
{
	BufferTag tag = make_tag(508);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x208), 29);
	int i;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);

	/* fake_allow_multi_lwlock stays false, so LWLockAcquire()'s Assert aborts
	 * the binary on the first nested acquisition. */
	UT_ASSERT(!fake_allow_multi_lwlock);
	for (i = 0; i < 8; i++) {
		fake_now += 450000;
		(void)cluster_gcs_block_forward_cancel_drive_once(0, NULL);
		UT_ASSERT_EQ(fake_lwlock_held_count, 0);
	}
}

/*
 * T2-10  HIGH#1(a) -- enumeration without out-of-band tag knowledge.
 *
 * Tier-1 b4 proves a live CANCELLING marker is census-visible yet addressable
 * only by a caller that already holds the exact tag and key.  This is the
 * capability that closes it:  a cursor walk that HANDS BACK the identity, so a
 * driver that has lost the requester slot can still act.
 */
UT_TEST(t2_10_live_cancelling_set_is_enumerable_and_then_addressable)
{
	BufferTag tags[3];
	GcsBlockDedupKey keys[3];
	GcsBlockForwardCancelLiveRef seen[3];
	GcsBlockForwardCancelLiveRef ref;
	uint32 cursor = 0;
	int found = 0;
	int i;
	int j;

	t2_reset();
	for (i = 0; i < 3; i++) {
		tags[i] = make_tag((uint32)(520 + i));
		keys[i] = make_key(1, 7, UINT64_C(0x210) + (uint64)i, 29);
		install_cancelling(0, &keys[i], tags[i], NULL);
	}

	memset(seen, 0, sizeof(seen));
	while (cluster_gcs_block_forward_cancel_enumerate_next(0, &cursor, &ref)) {
		UT_ASSERT(found < 3);
		seen[found++] = ref;
	}
	UT_ASSERT_EQ(found, 3);

	for (i = 0; i < 3; i++) {
		int matched = 0;

		for (j = 0; j < 3; j++) {
			if (memcmp(&seen[j].tag, &tags[i], sizeof(BufferTag)) != 0)
				continue;
			UT_ASSERT_EQ(memcmp(&seen[j].key, &keys[i], sizeof(GcsBlockDedupKey)), 0);
			UT_ASSERT_EQ((int)seen[j].phase, (int)GCS_BLOCK_FORWARD_MARK_CANCELLING);
			UT_ASSERT_NE(seen[j].staged_at_ts, (TimestampTz)0);
			matched++;
		}
		UT_ASSERT_EQ(matched, 1);
	}

	/* Enumerated identity is sufficient to address the cell -- no other
	 * knowledge required. */
	for (i = 0; i < 3; i++) {
		GcsBlockDedupEntry entry;

		memset(&entry, 0, sizeof(entry));
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
						 0, &seen[i].key, &seen[i].tag, (uint8)PCM_TRANS_N_TO_S, &entry),
					 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
	}
}

/*
 * T2-11  HIGH#1(c) -- autonomous, evidence-gated orphan termination, AND the
 * fence that keeps it from becoming d1 under a new name.
 *
 * Read together with Tier-1 d6.  The orphan path is a NEW entry point that
 * consumes explicit requester-death evidence;  the PREPARED abort keeps
 * refusing CANCELLING unconditionally.  Implementing the orphan path by
 * loosening forward_abort_prepared_exact()'s phase / completed_at_ts legs
 * (dedup.c:1359-1362) fails the second half of this test.
 */
UT_TEST(t2_11_orphaned_cancel_retires_on_evidence_while_prepared_abort_stays_refused)
{
	BufferTag tag = make_tag(530);
	BufferTag live_tag = make_tag(531);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x211), 29);
	GcsBlockDedupKey live_key = make_key(1, 9, UINT64_C(0x212), 29);
	GcsBlockForwardMarker marker;
	GcsBlockForwardMarker live_marker;
	GcsBlockForwardCancelOrphanEvidence evidence;
	GcsBlockDedupEntry out;
	GcsBlockDedupEntry entry;
	GcsBlockForwardCancelDriveStats before;
	GcsBlockForwardCancelDriveStats after;

	t2_reset();
	install_cancelling(0, &key, tag, &marker);
	install_cancelling(0, &live_key, live_tag, &live_marker);

	/* Insufficient evidence must be refused. */
	memset(&evidence, 0, sizeof(evidence));
	evidence.requester_incarnation = 83;
	evidence.fence_epoch = 29;
	evidence.requester_backend_gone = true; /* node NOT fenced */
	memset(&out, 0, sizeof(out));
	UT_ASSERT(
		!cluster_gcs_block_forward_cancel_abort_orphaned_exact(0, &key, &tag, &evidence, &out));

	/* Wrong incarnation must be refused even with full fencing. */
	evidence.requester_node_fenced = true;
	evidence.requester_incarnation = 84;
	UT_ASSERT(
		!cluster_gcs_block_forward_cancel_abort_orphaned_exact(0, &key, &tag, &evidence, &out));

	/* Complete, exact evidence retires it -- and is counted. */
	evidence.requester_incarnation = 83;
	memset(&before, 0, sizeof(before));
	cluster_gcs_block_forward_cancel_drive_stats(&before);
	memset(&out, 0, sizeof(out));
	UT_ASSERT(
		cluster_gcs_block_forward_cancel_abort_orphaned_exact(0, &key, &tag, &evidence, &out));
	memset(&after, 0, sizeof(after));
	cluster_gcs_block_forward_cancel_drive_stats(&after);
	UT_ASSERT_EQ(after.orphan_retired, before.orphan_retired + 1);

	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_NOT_FOUND);

	/* THE FENCE.  The PREPARED abort must still refuse the untouched live
	 * cell -- both before and after the orphan path exists. */
	UT_ASSERT(!cluster_gcs_block_dedup_forward_abort_prepared_exact(
		0, &live_key, &live_tag, 2, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &live_marker));
	memset(&entry, 0, sizeof(entry));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &live_tag, &entry),
				 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY);
}

/*
 * T2-12  H1 -- the five admission outcomes must each be counted.
 *
 * Tier-1 a6 proves gcs_block_forward_cancel_replay_ledger_park() already
 * returns five distinct values.  cluster_gcs_block.c:17148-17149 flattens
 * {INVALID, COLLISION, FULL} plus the ClusterGcsBlock == NULL early-out into
 * one `false`, and both callers (:2947, :17236) `(void)` even that.  Rule 17:
 * a capacity/degradation event gets a counter, not silence.
 */
UT_TEST(t2_12_every_admission_outcome_is_counted_not_collapsed)
{
	GcsBlockForwardCancelPayload barrier;
	GcsBlockForwardCancelPayload malformed;
	GcsBlockForwardCancelPayload collide;
	GcsBlockForwardCancelDriveStats before;
	GcsBlockForwardCancelDriveStats after;
	size_t slot = 0;
	int i;

	t2_reset();

	memset(&before, 0, sizeof(before));
	cluster_gcs_block_forward_cancel_drive_stats(&before);

	malformed = make_barrier(0x212, 29, 7, 540);
	malformed.request_id = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&malformed, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_INVALID);

	barrier = make_barrier(0x212, 29, 7, 540);
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&barrier, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&barrier, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE);

	collide = barrier;
	collide.pre_authority_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&collide, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_COLLISION);

	for (i = 1; i < GCS_BLOCK_FORWARD_CANCEL_REPLAY_MAX; i++) {
		GcsBlockForwardCancelPayload filler
			= make_barrier((uint64)(0x21200 + i), 29, 7, (uint32)(600 + i));

		UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&filler, &slot),
					 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	}
	{
		GcsBlockForwardCancelPayload fresh = make_barrier(0x212FF, 29, 7, 999);

		UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&fresh, &slot),
					 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_FULL);
	}

	memset(&after, 0, sizeof(after));
	cluster_gcs_block_forward_cancel_drive_stats(&after);

	/* park_result_count is indexed by ParkResult + 1 so INVALID (-1) fits. */
	UT_ASSERT_EQ(after.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_INVALID + 1],
				 before.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_INVALID + 1] + 1);
	UT_ASSERT_EQ(after.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE + 1],
				 before.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE + 1] + 1);
	UT_ASSERT_EQ(after.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_COLLISION + 1],
				 before.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_COLLISION + 1] + 1);
	UT_ASSERT_EQ(after.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_FULL + 1],
				 before.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_FULL + 1] + 1);
	UT_ASSERT_EQ(after.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED + 1],
				 before.park_result_count[GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED + 1]
					 + (uint64)GCS_BLOCK_FORWARD_CANCEL_REPLAY_MAX);
}

/*
 * T2-13  HIGH#1(c), requester half -- DISCARD must reclaim the slot.
 *
 * Tier-1 a5 proves next_action() already returns DISCARD for a RETIRED
 * identity.  gcs_block_forward_cancel_replay_drive() folds that verdict into
 * `case RETAIN: default: return;` (cluster_gcs_block.c:17357-17361), so the
 * entry stays in_use forever and the bounded lot leaks one slot per retired
 * requester until it is permanently FULL.  That -- not a missing policy arm --
 * is the requester-side starvation mechanism.
 */
UT_TEST(t2_13_discard_verdict_reclaims_the_ledger_slot)
{
	BufferTag tag = make_tag(550);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x213), 29);
	GcsBlockForwardCancelPayload barrier = make_barrier(0x213, 29, 7, 550);
	GcsBlockForwardCancelDriveStats before;
	GcsBlockForwardCancelDriveStats after;
	size_t slot = 0;
	int i;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&barrier, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);

	memset(&before, 0, sizeof(before));
	cluster_gcs_block_forward_cancel_drive_stats(&before);

	t2_observer.verdict = GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETIRED;
	(void)cluster_gcs_block_forward_cancel_drive_once(0, NULL);

	memset(&after, 0, sizeof(after));
	cluster_gcs_block_forward_cancel_drive_stats(&after);
	UT_ASSERT_EQ(after.ledger_discard_reclaimed, before.ledger_discard_reclaimed + 1);

	/* The lot is usable again: the whole ledger can be refilled. */
	for (i = 0; i < GCS_BLOCK_FORWARD_CANCEL_REPLAY_MAX; i++) {
		GcsBlockForwardCancelPayload filler
			= make_barrier((uint64)(0x21300 + i), 29, 7, (uint32)(700 + i));

		UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&filler, &slot),
					 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);
	}
}

/*
 * T2-14  H4 -- the three mark call sites, driven only by the periodic pass.
 *
 * Today mark_release_exact (:17318) and finish_exact (:17350) are both
 * `(void)`-discarded;  only mark_ack_exact (:17345) is consumed.  A failed
 * finish leaves the entry ACK_STAGED, next_action() then returns RETAIN
 * forever, and the slot leaks silently.  drive_once() must walk the ledger
 * PARKED -> RELEASE_STAGED -> ACK_STAGED -> reclaimed and report each step.
 */
UT_TEST(t2_14_periodic_pass_walks_the_ledger_through_all_three_marks)
{
	BufferTag tag = make_tag(560);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x214), 29);
	GcsBlockForwardCancelPayload barrier = make_barrier(0x214, 29, 7, 560);
	GcsBlockForwardCancelLiveRef ref;
	uint32 cursor = 0;
	size_t slot = 0;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&barrier, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);

	/* PARKED + local N -> STAGE_RELEASE -> mark_release_exact. */
	t2_observer.local_mode = PCM_LOCK_MODE_N;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, NULL);

	/* RELEASE_STAGED + local N -> STAGE_ACK -> mark_ack_exact + finish_exact.
	 * The slot must be reclaimed by finish, not left ACK_STAGED. */
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_REPLAY_STAGED_WAIT_ACK, NULL);

	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NO_WORK, NULL);

	/* Nothing live remains on either side. */
	cursor = 0;
	UT_ASSERT(!cluster_gcs_block_forward_cancel_enumerate_next(0, &cursor, &ref));
}

/*
 * T2-15  L4 -- the requester-side ACK certificate is built by PRODUCTION.
 *
 * Tier-1 b5 hand-builds the type-67 ACK from the master's cancel bytes because
 * the real construction (a struct copy of the parked barrier with exactly
 * phase / proof / requester_master_capability_generation overwritten,
 * cluster_gcs_block.c:17328-17333) lives inside the static drive().  Here the
 * driver must produce it, and the master must accept THAT certificate.
 */
UT_TEST(t2_15_requester_side_ack_certificate_is_accepted_by_the_master)
{
	BufferTag tag = make_tag(570);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x215), 29);
	GcsBlockForwardCancelPayload barrier = make_barrier(0x215, 29, 7, 570);
	GcsBlockForwardCancelPayload staged;
	GcsBlockDedupEntry denied;
	size_t slot = 0;

	t2_reset();
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_forward_cancel_admit(&barrier, &slot),
				 (int)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED);

	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, NULL);
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_REPLAY_STAGED_WAIT_ACK, NULL);

	/* The transport hook captured what the driver actually sent. */
	UT_ASSERT(cluster_gcs_block_forward_cancel_drive_test_last_staged(&staged));
	UT_ASSERT_EQ((int)staged.phase, (int)GCS_FORWARD_CANCEL_PHASE_REQUESTER_FENCE_ACK);
	UT_ASSERT_EQ((int)staged.proof, (int)GCS_FORWARD_CANCEL_PROOF_ACK_MASK);
	UT_ASSERT_EQ(staged.requester_master_capability_generation, UINT32_C(73));
	/* Everything else is the parked barrier, byte for byte. */
	{
		GcsBlockForwardCancelPayload expect = barrier;

		expect.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_REQUESTER_FENCE_ACK;
		expect.proof = GCS_FORWARD_CANCEL_PROOF_ACK_MASK;
		expect.requester_master_capability_generation = 73;
		UT_ASSERT_EQ(memcmp(&expect, &staged, sizeof(staged)), 0);
	}

	/* And the master terminalizes on it. */
	memset(&denied, 0, sizeof(denied));
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_forward_cancel_ack_exact(0, &key, 73, &staged, &denied),
		(int)GCS_BLOCK_FORWARD_MARK_INSTALLED);
	UT_ASSERT_EQ((int)GcsBlockDedupEntryForwardMarkerPhase(&denied),
				 (int)GCS_BLOCK_FORWARD_MARK_NONE);
}

/*
 * T2-16  C1 -- the contract Tier-1 b6 could not express without inventing a
 * bug as its own fixture.
 *
 * The previous round created its "orphaned cancel" by calling
 * cluster_gcs_block_dedup_mark_done() on a live CANCELLING cell -- the exact
 * P0 that d1 forbids -- and then asserted the two outcomes differ.  Fixing d1
 * would have deleted b6's fixture, and b6 asserted mark_done() == true while
 * d1 asserted == false on the same state.  Here the orphan is produced by a
 * legitimate path (requester fenced, no ACK possible) and the assertion is
 * simply that the driver's outcome channel separates them.
 */
UT_TEST(t2_16_orphaned_and_idle_are_different_outcomes)
{
	BufferTag tag = make_tag(580);
	GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x216), 29);
	GcsBlockForwardCancelDriveReport orphaned;
	GcsBlockForwardCancelDriveReport idle;

	t2_reset();

	/* An idle shard. */
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NO_WORK, &idle);

	/* A cancel whose requester can never ACK. */
	install_forwarded(0, &key, tag, NULL, NULL, NULL);
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_NEW_STAGED_WAIT_ACK, NULL);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(key.origin_node_id, key.requester_backend_id);
	cluster_gcs_block_dedup_cleanup_on_node_dead(key.origin_node_id);
	t2_observer.master_generation = 0;
	fake_now += 450000;
	t2_drive_and_assert(0, FORWARD_CANCEL_DRIVE_ACK_AUTHORITY_WAIT, &orphaned);

	UT_ASSERT_NE((int)orphaned.outcome, (int)idle.outcome);
	UT_ASSERT_NE((int)orphaned.outcome, (int)FORWARD_CANCEL_DRIVE_NO_WORK);
}

/*
 * T2-17  STANDING REGRESSION CONTRACT -- do not delete, do not weaken.
 *
 * The S3-P0-09 safety argument for a live FORWARDED marker rests on an
 * EXHAUSTIVE claim about HEAD 30d7cda2d7, not on a local invariant:
 *
 *   gcs_block_wake_local_pending_s_request() (cluster_gcs_block.c:18439-18488)
 *   is the ONLY producer that installs GCS_BLOCK_REPLY_DENIED_PENDING_X into a
 *   requester slot, and it refuses any slot whose direct-land lane is armed --
 *   the exclusion runs through GcsBlockLocalPendingSDenialMatches(), which is
 *   handed slot->direct_state and slot->direct_target_prepared (:18461-18465).
 *   The design note above it (:18433-18437) states the intent: "Direct-land
 *   attempts are deliberately excluded while their target is live; their lane
 *   has its own LMON abort protocol."
 *
 * Because direct-land is never armed on that path today, a late image cannot
 * be installed out-of-band and the FORWARDED window has no side door.  That
 * conclusion is only as good as the enumeration.  ADD ONE MORE PATH THAT CAN
 * PUT DENIED_PENDING_X ON A FORWARDED SLOT -- especially one where direct-land
 * IS armed -- AND THE WHOLE ARGUMENT MUST BE RECOMPUTED, including d0 above.
 *
 * This test exists so that recomputation is forced by CI rather than by
 * somebody remembering.  It must:
 *   (a) enumerate every producer of DENIED_PENDING_X reachable from a live
 *       FORWARDED marker, via the driver's own accounting rather than by
 *       grepping, and assert the set has exactly the one known member;
 *   (b) assert that a direct-land-armed slot is still excluded;
 *   (c) fail loudly -- not silently widen -- when the count changes.
 *
 * The driver must therefore expose:
 *   extern uint32 cluster_gcs_block_forward_cancel_denied_pending_x_producer_mask(void);
 *      // one bit per registered producer;  every producer site calls
 *      // cluster_gcs_block_forward_cancel_register_denied_pending_x_producer()
 *      // at init, so the mask is derived from code, not from a comment.
 *   #define GCS_BLOCK_DENIED_PENDING_X_PRODUCER_LOCAL_INVALIDATE_WAKE  0x1
 *   extern bool cluster_gcs_block_forward_cancel_test_arm_direct_land(
 *           int backend_idx, int slot_idx, bool armed);
 */
UT_TEST(t2_17_denied_pending_x_producer_set_is_exactly_one_and_excludes_direct_land)
{
	uint32 mask;

	t2_reset();

	mask = cluster_gcs_block_forward_cancel_denied_pending_x_producer_mask();

	/*
	 * If this fails, DO NOT relax it.  A new producer invalidates the
	 * exhaustive no-side-door argument for the FORWARDED window;  re-derive
	 * d0's safety before changing this number.
	 */
	UT_ASSERT_EQ(mask, (uint32)GCS_BLOCK_DENIED_PENDING_X_PRODUCER_LOCAL_INVALIDATE_WAKE);

	/* And the one known producer must keep excluding an armed direct-land
	 * lane, which is the other half of the argument. */
	UT_ASSERT(cluster_gcs_block_forward_cancel_test_arm_direct_land(0, 0, true));
	{
		BufferTag tag = make_tag(590);
		GcsBlockDedupKey key = make_key(1, 7, UINT64_C(0x217), 29);
		GcsBlockDedupEntry entry;

		install_forwarded(0, &key, tag, NULL, NULL, NULL);
		/* The armed lane must not have been denied out from under the
		 * forward leg;  the marker is still FORWARDED and still drivable. */
		memset(&entry, 0, sizeof(entry));
		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &entry),
					 (int)GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW);
	}
	UT_ASSERT(cluster_gcs_block_forward_cancel_test_arm_direct_land(0, 0, false));
}
#endif /* CLUSTER_FORWARD_CANCEL_DRIVER_LINKED */


int
main(void)
{
#ifdef CLUSTER_FORWARD_CANCEL_DRIVER_LINKED
	UT_PLAN(35);
#else
	UT_PLAN(18);
#endif

	UT_RUN(a1_requester_ledger_park_release_ack_finish_is_the_only_legal_order);
	UT_RUN(a2_requester_ledger_release_and_ack_rejection_stay_reentrant);
	UT_RUN(a3_requester_ledger_ack_before_release_is_refused_not_reordered);
	UT_RUN(a4_next_action_retains_on_the_four_blocking_observations);
	UT_RUN(a5_next_action_emits_discard_for_a_retired_identity);
	UT_RUN(a6_ledger_admission_is_five_valued_before_the_bool_wrapper_erases_it);

	UT_RUN(b1_real_dedup_apis_move_forwarded_to_cancelling);
	UT_RUN(b2_first_tick_stages_new_later_ticks_report_replay);
	UT_RUN(b3_cancel_replay_records_no_progress_stamp);
	UT_RUN(b4_live_cancelling_is_census_visible_but_only_tag_addressable);
	UT_RUN(b5_only_exact_type67_ack_terminalizes_the_marker);

	UT_RUN(d0_blind_done_must_not_retire_a_live_forwarded_marker);
	UT_RUN(d1_blind_done_must_not_retire_a_live_cancelling_marker);
	UT_RUN(d2_live_cancelling_must_survive_cleanup_after_a_blind_done);
	UT_RUN(d3_backend_exit_cleanup_reclaims_plain_entries_and_retains_the_marker);
	UT_RUN(d4_node_dead_cleanup_reclaims_plain_entries_and_retains_the_marker);
	UT_RUN(d5_late_duplicate_request_must_not_install_over_cancelling);
	UT_RUN(d6_prepared_abort_must_stay_refused_on_a_live_cancelling_marker);

#ifdef CLUSTER_FORWARD_CANCEL_DRIVER_LINKED
	UT_RUN(t2_01_no_work_is_reported_only_when_the_shard_is_truly_idle);
	UT_RUN(t2_02_first_pass_over_a_forwarded_marker_stages_new);
	UT_RUN(t2_03_replay_pass_exposes_monotonic_retry_progress);
	UT_RUN(t2_04_refused_admission_is_its_own_outcome_not_silence);
	UT_RUN(t2_05_retain_observations_report_identity_retry_with_a_trace);
	UT_RUN(t2_06_missing_master_generation_is_an_ack_authority_wait);
	UT_RUN(t2_07_legacy_denial_is_not_folded_into_the_forward_cancel_arms);
	UT_RUN(t2_08_unprovable_cell_reports_corrupt_and_is_never_idle);
	UT_RUN(t2_09_drive_once_never_nests_locks_and_releases_all_of_them);
	UT_RUN(t2_10_live_cancelling_set_is_enumerable_and_then_addressable);
	UT_RUN(t2_11_orphaned_cancel_retires_on_evidence_while_prepared_abort_stays_refused);
	UT_RUN(t2_12_every_admission_outcome_is_counted_not_collapsed);
	UT_RUN(t2_13_discard_verdict_reclaims_the_ledger_slot);
	UT_RUN(t2_14_periodic_pass_walks_the_ledger_through_all_three_marks);
	UT_RUN(t2_15_requester_side_ack_certificate_is_accepted_by_the_master);
	UT_RUN(t2_16_orphaned_and_idle_are_different_outcomes);
	UT_RUN(t2_17_denied_pending_x_producer_set_is_exactly_one_and_excludes_direct_land);
#endif

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
