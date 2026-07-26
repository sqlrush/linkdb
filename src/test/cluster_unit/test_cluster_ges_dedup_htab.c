/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_dedup_htab.c
 *	  S3-P0-10 behavioral tests against the real GES dedup/journal module.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_ges_dedup_max_entries = 8;
bool IsUnderPostmaster = false;
ProcessingMode Mode = NormalProcessing;

static TimestampTz fake_now = INT64CONST(1000000000);
static uint64 fake_master_generation = 77;
static LWLockPadded fake_tranche[1];
static PGPROC fake_procs[64];
static PROC_HDR fake_proc_global;
PGPROC *MyProc;
PROC_HDR *ProcGlobal;

#define FAKE_TABLES 3
#define FAKE_SLOTS 32
#define FAKE_ENTRY_BYTES 256
#define FAKE_MAX_SEQ_SCANS 8

typedef struct FakeHash {
	bool used[FAKE_SLOTS];
	Size keysize;
	Size entrysize;
	long max_entries;
	union {
		uint64 align;
		uint8 bytes[FAKE_SLOTS][FAKE_ENTRY_BYTES];
	} slots;
} FakeHash;

static FakeHash fake_hashes[FAKE_TABLES];
static int fake_hash_count;
static int fake_seq_active_count;
static int fake_seq_overflow_count;
static union {
	uint64 align;
	uint8 bytes[2048];
} fake_shared;
static bool fake_shared_found;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}

uint64
cluster_lms_get_shard_master_generation(void)
{
	return fake_master_generation;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
RequestNamedLWLockTranche(const char *name pg_attribute_unused(),
						  int num_lwlocks pg_attribute_unused())
{}

LWLockPadded *
GetNamedLWLockTranche(const char *name pg_attribute_unused())
{
	return fake_tranche;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(),
			  LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size,
				bool *foundPtr)
{
	Assert(size <= sizeof(fake_shared.bytes));
	*foundPtr = fake_shared_found;
	fake_shared_found = true;
	return fake_shared.bytes;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(),
			  long init_size pg_attribute_unused(), long max_size,
			  HASHCTL *infoP, int hash_flags)
{
	FakeHash *table;

	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(fake_hash_count < FAKE_TABLES);
	Assert(max_size <= FAKE_SLOTS);
	Assert(infoP->entrysize <= FAKE_ENTRY_BYTES);
	table = &fake_hashes[fake_hash_count++];
	memset(table, 0, sizeof(*table));
	table->keysize = infoP->keysize;
	table->entrysize = infoP->entrysize;
	table->max_entries = max_size;
	return (HTAB *)table;
}

static long
fake_live_count(FakeHash *table)
{
	long count = 0;

	for (int i = 0; i < FAKE_SLOTS; i++)
		if (table->used[i])
			count++;
	return count;
}

void *
hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action,
			bool *foundPtr)
{
	FakeHash *table = (FakeHash *)hashp;

	for (int i = 0; i < FAKE_SLOTS; i++) {
		if (!table->used[i])
			continue;
		if (memcmp(table->slots.bytes[i], keyPtr, table->keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE)
				table->used[i] = false;
			return table->slots.bytes[i];
		}
	}
	if (action != HASH_ENTER && action != HASH_ENTER_NULL) {
		if (foundPtr != NULL)
			*foundPtr = false;
		return NULL;
	}
	if (fake_live_count(table) >= table->max_entries) {
		if (foundPtr != NULL)
			*foundPtr = false;
		return NULL;
	}
	for (int i = 0; i < FAKE_SLOTS; i++) {
		if (!table->used[i]) {
			memset(table->slots.bytes[i], 0, table->entrysize);
			memcpy(table->slots.bytes[i], keyPtr, table->keysize);
			table->used[i] = true;
			if (foundPtr != NULL)
				*foundPtr = false;
			return table->slots.bytes[i];
		}
	}
	Assert(false);
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	if (fake_seq_active_count >= FAKE_MAX_SEQ_SCANS)
		fake_seq_overflow_count++;
	fake_seq_active_count++;
	status->hashp = hashp;
	status->curBucket = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeHash *table = (FakeHash *)status->hashp;

	while (status->curBucket < FAKE_SLOTS) {
		uint32 slot = status->curBucket++;

		if (table->used[slot])
			return table->slots.bytes[slot];
	}
	hash_seq_term(status);
	return NULL;
}

void
hash_seq_term(HASH_SEQ_STATUS *status)
{
	if (status->hashp != NULL) {
		Assert(fake_seq_active_count > 0);
		fake_seq_active_count--;
		status->hashp = NULL;
	}
}

static void
fixture_reset(int cap)
{
	cluster_ges_dedup_max_entries = cap;
	fake_now = INT64CONST(1000000000);
	fake_shared_found = false;
	fake_hash_count = 0;
	fake_seq_active_count = 0;
	fake_seq_overflow_count = 0;
	memset(&fake_shared, 0, sizeof(fake_shared));
	memset(fake_hashes, 0, sizeof(fake_hashes));
	memset(fake_procs, 0, sizeof(fake_procs));
	memset(&fake_proc_global, 0, sizeof(fake_proc_global));
	fake_proc_global.allProcs = fake_procs;
	fake_proc_global.allProcCount = lengthof(fake_procs);
	ProcGlobal = &fake_proc_global;
	MyProc = &fake_procs[17];
	MyProc->pid = 111;
	MyProc->cluster_grd_generation = 1001;
	cluster_ges_dedup_shmem_init();
}

static ClusterGesDedupKey
make_key(uint64 request_id)
{
	ClusterGesDedupKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 2;
	key.opcode = GES_REQ_OPCODE_REQUEST;
	key.request_id = request_id;
	key.cluster_epoch = 9;
	key.shard_master_generation = fake_master_generation;
	key.holder_procno = 17;
	return key;
}

static GesDedupLifecyclePayload
make_done(uint64 request_id)
{
	GesDedupLifecyclePayload done;

	memset(&done, 0, sizeof(done));
	done.version = GES_DEDUP_LIFECYCLE_VERSION;
	done.kind = GES_DEDUP_LIFECYCLE_EXACT_DONE;
	done.origin_node_id = 2;
	done.holder_procno = 17;
	done.opcode = GES_REQ_OPCODE_REQUEST;
	done.request_id = request_id;
	done.cluster_epoch = 9;
	done.shard_master_generation = fake_master_generation;
	done.origin_boot_incarnation = 100;
	done.target_boot_incarnation = 200;
	done.link_generation = 3;
	return done;
}

UT_TEST(exact_done_removes_cached_and_frontier_rejects_late_original)
{
	ClusterGesDedupKey key = make_key(10);
	uint8 reply[8] = { 1, 2, 3 };
	uint8 replay[8] = { 0 };
	uint16 replay_len = 0;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, replay, sizeof(replay), &replay_len));
	cluster_ges_dedup_record_reply_identity(&key, 100, reply, sizeof(reply));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_CACHED_REPLY,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, replay, sizeof(replay), &replay_len));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_REMOVED,
				 cluster_ges_dedup_retire_exact(&key, 100));
	UT_ASSERT_EQ(0, cluster_ges_dedup_entry_count());
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRED_LATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, replay, sizeof(replay), &replay_len));
}

UT_TEST(done_before_not_admitted_request_installs_persistent_frontier)
{
	ClusterGesDedupKey key = make_key(11);
	uint16 replay_len = 0;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_ALREADY_ABSENT,
				 cluster_ges_dedup_retire_exact(&key, 100));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRED_LATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(0, cluster_ges_dedup_entry_count());
}

UT_TEST(done_racing_inflight_retires_on_cache)
{
	ClusterGesDedupKey key = make_key(12);
	ClusterGesDedupKey no_reply_key = make_key(13);
	uint8 reply[4] = { 9 };
	uint16 replay_len = 0;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_PENDING,
				 cluster_ges_dedup_retire_exact(&key, 100));
	cluster_ges_dedup_record_reply_identity(&key, 100, reply, sizeof(reply));
	UT_ASSERT_EQ(0, cluster_ges_dedup_entry_count());
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_ALREADY_ABSENT,
				 cluster_ges_dedup_retire_exact(&key, 100));

	/* DONE-before-CANCEL: the later successful CANCEL_WAIT publishes a
	 * terminal NULL/zero outcome.  retire_on_cache must remove it just like a
	 * 52B GRANT/REJECT, not leave a permanent CACHED0 row. */
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &no_reply_key, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_PENDING,
				 cluster_ges_dedup_retire_exact(&no_reply_key, 100));
	cluster_ges_dedup_record_reply_identity(
		&no_reply_key, 100, NULL, 0);
	UT_ASSERT_EQ(0, cluster_ges_dedup_entry_count());
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_ALREADY_ABSENT,
				 cluster_ges_dedup_retire_exact(&no_reply_key, 100));
}

/*
 * A successful CANCEL_WAIT has no GES_REPLY to cache, but it is still a
 * terminal receiver outcome.  Retransmits must be suppressed and the matching
 * exact DONE must be able to remove the row instead of waiting forever for a
 * reply that will never exist.
 */
UT_TEST(canceled_request_terminal_without_reply_is_reclaimable)
{
	ClusterGesDedupKey key = make_key(13);
	uint16 replay_len = UINT16_MAX;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, NULL, 0, &replay_len));
	cluster_ges_dedup_record_reply_identity(&key, 100, NULL, 0);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_CACHED_REPLY,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(0, replay_len);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_REMOVED,
				 cluster_ges_dedup_retire_exact(&key, 100));
	UT_ASSERT_EQ(0, cluster_ges_dedup_entry_count());
}

UT_TEST(authenticated_done_retires_transient_boot_zero_row)
{
	ClusterGesDedupKey key = make_key(14);
	uint8 reply[4] = { 7 };
	uint16 replay_len = 0;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register(
					 &key, NULL, 0, &replay_len));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRE_PENDING,
				 cluster_ges_dedup_retire_exact(&key, 100));
	cluster_ges_dedup_record_reply(&key, reply, sizeof(reply));
	UT_ASSERT_EQ(0, cluster_ges_dedup_entry_count());
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRED_LATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, NULL, 0, &replay_len));
}

UT_TEST(boot_switch_blocks_old_work_from_poisoning_reused_key)
{
	ClusterGesDedupKey key = make_key(13);
	uint8 old_reply[4] = { 1 };
	uint8 new_reply[4] = { 2 };
	uint8 replay[4] = { 0 };
	uint16 replay_len = 0;
	uint64 superseded_boot = 0;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 100, replay, sizeof(replay), &replay_len));
	cluster_ges_dedup_record_reply_identity(
		&key, 100, old_reply, sizeof(old_reply));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity_ex(
					 &key, 200, &superseded_boot, replay,
					 sizeof(replay), &replay_len));
	UT_ASSERT_EQ(100, superseded_boot);
	cluster_ges_dedup_record_reply_identity(
		&key, 100, old_reply, sizeof(old_reply));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 200, replay, sizeof(replay), &replay_len));
	cluster_ges_dedup_record_reply_identity(
		&key, 200, new_reply, sizeof(new_reply));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_CACHED_REPLY,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key, 200, replay, sizeof(replay), &replay_len));
	UT_ASSERT_EQ(2, replay[0]);
}

UT_TEST(proc_hwm_retires_only_at_or_below_frontier)
{
	ClusterGesDedupKey key10 = make_key(10);
	ClusterGesDedupKey key15 = make_key(15);
	ClusterGesDedupKey key20 = make_key(20);
	ClusterGesDedupKey key21 = make_key(21);
	uint8 reply[4] = { 1 };
	uint16 replay_len = 0;
	uint32 pending = 0;
	bool applied = false;

	fixture_reset(8);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key10, 100, NULL, 0, &replay_len));
	cluster_ges_dedup_record_reply_identity(&key10, 100, reply, sizeof(reply));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key15, 100, NULL, 0, &replay_len));
	cluster_ges_dedup_record_reply_identity(&key15, 100, NULL, 0);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_CACHED_REPLY,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key15, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(0, replay_len);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key20, 100, NULL, 0, &replay_len));
	/* HWM removes both ordinary CACHED52 and terminal CACHED0 rows; only the
	 * still-in-flight key20 contributes to pending. */
	UT_ASSERT_EQ(2, cluster_ges_dedup_retire_origin_proc_up_to(
					 2, 17, 20, 100, &pending, &applied));
	UT_ASSERT(applied);
	UT_ASSERT_EQ(1, pending);
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRED_LATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key10, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRED_LATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key15, 100, NULL, 0, &replay_len));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_RETIRED_LATE,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key20, 100, NULL, 0, &replay_len));
	UT_ASSERT(cluster_ges_dedup_request_is_retired(&key10, 100));
	UT_ASSERT(cluster_ges_dedup_request_is_retired(&key15, 100));
	UT_ASSERT(cluster_ges_dedup_request_is_retired(&key20, 100));
	UT_ASSERT(!cluster_ges_dedup_request_is_retired(&key21, 100));
	UT_ASSERT(!cluster_ges_dedup_request_is_retired(&key20, 200));
	UT_ASSERT_EQ(CLUSTER_GES_DEDUP_MISS_REGISTERED,
				 cluster_ges_dedup_lookup_or_register_identity(
					 &key21, 100, NULL, 0, &replay_len));
}

UT_TEST(proc_hwm_frontier_capacity_failure_is_not_applied)
{
	uint32 pending = 0;
	bool applied = false;

	fixture_reset(2);
	(void)cluster_ges_dedup_retire_origin_proc_up_to(
		2, 11, 10, 100, &pending, &applied);
	UT_ASSERT(applied);
	applied = false;
	(void)cluster_ges_dedup_retire_origin_proc_up_to(
		2, 12, 20, 100, &pending, &applied);
	UT_ASSERT(applied);
	applied = true;
	(void)cluster_ges_dedup_retire_origin_proc_up_to(
		2, 13, 30, 100, &pending, &applied);
	UT_ASSERT(!applied);
}

UT_TEST(journal_pre_reserve_is_bounded_and_ack_loss_retries)
{
	GesDedupLifecyclePayload done1 = make_done(30);
	GesDedupLifecyclePayload done2 = make_done(31);
	GesDedupLifecyclePayload done3 = make_done(32);
	GesDedupLifecyclePayload claimed;
	GesDedupLifecyclePayload ack;
	uint32 dest = 0;

	fixture_reset(2);
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &done1));
	UT_ASSERT(!cluster_ges_dedup_journal_claim_due(fake_now, &dest, &claimed));
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &done2));
	UT_ASSERT(!cluster_ges_dedup_journal_register(4, &done3));
	UT_ASSERT_EQ(1, cluster_ges_dedup_journal_full_count());
	UT_ASSERT(cluster_ges_dedup_journal_cancel(4, &done2));
	UT_ASSERT(cluster_ges_dedup_journal_commit(4, &done1));
	UT_ASSERT(cluster_ges_dedup_journal_claim_due(fake_now, &dest, &claimed));
	UT_ASSERT_EQ(4, dest);
	UT_ASSERT_EQ(done1.request_id, claimed.request_id);
	UT_ASSERT(!cluster_ges_dedup_journal_claim_due(fake_now, &dest, &claimed));
	fake_now = TimestampTzPlusMilliseconds(fake_now, 100);
	UT_ASSERT(cluster_ges_dedup_journal_claim_due(fake_now, &dest, &claimed));

	ack = done1;
	ack.kind = GES_DEDUP_LIFECYCLE_ACK;
	ack.status = GES_DEDUP_ACK_RETIRE_PENDING;
	UT_ASSERT(!cluster_ges_dedup_journal_ack(4, &ack));
	UT_ASSERT_EQ(1, cluster_ges_dedup_journal_count());
	ack.status = GES_DEDUP_ACK_REMOVED;
	UT_ASSERT(cluster_ges_dedup_journal_ack(4, &ack));
	UT_ASSERT_EQ(0, cluster_ges_dedup_journal_count());
	UT_ASSERT_EQ(1, cluster_ges_dedup_journal_ack_count());
}

UT_TEST(uninitialized_owner_generation_cannot_reserve_journal)
{
	GesDedupLifecyclePayload done = make_done(39);

	fixture_reset(4);
	MyProc->cluster_grd_generation = 0;
	UT_ASSERT(!cluster_ges_dedup_journal_register(4, &done));
	UT_ASSERT_EQ(0, cluster_ges_dedup_journal_count());
}

UT_TEST(abrupt_backend_exit_compacts_exact_reservations_to_hwm)
{
	GesDedupLifecyclePayload done1 = make_done(40);
	GesDedupLifecyclePayload done2 = make_done(45);
	GesDedupLifecyclePayload claimed;
	uint32 dest = 0;

	fixture_reset(4);
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &done1));
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &done2));
	UT_ASSERT_EQ(2, cluster_ges_dedup_journal_count());

	/* SIGKILL/exit: procno no longer names the reserving generation. */
	fake_procs[17].pid = 0;
	UT_ASSERT_EQ(2, cluster_ges_dedup_journal_reap_dead_backend());
	UT_ASSERT_EQ(1, cluster_ges_dedup_journal_count());
	UT_ASSERT(cluster_ges_dedup_journal_claim_due(
		fake_now, &dest, &claimed));
	UT_ASSERT_EQ(GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM, claimed.kind);
	UT_ASSERT_EQ(45, claimed.request_id);
	UT_ASSERT_EQ(0, claimed.opcode);
	UT_ASSERT_EQ(17, claimed.holder_procno);

	/* A reused procno is a different generation and starts above the old HWM. */
	fake_procs[17].pid = 222;
	fake_procs[17].cluster_grd_generation = 1002;
	MyProc = &fake_procs[17];
	{
		GesDedupLifecyclePayload fresh = make_done(46);

		UT_ASSERT(cluster_ges_dedup_journal_register(4, &fresh));
		UT_ASSERT_EQ(2, cluster_ges_dedup_journal_count());
	}
}

/*
 * S3-P0-10 r20 smoke RED: each successful due claim stops its dynahash scan
 * early.  More than MAX_SEQ_SCANS LMON ticks must not accumulate active
 * sequential scans and FATAL the LMON process.
 */
UT_TEST(repeated_due_claims_terminate_each_early_hash_scan)
{
	GesDedupLifecyclePayload done = make_done(47);
	GesDedupLifecyclePayload claimed;
	uint32 dest = 0;

	fixture_reset(4);
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &done));
	UT_ASSERT(cluster_ges_dedup_journal_commit(4, &done));
	for (int i = 0; i < FAKE_MAX_SEQ_SCANS + 2; i++) {
		UT_ASSERT(cluster_ges_dedup_journal_claim_due(
			fake_now, &dest, &claimed));
		fake_now = TimestampTzPlusMilliseconds(fake_now, 2000);
	}
	UT_ASSERT_EQ(0, fake_seq_overflow_count);
	UT_ASSERT_EQ(0, fake_seq_active_count);
}

/*
 * The dead-backend finder also stops at the first exact group.  Reaping
 * distinct dead owners repeatedly must terminate that first scan before the
 * HWM compute/remove scans reuse the status object.
 */
UT_TEST(repeated_dead_backend_reaps_terminate_found_group_hash_scan)
{
	fixture_reset(16);
	for (int i = 0; i < FAKE_MAX_SEQ_SCANS + 2; i++) {
		GesDedupLifecyclePayload done = make_done(100 + i);
		uint32 procno = 20 + i;

		MyProc = &fake_procs[procno];
		MyProc->pid = 1000 + i;
		MyProc->cluster_grd_generation = 2000 + i;
		done.holder_procno = procno;
		UT_ASSERT(cluster_ges_dedup_journal_register(4, &done));
		MyProc->pid = 0;
	}
	for (int i = 0; i < FAKE_MAX_SEQ_SCANS + 2; i++)
		UT_ASSERT_EQ(1, cluster_ges_dedup_journal_reap_dead_backend());
	UT_ASSERT_EQ(0, fake_seq_overflow_count);
	UT_ASSERT_EQ(0, fake_seq_active_count);
}

UT_TEST(journal_ack_status_must_match_exact_or_hwm_kind)
{
	GesDedupLifecyclePayload exact = make_done(50);
	GesDedupLifecyclePayload hwm = make_done(60);
	GesDedupLifecyclePayload ack;

	fixture_reset(4);
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &exact));
	UT_ASSERT(cluster_ges_dedup_journal_commit(4, &exact));
	ack = exact;
	ack.kind = GES_DEDUP_LIFECYCLE_ACK;
	ack.status = GES_DEDUP_ACK_HWM_APPLIED;
	UT_ASSERT(!cluster_ges_dedup_journal_ack(4, &ack));
	ack.status = GES_DEDUP_ACK_ALREADY_ABSENT;
	UT_ASSERT(cluster_ges_dedup_journal_ack(4, &ack));

	hwm.kind = GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM;
	hwm.opcode = 0;
	hwm.cluster_epoch = 0;
	hwm.shard_master_generation = 0;
	UT_ASSERT(cluster_ges_dedup_journal_register(4, &hwm));
	UT_ASSERT(cluster_ges_dedup_journal_commit(4, &hwm));
	ack = hwm;
	ack.kind = GES_DEDUP_LIFECYCLE_ACK;
	ack.status = GES_DEDUP_ACK_REMOVED;
	UT_ASSERT(!cluster_ges_dedup_journal_ack(4, &ack));
	ack.status = GES_DEDUP_ACK_HWM_APPLIED;
	UT_ASSERT(cluster_ges_dedup_journal_ack(4, &ack));
	UT_ASSERT_EQ(0, cluster_ges_dedup_journal_count());
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(14);
	UT_RUN(exact_done_removes_cached_and_frontier_rejects_late_original);
	UT_RUN(done_before_not_admitted_request_installs_persistent_frontier);
	UT_RUN(done_racing_inflight_retires_on_cache);
	UT_RUN(canceled_request_terminal_without_reply_is_reclaimable);
	UT_RUN(authenticated_done_retires_transient_boot_zero_row);
	UT_RUN(boot_switch_blocks_old_work_from_poisoning_reused_key);
	UT_RUN(proc_hwm_retires_only_at_or_below_frontier);
	UT_RUN(proc_hwm_frontier_capacity_failure_is_not_applied);
	UT_RUN(journal_pre_reserve_is_bounded_and_ack_loss_retries);
	UT_RUN(uninitialized_owner_generation_cannot_reserve_journal);
	UT_RUN(abrupt_backend_exit_compacts_exact_reservations_to_hwm);
	UT_RUN(repeated_due_claims_terminate_each_early_hash_scan);
	UT_RUN(repeated_dead_backend_reaps_terminate_found_group_hash_scan);
	UT_RUN(journal_ack_status_must_match_exact_or_hwm_kind);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
